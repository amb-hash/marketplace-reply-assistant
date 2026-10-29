from __future__ import annotations

import contextlib
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
import urllib.error
from unittest.mock import patch

from mra import cli, generate
from mra.audit import redact
from mra.generate import GenerationResult
from mra.importing import copy_database, import_legacy_messages, import_snapshot
from mra.pipeline import Pipeline
from mra.review import create_review_demo, review_message
from mra.store import SCHEMA_VERSION, Store, open_store


class ReviewFixture(unittest.TestCase):
    def setUp(self):
        self.rules = "версия: 1.0\nПроверочные правила"
        self.addCleanup(patch.stopall)
        patch("mra.generate.load_rules", side_effect=lambda: self.rules).start()
        patch("urllib.request.urlopen", side_effect=AssertionError("network forbidden")).start()
        self.store = Store(Path(":memory:"))
        self.addCleanup(self.store.close)
        self.store.upsert_product("A")
        self.store.replace_material("A", [("connection", "Откройте Bluetooth")])
        self.store.sign_canon("fixture", "synthetic source")
        self.store.upsert_message("m1", "ozon", "account", "q1", "question", "Как подключить?", article="A")
        self.reply = GenerationResult(text="Исходный ответ", model="fixture", source="fixture", decision="answer",
                                      basis=(("connection", "Откройте Bluetooth"),))
        self.pipeline = Pipeline(self.store, generator=lambda *a, **kw: self.reply)

    def draft(self):
        return self.pipeline.process("m1").draft_id

    def events(self):
        return self.store.audit_trace("m1", full=True)

    def interact(self, inputs):
        iterator, output = iter(inputs), []
        def read(_prompt):
            try:
                return next(iterator)
            except StopIteration:
                raise EOFError
        code = review_message(self.store, "m1", actor="Tester", read=read, write=output.append)
        self.assertEqual(code, 0)
        return "\n".join(output)


class HumanReview(ReviewFixture):
    def test_inspection_only_rejects_real_draft_without_approval_or_network(self):
        draft = self.draft()
        inputs = iter(["ОТКЛОНИТЬ", "Не подходит формулировка"])
        output = []
        review_message(self.store, "m1", actor="Tester", read=lambda _: next(inputs), write=output.append, inspection_only=True)
        self.assertEqual(self.store.get_message("m1")[11], "rejected")
        self.assertEqual(self.store.outbox_count(), 0)
        self.assertEqual(self.store.review_history("m1")[-1]["draft_id"], draft)
        self.assertIn("Исходный ответ", "\n".join(output))

    def test_inspection_only_never_interprets_approval_as_action(self):
        self.draft()
        inputs = iter(["1", "ОТПРАВИТЬ", ""])
        review_message(self.store, "m1", actor="Tester", read=lambda _: next(inputs), write=lambda _: None, inspection_only=True)
        self.assertEqual(self.store.get_message("m1")[11], "drafted")
        self.assertEqual(self.store.outbox_count(), 0)

    def test_edit_preserves_original_and_requires_separate_approval(self):
        old = self.draft()
        self.pipeline.approve("m1", expected_draft_id=old, actor="Alice")
        new = self.pipeline.revise("m1", "Исправленный ответ", expected_draft_id=old, actor="Bob")
        self.assertNotEqual(old, new)
        self.assertEqual(self.store.get_message("m1")[11], "drafted")
        self.assertEqual(self.store._db.execute("SELECT text FROM drafts WHERE draft_id=?", (old,)).fetchone()[0], self.reply.text)
        self.assertEqual(self.store._db.execute("SELECT state FROM outbox WHERE draft_id=?", (old,)).fetchone()[0], "superseded")
        self.assertEqual(self.store._db.execute("SELECT count(*) FROM approvals WHERE draft_id=?", (new,)).fetchone()[0], 0)
        with self.assertRaises(ValueError):
            self.pipeline.approve("m1", expected_draft_id=old)
        self.pipeline.approve("m1", expected_draft_id=new, actor="Bob")
        self.assertEqual(self.store._db.execute("SELECT draft_id FROM outbox WHERE state='held'").fetchall(), [(new,)])
        history = self.store.review_history("m1")
        self.assertEqual([e["action"] for e in history], ["approved", "edited", "approved"])
        self.assertEqual([e["actor"] for e in history], ["Alice", "Bob", "Bob"])

    def test_rejection_revokes_hold_and_only_explicit_retry_generates(self):
        old = self.draft()
        self.pipeline.approve("m1", expected_draft_id=old)
        self.pipeline.reject("m1", "Нужны уточнения", expected_draft_id=old, actor="Tester")
        self.assertEqual(self.store.get_message("m1")[11], "rejected")
        self.assertEqual(self.store.message_note("m1"), "Нужны уточнения")
        self.assertEqual(self.store._db.execute("SELECT state FROM outbox").fetchall(), [("superseded",)])
        with self.assertRaises(ValueError):
            self.pipeline.process("m1")
        with self.assertRaises(ValueError):
            self.pipeline.approve("m1", expected_draft_id=old)
        self.assertEqual(self.pipeline.process("m1", retry=True).state, "drafted")
        self.assertEqual(self.store._db.execute("SELECT count(*) FROM outbox WHERE state='held'").fetchone()[0], 0)

    def test_stale_operator_cannot_edit_or_reject_new_draft(self):
        old = self.draft()
        new = self.pipeline.revise("m1", "Новый ответ", expected_draft_id=old)
        for action in (lambda: self.pipeline.revise("m1", "Потерянная правка", expected_draft_id=old),
                       lambda: self.pipeline.reject("m1", "Старое решение", expected_draft_id=old),
                       lambda: self.store.approve(old)):
            with self.assertRaises(ValueError):
                action()
        self.assertEqual(self.store.latest_draft("m1")[0], new)
        self.assertEqual(self.store.get_message("m1")[11], "drafted")

    def test_reject_without_draft_checks_message_revision(self):
        self.store.upsert_message("m1", "ozon", "account", "q1", "question", "Новый вопрос", article="A")
        with self.assertRaises(ValueError):
            self.pipeline.reject("m1", "Старый вопрос", expected_draft_id="", expected_revision=1)
        self.pipeline.reject("m1", "Нет данных", expected_draft_id="", expected_revision=2)
        self.assertEqual(self.store.review_history("m1")[0]["draft_id"], None)

    def test_source_change_disables_edit_and_approval(self):
        old = self.draft()
        self.rules += "\nИзменение"
        self.assertFalse(self.pipeline.review_snapshot("m1")["can_approve"])
        with self.assertRaises(ValueError):
            self.pipeline.revise("m1", "Ответ", expected_draft_id=old)
        with self.assertRaises(ValueError):
            self.pipeline.approve("m1", expected_draft_id=old)
        self.assertEqual(self.store.get_message("m1")[11], "needs_data")

    def test_review_storage_failure_rolls_back_edit_and_hold(self):
        old = self.draft()
        self.pipeline.approve("m1", expected_draft_id=old)
        with patch.object(self.store, "record_review", side_effect=sqlite3.OperationalError("test disk failure")):
            with self.assertRaises(sqlite3.Error):
                self.pipeline.revise("m1", "Не сохранится", expected_draft_id=old)
        self.assertEqual(self.store.latest_draft("m1")[0], old)
        self.assertEqual(self.store.get_message("m1")[11], "approved")
        self.assertEqual(self.store._db.execute("SELECT state FROM outbox").fetchall(), [("held",)])
        self.assertEqual(self.events()[-1]["event_type"], "review.failed")

    def test_empty_text_reason_or_actor_cannot_commit_review(self):
        old = self.draft()
        for action in (lambda: self.pipeline.revise("m1", " ", expected_draft_id=old),
                       lambda: self.pipeline.reject("m1", " "),
                       lambda: self.pipeline.approve("m1", expected_draft_id=old, actor=" ")):
            with self.assertRaises(ValueError):
                action()
        self.assertEqual(self.store.review_history("m1"), [])
        self.assertEqual(self.store.outbox_count(), 0)

    def test_interactive_facts_rules_edit_then_confirm(self):
        self.draft()
        output = self.interact(["4", "5", "2", "Исправленный ответ", "/готово", "6", "1"])
        self.assertIn("Откройте Bluetooth", output)
        self.assertIn(self.rules, output)
        self.assertIn("подтвердите отдельно", output)
        self.assertIn("HOLD", output)
        self.assertEqual(self.store.latest_draft("m1")[1:3], ("Исправленный ответ", "human"))
        self.assertEqual([r["action"] for r in self.store.review_history("m1")], ["edited", "approved"])

    def test_edit_alone_does_not_approve(self):
        self.draft()
        self.interact(["2", "Правка", "/готово", "0"])
        self.assertEqual(self.store.get_message("m1")[11], "drafted")
        self.assertEqual(self.store.outbox_count(), 0)

    def test_cancel_invalid_choice_and_eof_keep_original(self):
        old = self.draft()
        output = self.interact(["bad", "2", "/отмена"])
        self.assertIn("Выберите доступное действие", output)
        self.assertEqual(self.store.latest_draft("m1")[0], old)
        self.assertEqual(self.store.review_history("m1"), [])
        self.assertIn("review.edit_cancelled", [e["event_type"] for e in self.events()])
        self.assertEqual(self.events()[-1]["payload"]["reason"], "input_interrupted")

    def test_interactive_reject_no_draft_cannot_approve(self):
        output = self.interact(["1", "3", "Нет материала"])
        self.assertIn("Черновик отсутствует", output)
        self.assertEqual(self.store.get_message("m1")[11], "rejected")
        self.assertEqual(self.store.outbox_count(), 0)


class BusinessAudit(ReviewFixture):
    def test_existing_natural_identity_retains_received_text_in_its_trace(self):
        mid = self.store.upsert_message("another-id", "ozon", "account", "q1", "question", "Изменённый вопрос", article="A")
        self.assertEqual(mid, "m1")
        payload = next(e["payload"] for e in self.events() if e["event_type"] == "message.identity_resolved")
        self.assertEqual(payload["received"]["text"], "Изменённый вопрос")

    def test_legacy_import_missing_source_is_recorded(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(sqlite3.Error):
                import_legacy_messages(self.store, Path(temporary) / "absent.sqlite3")
        events = self.store.audit_trace(full=True)
        self.assertEqual(events[-1]["event_type"], "import.failed")

    def test_input_payload_and_revisions_survive_import_failure(self):
        snapshot = {"schema_version": 1, "messages": [
            {"message_id": "m2", "marketplace": "ozon", "account_id": "account", "external_id": "q2", "kind": "question",
             "text": "Исходный вопрос", "unknown_field": "сохранить", "hints": {"sku": "123"}},
            {"message_id": "m3", "marketplace": "bad", "account_id": "account", "external_id": "q3", "kind": "question"}]}
        with self.assertRaises(ValueError):
            import_snapshot(self.store, snapshot, source_reference="fixture.json")
        self.assertIsNone(self.store.get_message("m2"))
        trace = self.store.audit_trace("m2", full=True)
        self.assertIn("import.failed", [e["event_type"] for e in trace])
        received = next(e["payload"] for e in trace if e["event_type"] == "input.received")
        self.assertEqual(received["message"]["unknown_field"], "сохранить")
        self.assertEqual(received["message"]["hints"], {"sku": "123"})

    def test_source_snapshot_remains_after_sources_change(self):
        self.draft()
        self.rules = "Другие правила"
        self.store.replace_material("A", [("connection", "Изменённый факт")])
        selected = next(e["payload"] for e in self.events() if e["event_type"] == "sources.selected")
        self.assertIn("Проверочные правила", selected["rules"])
        self.assertEqual(selected["material"], [["connection", "Откройте Bluetooth"]])
        completed = next(e["payload"] for e in self.events() if e["event_type"] == "processing.completed")
        self.assertEqual(completed["answer"], self.reply.text)
        self.assertIsNone(completed["usage"])

    def test_content_dedup_preserves_event_order_and_hash(self):
        ids = [self.store.audit_event("m1", "test", {"same": "body"}) for _ in range(2)]
        events = [e for e in self.events() if e["event_id"] in ids]
        self.assertEqual(events[0]["document_sha256"], events[1]["document_sha256"])
        self.assertLess(events[0]["event_id"], events[1]["event_id"])
        digest = hashlib.sha256(b'{"same":"body"}').hexdigest()
        self.assertEqual(events[0]["document_sha256"], digest)
        self.assertEqual(self.store._db.execute("SELECT count(*) FROM audit_documents WHERE sha256=?", (digest,)).fetchone()[0], 1)

    def test_auth_fields_and_recognized_keys_are_redacted(self):
        fake_key = "sk-" + "x" * 30
        self.store.audit_event("m1", "test", {"authorization": fake_key, "nested": [{"api-key": "other"}], "text": "secret " + fake_key})
        payload = self.events()[-1]["payload"]
        self.assertNotIn(fake_key, json.dumps(payload))
        self.assertEqual(payload["nested"][0]["api-key"], "[REDACTED]")
        self.assertEqual(redact("echo arbitrary-key", secrets=("arbitrary-key",)), "echo [REDACTED]")

    def transport(self, body=None, raw=None):
        response = io.BytesIO(raw if raw is not None else json.dumps(body).encode())
        response.status = 200
        response.headers = {"x-request-id": "req_fixture"}
        return response

    def live_fixture(self, response):
        with patch("mra.generate.DISABLE_LLM", False), patch("mra.generate.load_openai_key", return_value="arbitrary-key"), \
                patch("urllib.request.urlopen", return_value=response) as post:
            result = Pipeline(self.store).process("m1", allow_llm=True)
        self.assertEqual(post.call_count, 1)
        return result, post

    def test_exact_request_response_usage_and_parsed_basis_are_logged(self):
        body = {"id": "resp_fixture", "status": "completed", "model": "fixture", "usage": {"input_tokens": 123, "output_tokens": 45},
                "output": [{"type": "message", "content": [{"type": "output_text", "text":
                    json.dumps({"decision": "answer", "answer": "Ответ модели", "used": [1], "note": ""})}]}]}
        result, post = self.live_fixture(self.transport(body))
        self.assertEqual(result.state, "drafted")
        events = {e["event_type"]: e["payload"] for e in self.events()}
        actual_payload = json.loads(post.call_args.args[0].data)
        self.assertEqual(events["model.request.started"]["request"], actual_payload)
        self.assertEqual(events["model.response.received"]["response"], body)
        self.assertEqual(events["model.response.received"]["request_id"], "req_fixture")
        self.assertGreaterEqual(events["model.response.received"]["elapsed_ms"], 0)
        self.assertEqual(events["processing.completed"]["usage"], {"input_tokens": 123, "output_tokens": 45})
        self.assertEqual(events["model.output.parsed"]["basis"], [["connection", "Откройте Bluetooth"]])
        self.assertNotIn("arbitrary-key", json.dumps(self.events()))

    def test_incomplete_response_usage_is_not_lost(self):
        result, _ = self.live_fixture(self.transport({"status": "incomplete", "usage": {"input_tokens": 5, "output_tokens": 2000}}))
        self.assertEqual(result.state, "needs_data")
        completed = next(e["payload"] for e in self.events() if e["event_type"] == "processing.completed")
        self.assertEqual(completed["usage"], {"input_tokens": 5, "output_tokens": 2000})
        self.assertIsNone(self.store.latest_draft("m1"))

    def test_invalid_json_is_kept_for_diagnostics_without_key(self):
        result, _ = self.live_fixture(self.transport(raw=b"invalid arbitrary-key body"))
        self.assertEqual(result.state, "needs_data")
        event = next(e["payload"] for e in self.events() if e["event_type"] == "model.response.invalid")
        self.assertEqual(event["response_excerpt"], "invalid [REDACTED] body")
        self.assertFalse(event["truncated"])

    def test_oversized_response_is_bounded_and_never_becomes_a_draft(self):
        result, _ = self.live_fixture(self.transport(raw=b"x" * 2_100_000))
        self.assertEqual(result.state, "needs_data")
        event = next(e["payload"] for e in self.events() if e["event_type"] == "model.response.invalid")
        self.assertTrue(event["truncated"])
        self.assertEqual(len(event["response_excerpt"]), 2_000_000)

    def test_timeout_has_unknown_outcome_and_no_automatic_repeat(self):
        with patch("mra.generate.DISABLE_LLM", False), patch("mra.generate.load_openai_key", return_value="arbitrary-key"), \
                patch("urllib.request.urlopen", side_effect=TimeoutError("synthetic timeout")) as post:
            result = Pipeline(self.store).process("m1", allow_llm=True)
        self.assertEqual(post.call_count, 1)
        self.assertEqual(result.state, "needs_data")
        event = next(e["payload"] for e in self.events() if e["event_type"] == "model.request.failed")
        self.assertEqual(event["outcome"], "unknown")
        self.assertEqual(event["error_type"], "TimeoutError")

    def test_source_change_discards_text_but_retains_token_usage(self):
        def generator(*a, **kw):
            self.rules += "\nИзменение во время генерации"
            return GenerationResult(text="Ответ", decision="answer", source="fixture", model="fixture",
                basis=tuple(self.store.material_for("A")), input_tokens=10, output_tokens=20, usage_reported=True)
        result = Pipeline(self.store, generator=generator).process("m1")
        self.assertEqual(result.state, "needs_data")
        completed = next(e["payload"] for e in self.events() if e["event_type"] == "processing.completed")
        self.assertEqual(completed["usage"], {"input_tokens": 10, "output_tokens": 20})
        self.assertEqual(completed["answer"], "")

    def test_429_is_one_attempt_with_code_body_and_request_id(self):
        error = urllib.error.HTTPError("https://example.invalid", 429, "limited", {"x-request-id": "req_error"},
            io.BytesIO(b'{"error":{"code":"insufficient_quota","message":"echo arbitrary-key"}}'))
        with patch("mra.generate.DISABLE_LLM", False), patch("mra.generate.load_openai_key", return_value="arbitrary-key"), \
                patch("urllib.request.urlopen", side_effect=error) as post:
            result = Pipeline(self.store).process("m1", allow_llm=True)
        self.assertEqual(post.call_count, 1)
        self.assertEqual(result.state, "needs_data")
        self.assertIn("insufficient_quota", result.reason)
        event = next(e["payload"] for e in self.events() if e["event_type"] == "model.request.failed")
        self.assertEqual((event["http_status"], event["request_id"]), (429, "req_error"))
        self.assertNotIn("arbitrary-key", json.dumps(self.events()))

    def test_audit_failure_before_network_prevents_paid_call(self):
        original = self.store.audit_event
        def fail(message_id, event_type, *args, **kwargs):
            if event_type == "model.request.started":
                raise sqlite3.OperationalError("test storage unavailable")
            return original(message_id, event_type, *args, **kwargs)
        with patch.object(self.store, "audit_event", side_effect=fail), patch("mra.generate.DISABLE_LLM", False), \
                patch("mra.generate.load_openai_key", return_value="arbitrary-key"), patch("urllib.request.urlopen") as post:
            with self.assertRaises(sqlite3.Error):
                Pipeline(self.store).process("m1", allow_llm=True)
        post.assert_not_called()
        self.assertEqual(self.store.get_message("m1")[11], "needs_data")

    def test_backup_restores_review_audit_and_outbox(self):
        with tempfile.TemporaryDirectory() as temporary:
            source, target = (Path(temporary) / name for name in ("source.sqlite3", "copy.sqlite3"))
            with contextlib.closing(open_store(source)) as store:
                self.store._db.backup(store._db)
                pipeline = Pipeline(store, generator=lambda *a, **kw: self.reply)
                old = pipeline.process("m1").draft_id
                new = pipeline.revise("m1", "Исправлено", expected_draft_id=old, actor="Tester")
                pipeline.approve("m1", expected_draft_id=new, actor="Tester")
                expected = store.audit_trace("m1", full=True)
                copy_database(source, target)
            with contextlib.closing(open_store(target)) as restored:
                self.assertEqual(restored.audit_trace("m1", full=True), expected)
                self.assertEqual(restored.get_message("m1")[11], "approved")
                self.assertEqual(restored.latest_draft("m1")[1], "Исправлено")
                self.assertEqual(restored._db.execute("PRAGMA foreign_key_check").fetchall(), [])

    def test_audit_checksum_detects_modified_document(self):
        self.draft()
        self.assertTrue(self.store.verify_audit()["ok"])
        digest = self.events()[-1]["document_sha256"]
        self.store._db.execute("UPDATE audit_documents SET content_json='{}' WHERE sha256=?", (digest,))
        self.assertFalse(self.store.verify_audit()["ok"])

    def test_schema_two_migration_preserves_old_data_without_inventing_audit(self):
        self.draft()
        self.pipeline.approve("m1")
        with tempfile.TemporaryDirectory() as temporary:
            source, target = (Path(temporary) / name for name in ("v2.sqlite3", "v3.sqlite3"))
            with contextlib.closing(sqlite3.connect(str(source))) as old:
                self.store._db.backup(old)
                old.executescript("DROP TABLE review_events; DROP TABLE audit_events; DROP TABLE audit_documents; PRAGMA user_version=2;")
            original = source.read_bytes()
            report = copy_database(source, target, migrate=True)
            self.assertEqual(report["schema"], SCHEMA_VERSION)
            self.assertEqual(source.read_bytes(), original)
            with contextlib.closing(open_store(target)) as migrated:
                self.assertEqual(migrated.get_message("m1")[11], "approved")
                self.assertEqual(migrated.latest_draft("m1")[1], self.reply.text)
                self.assertEqual(migrated.audit_trace(), [])
                self.assertTrue(migrated.runlog_for("m1"))


class PersistentTerminalDemo(unittest.TestCase):
    def test_terminal_exit_and_cancelled_generation_never_calls_provider(self):
        from mra.terminal import advanced_terminal as terminal
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "demo.sqlite3"
            with contextlib.closing(create_review_demo(path)[0]) as store:
                # A new local test input can be generated, but only after explicit confirmation.
                store.upsert_message("new-test", "wildberries", "local", "new-test", "question", "Question", article="44167")
            responses = iter(["6", "new-test", "cancel", "0"])
            with patch.dict("os.environ", {"MRA_OPENAI_KEY_FILE": "unused-file"}), patch("mra.pipeline.generate_reply") as model:
                self.assertEqual(terminal(path, read=lambda _: next(responses), write=lambda _: None), 0)
                model.assert_not_called()
            with contextlib.closing(open_store(path)) as store:
                self.assertEqual(store.get_message("new-test")[11], "new")

    def test_cli_bad_json_is_logged_and_does_not_create_message(self):
        with tempfile.TemporaryDirectory() as temporary:
            path, source = (Path(temporary) / name for name in ("db.sqlite3", "bad.json"))
            source.write_text("{invalid}", encoding="utf-8")
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(cli.main(["--db", str(path), "import-json", str(source)]), 1)
            with contextlib.closing(open_store(path)) as store:
                self.assertEqual(store.audit_trace()[-1]["event_type"], "import.file_failed")
                self.assertEqual(store.messages_by_state("new"), [])

    def test_demo_keeps_audit_and_never_overwrites_existing_file(self):
        with tempfile.TemporaryDirectory() as temporary, patch("urllib.request.urlopen", side_effect=AssertionError("network forbidden")):
            path = Path(temporary) / "demo.sqlite3"
            with contextlib.closing(create_review_demo(path)[0]) as store:
                self.assertEqual(store.latest_draft("check_44167_001")[2], "manual_demo")
                review_message(store, "check_44167_001", actor="Tester", read=lambda _: "1", write=lambda _: None)
            before = path.read_bytes()
            with self.assertRaises(FileExistsError):
                create_review_demo(path)
            self.assertEqual(path.read_bytes(), before)
            with contextlib.closing(open_store(path)) as store:
                events = store.audit_trace("check_44167_001", full=True)
                self.assertFalse(any(e["event_type"] == "model.request.started" for e in events))
                self.assertEqual(store.get_message("check_44167_001")[11], "approved")
                self.assertEqual(store.review_history("check_44167_001")[0]["actor"], "Tester")

    def test_cli_demo_review_and_trace(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "demo.sqlite3"
            with contextlib.redirect_stdout(io.StringIO()), patch("builtins.input", return_value="1"):
                self.assertEqual(cli.main(["demo-review", "--output-db", str(path), "--operator", "Tester"]), 0)
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                self.assertEqual(cli.main(["--db", str(path), "trace", "check_44167_001", "--full"]), 0)
            events = json.loads(output.getvalue())
            self.assertIn("review.approved", [e["event_type"] for e in events])


if __name__ == "__main__":
    unittest.main()
