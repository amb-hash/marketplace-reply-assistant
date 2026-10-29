from __future__ import annotations

import contextlib
import hashlib
import io
import json
from pathlib import Path
import socket
import sqlite3
import tempfile
import unittest
import urllib.error
from unittest.mock import patch

from mra import cli, generate
from mra.demo import run_demo
from mra.extract import extract_article, normalize_article
from mra.generate import GenerationResult
from mra.importing import (article_from_hints, copy_database, import_legacy_messages,
                           import_snapshot, message_id_for, readonly_database)
from mra.pipeline import Pipeline
from mra.store import Store, open_store

ROOT = Path(__file__).resolve().parents[1]


class Fixture(unittest.TestCase):
    def setUp(self):
        self.rules = "версия: 1.0\nСинтетические правила"
        self.addCleanup(patch.stopall)
        patch("mra.generate.load_rules", side_effect=lambda: self.rules).start()
        patch("mra.cli.load_rules", side_effect=lambda: self.rules).start()
        patch("socket.create_connection", side_effect=AssertionError("network forbidden")).start()
        patch.object(socket.socket, "connect", side_effect=AssertionError("network forbidden")).start()
        patch("urllib.request.urlopen", side_effect=AssertionError("network forbidden")).start()
        self.store = Store(Path(":memory:"))
        self.addCleanup(self.store.close)
        self.store.upsert_product("DEMO-A")
        self.store.replace_material("DEMO-A", [("demo.fact", "Синтетический факт A")])
        self.store.sign_canon("synthetic-test", "in-memory fixture")
        self.store.upsert_message("m1", "ozon", "account-a", "external-a", "question", "Вопрос", article="DEMO-A")
        self.reply = GenerationResult(text="Синтетический ответ", model="fixture", source="demo", decision="answer",
                                      basis=(("demo.fact", "Синтетический факт A"),))
        self.pipeline = Pipeline(self.store, generator=lambda *a, **k: self.reply)


class Extraction(Fixture):
    def test_normalize_suffixes(self):
        self.assertEqual(normalize_article(" 44165-OZ "), "44165")
        self.assertEqual(normalize_article("63162"), "63162")

    def test_wb_seller_article(self):
        self.assertEqual(extract_article("wildberries", {"sellerArticle": "44167"}), "44167")

    def test_yandex_offer(self):
        self.assertEqual(extract_article("yandex_market", {"offerId": "44165-OZ"}), "44165")

    def test_ozon_requires_mapping(self):
        self.assertEqual(extract_article("ozon", {"sku": "123456789"}), "")
        self.assertEqual(extract_article("ozon", {"sku": "123456789"}, lambda _: ""), "")
        self.assertEqual(extract_article("ozon", {"sku": "123456789"}, lambda _: "DEMO-A"), "DEMO-A")

    def test_direct_ozon_offer_wins(self):
        self.assertEqual(extract_article("ozon", {"offer_id": "44165-OZ", "sku": "unmapped"}), "44165")

    def test_missing_identifier_is_empty(self):
        self.assertEqual(extract_article("ozon", {}), "")

    def test_unknown_wb_nmid_is_not_an_article(self):
        self.assertEqual(extract_article("wildberries", {"nmID": "123456"}), "")

    def test_account_scoped_map(self):
        self.store.map_product("ozon", "same", "DEMO-A", "account-a")
        self.store.map_product("ozon", "same", "DEMO-B", "account-b")
        self.assertEqual(self.store.lookup_article("ozon", "same", "account-a"), "DEMO-A")
        self.assertEqual(self.store.lookup_article("ozon", "same", "account-b"), "DEMO-B")
        self.assertEqual(self.store.lookup_article("ozon", "same", "other"), "")


class ModelContracts(Fixture):
    def test_no_credentials_goes_to_operator(self):
        with patch("mra.generate.DISABLE_LLM", False), patch("mra.generate.load_openai_key", return_value=None):
            reply = generate.generate_reply("Вопрос", "DEMO-A", self.store.material_for("DEMO-A"), allow_llm=True)
        self.assertEqual(reply.decision, "operator")
        self.assertFalse(reply.text)

    def test_no_material_does_not_load_key(self):
        with patch("mra.generate.load_openai_key") as key:
            result = generate.generate_reply("Вопрос", "DEMO-A", [], allow_llm=True)
        key.assert_not_called()
        self.assertEqual(result.decision, "operator")

    def test_live_is_explicit(self):
        with patch("mra.generate.load_openai_key") as key:
            generate.generate_reply("Вопрос", "DEMO-A", self.store.material_for("DEMO-A"))
        key.assert_not_called()

    def call_model(self, response):
        with patch("mra.generate.DISABLE_LLM", False), patch("mra.generate.load_openai_key", return_value="synthetic-key"), \
                patch("mra.generate._post_responses", return_value=response) as post:
            result = generate.generate_reply("Вопрос", "DEMO-A", self.store.material_for("DEMO-A"), allow_llm=True)
        return result, post

    def test_provider_outage_is_not_a_fallback_answer(self):
        result, _ = self.call_model(None)
        self.assertEqual(result.decision, "operator")
        self.assertFalse(result.text)

    def test_incomplete_output_is_not_a_draft(self):
        result, _ = self.call_model({"status": "incomplete", "output": []})
        self.assertEqual(result.decision, "operator")

    def test_strict_response_format_and_usage(self):
        result, post = self.call_model({"status": "completed", "model": "fixture", "usage": {"input_tokens": 4, "output_tokens": 5},
            "output": [{"type": "message", "content": [{"type": "output_text", "text": '{"decision":"answer","answer":"Ответ","used":[1],"note":""}'}]}]})
        self.assertEqual(result.decision, "answer")
        self.assertEqual((result.input_tokens, result.output_tokens), (4, 5))
        payload = post.call_args.args[1]
        self.assertFalse(payload["store"])
        self.assertEqual(payload["text"]["format"]["type"], "json_schema")
        self.assertTrue(payload["text"]["format"]["strict"])
        self.assertNotIn("synthetic-key", json.dumps(payload))

    def test_refusal_is_operator(self):
        result, _ = self.call_model({"status": "completed", "output": [{"type": "message", "content": [{"type": "refusal", "refusal": "fixture"}]}]})
        self.assertEqual(result.decision, "operator")

    def test_invalid_json_unknown_decision_and_references(self):
        for raw in ["broken { json", "[]", "null", '{"decision":"unknown","answer":"Ответ","used":[1]}',
                    '{"decision":"answer","answer":"Ответ","used":[999]}',
                    '{"decision":"answer","answer":"Ответ","used":[true]}',
                    '{"decision":"answer","answer":"Ответ","used":[]}',
                    '{"decision":"answer","answer":42,"used":[1]}']:
            with self.subTest(raw=raw):
                self.assertEqual(generate._parse_structured(raw, self.store.material_for("DEMO-A"))[2], "operator")

    def test_valid_operator_discards_customer_text(self):
        text, basis, decision, _ = generate._parse_structured('{"decision":"operator","answer":"untrusted","note":"Уточнить","used":[]}', [])
        self.assertEqual((text, basis, decision), ("", (), "operator"))

    def test_http_errors_are_single_call_and_do_not_expose_key(self):
        error = urllib.error.HTTPError("https://example.invalid", 401, "synthetic-key", {}, None)
        with patch("urllib.request.urlopen", side_effect=error) as transport:
            with self.assertRaises(generate.ProviderError) as caught:
                generate._post_responses("synthetic-key", {"model": "fixture"})
        self.assertEqual(transport.call_count, 1)
        self.assertNotIn("synthetic-key", str(caught.exception))

    def test_quota_error_is_visible_without_exposing_response_body(self):
        error = urllib.error.HTTPError("https://example.invalid", 429, "limited", {},
            io.BytesIO(b'{"error":{"code":"insufficient_quota","message":"synthetic-key"}}'))
        with patch("urllib.request.urlopen", side_effect=error) as transport:
            with self.assertRaises(generate.ProviderError) as caught:
                generate._post_responses("synthetic-key", {"model": "fixture"})
        self.assertEqual(transport.call_count, 1)
        self.assertIn("insufficient_quota", str(caught.exception))
        self.assertNotIn("synthetic-key", str(caught.exception))


class Workflow(Fixture):
    def test_material_lookup(self):
        self.assertEqual(self.store.material_for("DEMO-A"), [("demo.fact", "Синтетический факт A")])

    def test_lifecycle_to_hold(self):
        outcome = self.pipeline.process("m1")
        self.assertEqual(outcome.state, "drafted")
        draft = self.pipeline.approve("m1")
        self.assertEqual(self.pipeline.approve("m1"), draft)
        self.assertEqual(self.store._db.execute("SELECT state FROM outbox").fetchall(), [("held",)])

    def test_scoped_processing_journal(self):
        self.assertFalse(self.store.is_processed("x"))
        self.store.mark_processed("x")
        self.assertTrue(self.store.is_processed("x"))
        self.assertFalse(self.store.is_processed("x", "ozon", "other", "review", 1))

    def test_repeated_processing_does_not_call_model(self):
        self.pipeline.process("m1")
        with patch("mra.pipeline.generate_reply") as model:
            with self.assertRaises(ValueError):
                Pipeline(self.store).process("m1")
        model.assert_not_called()
        self.assertEqual(self.store._db.execute("SELECT COUNT(*) FROM drafts").fetchone()[0], 1)

    def test_needs_data_cannot_approve_old_draft(self):
        self.pipeline.process("m1")
        self.store.set_message_state("m1", "needs_data")
        with self.assertRaises(ValueError):
            self.pipeline.approve("m1")
        self.assertEqual(self.store.outbox_count(), 0)

    def test_changed_message_invalidates_draft_and_hold(self):
        self.pipeline.process("m1")
        self.pipeline.approve("m1")
        self.store.upsert_message("m1", "ozon", "account-a", "external-a", "question", "Другой вопрос",
                                  article="DEMO-A", pros="Новые плюсы", cons="Новые минусы", rating=1)
        row = self.store.get_message("m1")
        self.assertEqual((row[11], row[6], row[7], row[8]), ("new", "Новые плюсы", "Новые минусы", 1))
        self.assertEqual(self.store.message_revision("m1"), 2)
        self.assertIsNone(self.store.latest_draft("m1"))
        self.assertEqual(self.store._db.execute("SELECT state FROM outbox").fetchone()[0], "superseded")

    def test_same_input_does_not_revoke_approval(self):
        self.pipeline.process("m1")
        self.pipeline.approve("m1")
        self.store.upsert_message("m1", "ozon", "account-a", "external-a", "question", "Вопрос", article="DEMO-A")
        self.assertEqual(self.store.get_message("m1")[11], "approved")
        self.assertEqual(self.store.message_revision("m1"), 1)

    def test_id_collision_across_accounts_is_rejected(self):
        with self.assertRaises(ValueError):
            self.store.upsert_message("m1", "ozon", "account-b", "external-a", "question", "Вопрос")

    def test_material_integrity_blocks_model(self):
        self.store.replace_material("DEMO-A", [("demo.fact", "Изменённый факт")])
        with patch("mra.pipeline.generate_reply") as model:
            outcome = Pipeline(self.store).process("m1")
        model.assert_not_called()
        self.assertEqual(outcome.state, "needs_data")

    def test_rules_change_invalidates_approval(self):
        self.pipeline.process("m1")
        self.rules = "Другие правила"
        with self.assertRaises(ValueError):
            self.pipeline.approve("m1")
        self.assertEqual(self.store.get_message("m1")[11], "needs_data")
        self.assertEqual(self.store.outbox_count(), 0)

    def test_material_change_revokes_hold(self):
        self.pipeline.process("m1")
        self.pipeline.approve("m1")
        self.store.replace_material("DEMO-A", [("demo.fact", "Новый факт")])
        self.assertEqual(self.store.get_message("m1")[11], "needs_data")
        self.assertEqual(self.store._db.execute("SELECT state FROM outbox").fetchone()[0], "superseded")

    def test_source_change_during_generation_discards_output(self):
        def change_rules(*_a, **_k):
            self.rules = "Правила изменились в процессе"
            return self.reply
        outcome = Pipeline(self.store, generator=change_rules).process("m1")
        self.assertEqual(outcome.state, "needs_data")
        self.assertIsNone(self.store.latest_draft("m1"))

    def test_new_input_during_generation_remains_new(self):
        def update_input(*_a, **_k):
            self.store.upsert_message("m1", "ozon", "account-a", "external-a", "question", "Новая ревизия", article="DEMO-A")
            return self.reply
        with self.assertRaises(ValueError):
            Pipeline(self.store, generator=update_input).process("m1")
        self.assertEqual(self.store.get_message("m1")[11], "new")
        self.assertIsNone(self.store.latest_draft("m1"))

    def test_draft_and_runlog_roll_back_together(self):
        with patch.object(self.store, "add_runlog", side_effect=sqlite3.OperationalError("injected storage failure")):
            with self.assertRaises(sqlite3.OperationalError):
                self.pipeline.process("m1")
        self.assertIsNone(self.store.latest_draft("m1"))
        self.assertEqual(self.store.get_message("m1")[11], "needs_data")
        self.assertEqual(self.store.recent_runlog(), [])

    def test_approval_and_journal_roll_back_together(self):
        self.pipeline.process("m1")
        with patch.object(self.store, "mark_processed", side_effect=sqlite3.OperationalError("injected storage failure")):
            with self.assertRaises(sqlite3.OperationalError):
                self.pipeline.approve("m1")
        self.assertEqual(self.store.get_message("m1")[11], "drafted")
        self.assertEqual(self.store.outbox_count(), 0)
        self.assertEqual(self.store._db.execute("SELECT COUNT(*) FROM approvals").fetchone()[0], 0)

    def test_recover_cancels_previous_worker(self):
        _row, token, revision = self.store.claim_message("m1")
        self.store.recover_message("m1")
        with self.assertRaises(ValueError):
            self.store.assert_claim("m1", token, revision)
        self.assertEqual(self.store.get_message("m1")[11], "new")

    def test_retry_is_explicit_and_only_for_pending_messages(self):
        outcome = Pipeline(self.store, generator=lambda *a, **k: GenerationResult(note="temporary fixture error")).process("m1")
        self.assertEqual(outcome.state, "needs_data")
        with self.assertRaises(ValueError):
            self.pipeline.process("m1")
        self.assertEqual(self.pipeline.process("m1", retry=True).state, "drafted")

    def test_second_connection_cannot_claim_same_input(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "work.sqlite3"
            with contextlib.closing(open_store(path)) as one, contextlib.closing(open_store(path)) as two:
                one.upsert_message("m", "ozon", "a", "e", "question", "Вопрос")
                one.claim_message("m")
                with self.assertRaises(ValueError):
                    two.claim_message("m")


class ImportsAndMigration(Fixture):
    def snapshot(self):
        return json.loads((ROOT / "examples/snapshot.json").read_text(encoding="utf-8"))

    def test_snapshot_resolves_ozon_account_mapping(self):
        counts = import_snapshot(self.store, self.snapshot())
        self.assertEqual(counts, {"products": 1, "mappings": 1, "messages": 1})
        mid = message_id_for("ozon", "demo-account", "question", "demo-question-001")
        self.assertEqual(self.store.get_message(mid)[9], "DEMO-001")
        self.assertTrue(self.store.verify_canon()["ok"])

    def test_bad_snapshot_is_atomic(self):
        snapshot = self.snapshot()
        snapshot["messages"].append({"marketplace": "ozon"})
        with self.assertRaises(ValueError):
            import_snapshot(self.store, snapshot)
        self.assertEqual(self.store.material_for("DEMO-001"), [])
        self.assertEqual(self.store.lookup_article("ozon", "demo-sku-001", "demo-account"), "")
        self.assertTrue(self.store.verify_canon()["ok"])

    def test_material_replacement_is_explicit(self):
        snapshot = self.snapshot()
        import_snapshot(self.store, snapshot)
        snapshot["products"][0]["material"][0][1] = "Изменённый синтетический факт"
        with self.assertRaises(ValueError):
            import_snapshot(self.store, snapshot)
        import_snapshot(self.store, snapshot, replace_materials=True)
        self.assertTrue(self.store.verify_canon()["ok"])

    def legacy_source(self, path):
        connection = sqlite3.connect(str(path))
        connection.executescript("""
            CREATE TABLE inbound_messages(message_id TEXT, marketplace TEXT, account_id TEXT, external_id TEXT,
                kind TEXT, current_revision_id TEXT, workflow_state TEXT, created_at TEXT);
            CREATE TABLE inbound_revisions(revision_id TEXT, message_id TEXT, text TEXT, pros TEXT, cons TEXT, rating INTEGER);
            CREATE TABLE inbound_product_hints(revision_id TEXT, identifier_type TEXT, identifier_value TEXT);
            INSERT INTO inbound_messages VALUES('legacy-message','ozon','account-a','legacy-ext','question','current','new','2026-01-01');
            INSERT INTO inbound_revisions VALUES('current','legacy-message','Вопрос','','',NULL);
            INSERT INTO inbound_revisions VALUES('old','legacy-message','Старый вопрос','','',NULL);
            INSERT INTO inbound_product_hints VALUES('current','sku','current-sku');
            INSERT INTO inbound_product_hints VALUES('old','sku','old-sku');
        """)
        connection.commit()
        connection.close()

    def test_legacy_import_uses_current_revision_and_account_map(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "collector.sqlite3"
            self.legacy_source(path)
            before = hashlib.sha256(path.read_bytes()).hexdigest()
            self.store.map_product("ozon", "current-sku", "DEMO-A", "account-a")
            self.store.map_product("ozon", "old-sku", "WRONG-ARTICLE", "account-a")
            with contextlib.closing(readonly_database(path)) as source:
                self.assertEqual(article_from_hints(source, "legacy-message", "ozon", self.store, "account-a"), "DEMO-A")
                with self.assertRaises(sqlite3.OperationalError):
                    source.execute("CREATE TABLE forbidden(x)")
            counts = import_legacy_messages(self.store, path, limit=1)
            self.assertEqual(counts, {"messages": 1, "with_article": 1})
            self.assertEqual(self.store.get_message("legacy-message")[9], "DEMO-A")
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), before)

    def test_migrate_legacy_copy_and_preserve_original(self):
        with tempfile.TemporaryDirectory() as tmp:
            source, target = Path(tmp) / "old.sqlite3", Path(tmp) / "new.sqlite3"
            old = sqlite3.connect(str(source))
            old.executescript((ROOT / "tests/fixtures/legacy-1.1.sql").read_text())
            old.executescript("""
                INSERT INTO products VALUES('DEMO-A','','');
                INSERT INTO materials(article,topic,statement) VALUES('DEMO-A','demo.fact','Synthetic');
                INSERT INTO product_map VALUES('ozon','account-a','sku-a','DEMO-A');
                INSERT INTO messages(message_id,marketplace,account_id,external_id,kind,text,article,state,created_at)
                  VALUES('old-message','ozon','account-a','e1','question','Synthetic','DEMO-A','approved','2026-01-01');
                INSERT INTO drafts VALUES('old-draft','old-message','Synthetic','old','2026-01-01');
                INSERT INTO approvals VALUES('old-draft','2026-01-01');
                INSERT INTO outbox VALUES('old-draft','held','old','2026-01-01');
            """)
            sha = hashlib.sha256("DEMO-A|demo.fact|Synthetic".encode()).hexdigest()
            signature = hashlib.sha256(f"legacy|{sha}|fixture|fixture".encode()).hexdigest()
            old.execute("INSERT INTO canon_manifest VALUES(?,?,?,?,?,?,?,?)", ("legacy", sha, "fixture", "fixture", "2026-01-01", 1, 1, signature))
            old.execute("INSERT INTO meta VALUES('canon_version','legacy')")
            old.commit(); old.close()
            before = hashlib.sha256(source.read_bytes()).hexdigest()
            report = copy_database(source, target, migrate=True)
            self.assertTrue(report["canon"])
            self.assertEqual(hashlib.sha256(source.read_bytes()).hexdigest(), before)
            with contextlib.closing(open_store(target)) as migrated:
                self.assertEqual(migrated.get_message("old-message")[11], "needs_data")
                self.assertEqual(migrated.lookup_article("ozon", "sku-a", "account-a"), "DEMO-A")
                self.assertEqual(migrated._db.execute("SELECT state FROM outbox").fetchone()[0], "superseded")
                self.assertTrue(migrated.verify_canon()["ok"])
            with self.assertRaises(FileExistsError):
                copy_database(source, target, migrate=True)

    def test_backup_can_be_restored(self):
        with tempfile.TemporaryDirectory() as tmp:
            source, backup = Path(tmp) / "work.sqlite3", Path(tmp) / "backup.sqlite3"
            with contextlib.closing(open_store(source)) as store:
                import_snapshot(store, self.snapshot())
            copy_database(source, backup)
            with contextlib.closing(open_store(backup)) as restored:
                self.assertTrue(restored.verify_canon()["ok"])
                self.assertEqual(len(restored.messages_by_state("new")), 1)


class OperatorCLI(Fixture):
    def test_help_and_version_do_not_create_db(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "never.sqlite3"
            for flag in ("--help", "--version"):
                with self.subTest(flag=flag), contextlib.redirect_stdout(io.StringIO()):
                    with self.assertRaises(SystemExit) as caught:
                        cli.main(["--db", str(target), flag])
                    self.assertEqual(caught.exception.code, 0)
            self.assertFalse(target.exists())

    def test_invalid_arguments_do_not_create_db(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "never.sqlite3"
            with contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as caught:
                    cli.main(["--db", str(target), "run", "--limit", "-1", "--dry-run"])
            self.assertEqual(caught.exception.code, 2)
            self.assertFalse(target.exists())

    def test_cli_import_queue_doctor_and_dry_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "work.sqlite3"
            base = ["--db", str(target)]
            with contextlib.redirect_stdout(io.StringIO()) as output:
                self.assertEqual(cli.main(base + ["import-json", str(ROOT / "examples/snapshot.json")]), 0)
                self.assertEqual(cli.main(base + ["doctor"]), 1)
                self.assertEqual(cli.main(base + ["import-bundled"]), 0)
                self.assertEqual(cli.main(base + ["doctor"]), 0)
                self.assertEqual(cli.main(base + ["run", "--dry-run"]), 0)
                self.assertEqual(cli.main(base + ["queue", "--state", "new"]), 0)
            self.assertIn("m_6bcb5de67c605f37d65078eef1adb253", output.getvalue())
            with contextlib.closing(open_store(target)) as store:
                self.assertEqual(len(store.messages_by_state("new")), 1)

    def test_demo_finishes_in_hold_without_network(self):
        result = run_demo()
        self.assertEqual((result["state"], result["outbox"], result["network_calls"]), ("approved", "held", 0))


if __name__ == "__main__":
    unittest.main()
