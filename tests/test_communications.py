from __future__ import annotations

from contextlib import closing
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import urllib.error

from mra import delivery
from mra.catalog import apply_snapshot, now
from mra.communications import sync_inbox
from mra.generate import GenerationResult
from mra.importing import import_snapshot
from mra.market_api import CommunicationClient, MarketError, normalize
from mra.pipeline import Pipeline
from mra.store import open_store
from mra.terminal import advanced_terminal as terminal


class FixtureAPI:
    def __init__(self, marketplace, kind):
        self.mp, self.kind, self.calls, self.writes = marketplace, kind, [], []
        self.reply, self.lose_ack, self.on_read = "", False, None
        if marketplace == "wildberries":
            self.raw = {"id": "q1", "text": "Question", "createdDate": "2026-10-29T00:00:00Z", "state": "none",
                        "productDetails": {"nmId": 11, "supplierArticle": "44167"}}
            if kind == "review":
                self.raw.update(productValuation=5, pros="Good", cons="")
        elif marketplace == "ozon":
            self.raw = {"id": "q1", "sku": 11, "text": "Question", "published_at": "2026-10-29T00:00:00Z", "status": "NEW"}
            if kind == "review":
                self.raw["rating"] = 5
        else:
            self.raw = {"questionIdentifiers": {"id": 1, "offerId": "44167"}, "businessId": 7, "text": "Question"} if kind == "question" else {
                "feedbackId": 1, "identifiers": {"offerId": "44167"}, "description": {"comment": "Question"}, "statistics": {"rating": 5}}

    def __call__(self, method, path, payload, query):
        self.calls.append((method, path, payload, query))
        write = path.endswith(("/answer/create", "/comment/create", "/comments/update", "/goods-questions/update", "/feedbacks/answer")) or method == "PATCH"
        if write:
            self.writes.append((method, path, payload))
            self.reply = (payload.get("answer") or payload.get("comment") or {}).get("text") or payload["text"]
            if self.lose_ack:
                raise OSError("synthetic lost acknowledgement")
            if self.mp == "ozon":
                return {"answer_id" if self.kind == "question" else "comment_id": "r1"}
            if self.mp == "yandex_market":
                return {"status": "OK", "result": {"entity": {"id": 2, "type": "ANSWER"}} if self.kind == "question" else {"id": 2}}
            return {}
        if self.on_read:
            callback, self.on_read = self.on_read, None
            callback()
        if self.mp == "wildberries":
            raw = dict(self.raw, answer={"text": self.reply} if self.reply else None,
                       state="wbRu" if self.reply else "none")
            return {"data": raw} if path.endswith(("/question", "/feedback")) else {
                "data": {"questions" if self.kind == "question" else "feedbacks": [raw]}}
        if self.mp == "ozon":
            if path.endswith("/info"):
                return dict(self.raw)
            if path.endswith("/answer/list"):
                return {"answers": [{"id": "r1", "question_id": "q1", "sku": 11, "text": self.reply,
                        "status_publication": "PUBLISHED"}] if self.reply else [], "last_id": ""}
            if path.endswith("/comment/list"):
                return {"comments": [{"id": "r1", "text": self.reply, "is_owner": True, "is_published": True}] if self.reply else [], "offset": 0}
            return {"questions" if self.kind == "question" else "reviews": [dict(self.raw)], "has_next": False, "last_id": ""}
        if path.endswith(("/answers", "/comments")):
            return {"status": "OK", "result": {"answers" if self.kind == "question" else "comments": [{
                "id": 2, "questionId" if self.kind == "question" else "feedbackId": 1, "text": self.reply,
                "canModify": True, "status": "PUBLISHED"}] if self.reply else []}}
        return {"status": "OK", "result": {"questions" if self.kind == "question" else "feedbacks": [dict(self.raw)]}}


class CommunicationWorkflow(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / "test.sqlite3"
        self.store = open_store(self.path)
        self.addCleanup(self.store.close)
        import_snapshot(self.store, {"schema_version": 1, "products": [{"article": "44167", "material": [["fixture", "Synthetic approved fact"]]}],
                                   "provenance": {"approved_by": "fixture", "source": "synthetic fixture"}})

    def prepare(self, mp="ozon", kind="question"):
        account = {"marketplace": mp, "account_id": "account_" + mp, "name": "Synthetic account", "business_id": 7}
        apply_snapshot(self.store, account, [{"listing_id": "11", "article": "44167", "aliases": [("sku", "11"), ("nmID", "11")]}], observed_at=now(), complete=True)
        api = FixtureAPI(mp, kind)
        result = sync_inbox(self.store, account, kind=kind, transport=api, pause=lambda _: None)
        mid = result["messages"][0]
        pipeline = Pipeline(self.store, generator=lambda *a, **kw: GenerationResult(text="Reviewed fixture answer", model="fixture", source="fixture",
            basis=tuple(self.store.material_for("44167")), decision="answer"))
        draft = pipeline.process(mid)
        pipeline.approve(mid, expected_draft_id=draft.draft_id, actor="Reviewer")
        plan = delivery.prepare(self.store, [account], mid, expected_draft_id=draft.draft_id, actor="Reviewer")
        return account, api, mid, plan

    def send(self, account, api, plan):
        return delivery.send(self.store, [account], plan["job_id"], expected_text_sha256=plan["text_sha256"], actor="Sender",
                             live=True, transport=api, pause=lambda _: None)

    def test_known_scoped_sku_from_fresh_native_question_does_not_require_full_catalog(self):
        from mra.catalog import register_accounts, require_message_product
        account={"marketplace":"ozon","account_id":"communication_only","name":"Fixture"}
        register_accounts(self.store,[account])
        self.store.map_product("ozon","11","44167",account["account_id"])
        api=FixtureAPI("ozon","question")
        mid=sync_inbox(self.store,account,transport=api,pause=lambda _:None)["messages"][0]
        row=self.store.get_message(mid)
        require_message_product(self.store,row)
        self.store._db.execute("UPDATE remote_inputs SET observed_at='2020-01-01T00:00:00+00:00' WHERE message_id=?",(mid,))
        with self.assertRaises(ValueError):require_message_product(self.store,row)
        self.store._db.execute("UPDATE remote_inputs SET observed_at=? WHERE message_id=?",(now(),mid))
        self.store.map_product("ozon","11","OTHER",account["account_id"])
        with self.assertRaises(ValueError):require_message_product(self.store,row)

    def test_native_product_check_refuses_mutated_text_and_other_account_mapping(self):
        from mra.catalog import register_accounts, require_message_product
        account={"marketplace":"ozon","account_id":"native_check","name":"Fixture"}
        register_accounts(self.store,[account])
        self.store.map_product("ozon","11","44167",account["account_id"])
        api=FixtureAPI("ozon","question")
        mid=sync_inbox(self.store,account,transport=api,pause=lambda _:None)["messages"][0]
        original=self.store.get_message(mid)
        changed=list(original);changed[5]="Changed question"
        with self.assertRaises(ValueError):require_message_product(self.store,changed)
        self.store._db.execute("DELETE FROM product_map WHERE account_id=?",(account["account_id"],))
        self.store.map_product("ozon","11","44167","different_account")
        with self.assertRaises(ValueError):require_message_product(self.store,original)

    def test_single_question_workflow_requires_typed_confirmation_before_one_write(self):
        from mra.review import work_message
        account,api,mid,plan=self.prepare()
        real_send=delivery.send
        with patch("mra.catalog_api.load_accounts",return_value=[account]):
            work_message(self.store,mid,actor="Human",read=lambda _:"",write=lambda _:None)
            self.assertEqual(api.writes,[])
            with patch("mra.delivery.send",side_effect=lambda *args,**kwargs:real_send(*args,**kwargs,transport=api,pause=lambda _:None)):
                work_message(self.store,mid,actor="Human",read=lambda _:"ОПУБЛИКОВАТЬ",write=lambda _:None)
        self.assertEqual(len(api.writes),1)
        events=self.store.audit_trace(mid,full=True)
        self.assertTrue(any(e["event_type"]=="delivery.publication_authorized" and e["actor"]=="Human" for e in events))

    def test_single_question_workflow_rejection_never_writes(self):
        from mra.review import work_message
        account,api,mid,plan=self.prepare()
        inputs=iter(["ОТКЛОНИТЬ","Проверяю отклонение"])
        with patch("mra.catalog_api.load_accounts",return_value=[account]):
            work_message(self.store,mid,actor="Human",read=lambda _:next(inputs),write=lambda _:None)
        self.assertEqual(api.writes,[])
        self.assertEqual(self.store.get_message(mid)[11],"rejected")

    def test_regeneration_is_forbidden_after_write_started(self):
        account,api,mid,plan=self.prepare()
        self.send(account,api,plan)
        with patch("mra.pipeline.generate_reply") as model:
            with self.assertRaises(ValueError):
                Pipeline(self.store).regenerate(mid,"Измени уже опубликованный ответ",expected_draft_id=plan["draft_id"],actor="Reviewer")
        model.assert_not_called()
        self.assertEqual(len(api.writes),1)

    def test_complete_lifecycle_for_all_three_marketplaces_and_both_channels(self):
        for mp in ("ozon", "wildberries", "yandex_market"):
            for kind in ("question", "review"):
                with self.subTest(mp=mp, kind=kind):
                    account, api, mid, plan = self.prepare(mp, kind)
                    self.assertEqual(api.writes, [])
                    result = self.send(account, api, plan)
                    self.assertEqual(len(api.writes), 1)
                    self.assertIn(result["state"], {"published", "observed"})
                    self.assertTrue(result["write_started"])
                    with self.assertRaises(ValueError):
                        self.send(account, api, plan)
                    self.assertEqual(len(api.writes), 1)
                    self.assertTrue(self.store.verify_audit()["ok"])

    def test_lost_ack_is_reconciled_without_a_second_write(self):
        account, api, mid, plan = self.prepare()
        api.lose_ack = True
        with self.assertRaises(MarketError):
            self.send(account, api, plan)
        self.assertEqual(delivery.get_job(self.store, plan["job_id"])["state"], "uncertain")
        with self.assertRaises(ValueError):
            self.send(account, api, plan)
        result = delivery.reconcile(self.store, [account], plan["job_id"], transport=api, pause=lambda _: None)
        self.assertEqual(result["state"], "published")
        self.assertEqual(len(api.writes), 1)

    def test_changed_remote_question_blocks_write_and_invalidates_old_approval(self):
        account, api, mid, plan = self.prepare()
        api.raw["text"] = "Changed question"
        with self.assertRaisesRegex(ValueError, "изменился"):
            self.send(account, api, plan)
        self.assertEqual(api.writes, [])
        self.assertEqual(self.store.get_message(mid)[11], "new")

    def test_colleague_answer_and_changed_review_block_a_second_answer(self):
        account, api, mid, plan = self.prepare()
        api.reply = "Colleague has already replied"
        with self.assertRaisesRegex(ValueError, "уже есть ответ"):
            self.send(account, api, plan)
        self.assertEqual(api.writes, [])
        api.reply = ""
        plan = delivery.prepare(self.store, [account], mid, expected_draft_id=plan["draft_id"], actor="Reviewer")
        api.on_read = lambda: Pipeline(self.store).revise(mid, "Corrected answer", expected_draft_id=plan["draft_id"], actor="Other reviewer")
        with self.assertRaises(ValueError):
            self.send(account, api, plan)
        self.assertEqual(api.writes, [])

    def test_wrong_confirmation_scope_and_synthetic_input_cannot_send(self):
        account, api, mid, plan = self.prepare()
        with self.assertRaisesRegex(ValueError, "другой текст"):
            delivery.send(self.store, [account], plan["job_id"], expected_text_sha256="wrong", actor="Sender", live=True, transport=api)
        with self.assertRaises(ValueError):
            delivery.send(self.store, [account], plan["job_id"], expected_text_sha256=plan["text_sha256"], actor="Sender", transport=api)
        other = dict(account, account_id="other")
        with self.assertRaises(ValueError):
            CommunicationClient(other, transport=api).get(plan["target"])
        self.assertEqual(api.writes, [])
        self.store.upsert_message("local", "ozon", account["account_id"], "local", "question", "Local", article="44167")
        with self.assertRaisesRegex(ValueError, "напрямую"):
            delivery.prepare(self.store, [account], "local", expected_draft_id="none", actor="Reviewer")

    def test_second_process_cannot_claim_job_and_prewrite_recovery_does_not_reset_a_write(self):
        account, api, mid, plan = self.prepare()
        def competing():
            with closing(open_store(self.path)) as other:
                with self.assertRaises(ValueError):
                    delivery.send(other, [account], plan["job_id"], expected_text_sha256=plan["text_sha256"], actor="Second", live=True, transport=api)
        api.on_read = competing
        self.send(account, api, plan)
        self.assertEqual(len(api.writes), 1)
        with self.assertRaises(ValueError):
            delivery.recover(self.store, plan["job_id"], actor="Operator")

    def test_duplicate_inbox_does_not_revoke_review_and_nonempty_terminal_queue_opens(self):
        account, api, mid, plan = self.prepare()
        revision = self.store.message_revision(mid)
        sync_inbox(self.store, account, transport=api, pause=lambda _: None)
        self.assertEqual(self.store.message_revision(mid), revision)
        self.assertEqual(self.store.get_message(mid)[11], "approved")
        replies = iter(["2", "", "0"])
        output = []
        self.assertEqual(terminal(self.path, read=lambda _: next(replies), write=output.append), 0)
        self.assertTrue(any(mid in line for line in output))

    def test_terminal_shows_exact_destination_and_cancel_keeps_marketplace_untouched(self):
        account, api, mid, plan = self.prepare()
        replies = iter(["8", mid, "", "0"])
        output = []
        with patch("mra.terminal.load_accounts", return_value=[account]), patch("mra.market_api.CommunicationClient.request") as request:
            self.assertEqual(terminal(self.path, read=lambda _: next(replies), write=output.append), 0)
            request.assert_not_called()
        self.assertTrue(any(account["account_id"] in line for line in output))
        self.assertTrue(any(plan["text"] in line for line in output))

    def test_post_write_review_is_frozen_and_pending_moderation_does_not_mean_published(self):
        account, api, mid, plan = self.prepare()
        original = api.__call__
        def pending(method, path, payload, query):
            body = original(method, path, payload, query)
            if path.endswith("/answer/list"):
                for reply in body["answers"]:
                    reply["status_publication"] = "UNMODERATED"
            return body
        result = delivery.send(self.store, [account], plan["job_id"], expected_text_sha256=plan["text_sha256"], actor="Sender",
            live=True, transport=pending, pause=lambda _: None)
        self.assertEqual(result["state"], "observed")
        self.assertEqual(self.store.get_message(mid)[11], "approved")
        with self.assertRaisesRegex(ValueError, "отправка уже началась"):
            Pipeline(self.store).revise(mid, "A later edit", expected_draft_id=plan["draft_id"], actor="Other")
        self.assertEqual(len(api.writes), 1)

    def test_crash_before_write_can_be_recovered_without_resetting_ever_written_intents(self):
        account, api, mid, plan = self.prepare()
        self.store._db.execute("UPDATE delivery_jobs SET state='checking' WHERE job_id=?", (plan["job_id"],))
        result = delivery.recover(self.store, plan["job_id"], actor="Operator")
        self.assertEqual(result["state"], "blocked")
        plan = delivery.prepare(self.store, [account], mid, expected_draft_id=plan["draft_id"], actor="Reviewer")
        self.send(account, api, plan)
        self.assertEqual(len(api.writes), 1)

    def test_definitive_403_requires_manual_recovery_before_a_new_attempt(self):
        account, api, mid, plan = self.prepare()
        def denied(method, path, payload, query):
            if path == "/v1/question/answer/create":
                raise urllib.error.HTTPError("https://example.invalid",403,"fixture",{},io.BytesIO(b'{"message":"access denied"}'))
            return api(method,path,payload,query)
        with self.assertRaises(MarketError):
            delivery.send(self.store,[account],plan["job_id"],expected_text_sha256=plan["text_sha256"],actor="Sender",live=True,
                          transport=denied,pause=lambda _:None)
        self.assertEqual(delivery.get_job(self.store,plan["job_id"])["state"],"failed")
        self.assertEqual(api.writes,[])
        with self.assertRaises(ValueError):
            self.send(account,api,plan)
        delivery.recover(self.store,plan["job_id"],actor="Owner")
        plan=delivery.prepare(self.store,[account],mid,expected_draft_id=plan["draft_id"],actor="Reviewer")
        self.send(account,api,plan)
        self.assertEqual(len(api.writes),1)

    def test_six_accounts_with_same_native_id_remain_separate(self):
        from mra.config import VERSION_ROOT
        configured=json.loads((VERSION_ROOT/'examples/accounts.example.json').read_text())["accounts"]
        ids=[]
        for i,config in enumerate(configured):
            account=dict(config,business_id=7+i)
            api=FixtureAPI(account["marketplace"],"question")
            if account["marketplace"]=="yandex_market":api.raw["businessId"]=7+i
            apply_snapshot(self.store,account,[{"listing_id":"11","article":"44167","aliases":[("sku","11"),("nmID","11")]}],observed_at=now(),complete=True)
            result=sync_inbox(self.store,account,transport=api,pause=lambda _:None)
            ids.append(result["messages"][0])
            self.assertEqual(self.store.get_message(ids[-1])[2],account["account_id"])
        self.assertEqual(len(set(ids)),6)

    def test_readiness_does_not_call_mock_delivery_a_complete_live_acceptance(self):
        from mra.readiness import report
        account,api,mid,plan=self.prepare()
        result=report(self.store,[account])
        self.assertFalse(result["all_channels_live_publication_verified"])
        self.assertFalse(result["first_live_test"]["ready"])
        self.assertEqual(result["network_calls"],0)

    def test_cli_generates_only_selected_message_and_records_operator(self):
        from mra import cli
        from contextlib import redirect_stdout
        account,api,mid,plan=self.prepare()
        api.raw["id"]="new-2"
        selected=sync_inbox(self.store,account,transport=api,pause=lambda _:None)["messages"][0]
        api.raw["id"]="new-3"
        other=sync_inbox(self.store,account,transport=api,pause=lambda _:None)["messages"][0]
        answer=GenerationResult(text="Fixture answer",source="fixture",model="fixture",decision="answer",basis=tuple(self.store.material_for("44167")))
        with patch("mra.cli.load_accounts",return_value=[account]),patch("mra.pipeline.generate_reply",return_value=answer) as model,redirect_stdout(io.StringIO()):
            self.assertEqual(cli.main(["--db",str(self.path),"generate",selected,"--operator","Colleague","--live"]),0)
            self.assertEqual(model.call_count,1)
        self.assertEqual(self.store.get_message(selected)[11],"drafted")
        self.assertEqual(self.store.get_message(other)[11],"new")
        authorizations=[e for e in self.store.audit_trace(selected) if e["event_type"]=="model.call_authorized"]
        self.assertEqual(authorizations[-1]["actor"],"Colleague")


class ContractsAndTransport(unittest.TestCase):
    def test_fixed_write_routes_require_explicit_write_enable(self):
        for mp in ("ozon", "wildberries", "yandex_market"):
            api = FixtureAPI(mp, "question")
            account = {"marketplace": mp, "account_id": "A", "business_id": 7}
            client = CommunicationClient(account, transport=api, pause=lambda _: None)
            target = normalize(account, "question", api.raw, business_id="7")["target"]
            with self.assertRaises(MarketError):
                client.send(target, "Reviewed answer")
            with self.assertRaises(MarketError):
                client.request("POST", "/v1/question/answer/delete", {}, write=True)
            self.assertEqual(api.writes, [])

    def test_official_write_payloads_use_numeric_ozon_sku_and_create_only_yandex_operation(self):
        for mp, kind in [("ozon", "question"), ("wildberries", "question"), ("yandex_market", "question"), ("yandex_market", "review")]:
            account = {"marketplace": mp, "account_id": "A", "business_id": 7}
            target = normalize(account, kind, FixtureAPI(mp, kind).raw, business_id="7")["target"]
            plan = CommunicationClient(account).write_plan(target, "Reviewed answer")
            if mp == "ozon":
                self.assertEqual(plan["body"]["sku"], 11)
                self.assertEqual(plan["path"], "/v1/question/answer/create")
            elif mp == "wildberries":
                self.assertEqual(plan["body"], {"id": "q1", "answer": {"text": "Reviewed answer"}, "state": "wbRu"})
            elif kind == "question":
                self.assertEqual(plan["body"], {"parentEntityId": {"id": 1, "type": "QUESTION"}, "operationType": "CREATE", "text": "Reviewed answer"})
            else:
                self.assertEqual(plan["body"], {"feedbackId": 1, "comment": {"text": "Reviewed answer"}})

    def test_cursor_limits_unknown_shape_and_cross_business_fail_without_guessing(self):
        account = {"marketplace": "ozon", "account_id": "A"}
        with self.assertRaises(MarketError):
            CommunicationClient(account, transport=lambda *a: {}, pause=lambda _: None).inbox("question")
        with self.assertRaises(MarketError):
            CommunicationClient(account, max_requests=0)
        client = CommunicationClient(account, transport=lambda *a: {"questions": [], "has_next": True}, pause=lambda _: None)
        # An asserted next page without a cursor is a malformed response.
        with self.assertRaises(MarketError):
            client.inbox("question")
        with self.assertRaises(MarketError):
            normalize({"marketplace": "yandex_market", "account_id": "A"}, "question",
                {"questionIdentifiers": {"id": 1}, "businessId": 99, "text": "Q"}, business_id="7")

    def test_http_204_and_key_redaction_no_redirects_or_automatic_retries(self):
        account = {"marketplace": "wildberries", "account_id": "A"}
        response = io.BytesIO(b"")
        response.status, response.headers = 204, {}
        events=[]
        limiter=unittest.mock.Mock()
        limiter.acquire.return_value=("fixture",0)
        client = CommunicationClient(account, allow_write=True, limiter=limiter, audit=lambda kind,payload:events.append((kind,payload)))
        with patch("mra.market_api.runtime_value", return_value="synthetic-secret"), patch("urllib.request.build_opener") as opener:
            opener.return_value.open.return_value = response
            self.assertEqual(client.request("POST", "/api/v1/feedbacks/answer", {"id":"1","text":"Reply"}, write=True), {})
            self.assertEqual(opener.return_value.open.call_count, 1)
        self.assertNotIn("synthetic-secret", json.dumps(events))

    def test_timeouts_and_429_never_retry_or_expose_key(self):
        account = {"marketplace": "wildberries", "account_id": "A"}
        for error in (OSError("synthetic-secret"), urllib.error.HTTPError("https://example.invalid", 429, "fixture", {},
                       io.BytesIO(b'{"message":"synthetic-secret"}'))):
            events=[]
            limiter=unittest.mock.Mock()
            limiter.acquire.return_value=("fixture",0)
            client = CommunicationClient(account, allow_write=True, limiter=limiter, audit=lambda kind,payload:events.append((kind,payload)))
            with patch("mra.market_api.runtime_value", return_value="synthetic-secret"), patch("urllib.request.build_opener") as opener:
                opener.return_value.open.side_effect=error
                with self.assertRaises(MarketError) as result:
                    client.request("POST", "/api/v1/feedbacks/answer", {"id":"1","text":"Reply"}, write=True)
                self.assertNotIn("synthetic-secret", str(result.exception))
                self.assertEqual(opener.return_value.open.call_count, 1)
            self.assertNotIn("synthetic-secret", json.dumps(events))
            if isinstance(error,urllib.error.HTTPError):
                self.assertGreaterEqual(limiter.release.call_args.kwargs["cooldown"],60)

    def test_mutation_payload_cannot_delete_or_edit_using_an_allowed_endpoint(self):
        account = {"marketplace":"yandex_market","account_id":"A","business_id":7}
        transport = unittest.mock.Mock()
        client = CommunicationClient(account, transport=transport, allow_write=True)
        with self.assertRaises(MarketError):
            client.request("POST", "/v1/businesses/7/goods-questions/update", {"entityId":{"id":1,"type":"ANSWER"},"operationType":"DELETE"}, write=True)
        transport.assert_not_called()

    def test_access_probe_is_one_read_even_when_all_reviews_are_processed(self):
        account={"marketplace":"ozon","account_id":"A"}
        calls=[]
        def api(*args):
            calls.append(args)
            return {"reviews":[{"status":"PROCESSED"}],"last_id":"next","has_next":True}
        result=CommunicationClient(account,transport=api).probe("review")
        self.assertTrue(result["read_access"])
        self.assertEqual(len(calls),1)


if __name__ == "__main__":
    unittest.main()
