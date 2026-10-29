from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from mra.catalog import apply_snapshot, lookup, matrix, refresh_for_message, register_accounts, require_current_catalog, sync_account
from mra.catalog_api import CatalogClient, CatalogError, access_issue, listing, load_accounts
from mra.generate import GenerationResult
from mra.importing import import_snapshot
from mra.pipeline import Pipeline
from mra.store import Store

ROOT = Path(__file__).resolve().parents[1]


class CatalogWorkflow(unittest.TestCase):
    def setUp(self):
        self.store = Store(Path(":memory:"))
        self.addCleanup(self.store.close)
        self.accounts = load_accounts(ROOT / "examples/accounts.example.json")
        register_accounts(self.store, self.accounts)
        self.account = next(a for a in self.accounts if a["marketplace"] == "ozon")
        self.time = datetime.now(timezone.utc) - timedelta(minutes=10)
        self.store.upsert_product("A")
        self.store.replace_material("A", [("fact", "Known fact")])
        self.store.sign_canon("fixture", "fixture")

    def apply(self, rows, account=None, **kwargs):
        self.time += timedelta(seconds=1)
        return apply_snapshot(self.store, account or self.account, rows, observed_at=self.time.isoformat(), complete=True, **kwargs)

    def row(self, lid="p1", article="A", sku="123"):
        return listing(lid, article, "Test product", aliases=[("sku", sku)])

    def test_all_six_cabinets_are_separate_even_with_same_sku(self):
        second = next(a for a in self.accounts if a["account_id"] == "acct_deppa_ozon_msk")
        self.apply([self.row(article="A")])
        self.apply([self.row(article="OTHER")], second)
        self.assertEqual(lookup(self.store, "ozon", self.account["account_id"], "sku", "123"), "A")
        self.assertEqual(lookup(self.store, "ozon", second["account_id"], "sku", "123"), "OTHER")
        report = matrix(self.store)
        self.assertEqual(len(report), 6)
        self.assertEqual(sum(r["last_sync_status"] == "never" for r in report), 4)
        self.assertEqual(next(r for r in report if r["account_id"] == second["account_id"])["without_canon"], 1)

    def test_new_product_without_canon_is_visible_and_never_calls_model(self):
        self.apply([self.row(article="NEW")])
        import_snapshot(self.store, {"schema_version": 1, "messages": [{"message_id": "m1", "marketplace": "ozon",
            "account_id": self.account["account_id"], "external_id": "q1", "kind": "question", "text": "Question", "hints": {"sku": "123"}}]})
        called = []
        outcome = Pipeline(self.store, generator=lambda *a, **k: called.append(True)).process("m1", allow_llm=True)
        self.assertEqual(outcome.state, "needs_data")
        self.assertEqual(called, [])
        self.assertEqual(matrix(self.store, account_id=self.account["account_id"], gaps_only=True)[0]["rows"][0]["article"], "NEW")

    def test_add_missing_return_and_stale_snapshot(self):
        self.assertEqual(self.apply([self.row()])["added"], ["p1"])
        self.assertEqual(self.apply([])["missing"], ["p1"])
        self.assertEqual(matrix(self.store, account_id=self.account["account_id"])[0]["missing"], 1)
        self.assertEqual(self.apply([self.row()])["returned"], ["p1"])
        with self.assertRaises(CatalogError):
            apply_snapshot(self.store, self.account, [], observed_at=self.time.isoformat(), complete=True)
        self.assertEqual(matrix(self.store, account_id=self.account["account_id"])[0]["listed"], 1)

    def test_incomplete_empty_does_not_remove_current_assortment(self):
        self.apply([self.row()])
        with self.assertRaises(CatalogError):
            apply_snapshot(self.store, self.account, [], observed_at=self.time.isoformat(), complete=False)
        self.assertEqual(matrix(self.store, account_id=self.account["account_id"])[0]["listed"], 1)

    def test_conflicting_duplicate_rejects_whole_snapshot(self):
        with self.assertRaises(CatalogError):
            self.apply([self.row(), self.row(article="B")])
        self.assertEqual(self.store._db.execute("SELECT count(*) FROM catalog_listings").fetchone()[0], 0)

    def test_ambiguous_alias_is_retained_without_selecting_arbitrary_article(self):
        report = self.apply([self.row(), self.row(lid="p2", article="B")])
        self.assertEqual(len(report["ambiguous_identifiers"]), 1)
        self.assertEqual(lookup(self.store, "ozon", self.account["account_id"], "sku", "123"), "")
        self.assertEqual(matrix(self.store, account_id=self.account["account_id"])[0]["listed"], 2)

    def test_changed_mapping_revokes_hold_and_changes_message_revision(self):
        self.apply([self.row()])
        snapshot = {"schema_version": 1, "messages": [{"message_id": "m1", "marketplace": "ozon",
            "account_id": self.account["account_id"], "external_id": "q1", "kind": "question", "text": "Question", "hints": {"sku": "123"}}]}
        import_snapshot(self.store, snapshot)
        reply = GenerationResult(text="Answer", decision="answer", source="fixture", basis=(("fact", "Known fact"),))
        pipeline = Pipeline(self.store, generator=lambda *a, **kw: reply)
        with patch("mra.generate.load_rules", return_value="Fixture rules"):
            old = pipeline.process("m1").draft_id
            pipeline.approve("m1", expected_draft_id=old)
        report = self.apply([self.row(article="NEW")])
        self.assertEqual(report["messages_reconciled"], ["m1"])
        self.assertEqual(self.store.get_message("m1")[9:12:2], ("NEW", "new"))
        self.assertEqual(self.store.message_revision("m1"), 2)
        self.assertEqual(self.store._db.execute("SELECT state FROM outbox").fetchall(), [("superseded",)])

    def test_conflicting_direct_article_is_operator_case(self):
        self.apply([self.row()])
        import_snapshot(self.store, {"schema_version": 1, "messages": [{"message_id": "m1", "marketplace": "ozon",
            "account_id": self.account["account_id"], "external_id": "q1", "kind": "question", "text": "Question",
            "hints": {"sku": "123", "offer_id": "WRONG"}}]})
        self.assertEqual(self.store.get_message("m1")[9], "")

    def test_failure_mid_sync_preserves_last_success_and_logs_failure(self):
        self.apply([self.row()])
        def transport(*args):
            raise CatalogError("fixture timeout")
        with self.assertRaises(CatalogError):
            sync_account(self.store, self.account, transport=transport, pause=lambda _: None)
        report = matrix(self.store, account_id=self.account["account_id"])[0]
        self.assertEqual((report["listed"], report["last_sync_status"]), (1, "failed"))
        self.assertEqual(self.store.audit_trace()[-1]["event_type"], "catalog.sync.failed")

    def test_stale_or_missing_catalog_blocks_model_and_approval(self):
        self.store.upsert_message("m1", "ozon", self.account["account_id"], "q1", "question", "Question", article="A")
        called = []
        self.assertEqual(Pipeline(self.store, generator=lambda *a, **kw: called.append(True)).process("m1").state, "needs_data")
        self.assertEqual(called, [])
        with self.assertRaises(CatalogError):
            require_current_catalog(self.store, "ozon", self.account["account_id"])
        self.apply([self.row()])
        require_current_catalog(self.store, "ozon", self.account["account_id"])

    def test_unknown_sku_refreshes_once_per_account_in_a_batch(self):
        self.apply([self.row()])
        for mid in ("m1", "m2"):
            self.store.upsert_message(mid, "ozon", self.account["account_id"], mid, "question", "Question")
        checked = set()
        with patch("mra.catalog.sync_account") as sync:
            for mid in ("m1", "m2"):
                refresh_for_message(self.store, mid, self.accounts, already_checked=checked)
        self.assertEqual(sync.call_count, 1)


class DirectCatalogClients(unittest.TestCase):
    def test_access_diagnosis_distinguishes_revoked_keys_from_generic_403(self):
        self.assertEqual(access_issue({"detail": "Access token withdrawn."}), "revoked")
        self.assertEqual(access_issue({"errors": [{"message": "Api-Key token is revoked"}]}), "revoked")
        self.assertEqual(access_issue({"message": "Api-key is deactivated, use another one"}), "deactivated")
        self.assertEqual(access_issue({"message": "Generic forbidden"}), "")
    def client(self, marketplace, responses, **extra):
        iterator, calls = iter(responses), []
        def transport(*args):
            calls.append(args)
            return next(iterator)
        client = CatalogClient({"marketplace": marketplace, "account_id": "fixture", **extra}, transport=transport, pause=lambda _: None)
        return client, calls

    def test_wb_follows_cursor_and_returns_vendor_article(self):
        cards = [{"nmID": i, "vendorCode": "A", "title": "Test"} for i in range(1, 101)]
        client, calls = self.client("wildberries", [{"cards": cards, "cursor": {"total": 100, "nmID": 100, "updatedAt": "timestamp"}},
            {"cards": [{"nmID": 101, "vendorCode": "B"}], "cursor": {"total": 1}}])
        self.assertEqual(len(client.fetch()), 101)
        self.assertEqual(calls[1][2]["settings"]["cursor"]["nmID"], 100)
        self.assertEqual(calls[0][2]["settings"]["filter"], {"withPhoto": -1})

    def test_wb_repeated_cursor_does_not_claim_complete(self):
        page = {"cards": [{"nmID": 1, "vendorCode": "A"}], "cursor": {"total": 100, "nmID": 1, "updatedAt": "same"}}
        client, _ = self.client("wildberries", [page, page])
        with self.assertRaises(CatalogError):
            client.fetch()

    def test_ozon_joins_product_id_with_all_source_skus_and_excludes_prices(self):
        client, calls = self.client("ozon", [{"result": {"items": [{"product_id": 17, "offer_id": "A"}], "total": 1}},
            {"items": [{"id": 17, "offer_id": "A", "name": "Test", "price": "DO_NOT_STORE", "sources": [{"sku": 123}, {"sku": 456}]}]}])
        rows = client.fetch()
        self.assertEqual(rows[0]["aliases"], [("product_id", "17"), ("sku", "123"), ("sku", "456")])
        self.assertNotIn("DO_NOT_STORE", json.dumps(rows))
        self.assertEqual(calls[1][2], {"product_id": ["17"]})

    def test_ozon_missing_details_is_not_a_complete_catalog(self):
        client, _ = self.client("ozon", [{"result": {"items": [{"product_id": 17, "offer_id": "A"}], "total": 1}}, {"items": []}])
        with self.assertRaises(CatalogError):
            client.fetch()

    def test_yandex_discovers_one_business_and_follows_page_token(self):
        client, calls = self.client("yandex_market", [{"campaigns": [{"business": {"id": 101}}]},
            {"result": {"offerMappings": [{"offer": {"offerId": "A", "name": "One", "purchasePrice": "DO_NOT_STORE"}, "mapping": {"marketSku": 12}}], "paging": {"nextPageToken": "next"}}},
            {"result": {"offerMappings": []}}])
        rows = client.fetch()
        self.assertEqual(rows[0]["article"], "A")
        self.assertEqual(calls[1][1], "/v2/businesses/101/offer-mappings")
        self.assertEqual(calls[2][3]["pageToken"], "next")
        self.assertNotIn("DO_NOT_STORE", json.dumps(rows))

    def test_yandex_never_selects_first_of_multiple_businesses(self):
        client, calls = self.client("yandex_market", [{"campaigns": [{"business": {"id": 101}}, {"business": {"id": 202}}]}])
        with self.assertRaises(CatalogError):
            client.fetch()
        self.assertEqual(len(calls), 1)

    def test_writing_route_and_request_overflow_are_blocked_before_transport(self):
        client, calls = self.client("ozon", [])
        with self.assertRaises(CatalogError):
            client.request("POST", "/v3/product/import", {})
        client.max_pages = 0
        with self.assertRaises(CatalogError):
            client.request("POST", "/v3/product/list", {})
        self.assertEqual(calls, [])

    def test_template_has_six_accounts_and_rejects_inline_secrets(self):
        self.assertEqual(len(load_accounts(ROOT / "examples/accounts.example.json")), 6)
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "bad.json"
            path.write_text(json.dumps({"schema_version": 1, "accounts": [{"marketplace": "ozon", "account_id": "a", "name": "Test", "api_key": "fake"}]}))
            with self.assertRaises(CatalogError):
                load_accounts(path)


if __name__ == "__main__":
    unittest.main()
