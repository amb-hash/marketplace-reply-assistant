from __future__ import annotations

import contextlib
import hashlib
import io
import json
from pathlib import Path
import shutil
import socket
import tempfile
import unittest
from unittest.mock import patch

from mra import cli, generate
from mra.config import KNOWLEDGE_DIR
from mra.extract import normalize_article
from mra.importing import import_snapshot, message_id_for
from mra.knowledge import bundle_connection, import_bundle, parse_canon, read_bundle
from mra.pipeline import Pipeline
from mra.store import Store, open_store


class WorkingKnowledge(unittest.TestCase):
    def setUp(self):
        self.addCleanup(patch.stopall)
        patch("urllib.request.urlopen", side_effect=AssertionError("network forbidden")).start()
        patch.object(socket.socket, "connect", side_effect=AssertionError("network forbidden")).start()
        self.store = Store(Path(":memory:"))
        self.addCleanup(self.store.close)

    def test_real_bundle_has_rules_490_articles_and_1817_mappings(self):
        manifest, files, cards, mapping, inventory = read_bundle()
        self.assertEqual((inventory["rules_version"], len(cards), inventory["categories"], len(mapping["mappings"])), ("2.1", 490, 16, 1817))
        self.assertEqual(len({card.article for card in cards}), 490)
        for path, item in files.values():
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), item["sha256"])
        self.assertEqual(manifest["expected_articles"], 490)

    def test_parser_preserves_original_multiline_material(self):
        source = "# Категория\n\n## A1\n\n- **Название:** Товар.\n- Особенность:\n  Вторая строка.\n\n## A2\n\n- **Название:** Второй.\n"
        cards, categories = parse_canon(source)
        self.assertEqual(cards[0].text, "## A1\n\n- **Название:** Товар.\n- Особенность:\n  Вторая строка.")
        self.assertEqual(cards[0].material[0][1], cards[0].text)
        self.assertEqual(categories, {"Категория"})

    def test_duplicate_article_is_rejected(self):
        with self.assertRaises(ValueError):
            parse_canon("# Категория\n## A1\n- Факт\n## A1\n- Другой факт\n")

    def test_modified_source_is_rejected_without_modifying_database(self):
        with tempfile.TemporaryDirectory() as temporary:
            copied = Path(temporary) / "knowledge"
            shutil.copytree(KNOWLEDGE_DIR, copied)
            path = copied / "КАНОН ДЛЯ АГЕНТА.md"
            path.write_bytes(path.read_bytes() + b"\n")
            with self.assertRaises(ValueError):
                import_bundle(self.store, copied)
        self.assertEqual(self.store._db.execute("SELECT COUNT(*) FROM products").fetchone()[0], 0)

    def test_import_connects_all_sources_and_is_idempotent(self):
        import_bundle(self.store)
        first = self.store.get_meta("canon_version")
        import_bundle(self.store)
        result = bundle_connection(self.store)
        self.assertTrue(result["connected"])
        self.assertEqual((result["connected_articles"], result["connected_mapping_rows"]), (490, 1817))
        self.assertEqual(self.store.get_meta("canon_version"), first)
        self.assertEqual(self.store._db.execute("SELECT COUNT(*) FROM materials").fetchone()[0], 490)

    def test_original_article_material_is_used_without_rewriting(self):
        _manifest, _files, cards, _mapping, _inventory = read_bundle()
        expected = next(card for card in cards if card.article == "44167")
        import_bundle(self.store)
        self.assertEqual(self.store.material_for("44167"), expected.material)

    def test_real_ozon_sku_selects_only_its_article(self):
        import_bundle(self.store)
        _manifest, _files, cards, mapping, _inventory = read_bundle()
        known = {c.article for c in cards}
        row = next(r for r in mapping["mappings"] if normalize_article(r["article"]) in known)
        import_snapshot(self.store, {"schema_version": 1, "messages": [{"marketplace": "ozon", "account_id": row["account_id"],
            "external_id": "test-question", "kind": "question", "text": "Проверочный вопрос", "hints": {"sku": row["external_id"]}}]})
        mid = message_id_for("ozon", row["account_id"], "question", "test-question")
        self.assertEqual(self.store.get_message(mid)[9], normalize_article(row["article"]))

    def test_preview_uses_real_rules_and_selected_canon_without_network(self):
        import_bundle(self.store)
        before = self.store._db.total_changes
        result = Pipeline(self.store).preview("44167", "Что это за товар?")
        self.assertEqual(result["rules_version"], "2.1")
        self.assertEqual(result["name"], "Air Pro 2.")
        self.assertIn(generate.load_rules(), result["payload"]["instructions"])
        self.assertIn(self.store.material_for("44167")[0][1], result["payload"]["input"])
        self.assertNotIn(self.store.material_for("11113")[0][1], result["payload"]["input"])
        self.assertEqual(result["network_calls"], 0)
        self.assertEqual(self.store._db.total_changes, before)

    def test_preview_and_live_transport_use_the_same_payload(self):
        import_bundle(self.store)
        preview = Pipeline(self.store).preview("44167", "Что это за товар?")
        with patch("mra.generate.DISABLE_LLM", False), patch("mra.generate.load_openai_key", return_value="synthetic-key"), \
                patch("mra.generate._post_responses", return_value={"status": "completed", "output": []}) as transport:
            generate.generate_reply("Что это за товар?", "44167", self.store.material_for("44167"), allow_llm=True)
        self.assertEqual(transport.call_args.args[1], preview["payload"])

    def test_unknown_article_does_not_fabricate_material(self):
        import_bundle(self.store)
        with self.assertRaises(ValueError):
            Pipeline(self.store).preview("NOT-IN-CANON", "Что это за товар?")

    def test_cli_init_builds_a_working_database_and_doctor_checks_connection(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "working.sqlite3"
            args = ["--db", str(path)]
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(cli.main(args + ["init"]), 0)
                self.assertEqual(cli.main(args + ["doctor"]), 0)
                self.assertEqual(cli.main(args + ["preview", "--article", "44167", "--question", "Что это за товар?"]), 0)
            with contextlib.closing(open_store(path)) as store:
                self.assertTrue(bundle_connection(store)["connected"])
                self.assertEqual(store.messages_by_state("new"), [])

    def test_canon_change_disconnects_the_working_bundle(self):
        import_bundle(self.store)
        self.store.replace_material("44167", [("test", "Изменённый материал в тестовой памяти")])
        self.assertFalse(bundle_connection(self.store)["connected"])
        self.assertFalse(self.store.verify_canon()["ok"])


if __name__ == "__main__":
    unittest.main()
