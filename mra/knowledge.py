"""Index the owner's unmodified Markdown canon and bundled account-scoped product mapping."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re

from .config import KNOWLEDGE_DIR
from .extract import normalize_article
from .importing import import_snapshot


@dataclass(frozen=True)
class Card:
    article: str
    category: str
    name: str
    text: str
    start_line: int
    end_line: int

    @property
    def material(self):
        return [(f"canon:{self.article}:L{self.start_line}-{self.end_line}", self.text)]


def parse_canon(text: str):
    lines = text.splitlines(keepends=True)
    cards, seen, categories = [], set(), set()
    category, article, start = "", "", 0

    def finish(end):
        if not article:
            return
        section = "".join(lines[start:end]).strip()
        if not category or not section or len(section.splitlines()) < 2:
            raise ValueError(f"пустой или неполный раздел канона: {article}")
        match = re.search(r"^- \*\*(?:Название|Модель):\*\*\s*(.+)$", section, re.MULTILINE)
        cards.append(Card(article, category, match.group(1).strip() if match else "", section, start + 1, end))

    for index, line in enumerate(lines):
        if line.startswith("# "):
            finish(index)
            article = ""
            category = line[2:].strip()
        elif line.startswith("## "):
            finish(index)
            article = line[3:].strip()
            if not article or article in seen or any(c.isspace() for c in article):
                raise ValueError("пустой, повторный или некорректный артикул в каноне")
            seen.add(article)
            categories.add(category)
            start = index
    finish(len(lines))
    if not cards:
        raise ValueError("канон не содержит карточек артикулов")
    return cards, categories


def read_bundle(directory: Path = KNOWLEDGE_DIR):
    manifest = json.loads((directory / "SOURCES.json").read_text(encoding="utf-8"))
    if manifest.get("schema_version") != 1:
        raise ValueError("неизвестный формат SOURCES.json")
    files = {}
    for item in manifest["files"]:
        relative = Path(item["path"])
        if relative.parent != Path("knowledge") or relative.name in {"", ".", ".."}:
            raise ValueError("недопустимый путь источника знаний")
        path = directory / relative.name
        if path.is_symlink() or hashlib.sha256(path.read_bytes()).hexdigest() != item["sha256"]:
            raise ValueError(f"источник изменён относительно манифеста: {relative.name}")
        if item["kind"] in files:
            raise ValueError("повтор типа источника знаний")
        files[item["kind"]] = (path, item)
    if set(files) != {"rules", "canon", "mapping"}:
        raise ValueError("неполный комплект: нужны rules, canon, mapping")
    rules = files["rules"][0].read_text(encoding="utf-8")
    version = re.search(r"^версия:\s*([^\s|]+)", rules, re.MULTILINE)
    if not version or version.group(1) != manifest["rules_version"]:
        raise ValueError("версия правил не совпадает с SOURCES.json")
    cards, categories = parse_canon(files["canon"][0].read_text(encoding="utf-8"))
    mapping = json.loads(files["mapping"][0].read_text(encoding="utf-8"))
    rows = mapping["mappings"]
    if len(cards) != manifest["expected_articles"] or len(categories) != manifest["expected_categories"] or len(rows) != manifest["expected_mappings"]:
        raise ValueError("число артикулов, категорий или связей не совпадает с SOURCES.json")
    article_set = {card.article for card in cards}
    covered = sum(normalize_article(row["article"]) in article_set for row in rows)
    inventory = {"rules_version": version.group(1), "articles": len(cards), "categories": len(categories),
                 "mapping_rows": len(rows), "mapping_rows_with_canon": covered,
                 "mapping_rows_without_canon": len(rows) - covered, "accounts": mapping["accounts"],
                 "files": [{"path": item["path"], "sha256": item["sha256"]} for _path, item in files.values()]}
    return manifest, files, cards, mapping, inventory


def import_bundle(store, directory: Path = KNOWLEDGE_DIR, *, replace_materials=False):
    _manifest, files, cards, mapping, inventory = read_bundle(directory)
    canon_sha = files["canon"][1]["sha256"]
    snapshot = {"schema_version": 1,
                "provenance": {"approved_by": "MRA deterministic importer of the project-owned source",
                               "source": "knowledge/КАНОН ДЛЯ АГЕНТА.md; sha256=" + canon_sha},
                "products": [{"article": c.article, "category": c.category, "name": c.name, "material": c.material} for c in cards],
                "mappings": mapping["mappings"], "messages": []}
    with store.transaction():
        counts = import_snapshot(store, snapshot, replace_materials=replace_materials)
        store.set_meta("bundle_canon_sha256", canon_sha)
        store.set_meta("bundle_mapping_sha256", files["mapping"][1]["sha256"])
    return {**inventory, "imported": counts, "canon_integrity_ok": store.verify_canon()["ok"]}


def bundle_connection(store, directory: Path = KNOWLEDGE_DIR):
    _manifest, files, cards, mapping, inventory = read_bundle(directory)
    connected_articles = sum(store.material_for(c.article) == c.material for c in cards)
    connected_mapping, overridden = 0, 0
    for row in mapping["mappings"]:
        if store.lookup_article(row["marketplace"], row["external_id"], row["account_id"]) == normalize_article(row["article"]):
            connected_mapping += 1
        elif store._db.execute("""SELECT 1 FROM catalog_aliases WHERE marketplace=? AND account_id=?
            AND id_type='sku' AND external_id=?""", (row["marketplace"], row["account_id"], row["external_id"])).fetchone():
            overridden += 1
    return {**inventory, "connected_articles": connected_articles, "connected_mapping_rows": connected_mapping,
            "mapping_rows_updated_by_catalog": overridden,
            "connected": connected_articles == len(cards) and connected_mapping + overridden == len(mapping["mappings"])
            and store.get_meta("bundle_canon_sha256", "") == files["canon"][1]["sha256"]}
