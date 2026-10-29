"""Import approved local snapshots and the legacy collector's read-only SQLite interface."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from contextlib import closing
from pathlib import Path

from .extract import extract_article, normalize_article
from .store import SCHEMA_VERSION, Store, open_store
from .catalog import remember_input, resolve


def _text(value, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label}: требуется непустая строка")
    return value.strip()


def message_id_for(marketplace: str, account_id: str, kind: str, external_id: str) -> str:
    scope = json.dumps([marketplace, account_id, kind, external_id], ensure_ascii=False)
    return "m_" + hashlib.sha256(scope.encode()).hexdigest()[:32]


def import_snapshot(store: Store, snapshot: dict, *, replace_materials: bool = False,
                    source_reference: str = "local JSON snapshot") -> dict:
    run_id = "import_" + uuid.uuid4().hex
    store.audit_event(None, "import.received", {"source": source_reference, "snapshot": snapshot}, run_id=run_id)
    if isinstance(snapshot, dict) and isinstance(snapshot.get("messages"), list):
        for message in snapshot["messages"]:
            if not isinstance(message, dict):
                continue
            mid = message.get("message_id")
            if not mid and all(isinstance(message.get(k), str) for k in ("marketplace", "account_id", "kind", "external_id")):
                mid = message_id_for(*(message[k] for k in ("marketplace", "account_id", "kind", "external_id")))
            store.audit_event(mid, "input.received", {"source": source_reference, "message": message}, run_id=run_id)
    try:
        result = _import_snapshot(store, snapshot, replace_materials=replace_materials, audit_run_id=run_id)
    except Exception as exc:
        store.audit_event(None, "import.failed", {"source": source_reference, "error_type": type(exc).__name__}, run_id=run_id)
        raise
    store.audit_event(None, "import.completed", result, run_id=run_id)
    return result


def _import_snapshot(store: Store, snapshot: dict, *, replace_materials: bool = False, audit_run_id: str = "") -> dict:
    if not isinstance(snapshot, dict) or snapshot.get("schema_version") != 1:
        raise ValueError("ожидается объект snapshot с schema_version=1")
    for name in ("products", "mappings", "messages"):
        if not isinstance(snapshot.get(name, []), list):
            raise ValueError(f"{name}: ожидается массив")
    counts = {"products": 0, "mappings": 0, "messages": 0}
    with store.transaction():
        material_changed = False
        for product in snapshot.get("products", []):
            if not isinstance(product, dict):
                raise ValueError("products: ожидается объект")
            article = normalize_article(_text(product.get("article"), "article"))
            rows = product.get("material")
            if not isinstance(rows, list) or not rows:
                raise ValueError("material: нужны утверждённые факты")
            material = []
            for row in rows:
                if not isinstance(row, (tuple, list)) or len(row) != 2:
                    raise ValueError("material: строка должна содержать topic и statement")
                material.append((_text(row[0], "topic"), _text(row[1], "statement")))
            old = store.material_for(article)
            if old != sorted(material, key=lambda r: r[0]):
                if old and not replace_materials:
                    raise ValueError("материал уже существует; изменение требует --replace-materials")
                store.replace_material(article, material)
                material_changed = True
            store.upsert_product(article, str(product.get("name") or ""), str(product.get("category") or ""))
            counts["products"] += 1
        if material_changed:
            provenance = snapshot.get("provenance") or {}
            if not isinstance(provenance, dict):
                raise ValueError("provenance: ожидается объект")
            store.sign_canon(_text(provenance.get("approved_by"), "provenance.approved_by"),
                             _text(provenance.get("source"), "provenance.source"))
        for mapping in snapshot.get("mappings", []):
            if not isinstance(mapping, dict):
                raise ValueError("mappings: ожидается объект")
            marketplace = _text(mapping.get("marketplace"), "marketplace")
            if marketplace not in {"wildberries", "ozon", "yandex_market"}:
                raise ValueError("неизвестная площадка")
            store.map_product(marketplace, _text(mapping.get("external_id"), "external_id"),
                              normalize_article(_text(mapping.get("article"), "article")),
                              _text(mapping.get("account_id"), "account_id"))
            counts["mappings"] += 1
        for message in snapshot.get("messages", []):
            if not isinstance(message, dict):
                raise ValueError("messages: ожидается объект")
            mp, account, kind, external = (_text(message.get(k), k) for k in ("marketplace", "account_id", "kind", "external_id"))
            hints = message.get("hints") or {}
            if not isinstance(hints, dict):
                raise ValueError("hints: ожидается объект")
            explicit = normalize_article(message.get("article", ""))
            article = resolve(store, mp, account, hints, explicit)
            for field in ("text", "pros", "cons", "product_name"):
                if not isinstance(message.get(field, ""), str):
                    raise ValueError(f"{field}: ожидается строка")
            mid = store.upsert_message(str(message.get("message_id") or message_id_for(mp, account, kind, external)),
                mp, account, external, kind, message.get("text", ""), pros=message.get("pros", ""),
                cons=message.get("cons", ""), rating=message.get("rating"), article=article,
                product_name=message.get("product_name", ""), audit_run_id=audit_run_id, received_payload=message)
            remember_input(store, mid, hints, explicit)
            store.audit_event(mid, "product.resolved", {"marketplace": mp, "account_id": account, "hints": hints,
                "explicit_article": explicit, "article": article, "status": "resolved" if article else "mapping_missing_or_conflicting"}, run_id=audit_run_id)
            counts["messages"] += 1
    return counts


def read_snapshot(path: Path) -> dict:
    if path.stat().st_size > 16_000_000:
        raise ValueError("snapshot больше 16 МБ; разделите его на несколько файлов")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeError, json.JSONDecodeError):
        raise ValueError("файл snapshot не является корректным UTF-8 JSON") from None


def readonly_database(path: Path):
    return sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)


def article_from_hints(source, message_id: str, marketplace: str, store: Store, account_id: str) -> str:
    rows = source.execute("""SELECT h.identifier_type, h.identifier_value FROM inbound_messages m
        JOIN inbound_product_hints h ON h.revision_id=m.current_revision_id WHERE m.message_id=?""", (message_id,)).fetchall()
    return extract_article(marketplace, dict(rows), lambda value: store.lookup_article(marketplace, value, account_id))


def import_legacy_messages(store: Store, path: Path, *, limit: int = 100, marketplace: str = "", kind: str = "") -> dict:
    run_id = "import_" + uuid.uuid4().hex
    store.audit_event(None, "import.requested", {"source": str(path), "format": "collector SQLite", "limit": limit,
        "marketplace_filter": marketplace, "kind_filter": kind}, run_id=run_id)
    try:
        return _import_legacy_messages(store, path, limit=limit, marketplace=marketplace, kind=kind, run_id=run_id)
    except Exception as exc:
        store.audit_event(None, "import.failed", {"source": str(path), "error_type": type(exc).__name__}, run_id=run_id)
        raise


def _import_legacy_messages(store, path, *, limit, marketplace, kind, run_id):
    if limit <= 0:
        raise ValueError("limit должен быть положительным")
    with closing(readonly_database(path)) as source:
        rows = source.execute("""SELECT m.message_id, m.marketplace, m.account_id, m.external_id, m.kind,
            r.text, r.pros, r.cons, r.rating FROM inbound_messages m
            JOIN inbound_revisions r ON r.revision_id=m.current_revision_id
            WHERE m.workflow_state IN ('new','needs_data','needs_product_mapping')
              AND (?='' OR m.marketplace=?) AND (?='' OR m.kind=?)
            ORDER BY m.created_at, m.message_id LIMIT ?""", (marketplace, marketplace, kind, kind, limit)).fetchall()
        store.audit_event(None, "import.received", {"source": str(path), "format": "collector SQLite",
            "columns": ["message_id", "marketplace", "account_id", "external_id", "kind", "text", "pros", "cons", "rating"],
            "rows": rows, "limit": limit, "marketplace_filter": marketplace, "kind_filter": kind}, run_id=run_id)
        hints_by_message = {}
        for row in rows:
            hints = source.execute("""SELECT h.identifier_type, h.identifier_value FROM inbound_messages m
                JOIN inbound_product_hints h ON h.revision_id=m.current_revision_id WHERE m.message_id=?""", (row[0],)).fetchall()
            hints_by_message[row[0]] = dict(hints)
            store.audit_event(row[0], "input.received", {"source": str(path), "row": row, "hints": hints}, run_id=run_id)
        with store.transaction():
            with_article = 0
            for mid, mp, account, external, message_kind, text, pros, cons, rating in rows:
                article = resolve(store, mp, account, hints_by_message[mid])
                actual_mid = store.upsert_message(mid, mp, account, external, message_kind, text or "", pros=pros or "",
                                     cons=cons or "", rating=rating, article=article, audit_run_id=run_id)
                remember_input(store, actual_mid, hints_by_message[mid])
                with_article += bool(article)
        store.audit_event(None, "import.completed", {"messages": len(rows), "with_article": with_article}, run_id=run_id)
    return {"messages": len(rows), "with_article": with_article}


def copy_database(source_path: Path, target_path: Path, *, migrate: bool = False) -> dict:
    if source_path.resolve() == target_path.resolve():
        raise ValueError("источник и назначение должны различаться")
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with closing(readonly_database(source_path)) as source:
        if source.execute("PRAGMA integrity_check").fetchone() != ("ok",):
            raise ValueError("проверка целостности исходной БД не пройдена")
        with target_path.open("xb"):
            target_path.chmod(0o600)
        with closing(sqlite3.connect(str(target_path))) as target:
            source.backup(target)
            if target.execute("PRAGMA integrity_check").fetchone() != ("ok",):
                raise ValueError("проверка целостности копии не пройдена")
    if migrate:
        with closing(open_store(target_path)) as migrated:
            if migrated._db.execute("PRAGMA foreign_key_check").fetchall():
                raise ValueError("нарушены связи в перенесённой БД")
            return {"target": str(target_path), "schema": SCHEMA_VERSION, "integrity": "ok", "canon": migrated.verify_canon()["ok"]}
    return {"target": str(target_path), "integrity": "ok"}
