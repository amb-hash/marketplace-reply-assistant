"""Account-scoped assortment snapshots and deterministic coverage against the owner's canon."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import hashlib
import uuid

from .catalog_api import CatalogClient, CatalogError, access_issue, listing
from .extract import extract_article, normalize_article
from .rate_limits import AccountLimiter


def now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _timestamp(value):
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError
        return parsed.astimezone(timezone.utc)
    except (ValueError, AttributeError, TypeError):
        raise CatalogError("observed_at должен содержать дату, время и часовой пояс") from None


def register_accounts(store, accounts):
    with store.transaction():
        for account in accounts:
            store._db.execute("""INSERT INTO catalog_accounts VALUES(?,?,?) ON CONFLICT(marketplace,account_id)
                DO UPDATE SET name=excluded.name""", (account["marketplace"], account["account_id"], account["name"]))


def lookup(store, marketplace, account_id, kind, external):
    row = store._db.execute("""SELECT article FROM catalog_aliases
        WHERE marketplace=? AND account_id=? AND id_type=? AND external_id=?""",
        (marketplace, account_id, kind, str(external))).fetchone()
    return row[0] if row else ""


def resolve(store, marketplace, account_id, hints, explicit_article=""):
    direct = normalize_article(explicit_article) or extract_article(marketplace, hints)
    native = {"wildberries": ("nmID",), "ozon": ("sku", "product_id"), "yandex_market": ("marketSku",)}
    matches = {lookup(store, marketplace, account_id, kind, hints[kind])
               for kind in native.get(marketplace, ()) if hints.get(kind)} - {""}
    if len(matches) > 1 or (matches and direct and direct not in matches):
        return ""  # A conflicting article is handed to the operator, never guessed.
    ambiguous = any(store._db.execute("""SELECT 1 FROM catalog_aliases WHERE marketplace=? AND account_id=?
        AND id_type=? AND external_id=? AND article=''""", (marketplace, account_id, kind, str(hints[kind]))).fetchone()
        for kind in native.get(marketplace, ()) if hints.get(kind))
    if ambiguous and not direct:
        return ""
    return direct or next(iter(matches), "") or extract_article(marketplace, hints,
        lambda value: store.lookup_article(marketplace, value, account_id))


def remember_input(store, message_id, hints, explicit_article=""):
    store._db.execute("""INSERT INTO message_inputs VALUES(?,?,?) ON CONFLICT(message_id)
        DO UPDATE SET hints_json=excluded.hints_json, explicit_article=excluded.explicit_article""",
        (message_id, json.dumps(hints, ensure_ascii=False), explicit_article))


def reconcile_messages(store, marketplace, account_id):
    rows = store._db.execute("""SELECT m.message_id,i.hints_json,i.explicit_article FROM messages m
        JOIN message_inputs i ON i.message_id=m.message_id WHERE m.marketplace=? AND m.account_id=? AND m.state<>'done'""",
        (marketplace, account_id)).fetchall()
    changed = []
    for mid, hints, explicit in rows:
        row = store.get_message(mid)
        article = resolve(store, marketplace, account_id, json.loads(hints), explicit)
        if article == (row[9] or ""):
            continue
        store.upsert_message(mid, marketplace, account_id, row[3], row[4], row[5],
            pros=row[6] or "", cons=row[7] or "", rating=row[8], article=article, product_name=row[10] or "",
            received_payload={"source": "catalog reconciliation", "message_id": mid, "previous_article": row[9], "article": article})
        if not article:
            store.set_message_state(mid, "needs_data")
            store._db.execute("UPDATE messages SET last_error=? WHERE message_id=?", ("конфликт или отсутствие связи товара в каталоге", mid))
        store.audit_event(mid, "catalog.message_reconciled", {"previous_article": row[9], "article": article,
            "revision": store.message_revision(mid)})
        changed.append(mid)
    return changed


def apply_snapshot(store, account, rows, *, observed_at, complete, run_id=None):
    if complete is not True:
        raise CatalogError("неполный снимок не заменяет матрицу")
    observed = _timestamp(observed_at)
    if observed > datetime.now(timezone.utc):
        raise CatalogError("снимок датирован будущим")
    mp, aid = account["marketplace"], account["account_id"]
    unique, aliases = {}, {}
    if not isinstance(rows, list):
        raise CatalogError("listings: ожидается массив")
    for item in rows:
        if not isinstance(item, dict) or item.get("status", "listed") not in {"listed", "archived"}:
            raise CatalogError("неверная карточка каталога")
        row = listing(item.get("listing_id"), item.get("seller_article") or item.get("article"), item.get("title"),
                      aliases=item.get("aliases") or [], status=item.get("status", "listed"))
        if row["listing_id"] in unique and unique[row["listing_id"]] != row:
            raise CatalogError("противоречивые карточки с одним listing_id")
        unique[row["listing_id"]] = row
        for kind, external in row["aliases"]:
            aliases.setdefault((kind, external), set()).add((row["listing_id"], row["article"]))
    ambiguities = [{"id_type": kind, "external_id": external, "candidates": sorted(targets)}
                   for (kind, external), targets in aliases.items() if len({article for _lid, article in targets}) > 1]
    run_id = run_id or "catalog_" + uuid.uuid4().hex
    with store.transaction():
        last = store._db.execute("""SELECT max(observed_at) FROM catalog_runs
            WHERE marketplace=? AND account_id=? AND status='completed'""", (mp, aid)).fetchone()[0]
        if last and observed <= _timestamp(last):
            raise CatalogError("снимок старше или равен уже загруженному")
        register_accounts(store, [account])
        old = {r[0]: r[1:] for r in store._db.execute("""SELECT listing_id,article,status FROM catalog_listings
            WHERE marketplace=? AND account_id=?""", (mp, aid))}
        store._db.execute("""INSERT OR IGNORE INTO catalog_runs(run_id,marketplace,account_id,started_at,status)
            VALUES(?,?,?,?,'running')""", (run_id, mp, aid, now()))
        for key, row in unique.items():
            store._db.execute("""INSERT INTO catalog_listings VALUES(?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(marketplace,account_id,listing_id) DO UPDATE SET seller_article=excluded.seller_article,
                article=excluded.article,title=excluded.title,status=excluded.status,last_seen=excluded.last_seen,run_id=excluded.run_id""",
                (mp, aid, key, row["seller_article"], row["article"], row["title"], row["status"], observed_at, observed_at, run_id))
            # Replace aliases only for observed listings. Missing listings keep their historical identity.
            store._db.execute("DELETE FROM catalog_aliases WHERE marketplace=? AND account_id=? AND listing_id=?", (mp, aid, key))
        for (kind, external), targets in aliases.items():
            key, article = sorted(targets)[0] if len({article for _lid, article in targets}) == 1 else ("", "")
            store._db.execute("""INSERT INTO catalog_aliases VALUES(?,?,?,?,?,?)
                ON CONFLICT(marketplace,account_id,id_type,external_id) DO UPDATE SET listing_id=excluded.listing_id,article=excluded.article""",
                (mp, aid, kind, external, key, article))
            if (mp == "ozon" and kind == "sku") or (mp == "wildberries" and kind == "nmID"):
                store.map_product(mp, external, article, aid)
        missing = [key for key in old if key not in unique and old[key][1] != "missing"]
        for key in missing:
            store._db.execute("UPDATE catalog_listings SET status='missing' WHERE marketplace=? AND account_id=? AND listing_id=?", (mp, aid, key))
        changed_messages = reconcile_messages(store, mp, aid)
        report = {"account_id": aid, "marketplace": mp, "listings": len(unique),
                  "added": sorted(set(unique) - set(old)), "missing": missing,
                  "article_changed": [key for key, r in unique.items() if key in old and old[key][0] != r["article"]],
                  "returned": [key for key in unique if key in old and old[key][1] == "missing"],
                  "ambiguous_identifiers": ambiguities,
                  "messages_reconciled": changed_messages, "observed_at": observed_at, "complete": True}
        store._db.execute("UPDATE catalog_runs SET status='completed',observed_at=?,item_count=? WHERE run_id=?", (observed_at, len(unique), run_id))
        store.audit_event(None, "catalog.snapshot.applied", {"report": report, "listings": list(unique.values())}, run_id=run_id)
        return report


def sync_account(store, account, *, max_pages=100, transport=None, pause=None):
    run_id = "catalog_" + uuid.uuid4().hex
    register_accounts(store, [account])
    store._db.execute("INSERT INTO catalog_runs(run_id,marketplace,account_id,started_at,status) VALUES(?,?,?,?,'running')",
                      (run_id, account["marketplace"], account["account_id"], now()))
    def audit(event, payload):
        store.audit_event(None, event, payload, run_id=run_id)
    audit("catalog.sync.started", {"account_id": account["account_id"], "marketplace": account["marketplace"], "max_requests": max_pages})
    try:
        options = {"pause": pause} if pause else {}
        client = CatalogClient(account, max_pages=max_pages, transport=transport, audit=audit, limiter=AccountLimiter(store, account), **options)
        rows = client.fetch()
        audit("catalog.fetch.completed", {"listings": len(rows), "http_requests": client.calls})
        return apply_snapshot(store, account, rows, observed_at=now(), complete=True, run_id=run_id)
    except Exception as exc:
        reason = str(exc) if isinstance(exc, CatalogError) else type(exc).__name__
        store._db.execute("UPDATE catalog_runs SET status='failed',error=? WHERE run_id=?", (reason, run_id))
        audit("catalog.sync.failed", {"error": reason, "error_type": type(exc).__name__})
        raise


def matrix(store, *, account_id=None, gaps_only=False, full=False):
    result = []
    for mp, aid, name in store._db.execute("SELECT marketplace,account_id,name FROM catalog_accounts ORDER BY marketplace,account_id"):
        if account_id and aid != account_id:
            continue
        latest = store._db.execute("""SELECT status,error,started_at,run_id FROM catalog_runs WHERE marketplace=? AND account_id=?
            ORDER BY rowid DESC LIMIT 1""", (mp, aid)).fetchone()
        issue = ""
        if latest and latest[0] == "failed":
            failure = store._db.execute("""SELECT d.content_json FROM audit_events e JOIN audit_documents d ON d.sha256=e.document_sha256
                WHERE e.run_id=? AND e.event_type='catalog.request.failed' ORDER BY e.event_id DESC LIMIT 1""", (latest[3],)).fetchone()
            issue = access_issue(json.loads(failure[0]).get("response")) if failure else ""
        observed = store._db.execute("""SELECT max(observed_at) FROM catalog_runs WHERE marketplace=? AND account_id=? AND status='completed'""", (mp, aid)).fetchone()[0]
        age = round((datetime.now(timezone.utc) - _timestamp(observed)).total_seconds() / 3600, 2) if observed else None
        rows = []
        for lid, article, title, status in store._db.execute("""SELECT listing_id,article,title,status FROM catalog_listings
            WHERE marketplace=? AND account_id=? ORDER BY article,listing_id""", (mp, aid)):
            covered = bool(store.material_for(article))
            rows.append({"listing_id": lid, "article": article, "title": title, "listing_state": status, "has_canon": covered})
        item = {"marketplace": mp, "account_id": aid, "name": name, "observed_at": observed or None, "age_hours": age,
            "fresh": age is not None and age <= 24, "last_sync_status": latest[0] if latest else "never",
            "last_error": latest[1] if latest else "", "access_issue": issue,
            "listed": sum(r["listing_state"] == "listed" for r in rows) if observed else None,
            "missing": sum(r["listing_state"] == "missing" for r in rows) if observed else None,
            "without_canon": sum(not r["has_canon"] and r["listing_state"] == "listed" for r in rows) if observed else None,
            "ambiguous_identifiers": store._db.execute("SELECT count(*) FROM catalog_aliases WHERE marketplace=? AND account_id=? AND article=''", (mp, aid)).fetchone()[0] if observed else None}
        if full or gaps_only:
            item["rows"] = [r for r in rows if not gaps_only or (not r["has_canon"] and r["listing_state"] == "listed")]
        result.append(item)
    return result


def require_current_catalog(store, marketplace, account_id):
    configured = store._db.execute("SELECT 1 FROM catalog_accounts WHERE marketplace=? AND account_id=?", (marketplace, account_id)).fetchone()
    if not configured:
        return  # Local examples or legacy callers may have no configured cabinet.
    observed = store._db.execute("""SELECT max(observed_at) FROM catalog_runs
        WHERE marketplace=? AND account_id=? AND status='completed'""", (marketplace, account_id)).fetchone()[0]
    if not observed or (datetime.now(timezone.utc) - _timestamp(observed)).total_seconds() > 86400:
        raise CatalogError("каталог кабинета не загружен или старше 24 часов; выполните catalog-sync")



def require_message_product(store, message):
    """A known SKU in a fresh native question can be checked without a full catalog."""
    try:
        require_current_catalog(store, message[1], message[2])
        return
    except CatalogError as original:
        if message[1] != "ozon":
            raise
        remote = store._db.execute("SELECT target_json,fingerprint,observed_at FROM remote_inputs WHERE message_id=?", (message[0],)).fetchone()
        inputs = store._db.execute("SELECT hints_json,explicit_article FROM message_inputs WHERE message_id=?", (message[0],)).fetchone()
        if not remote or not inputs or inputs[1]:
            raise original
        age = (datetime.now(timezone.utc) - _timestamp(remote[2])).total_seconds()
        target, hints = json.loads(remote[0]), json.loads(inputs[0])
        if not 0 <= age <= 86400 or tuple(target.get(k) for k in ("marketplace","account_id","external_id","kind")) != tuple(message[1:5]):
            raise original
        sku = str(hints.get("sku") or "")
        if not sku or sku != str(target.get("sku") or "") or not message[9]:
            raise original
        mapped = store.lookup_article("ozon", sku, message[2])
        if mapped != message[9] or resolve(store, "ozon", message[2], hints) != message[9]:
            raise CatalogError("SKU обращения не подтверждён картой именно этого кабинета")
        payload = {"target":target,"text":message[5],"pros":message[6] or "","cons":message[7] or "","rating":message[8],"hints":hints}
        fingerprint = hashlib.sha256(json.dumps(payload,ensure_ascii=False,sort_keys=True,separators=(",",":")).encode()).hexdigest()
        if fingerprint != remote[1]:
            raise CatalogError("обращение изменилось после получения из API")


def refresh_for_message(store, message_id, accounts, *, already_checked=None):
    row = store.get_message(message_id)
    if not row:
        raise CatalogError("нет сообщения")
    account = next((a for a in accounts if a["marketplace"] == row[1] and a["account_id"] == row[2]), None)
    if not account:
        return
    scope = (row[1], row[2])
    if already_checked is not None and scope in already_checked:
        return
    register_accounts(store, [account])
    if already_checked is not None:
        already_checked.add(scope)
    try:
        require_message_product(store, row)
        fresh = True
    except CatalogError:
        fresh = False
    if not fresh or not row[9]:
        sync_account(store, account)
