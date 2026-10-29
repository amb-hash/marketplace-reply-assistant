"""Import native incoming messages, retaining their exact account and reply target."""

from __future__ import annotations

import json
import uuid

from .catalog import now, register_accounts
from .importing import import_snapshot, message_id_for
from .market_api import CommunicationClient, MarketError
from .rate_limits import AccountLimiter


def sync_inbox(store, account, *, kind="question", limit=20, max_requests=30, transport=None, pause=None):
    run_id = "inbox_" + uuid.uuid4().hex
    register_accounts(store, [account])
    def audit(event, payload):
        store.audit_event(None, event, payload, run_id=run_id)
    options = {"pause": pause} if pause else {}
    client = CommunicationClient(account, transport=transport, audit=audit, max_requests=max_requests,
                                 limiter=AccountLimiter(store, account), **options)
    audit("inbox.sync.started", {"account_id": account["account_id"], "marketplace": account["marketplace"],
        "kind": kind, "limit": limit, "max_requests": max_requests})
    try:
        entries = client.inbox(kind, limit=limit)
        ids, seen = [], {}
        for entry in entries:
            item = entry["message"]
            mid = message_id_for(*(item[k] for k in ("marketplace", "account_id", "kind", "external_id")))
            if mid in seen and seen[mid] != entry["fingerprint"]:
                raise MarketError("получены противоречивые версии одного обращения")
            seen[mid] = entry["fingerprint"]
        with store.transaction():
            for entry in entries:
                item = entry["message"]
                mid = message_id_for(*(item[k] for k in ("marketplace", "account_id", "kind", "external_id")))
                import_snapshot(store, {"schema_version": 1, "messages": [item]}, source_reference="direct marketplace API")
                store._db.execute("""INSERT INTO remote_inputs VALUES(?,?,?,?) ON CONFLICT(message_id)
                    DO UPDATE SET target_json=excluded.target_json,fingerprint=excluded.fingerprint,observed_at=excluded.observed_at""",
                    (mid, json.dumps(entry["target"], ensure_ascii=False, sort_keys=True), entry["fingerprint"], now()))
                store.audit_event(mid, "inbox.target.recorded", entry, run_id=run_id)
                ids.append(mid)
        result = {"account_id": account["account_id"], "kind": kind, "messages": list(dict.fromkeys(ids)),
                  "received": len(entries), "http_requests": client.calls}
        audit("inbox.sync.completed", result)
        return result
    except Exception as exc:
        audit("inbox.sync.failed", {"error_type": type(exc).__name__, "reason": str(exc) if isinstance(exc, MarketError) else "ошибка импорта"})
        raise
