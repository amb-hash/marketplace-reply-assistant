"""Report local readiness separately from evidence of live marketplace acceptance."""

from __future__ import annotations

import os
from pathlib import Path

from .catalog import matrix
from .config import MODEL, PROVIDER, RUNTIME_ERROR, RUNTIME_KEY_FILE
from .knowledge import bundle_connection
from .rate_limits import AccountLimiter


def report(store, accounts):
    knowledge = bundle_connection(store)
    audit, canon = store.verify_audit(), store.verify_canon()
    sqlite_ok = store._db.execute("PRAGMA integrity_check").fetchone() == ("ok",)
    foreign_keys_ok = not store._db.execute("PRAGMA foreign_key_check").fetchall()
    catalog = {row["account_id"]: row for row in matrix(store)}
    access = {}
    for event in store.audit_trace(full=True):
        if event["event_type"] == "access.checked":
            payload = event["payload"]
            access[(payload.get("account_id"), payload.get("kind"))] = {
                "read_access": payload.get("read_access") is True, "checked_at": event["created_at"], "reason": payload.get("reason", "")}
    account_results = []
    for account in accounts:
        aid = account["account_id"]
        credentials = bool((account.get("api_key_env") and os.environ.get(account["api_key_env"])) or
                           (account.get("api_key_file") and Path(account["api_key_file"]).expanduser().is_file() and Path(account["api_key_file"]).expanduser().stat().st_size > 0))
        status = {"account_id": aid, "name": account["name"], "marketplace": account["marketplace"],
                  "credential_reference_available": credentials, "catalog": catalog.get(aid), "channels": {},
                  "rate_limits": AccountLimiter(store, account).snapshot()}
        for kind in ("question", "review"):
            deliveries = store._db.execute("""SELECT j.state,count(*) FROM delivery_jobs j JOIN messages m ON m.message_id=j.message_id
                WHERE m.marketplace=? AND m.account_id=? AND m.kind=? GROUP BY j.state""", (account["marketplace"], aid, kind)).fetchall()
            status["channels"][kind] = {"connector": "implemented", "read_access": access.get((aid, kind), {"read_access": None, "checked_at": None}),
                                       "delivery_states": dict(deliveries), "live_publication_verified": any(state == "published" and n for state,n in deliveries)}
        account_results.append(status)
    local_ok = bool(knowledge["connected"] and audit["ok"] and canon["ok"] and sqlite_ok and foreign_keys_ok)
    first = next((item for item in account_results if item["account_id"] == "acct_deppa_ozon"), None)
    reasons = []
    if RUNTIME_ERROR:
        reasons.append(RUNTIME_ERROR)
    if not local_ok:
        reasons.append("локальная целостность или комплект знаний не подтверждены")
    if not first or not first["credential_reference_available"]:
        reasons.append("нужен действующий ключ Ozon Deppa")
    if not first or not first["catalog"] or not first["catalog"]["fresh"]:
        reasons.append("нужно получить свежий каталог Ozon Deppa")
    if not first or first["channels"]["question"]["read_access"].get("read_access") is not True:
        reasons.append("нужно проверить доступ к вопросам Ozon Deppa и подписку")
    key_names = ("OPENROUTER_API_KEY", "MRA_OPENROUTER_KEY_FILE", "MRA_OPENROUTER_AUTH_FILE") if PROVIDER == "openrouter" else ("OPENAI_API_KEY", "MRA_OPENAI_KEY_FILE")
    if not RUNTIME_KEY_FILE and not any(os.environ.get(name) for name in key_names):
        reasons.append("в этом процессе не выбран ключ модели; используйте mra-test.command")
    return {"local_integrity_ok": local_ok, "code_supports_all_marketplaces": True, "accounts_configured": len(accounts),
            "accounts": account_results, "knowledge": knowledge, "audit": audit, "database_ok": sqlite_ok,
            "foreign_keys_ok": foreign_keys_ok, "model": {"provider": PROVIDER, "model": MODEL, "network_verified_now": False, "configuration_error": RUNTIME_ERROR},
            "first_live_test": {"account_id": "acct_deppa_ozon", "ready": not reasons, "remaining": reasons},
            "all_channels_live_publication_verified": bool(account_results) and all(channel["live_publication_verified"]
                for account in account_results for channel in account["channels"].values()),
            "network_calls": 0, "paid_calls": 0}
