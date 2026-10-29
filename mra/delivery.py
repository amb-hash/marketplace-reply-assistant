"""Persist a reviewed intent, perform at most one write, then reconcile by reading."""

from __future__ import annotations

import hashlib
import json
import uuid

from .audit import redact
from .catalog import now
from .importing import import_snapshot
from .market_api import CommunicationClient, MarketError, digest
from .pipeline import Pipeline
from .rate_limits import AccountLimiter, RateLimitWait


def get_job(store, job_id):
    cursor = store._db.execute("SELECT * FROM delivery_jobs WHERE job_id=?", (job_id,))
    row = cursor.fetchone()
    if not row:
        raise ValueError("задание отправки не найдено")
    result = dict(zip((column[0] for column in cursor.description), row))
    result["target"] = json.loads(result.pop("target_json"))
    return result


def jobs(store):
    return [get_job(store, row[0]) for row in store._db.execute("SELECT job_id FROM delivery_jobs ORDER BY created_at,job_id")]


def account_for(accounts, target):
    selected = [a for a in accounts if all(a.get(k) == target.get(k) for k in ("marketplace", "account_id"))]
    if len(selected) != 1:
        raise ValueError("кабинет назначения не настроен однозначно")
    return selected[0]


def _local_fingerprint(store, row, target):
    hints = store._db.execute("SELECT hints_json FROM message_inputs WHERE message_id=?", (row[0],)).fetchone()
    if not hints:
        raise ValueError("нет исходных идентификаторов товара")
    return digest({"target": target, "text": row[5], "pros": row[6] or "", "cons": row[7] or "",
                   "rating": row[8], "hints": json.loads(hints[0])})


def _current(store, intent):
    snapshot = Pipeline(store).review_snapshot(intent["message_id"], actor=intent["actor"])
    row, draft = snapshot["message"], snapshot["draft"]
    held = store._db.execute("SELECT state FROM outbox WHERE draft_id=?", (intent["draft_id"],)).fetchone()
    if row[11] != "approved" or not snapshot["can_approve"] or not draft or draft[0] != intent["draft_id"] or held != ("held",):
        raise ValueError("нет действующего одобрения этого ответа; откройте обращение заново")
    if snapshot["revision"] != intent["message_revision"] or draft[1] != intent["text"]:
        raise ValueError("обращение или черновик изменились")
    if _local_fingerprint(store, row, intent["target"]) != intent["input_fingerprint"]:
        raise ValueError("локальный вопрос отличается от полученного из кабинета")
    return row


def prepare(store, accounts, message_id, *, expected_draft_id, actor):
    if not actor.strip():
        raise ValueError("укажите оператора")
    with store.transaction():
        remote = store._db.execute("SELECT target_json,fingerprint FROM remote_inputs WHERE message_id=?", (message_id,)).fetchone()
        if not remote:
            raise ValueError("отправка доступна только для обращения, полученного напрямую из кабинета")
        row, draft = store.get_message(message_id), store.latest_draft(message_id)
        if not row or not draft or draft[0] != expected_draft_id:
            raise ValueError("черновик изменился; откройте его заново")
        target = json.loads(remote[0])
        if tuple(target.get(k) for k in ("marketplace", "account_id", "external_id", "kind")) != tuple(row[1:5]):
            raise ValueError("исходное назначение не совпадает с обращением")
        account = account_for(accounts, target)
        plan = CommunicationClient(account).write_plan(target, draft[1])
        intent = {"message_id": message_id, "draft_id": draft[0], "message_revision": store.message_revision(message_id),
                  "target": target, "input_fingerprint": remote[1], "text": draft[1], "actor": actor}
        _current(store, intent)
        previous = store._db.execute("SELECT job_id,write_started,state FROM delivery_jobs WHERE message_id=?", (message_id,)).fetchone()
        if previous and (previous[1] or previous[2] not in {"prepared", "blocked"}):
            raise ValueError("по этому обращению уже начата отправка; используйте проверку результата")
        job_id = previous[0] if previous else "send_" + uuid.uuid4().hex
        timestamp, text_sha = now(), hashlib.sha256(draft[1].encode()).hexdigest()
        store._db.execute("""INSERT INTO delivery_jobs(job_id,message_id,draft_id,message_revision,target_json,input_fingerprint,
            text,text_sha256,state,actor,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,'prepared',?,?,?)
            ON CONFLICT(message_id) DO UPDATE SET draft_id=excluded.draft_id,message_revision=excluded.message_revision,
            target_json=excluded.target_json,input_fingerprint=excluded.input_fingerprint,text=excluded.text,
            text_sha256=excluded.text_sha256,state='prepared',actor=excluded.actor,updated_at=excluded.updated_at,note=''""",
            (job_id, message_id, draft[0], intent["message_revision"], remote[0], remote[1], draft[1], text_sha, actor, timestamp, timestamp))
        store.audit_event(message_id, "delivery.prepared", {"job_id": job_id, "target": target, "text": draft[1],
            "text_sha256": text_sha, "request": plan, "message_revision": intent["message_revision"]}, run_id=job_id, actor=actor)
        return dict(get_job(store, job_id), request=plan, account_name=account["name"])


def _state(store, intent, state, note, *, remote_id=None, actor=None):
    with store.transaction():
        store._db.execute("UPDATE delivery_jobs SET state=?,note=?,updated_at=? WHERE job_id=?",
                          (state, redact(note), now(), intent["job_id"]))
        if remote_id is not None:
            store._db.execute("UPDATE delivery_jobs SET remote_id=? WHERE job_id=?", (str(remote_id), intent["job_id"]))
        store.audit_event(intent["message_id"], "delivery." + state, {"job_id": intent["job_id"], "state": state,
            "note": note, "remote_id": remote_id}, run_id=intent["job_id"], actor=actor or intent["actor"])


def _client(store, intent, accounts, *, transport=None, pause=None, allow_write=False):
    def audit(event, payload):
        store.audit_event(intent["message_id"], event, payload, run_id=intent["job_id"], actor=intent["actor"])
    options = {"pause": pause} if pause else {}
    return CommunicationClient(account_for(accounts, intent["target"]), transport=transport, audit=audit,
                               allow_write=allow_write, limiter=AccountLimiter(store, account_for(accounts, intent["target"])), **options)


def send(store, accounts, job_id, *, expected_text_sha256, actor, live=False, transport=None, pause=None):
    if not live or not actor.strip():
        raise ValueError("для отправки нужны --live и имя оператора")
    with store.transaction():
        intent = get_job(store, job_id)
        if intent["text_sha256"] != expected_text_sha256:
            raise ValueError("подтверждён другой текст ответа")
        if intent["state"] != "prepared" or intent["write_started"]:
            raise ValueError("отправка уже начата или заблокирована; сначала проверьте результат")
        _current(store, intent)
        store._db.execute("UPDATE delivery_jobs SET state='checking',updated_at=? WHERE job_id=?", (now(), job_id))
        store.audit_event(intent["message_id"], "delivery.send_authorized", {"job_id": job_id,
            "text_sha256": expected_text_sha256, "target": intent["target"]}, run_id=job_id, actor=actor)
    client = _client(store, intent, accounts, transport=transport, pause=pause, allow_write=True)
    try:
        fresh = client.get(intent["target"])
        if fresh["fingerprint"] != intent["input_fingerprint"]:
            with store.transaction():
                import_snapshot(store, {"schema_version": 1, "messages": [fresh["message"]]}, source_reference="delivery preflight")
                store._db.execute("UPDATE remote_inputs SET fingerprint=?,observed_at=? WHERE message_id=?",
                    (fresh["fingerprint"], now(), intent["message_id"]))
            raise ValueError("вопрос или товар в кабинете изменился; нужен новый просмотр")
        replies = client.replies(intent["target"])
        if any(reply["owner"] is not False and reply["status"] != "DELETED" for reply in replies):
            raise ValueError("в кабинете уже есть ответ продавца или ответ с неустановленным автором; повтор не отправлен")
        with store.transaction():
            _current(store, intent)
            changed = store._db.execute("""UPDATE delivery_jobs SET state='sending',write_started=1,updated_at=?
                WHERE job_id=? AND state='checking' AND write_started=0""", (now(), job_id))
            if changed.rowcount != 1:
                raise ValueError("задание отправки изменилось")
            store.audit_event(intent["message_id"], "delivery.write_committed", {"job_id": job_id,
                "text_sha256": intent["text_sha256"]}, run_id=job_id, actor=actor)
        response = client.send(intent["target"], intent["text"])
        if intent["target"]["marketplace"] == "ozon":
            remote_id = response.get("answer_id") or response.get("comment_id") or ""
        else:
            result = response.get("result") or {}
            remote_id = (result.get("entity") or {}).get("id") or result.get("id") or ""
        _state(store, intent, "accepted", "API принял запрос; публичная публикация ещё не подтверждена", remote_id=remote_id, actor=actor)
    except Exception as exc:
        current = get_job(store, job_id)
        definitive = isinstance(exc, MarketError) and exc.status in {400, 401, 403, 422, 429} and not exc.uncertain
        not_dispatched = isinstance(exc, RateLimitWait) or (isinstance(exc, MarketError) and not exc.uncertain and exc.status is None)
        if current["write_started"] and not_dispatched:
            store._db.execute("UPDATE delivery_jobs SET write_started=0 WHERE job_id=?", (job_id,))
        state = "blocked" if not_dispatched else "failed" if current["write_started"] and definitive else "uncertain" if current["write_started"] else "blocked"
        _state(store, intent, state, str(exc) if isinstance(exc, ValueError) else "ошибка отправки; требуется проверка результата", actor=actor)
        raise
    return reconcile(store, accounts, job_id, actor=actor, client=client)


def reconcile(store, accounts, job_id, *, actor="operator", transport=None, pause=None, client=None):
    intent = get_job(store, job_id)
    if not intent["write_started"]:
        raise ValueError("отправка ещё не запускалась; нечего подтверждать")
    client = client or _client(store, intent, accounts, transport=transport, pause=pause)
    try:
        replies = client.replies(intent["target"])
    except Exception:
        store.audit_event(intent["message_id"], "delivery.verification_failed", {"job_id": job_id,
            "state": intent["state"], "automatic_retry": False}, run_id=job_id, actor=actor)
        return get_job(store, job_id)
    matches = [reply for reply in replies if reply["text"] == intent["text"] and reply["owner"] is not False
               and (not intent["remote_id"] or reply["id"] == intent["remote_id"])]
    if len(matches) != 1:
        store.audit_event(intent["message_id"], "delivery.not_yet_visible", {"job_id": job_id, "matches": len(matches),
            "state": intent["state"], "automatic_retry": False}, run_id=job_id, actor=actor)
        return get_job(store, job_id)
    reply = matches[0]
    state = "published" if reply["status"] == "PUBLISHED" else "rejected" if reply["status"] in {"BANNED", "REJECTED", "DELETED"} else "observed"
    _state(store, intent, state, "Ответ прочитан из API; статус площадки: " + reply["status"], remote_id=reply["id"], actor=actor)
    if state == "published" and store.message_revision(intent["message_id"]) == intent["message_revision"]:
        with store.transaction():
            store.set_message_state(intent["message_id"], "done")
            store._db.execute("UPDATE outbox SET state='delivered',hold_reason='публикация подтверждена API' WHERE draft_id=?", (intent["draft_id"],))
    return get_job(store, job_id)


def recover(store, job_id, *, actor):
    with store.transaction():
        intent = get_job(store, job_id)
        if intent["state"] == "failed" and intent["write_started"]:
            failures = [e["payload"] for e in store.audit_trace(intent["message_id"], full=True)
                        if e["run_id"] == job_id and e["event_type"] == "market.request.failed" and e["payload"].get("write")]
            if not failures or failures[-1].get("http_status") not in {400, 401, 403, 422, 429}:
                raise ValueError("нет подтверждённого отказа API; повтор запрещён")
            store._db.execute("UPDATE delivery_jobs SET write_started=0 WHERE job_id=?", (job_id,))
            _state(store, intent, "blocked", "подтверждённый отказ API снят оператором; подготовьте и проверьте отправку заново", actor=actor)
            return get_job(store, job_id)
        if intent["state"] != "checking" or intent["write_started"]:
            raise ValueError("восстановить можно только прерванную проверку до начала записи; после записи используйте reconcile")
        _state(store, intent, "blocked", "проверка до отправки прервана; подготовьте задание заново", actor=actor)
        return get_job(store, job_id)
