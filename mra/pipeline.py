"""Claim an input revision, generate once, and atomically record a draft or operator handoff."""

from __future__ import annotations

import hashlib
import json
import uuid
from functools import wraps
from dataclasses import dataclass, replace
from typing import Optional, Tuple

from .generate import GenerationResult, build_payload, generate_reply, rules_digest, rules_version
from . import generate as generation
from .store import Store
from .audit import redact
from .catalog import require_message_product


@dataclass(frozen=True)
class Outcome:
    message_id: str
    article: str
    state: str
    draft_id: Optional[str] = None
    text: Optional[str] = None
    source: str = ""
    reason: str = ""
    basis: Tuple[Tuple[str, str], ...] = ()
    log_id: Optional[int] = None


def review_action(action):
    def decorate(function):
        @wraps(function)
        def execute(self, message_id, *args, **kwargs):
            run_id = "review_" + uuid.uuid4().hex
            actor = kwargs.get("actor", "operator")
            self._store.audit_event(message_id, "review.requested", {"action": action, "args": args, "options": kwargs},
                                    run_id=run_id, actor=actor)
            try:
                delivery = self._store._db.execute("SELECT write_started FROM delivery_jobs WHERE message_id=?", (message_id,)).fetchone()
                if delivery and delivery[0]:
                    raise ValueError("отправка уже началась; сначала проверьте результат, повторное одобрение или правка не создают новую отправку")
                return function(self, message_id, *args, **kwargs)
            except Exception as exc:
                self._store.audit_event(message_id, "review.failed", {"action": action, "error_type": type(exc).__name__,
                    "reason": str(exc)}, run_id=run_id, actor=actor)
                raise
        return execute
    return decorate


class Pipeline:
    def __init__(self, store: Store, generator=None) -> None:
        self._store = store
        self._generator = generator

    def _context(self, article: str, message=None):
        if message:
            require_message_product(self._store, message)
        material = self._store.material_for(article) if article else []
        if not material:
            raise ValueError("нет артикула или материала товара")
        integrity = self._store.verify_canon()
        if not integrity["ok"]:
            raise ValueError("материалы не подтверждены или изменены после импорта")
        rules = generation.load_rules()
        return (material, integrity["canon_version"], rules_digest(rules), self._store.material_digest_for(article), rules_version(rules), rules)

    def preview(self, article: str, question: str, *, kind: str = "question", rating=None):
        material, canon_version, rules_sha, material_sha, rules_ver, rules = self._context(article)
        payload = build_payload(question, article, material, kind=kind, rating=rating, rules=rules)
        return {"article": article, "name": self._store.product_name(article), "rules_version": rules_ver,
                "rules_sha256": rules_sha, "canon_version": canon_version, "material_sha256": material_sha,
                "basis_topics": [topic for topic, _statement in material], "network_calls": 0, "payload": payload}

    def process(self, message_id: str, *, retry: bool = False, allow_llm: bool = False,
                operator_feedback: str = "", expected_draft_id=None) -> Outcome:
        run_id = "generation_" + uuid.uuid4().hex
        def audit(event, payload):
            self._store.audit_event(message_id, event, payload, run_id=run_id)
        audit("processing.requested", {"retry": retry, "allow_llm": allow_llm, "operator_feedback":operator_feedback, "previous_draft_id":expected_draft_id})
        try:
            row, token, revision = self._store.claim_message(message_id, retry, **({"expected_draft_id":expected_draft_id} if expected_draft_id else {}))
        except Exception as exc:
            audit("processing.refused", {"error_type": type(exc).__name__, "reason": str(exc)})
            raise
        (_id, _mp, _account, _external, kind, text, pros, cons, rating, article, _name, _state) = row
        full_text = text or ""
        for label, part in (("Плюсы", pros), ("Минусы", cons)):
            if part:
                full_text += f"\n{label}: {part}"
        context = None
        try:
            audit("processing.started", {"message_revision": revision, "previous_state": row[11]})
            try:
                with self._store.transaction():
                    self._store.assert_claim(message_id, token, revision)
                    context = self._context(article, row)
                    audit("sources.selected", {"article": article, "canon_version": context[1], "rules_version": context[4],
                        "rules_sha256": context[2], "material_sha256": context[3], "rules": context[5], "material": context[0]})
            except ValueError as exc:
                audit("sources.unavailable", {"article": article, "reason": str(exc)})
                result = GenerationResult(note=str(exc))
            else:
                options = {}
                if operator_feedback:
                    previous = self._store._db.execute("SELECT text FROM drafts WHERE draft_id=? AND message_id=?", (expected_draft_id,message_id)).fetchone()
                    options = {"operator_feedback":operator_feedback,"previous_answer":previous[0] if previous else ""}
                result = (self._generator or generate_reply)(full_text, article, context[0], kind=kind, rating=rating,
                    allow_llm=allow_llm, rules=context[5], audit=audit, **options)
            with self._store.transaction():
                self._store.assert_claim(message_id, token, revision)
                if context is not None:
                    try:
                        if self._context(article, row)[1:] != context[1:]:
                            result = replace(result, decision="operator", text="", basis=(), source="operator",
                                             note="во время обработки изменились правила или материалы")
                    except ValueError:
                        result = replace(result, decision="operator", text="", basis=(), source="operator",
                                         note="во время обработки изменились правила или материалы")
                if result.decision != "answer" or not result.text or not result.basis:
                    result = GenerationResult(model=result.model, note=result.note or "требуется проверка оператора",
                                              input_tokens=result.input_tokens, output_tokens=result.output_tokens,
                                              usage_reported=result.usage_reported)
                basis = json.dumps([{"topic": t, "statement": s} for t, s in result.basis], ensure_ascii=False)
                is_answer = result.decision == "answer"
                state = "drafted" if is_answer else "needs_data"
                draft_id = None
                if is_answer:
                    draft_id = "dr_" + hashlib.sha256(f"{message_id}|{revision}|{token}|{result.text}".encode()).hexdigest()[:24]
                    self._store.create_draft(draft_id, message_id, result.text, result.source, context[2], context[3])
                self._store.finish_claim(message_id, token, revision, state, result.note)
                audit("processing.completed", {"message_revision": revision, "state": state, "draft_id": draft_id,
                    "decision": result.decision, "answer": result.text, "basis": result.basis, "reason": result.note,
                    "source": result.source, "model": result.model,
                    "usage": {"input_tokens": result.input_tokens, "output_tokens": result.output_tokens} if result.usage_reported else None})
                log_id = self._store.add_runlog(
                    message_id, article, kind, full_text, result.text if is_answer else "(оператору: ответ не выдаётся)", basis,
                    context[1] if context else self._store.get_meta("canon_version", "unset"),
                    context[4] if context else "unavailable", model=result.model, source=result.source,
                    input_tokens=result.input_tokens, output_tokens=result.output_tokens,
                )
                return Outcome(message_id, article, state, draft_id, result.text or None, result.source, result.note, result.basis, log_id)
        except BaseException as exc:
            # Leave a recoverable state after a storage failure; never call the model here again.
            with self._store.transaction():
                try:
                    self._store.finish_claim(message_id, token, revision, "needs_data", "обработка прервана; требуется retry")
                except ValueError:
                    pass  # A newer revision or an explicit recovery owns the message now.
            audit("processing.failed", {"error_type": type(exc).__name__, "state": self._store.get_message(message_id)[11]})
            raise

    def regenerate(self, message_id, feedback, *, expected_draft_id, actor="operator"):
        if not isinstance(feedback,str) or not feedback.strip() or len(feedback) > 8000:
            raise ValueError("напишите замечание оператора: от 1 до 8000 символов")
        if not isinstance(actor,str) or not actor.strip() or not expected_draft_id:
            raise ValueError("нужны оператор и проверенный черновик")
        feedback = redact(feedback.strip())
        self._store.audit_event(message_id,"review.feedback",{"draft_id":expected_draft_id,"feedback":feedback},actor=actor)
        outcome = self.process(message_id,retry=True,allow_llm=True,operator_feedback=feedback,expected_draft_id=expected_draft_id)
        with self._store.transaction():
            self._store.record_review(message_id,"regenerated",actor,draft_id=expected_draft_id,
                replacement_draft_id=outcome.draft_id,note=feedback)
        return outcome

    @review_action("approve")
    def approve(self, message_id: str, *, expected_draft_id: Optional[str] = None, actor: str = "operator") -> Optional[str]:
        invalid = ""
        with self._store.transaction():
            row = self._store.get_message(message_id)
            if not row:
                raise ValueError("нет сообщения")
            if row[11] not in {"drafted", "approved"}:
                raise ValueError("подтверждать можно только актуальный черновик")
            draft = self._store.latest_draft(message_id)
            if not draft:
                return None
            if expected_draft_id is not None and draft[0] != expected_draft_id:
                raise ValueError("черновик изменился; откройте его заново")
            try:
                context = self._context(row[9], row)
                if self._store.draft_context(draft[0]) != (context[2], context[3]):
                    invalid = "после генерации изменились правила или материалы; нужен retry"
            except ValueError:
                invalid = "правила или материалы недоступны либо изменены; нужен retry"
            if invalid:
                self._store.set_message_state(message_id, "needs_data")
                self._store._db.execute("UPDATE messages SET last_error=? WHERE message_id=?", (invalid, message_id))
                self._store.supersede_outbox(message_id, invalid)
            else:
                self._store.approve(draft[0])
                self._store.set_message_state(message_id, "approved")
                self._store.mark_processed(row[3], row[1], row[2], row[4], self._store.message_revision(message_id))
                if row[11] != "approved":
                    self._store.record_review(message_id, "approved", actor, draft_id=draft[0])
        if invalid:
            raise ValueError(invalid)
        return draft[0]

    def review_snapshot(self, message_id: str, *, actor: str = "operator"):
        with self._store.transaction():
            row = self._store.get_message(message_id)
            if not row:
                raise ValueError("нет сообщения")
            draft = self._store.latest_draft(message_id)
            material = self._store.material_for(row[9]) if row[9] else []
            try:
                context = self._context(row[9], row)
                valid = bool(draft and self._store.draft_context(draft[0]) == (context[2], context[3]))
                source_error = "" if valid or not draft else "источники изменились после создания черновика"
            except ValueError as exc:
                source_error, valid = str(exc), False
            self._store.audit_event(message_id, "review.viewed", {"draft_id": draft[0] if draft else None,
                "message_revision": self._store.message_revision(message_id), "state": row[11], "sources_current": valid}, actor=actor)
            return {"message": row, "draft": draft, "material": material, "source_error": source_error,
                    "note": self._store.message_note(message_id),
                    "revision": self._store.message_revision(message_id),
                    "can_approve": valid and row[11] in {"drafted", "approved"}, "events": self._store.review_history(message_id)}

    @review_action("revise")
    def revise(self, message_id: str, text: str, *, expected_draft_id: str, actor: str = "operator") -> str:
        if not isinstance(text, str) or not text.strip():
            raise ValueError("ответ не может быть пустым")
        text = redact(text)
        with self._store.transaction():
            row = self._store.get_message(message_id)
            draft = self._store.latest_draft(message_id)
            if not row or row[11] not in {"drafted", "approved"} or not draft or draft[0] != expected_draft_id:
                raise ValueError("черновик изменился или недоступен для правки; откройте его заново")
            context = self._context(row[9], row)
            if self._store.draft_context(draft[0]) != (context[2], context[3]):
                raise ValueError("источники изменились; сначала получите актуальный черновик")
            replacement = "dr_" + uuid.uuid4().hex
            self._store.create_draft(replacement, message_id, text.strip(), "human", context[2], context[3])
            self._store.supersede_outbox(message_id, "правка оператора; нужно новое подтверждение")
            self._store.set_message_state(message_id, "drafted")
            self._store._db.execute("UPDATE messages SET last_error='' WHERE message_id=?", (message_id,))
            self._store.add_runlog(message_id, row[9], row[4], row[5], text.strip(),
                json.dumps([{"topic": t, "statement": s} for t, s in context[0]], ensure_ascii=False),
                context[1], context[4], model="", source="human")
            self._store.record_review(message_id, "edited", actor, draft_id=draft[0], replacement_draft_id=replacement)
            self._store.audit_event(message_id, "draft.revised", {"previous_draft_id": draft[0], "draft_id": replacement,
                "answer": text.strip(), "rules_sha256": context[2], "material_sha256": context[3], "state": "drafted"}, actor=actor)
            return replacement

    @review_action("reject")
    def reject(self, message_id: str, reason: str, *, expected_draft_id: Optional[str] = None,
               expected_revision: Optional[int] = None, actor: str = "operator") -> None:
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("укажите причину отклонения")
        reason = redact(reason)
        with self._store.transaction():
            row = self._store.get_message(message_id)
            draft = self._store.latest_draft(message_id)
            if not row or row[11] not in {"new", "drafted", "approved", "needs_data", "rejected"}:
                raise ValueError("это обращение сейчас нельзя отклонить")
            if expected_draft_id is not None and (draft[0] if draft else "") != expected_draft_id:
                raise ValueError("черновик изменился; откройте его заново")
            if expected_revision is not None and self._store.message_revision(message_id) != expected_revision:
                raise ValueError("обращение изменилось; откройте его заново")
            self._store.supersede_outbox(message_id, "отклонено оператором")
            self._store.set_message_state(message_id, "rejected")
            self._store._db.execute("UPDATE messages SET last_error=? WHERE message_id=?", (reason.strip(), message_id))
            self._store.record_review(message_id, "rejected", actor, draft_id=draft[0] if draft else None, note=reason.strip())
