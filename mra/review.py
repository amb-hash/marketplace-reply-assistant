"""Terminal review is a thin human interface over the same workflow used by the service."""

from __future__ import annotations

import json
from pathlib import Path
import uuid

from .generate import GenerationResult
from . import generate as generation
from .importing import import_snapshot
from .knowledge import import_bundle
from .pipeline import Pipeline
from .store import open_store

_STATES = {"new": "новое", "processing": "обрабатывается", "drafted": "черновик", "approved": "подтверждено, HOLD",
           "needs_data": "нужен оператор", "rejected": "отклонено", "done": "завершено"}


def _read_text(read, write):
    write("Вставьте полный исправленный ответ. В конце отдельная строка /готово; /отмена отменяет правку.")
    lines = []
    while True:
        line = read("")
        if line.strip().lower() == "/отмена":
            return None
        if line.strip().lower() == "/готово":
            return "\n".join(lines)
        lines.append(line)


def inspect_message(store, message_id, *, actor="operator", read=None, write=None):
    read, write = read or input, write or print
    pipeline = Pipeline(store)
    try:
        while True:
            snapshot = pipeline.review_snapshot(message_id, actor=actor)
            row, draft = snapshot["message"], snapshot["draft"]
            from .config import ACCOUNTS_FILE
            from .catalog_api import load_accounts
            account = next((a for a in load_accounts(ACCOUNTS_FILE) if a["account_id"] == row[2]), None)
            name = account["name"] if account else row[2]
            write(f"\n{row[1]} · {name} · артикул {row[9]}\n")
            write("Вопрос покупателя:\n" + row[5])
            write("\nОтвет модели:\n" + (draft[1] if draft else "Ответ пока не подготовлен."))
            write("\nРежим: просмотр и отклонение. Отправка отключена.")
            choice = read("\nОТКЛОНИТЬ — отклонить ответ; Enter — закончить просмотр: ").strip()
            if not choice:
                store.audit_event(message_id, "review.closed", {"inspection_only": True}, actor=actor)
                return 0
            if choice.casefold() != "отклонить":
                write("Введите ОТКЛОНИТЬ или нажмите Enter.")
                continue
            reason = read("Причина отклонения: ").strip()
            try:
                pipeline.reject(message_id, reason, expected_draft_id=draft[0] if draft else "",
                    expected_revision=snapshot["revision"], actor=actor)
                write("\nОтвет отклонён. Решение и причина сохранены. На площадку ничего не отправлено.")
                return 0
            except ValueError as exc:
                write("Решение не сохранено: " + str(exc))
    except (EOFError, KeyboardInterrupt):
        store.audit_event(message_id, "review.closed", {"inspection_only": True, "input_interrupted": True}, actor=actor)
        return 0


def review_message(store, message_id: str, *, actor: str = "operator", read=None, write=None, inspection_only=False) -> int:
    if inspection_only:
        return inspect_message(store, message_id, actor=actor, read=read, write=write)
    read = read or input
    write = write or print
    pipeline = Pipeline(store)
    try:
        while True:
            snapshot = pipeline.review_snapshot(message_id, actor=actor)
            row, draft = snapshot["message"], snapshot["draft"]
            write(f"\nОбращение: {message_id} | {_STATES.get(row[11], row[11])} | версия {snapshot['revision']}")
            write(f"Площадка: {row[1]} | кабинет: {row[2]} | артикул: {row[9] or 'не определён'}")
            write(f"Вопрос/отзыв: {row[5]}")
            for label, value in (("Плюсы", row[6]), ("Минусы", row[7]), ("Оценка", row[8])):
                if value:
                    write(f"{label}: {value}")
            if draft:
                write(f"\nЧерновик {draft[0]} | источник: {draft[2]}\n{draft[1]}")
            else:
                write("\nЧерновик отсутствует.")
            if snapshot["note"]:
                write("Причина/заметка: " + snapshot["note"])
            if snapshot["source_error"]:
                write("Источники: " + snapshot["source_error"])
            if snapshot["can_approve"]:
                write("1 — подтвердить в HOLD; 2 — править ответ")
            write("3 — отклонить; 4 — факты товара; 5 — правила; 6 — история решений; 0 — выйти")
            choice = read("Действие: ").strip()
            expected = draft[0] if draft else ""
            try:
                if choice == "0":
                    store.audit_event(message_id, "review.closed", {"draft_id": expected}, actor=actor)
                    return 0
                if choice == "1" and snapshot["can_approve"]:
                    confirmed = pipeline.approve(message_id, expected_draft_id=expected, actor=actor)
                    write(f"Подтверждено: {confirmed}. Ответ сохранён в HOLD. Отправка выполняется отдельным подтверждением через меню 8.")
                    return 0
                if choice == "2" and snapshot["can_approve"]:
                    text = _read_text(read, write)
                    if text is None:
                        store.audit_event(message_id, "review.edit_cancelled", {"draft_id": expected}, actor=actor)
                    else:
                        pipeline.revise(message_id, text, expected_draft_id=expected, actor=actor)
                        write("Правка сохранена новым черновиком. Проверьте текст и подтвердите отдельно.")
                elif choice == "3":
                    reason = read("Причина отклонения: ").strip()
                    pipeline.reject(message_id, reason, expected_draft_id=expected,
                                    expected_revision=snapshot["revision"], actor=actor)
                    write("Ответ отклонён. Причина сохранена; повторная генерация выполняется отдельной командой retry --live.")
                    return 0
                elif choice == "4":
                    store.audit_event(message_id, "review.material_viewed", {"material": snapshot["material"]}, actor=actor)
                    for topic, statement in snapshot["material"]:
                        write(f"\n[{topic}]\n{statement}")
                elif choice == "5":
                    rules = generation.load_rules()
                    store.audit_event(message_id, "review.rules_viewed", {"rules": rules}, actor=actor)
                    write(rules)
                elif choice == "6":
                    write(json.dumps(snapshot["events"], ensure_ascii=False, indent=2))
                else:
                    write("Выберите доступное действие из списка.")
            except ValueError as exc:
                write("Действие не выполнено: " + str(exc))
    except (EOFError, KeyboardInterrupt):
        store.audit_event(message_id, "review.closed", {"reason": "input_interrupted"}, actor=actor)
        write("\nПросмотр завершён.")
        return 0



def work_message(store, message_id, *, actor="operator", read=None, write=None):
    """Review one real draft and explicitly publish or reject it in the same view."""
    from .config import ACCOUNTS_FILE
    from .catalog_api import load_accounts
    from . import delivery
    read, write = read or input, write or print
    pipeline = Pipeline(store)
    try:
        while True:
            accounts = load_accounts(ACCOUNTS_FILE)
            snapshot = pipeline.review_snapshot(message_id, actor=actor)
            row, draft = snapshot["message"], snapshot["draft"]
            account = next((a for a in accounts if a["account_id"] == row[2]), None)
            if not account or not draft:
                raise ValueError("нет кабинета или подготовленного ответа")
            write(f"\n{row[1]} · {account['name']} · артикул {row[9]}")
            write(f"Вопрос покупателя:\n{row[5]}\n")
            write("Ответ модели:\n" + draft[1] + "\n")
            existing = store._db.execute("SELECT job_id,write_started FROM delivery_jobs WHERE message_id=?", (message_id,)).fetchone()
            if existing and existing[1]:
                result = delivery.reconcile(store, accounts, existing[0], actor=actor)
                write(f"Отправка уже выполнялась: {result['state']}. {result['note']}")
                return 0
            if not snapshot["can_approve"]:
                write("Публикация пока недоступна: " + (snapshot["source_error"] or "проверьте состояние обращения"))
            choice = read("1 — опубликовать; 2 — исправить; 3 — отклонить; 4 — сохранить без отправки; 5 — факты; 0 — меню: ").strip().casefold()
            choice = {"1":"опубликовать","2":"исправить","3":"отклонить","4":"сохранить","5":"факты","0":""}.get(choice,choice)
            if not choice:
                store.audit_event(message_id, "review.closed", {"mode":"live_work"}, actor=actor)
                return 0
            if choice == "факты":
                for _topic, statement in snapshot["material"]:
                    write(statement)
                continue
            if choice == "исправить" and snapshot["can_approve"]:
                text = _read_text(read, write)
                if text is not None:
                    pipeline.revise(message_id, text, expected_draft_id=draft[0], actor=actor)
                    write("Правка сохранена. Перед публикацией проверьте новый текст.")
                continue
            if choice == "сохранить" and snapshot["can_approve"]:
                pipeline.approve(message_id, expected_draft_id=draft[0], actor=actor)
                write("Одобрено и сохранено в HOLD. На площадку ничего не отправлено.")
                return 0
            if choice == "отклонить":
                reason = read("Причина отклонения: ").strip()
                pipeline.reject(message_id, reason, expected_draft_id=draft[0], expected_revision=snapshot["revision"], actor=actor)
                write("Ответ отклонён. Решение сохранено. На площадку ничего не отправлено.")
                return 0
            if choice != "опубликовать" or not snapshot["can_approve"]:
                write("Выберите доступное действие.")
                continue
            pipeline.approve(message_id, expected_draft_id=draft[0], actor=actor)
            plan = delivery.prepare(store, accounts, message_id, expected_draft_id=draft[0], actor=actor)
            store.audit_event(message_id, "delivery.publication_authorized", {"job_id":plan["job_id"],"text_sha256":plan["text_sha256"],"confirmation":"ОПУБЛИКОВАТЬ","target":plan["target"]}, run_id=plan["job_id"], actor=actor)
            write("Проверяю вопрос и отсутствие другого ответа, затем отправляю…")
            result = delivery.send(store, accounts, plan["job_id"], expected_text_sha256=plan["text_sha256"], actor=actor, live=True)
            write(f"\nРезультат площадки: {result['state']}. {result['note']}")
            if result.get("remote_id"):
                write("ID ответа площадки: " + result["remote_id"])
            write("Повторная отправка этого ответа заблокирована. Решение и результат записаны в журнал.")
            return 0
    except (EOFError, KeyboardInterrupt):
        store.audit_event(message_id, "review.closed", {"mode":"live_work","input_interrupted":True}, actor=actor)
        return 0


def create_review_demo(path: Path):
    """Create a separate persistent demo DB with a clearly labelled manual draft."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb"):
        path.chmod(0o600)
    store = open_store(path)
    try:
        import_bundle(store)
        snapshot = json.loads((Path(__file__).resolve().parents[1] / "examples/inbox.example.json").read_text(encoding="utf-8"))
        import_snapshot(store, snapshot, source_reference="manual terminal demo")
        message_id = snapshot["messages"][0]["message_id"]

        def manual_draft(_message, _article, material, **_kwargs):
            return GenerationResult(
                text="Добрый день! Для первого подключения откройте Bluetooth на телефоне, выберите название модели наушников и нажмите «Подключить». Искренне ваша, команда Deppa.",
                model="not_called", source="manual_demo", decision="answer", basis=tuple(material))

        Pipeline(store, generator=manual_draft).process(message_id)
        store.audit_event(message_id, "demo.created", {"manual_draft": True, "model_calls": 0, "database": str(path)})
        return store, message_id
    except BaseException:
        store.close()
        raise


def binary_review(store, message_id, accounts, *, actor="operator", read=None, write=None):
    """Review a draft, publish, reject, or regenerate from operator feedback."""
    from . import delivery
    from .config import MODEL, PROVIDER, positive_int
    read, write = read or input, write or print
    pipeline = Pipeline(store)
    while True:
        snapshot = pipeline.review_snapshot(message_id, actor=actor)
        row, draft = snapshot["message"], snapshot["draft"]
        if not draft:
            raise ValueError("нет подготовленного ответа")
        account = delivery.account_for(accounts, {"marketplace":row[1],"account_id":row[2]})
        write(f"\n{account['name']} · артикул {row[9]}")
        write("Вопрос:\n" + row[5])
        write("\nОтвет:\n" + draft[1] + "\n")
        if not snapshot["can_approve"]:
            write("Публикация недоступна: " + (snapshot["source_error"] or store.message_note(message_id)))
        raw = read("Да — опубликовать / Нет — отклонить и перейти дальше / Переделать — учесть замечание (один запрос модели): ").strip()
        choice = raw.casefold()
        if choice in {"нет","н","no","n","2"}:
            pipeline.reject(message_id,"Оператор выбрал Нет",expected_draft_id=draft[0],expected_revision=snapshot["revision"],actor=actor)
            write("Отклонено. Перехожу к следующему вопросу.\n")
            return
        if choice in {"переделать","переподготовить","перегенерировать","3"}:
            feedback = read("Что изменить? Напишите замечание для модели (Enter — отмена): ").strip()
            if not feedback:
                continue
            store.audit_event(message_id,"model.call_authorized",{"provider":PROVIDER,"model":MODEL,"calls":1,
                "max_output_tokens":positive_int("MRA_MAX_OUTPUT_TOKENS",2000),"mode":"operator_feedback",
                "previous_draft_id":draft[0],"feedback":feedback},actor=actor)
            write("Переподготавливаю ответ с учётом замечания…")
            outcome = pipeline.regenerate(message_id,feedback,expected_draft_id=draft[0],actor=actor)
            if not outcome.draft_id:
                write("Новый вариант не подготовлен: " + outcome.reason + ". Публикации и автоматического повтора нет.")
            continue
        if choice not in {"да","д","yes","y","1"}:
            write("Введите Да, Нет или Переделать. Для выхода — Ctrl+C.")
            continue
        if not snapshot["can_approve"]:
            write("Этот вариант нельзя опубликовать: требуется переподготовка или решение Нет.")
            continue
        pipeline.approve(message_id,expected_draft_id=draft[0],actor=actor)
        plan=delivery.prepare(store,accounts,message_id,expected_draft_id=draft[0],actor=actor)
        store.audit_event(message_id,"delivery.publication_authorized",{"job_id":plan["job_id"],"text_sha256":plan["text_sha256"],
            "confirmation":raw,"target":plan["target"],"mode":"human_review"},run_id=plan["job_id"],actor=actor)
        write("Проверяю и публикую…")
        result=delivery.send(store,accounts,plan["job_id"],expected_text_sha256=plan["text_sha256"],actor=actor,live=True)
        write(f"Результат: {result['state']}. {result['note']}\n")
        return
