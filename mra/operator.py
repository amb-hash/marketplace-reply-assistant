"""Persistent human-in-the-loop menu for consecutive real marketplace messages."""

from __future__ import annotations

from contextlib import closing
import getpass
import json
import os
import sqlite3

from . import delivery
from .catalog import matrix, refresh_for_message, sync_account
from .catalog_api import load_accounts
from .communications import sync_inbox
from .config import ACCOUNTS_FILE, MODEL, PROVIDER, RUNTIME_OPERATOR, positive_int
from .pipeline import Pipeline
from .review import work_message
from .store import open_store

STATES = {"new":"новое", "drafted":"ответ готов", "approved":"одобрено, не отправлено",
          "needs_data":"нужны данные", "rejected":"отклонено", "processing":"готовится", "done":"опубликовано"}


def scoped_rows(store, account, kind):
    return [store.get_message(r[0]) for r in store._db.execute("""SELECT message_id FROM messages
        WHERE marketplace=? AND account_id=? AND kind=? ORDER BY created_at,message_id""",
        (account["marketplace"], account["account_id"], kind))]


def deferred(store, account, kind):
    selected = {}
    for mid, event, payload in store._db.execute("""SELECT e.message_id,e.event_type,d.content_json FROM audit_events e
        JOIN audit_documents d ON d.sha256=e.document_sha256 JOIN messages m ON m.message_id=e.message_id
        WHERE m.marketplace=? AND m.account_id=? AND m.kind=? AND e.event_type IN ('operator.deferred','operator.resumed')
        ORDER BY e.event_id""", (account["marketplace"], account["account_id"], kind)):
        if event == "operator.deferred":
            selected[mid] = json.loads(payload)["revision"]
        else:
            selected.pop(mid, None)
    return selected


def next_message(store, account, kind, *, category=None, canon_only=False):
    skipped = deferred(store, account, kind)
    rows = scoped_rows(store, account, kind)
    for state in ("drafted", "approved", "new"):
        for row in rows:
            if canon_only and (not row[9] or not store.material_for(row[9])):
                continue
            if category:
                product = store._db.execute("SELECT category FROM products WHERE article=?", (row[9],)).fetchone()
                if not product or product[0] != category:
                    continue
            if row[11] != state or skipped.get(row[0]) == store.message_revision(row[0]):
                continue
            sent = store._db.execute("SELECT write_started FROM delivery_jobs WHERE message_id=?", (row[0],)).fetchone()
            if not sent or not sent[0]:
                return row[0]
    return None


def postpone(store, mid, actor):
    store.audit_event(mid, "operator.deferred", {"revision":store.message_revision(mid)}, actor=actor)


def open_message(store, mid, accounts, *, actor, read, write):
    row = store.get_message(mid)
    job = store._db.execute("SELECT job_id,write_started,state FROM delivery_jobs WHERE message_id=?", (mid,)).fetchone()
    if job and job[1]:
        write(f"Отправка уже выполнялась: {job[2]}. Проверка доступна в разделе «Отправки».")
        return
    draft = store.latest_draft(mid)
    if draft and row[11] in {"drafted", "approved"}:
        work_message(store, mid, actor=actor, read=read, write=write)
        if store.get_message(mid)[11] in {"drafted", "approved"}:
            postpone(store, mid, actor)
        return
    account = next(a for a in accounts if a["account_id"] == row[2])
    write(f"\n{account['marketplace']} · {account['name']} · артикул {row[9] or 'не определён'}")
    write("Вопрос/отзыв покупателя:\n" + row[5])
    for label, value in (("Плюсы",row[6]),("Минусы",row[7]),("Оценка",row[8])):
        if value:
            write(f"{label}: {value}")
    if draft:
        write("\nПредыдущий ответ:\n" + draft[1])
    if store.message_note(mid):
        write("Заметка: " + store.message_note(mid))
    maximum = positive_int("MRA_MAX_OUTPUT_TOKENS", 2000)
    write(f"\nМодель: {PROVIDER} / {MODEL}. Подготовка — один платный запрос, максимум {maximum} выходных токенов.")
    write("Провайдеру передаются этот вопрос, действующие правила и только материал выбранного артикула. Автоповторов нет.")
    while True:
        choice = read("1 — подготовить ответ; 2 — отложить; 3 — отклонить; 4 — факты товара; 0 — меню: ").strip()
        if choice in {"0", "2", ""}:
            postpone(store, mid, actor)
            return
        if choice == "4":
            for _topic, statement in store.material_for(row[9]):
                write(statement)
            continue
        if choice == "3":
            Pipeline(store).reject(mid, read("Причина отклонения: ").strip(), expected_draft_id=draft[0] if draft else "",
                                  expected_revision=store.message_revision(mid), actor=actor)
            write("Отклонено. На площадку ничего не отправлено.")
            return
        if choice != "1":
            write("Выберите действие из меню.")
            continue
        refresh_for_message(store, mid, accounts)
        row = store.get_message(mid)
        pipeline = Pipeline(store)
        pipeline._context(row[9], row)
        store.audit_event(mid, "model.call_authorized", {"provider":PROVIDER,"model":MODEL,"calls":1,
            "max_output_tokens":maximum,"confirmation":"1 — подготовить ответ","data_scope":"question, rules, selected article canon"}, actor=actor)
        write("Модель готовит ответ…")
        outcome = pipeline.process(mid, retry=row[11] != "new", allow_llm=True)
        if outcome.draft_id:
            work_message(store, mid, actor=actor, read=read, write=write)
            if store.get_message(mid)[11] in {"drafted", "approved"}:
                postpone(store, mid, actor)
        else:
            write("Ответ не подготовлен: " + outcome.reason)
        return


def choose_scope(accounts, current, kind, read, write):
    for i, account in enumerate(accounts, 1):
        write(f"{i}. {account['marketplace']} · {account['name']}")
    value = read("Номер кабинета (Enter — оставить текущий): ").strip()
    if value:
        if not value.isdigit() or not 1 <= int(value) <= len(accounts):
            raise ValueError("выберите кабинет из списка")
        current = accounts[int(value) - 1]
    value = read("1 — вопросы; 2 — отзывы; Enter — оставить текущий режим: ").strip()
    if value and value not in {"1", "2"}:
        raise ValueError("выберите вопросы или отзывы")
    return current, {"1":"question","2":"review"}.get(value, kind)


def terminal(db_path, *, read=None, write=None, account_id=None):
    read, write = read or input, write or print
    actor = os.environ.get("MRA_OPERATOR") or RUNTIME_OPERATOR or getpass.getuser()
    if not db_path.is_file():
        write("Рабочая база не создана. Выполните ./install.sh.")
        return 1
    with closing(open_store(db_path)) as store:
        accounts = load_accounts(ACCOUNTS_FILE)
        if not accounts:
            write("Нет настроенных кабинетов. Запустите mra-setup.command.")
            return 1
        last = store._db.execute("""SELECT m.account_id FROM delivery_jobs j JOIN messages m ON m.message_id=j.message_id
            WHERE j.state='published' ORDER BY j.updated_at DESC LIMIT 1""").fetchone()
        selected = account_id or store.get_meta("operator.account_id", "") or (last[0] if last else "")
        account = next((a for a in accounts if a["account_id"] == selected), None)
        if account_id and account is None:
            raise ValueError("неизвестный кабинет")
        account = account or accounts[0]
        kind = store.get_meta("operator.kind", "question")
        if kind not in {"question", "review"}:
            kind = "question"
        write(f"Marketplace Reply Assistant · оператор {actor}")
        write(f"Модель: {PROVIDER} / {MODEL}. Каждый платный запрос и публикация подтверждаются отдельно.")
        try:
            while True:
                rows = scoped_rows(store, account, kind)
                counts = {state:sum(r[11] == state for r in rows) for state in STATES}
                write(f"\n{account['marketplace']} · {account['name']} · {'Вопросы' if kind == 'question' else 'Отзывы'}")
                write(f"Новые: {counts['new']} | Ответ готов: {counts['drafted']} | Нужны данные: {counts['needs_data']} | Опубликовано: {counts['done']}")
                write("1 — следующий вопрос/отзыв; 2 — очередь; 3 — обновить входящие")
                write("4 — отправки и проверка; 5 — сменить кабинет или тип обращений; 6 — каталог и диагностика; 0 — выход")
                choice = read("Действие: ").strip()
                try:
                    if choice == "0":
                        return 0
                    if choice == "1":
                        mid = next_message(store, account, kind)
                        if not mid:
                            result = sync_inbox(store, account, kind=kind, limit=50, max_requests=5)
                            write(f"Получено из API: {result['received']}. Модель не вызывалась.")
                            mid = next_message(store, account, kind)
                        if mid:
                            open_message(store, mid, accounts, actor=actor, read=read, write=write)
                        else:
                            write("Новых необработанных обращений нет. Отложенные, отклонённые и требующие данных доступны в очереди.")
                    elif choice == "2":
                        rows = scoped_rows(store, account, kind)
                        for i, row in enumerate(rows, 1):
                            write(f"{i}. {STATES.get(row[11],row[11])} | {row[9] or 'нет артикула'} | {row[5][:110]}")
                        value = read("Номер обращения (Enter — меню): ").strip()
                        if value:
                            if not value.isdigit() or not 1 <= int(value) <= len(rows):
                                raise ValueError("выберите обращение из списка")
                            mid = rows[int(value)-1][0]
                            store.audit_event(mid, "operator.resumed", {"revision":store.message_revision(mid)}, actor=actor)
                            open_message(store, mid, accounts, actor=actor, read=read, write=write)
                    elif choice == "3":
                        result = sync_inbox(store, account, kind=kind, limit=50, max_requests=5)
                        write(f"Получено из API: {result['received']}. Следующее обращение — пункт 1.")
                    elif choice == "4":
                        jobs = [j for j in delivery.jobs(store) if j["target"]["account_id"] == account["account_id"]]
                        for i, job in enumerate(jobs, 1):
                            write(f"{i}. {job['state']} | {job['text'][:100]}")
                        value = read("Номер отправки для проверки API (Enter — меню): ").strip()
                        if value:
                            if not value.isdigit() or not 1 <= int(value) <= len(jobs):
                                raise ValueError("выберите отправку из списка")
                            result = delivery.reconcile(store, accounts, jobs[int(value)-1]["job_id"], actor=actor)
                            write(f"{result['state']}. {result['note']}")
                    elif choice == "5":
                        account, kind = choose_scope(accounts, account, kind, read, write)
                        with store.transaction():
                            store.set_meta("operator.account_id",account["account_id"])
                            store.set_meta("operator.kind",kind)
                    elif choice == "6":
                        for entry in matrix(store, account_id=account["account_id"]):
                            write(f"Каталог: {entry['observed_at'] or 'не получен'}; без канона: {entry['without_canon']}; {entry['last_error'] or ''}")
                        write("Для известного SKU Ozon свежий вопрос и карта кабинета позволяют работать без полного каталога.")
                        if read("1 — обновить каталог; Enter — меню: ").strip() == "1":
                            result = sync_account(store, account, max_pages=30)
                            write(f"Каталог обновлён: {result['listings']} карточек.")
                    else:
                        write("Выберите действие из меню.")
                except (ValueError, OSError, sqlite3.Error) as exc:
                    store.audit_event(None,"operator.action.failed",{"action":choice,"account_id":account["account_id"],"error_type":type(exc).__name__},actor=actor)
                    write("Действие не завершено: " + str(exc))
        except (EOFError, KeyboardInterrupt):
            write("\nРабота завершена. Сохранённые ответы и решения остаются в базе.")
            return 0


def review_session(db_path, *, account_id=None, category=None, limit=20, first=None, read=None, write=None):
    """Prepare one draft at a time; the operator only accepts or rejects it."""
    from .review import binary_review
    read, write = read or input, write or print
    actor = os.environ.get("MRA_OPERATOR") or RUNTIME_OPERATOR or getpass.getuser()
    if not db_path.is_file():
        raise ValueError("сначала выполните установку")
    with closing(open_store(db_path)) as store:
        accounts = load_accounts(ACCOUNTS_FILE)
        selected = account_id or store.get_meta("operator.account_id", "")
        account = next((a for a in accounts if a["account_id"] == selected), None)
        if not account:
            raise ValueError("задайте кабинет для рабочей сессии")
        category = category or store.get_meta("operator.category", "") or None
        kind = "question"
        write(f"{account['marketplace']} · {account['name']} · {category or 'товары с каноном'}")
        write(f"Модель: {PROVIDER} / {MODEL}. Вопрос и ответ готовятся автоматически, по одному; выход — Ctrl+C.")
        write("Да — опубликовать. Нет — отклонить и перейти дальше. Переделать — учесть замечание и показать новый вариант.")
        store.audit_event(None,"operator.session.started",{"account_id":account["account_id"],"category":category,
            "canon_only":True,"limit":limit,"automatic_drafts":True,"provider":PROVIDER,"model":MODEL},actor=actor)
        refreshed = False
        try:
            for index in range(limit):
                mid = next_message(store, account, kind, category=category, canon_only=True)
                if first and index == 0:
                    row = store.get_message(first)
                    product = store._db.execute("SELECT category FROM products WHERE article=?", (row[9],)).fetchone() if row else None
                    job = store._db.execute("SELECT write_started FROM delivery_jobs WHERE message_id=?", (first,)).fetchone()
                    if not row or tuple(row[1:3]) != (account["marketplace"],account["account_id"]) or row[4] != kind or row[11] not in {"new","drafted","approved","rejected"} or not store.material_for(row[9]) or (category and (not product or product[0] != category)) or (job and job[0]):
                        raise ValueError("первый вопрос не соответствует выбранному кабинету, категории или канону")
                    mid = first
                if not mid and not refreshed:
                    write("Получаю реальные вопросы…")
                    sync_inbox(store, account, kind=kind, limit=200, max_requests=5)
                    refreshed = True
                    mid = next_message(store, account, kind, category=category, canon_only=True)
                if not mid:
                    write("Подходящих новых вопросов с материалом в каноне больше нет.")
                    return 0
                row, draft = store.get_message(mid), store.latest_draft(mid)
                if not draft:
                    refresh_for_message(store, mid, accounts)
                    pipeline = Pipeline(store)
                    pipeline._context(row[9], row)
                    store.audit_event(mid,"model.call_authorized",{"provider":PROVIDER,"model":MODEL,"calls":1,
                        "max_output_tokens":positive_int("MRA_MAX_OUTPUT_TOKENS",2000),"authorization":"owner requested automatic drafts, known-canon category only",
                        "category":category,"automatic_session":True},actor=actor)
                    write("Готовлю ответ по канону…")
                    outcome = pipeline.process(mid,allow_llm=True)
                    if not outcome.draft_id:
                        write("Надёжный ответ не подготовлен: " + outcome.reason)
                        write("Сессия остановлена. Автоматических повторов и публикации нет.")
                        return 1
                binary_review(store, mid, accounts, actor=actor, read=read, write=write)
            write("Лимит сессии достигнут. Все решения сохранены.")
            return 0
        except (EOFError,KeyboardInterrupt):
            write("\nСессия завершена. Подготовленные ответы сохранены.")
            return 0
        except (ValueError,OSError,sqlite3.Error) as exc:
            store.audit_event(None,"operator.session.failed",{"account_id":account["account_id"],"reason":str(exc)},actor=actor)
            write("Сессия остановлена: " + str(exc))
            return 1
