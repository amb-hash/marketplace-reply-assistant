"""Small terminal entry point for the initial operator trial."""

from __future__ import annotations

from contextlib import closing
import getpass
import os

from .config import ACCOUNTS_FILE, MODEL, PROVIDER, RUNTIME_KEY_FILE, RUNTIME_OPERATOR
from .catalog import matrix, refresh_for_message, register_accounts
from .catalog_api import load_accounts
from .importing import import_snapshot, read_snapshot
from .pipeline import Pipeline
from .review import create_review_demo, review_message
from .store import open_store
from .communications import sync_inbox
from . import delivery


def advanced_terminal(db_path, *, read=None, write=None):
    from .cli import main
    from .config import VERSION_ROOT
    read, write = read or input, write or print
    actor = os.environ.get("MRA_OPERATOR") or RUNTIME_OPERATOR or getpass.getuser()
    write("Marketplace Reply Assistant · терминал оператора")
    write("Подтверждённые ответы сохраняются локально в HOLD.")
    write(f"Оператор: {actor}; модель: {PROVIDER} / {MODEL}. Отправка — отдельное действие после проверки текста.")
    try:
        while True:
            write("\n1 — учебный прогон без API; 2 — очередь и проверка ответа; 3 — матрица кабинетов;")
            write("4 — журнал обращения; 5 — подготовить проверочный вопрос; 6 — один вызов модели; 0 — выйти")
            write("7 — получить вопросы/отзывы из кабинета; 8 — отправить одобренный ответ; 9 — проверить результат отправки")
            choice = read("Действие: ").strip()
            if choice == "0":
                return 0
            if choice == "1":
                main(["demo-review", "--operator", actor])
                continue
            if not db_path.is_file():
                write("Рабочая база ещё не создана. Выполните ./install.sh.")
                continue
            with closing(open_store(db_path)) as store:
                pipeline = Pipeline(store)
                try:
                    if choice == "2":
                        rows = [store.get_message(r[0]) for state in ("new", "drafted", "needs_data", "rejected", "approved", "processing", "done")
                                for r in store.messages_by_state(state)]
                        for index, row in enumerate(rows, 1):
                            write(f"{index}. {row[0]} | {row[1]} | {row[2]} | {row[11]} | {row[5][:100]}")
                        selected = read("Номер обращения (Enter — назад): ").strip()
                        if selected:
                            if not selected.isdigit() or not 1 <= int(selected) <= len(rows):
                                raise ValueError("выберите номер из списка")
                            review_message(store, rows[int(selected) - 1][0], actor=actor, read=read, write=write)
                    elif choice == "3":
                        register_accounts(store, load_accounts(ACCOUNTS_FILE))
                        for row in matrix(store):
                            count = str(row["listed"]) if row["observed_at"] else "не загружено"
                            write(f"{row['marketplace']} · {row['name']}: карточки {count}; без канона {row['without_canon']}; снимок {row['observed_at'] or 'отсутствует'}")
                            if row["last_error"]:
                                write("  Последняя ошибка: " + row["last_error"])
                            if row["access_issue"]:
                                write("  Ключ отозван площадкой." if row["access_issue"] == "revoked" else "  Ключ деактивирован площадкой.")
                    elif choice == "4":
                        mid = read("ID обращения: ").strip()
                        for event in store.audit_trace(mid):
                            write(f"{event['created_at']} | {event['event_type']} | {event['actor']}")
                        write("Полный журнал с содержимым: ./mra-cli trace ID --full")
                    elif choice == "5":
                        import_snapshot(store, read_snapshot(VERSION_ROOT / "examples/inbox.example.json"), source_reference="operator test question")
                        write("Подготовлен check_44167_001: «Как подключить эти наушники к телефону?» Модель не вызывалась.")
                    elif choice == "6":
                        mid = read("ID обращения: ").strip()
                        row = store.get_message(mid)
                        if not row or row[11] not in {"new", "needs_data", "rejected"}:
                            raise ValueError("обращение недоступно для генерации; сначала проверьте его состояние")
                        refresh_for_message(store, mid, load_accounts(ACCOUNTS_FILE))
                        row = store.get_message(mid)
                        pipeline._context(row[9], row)
                        pipeline.preview(row[9], row[5], kind=row[4], rating=row[8])
                        key_names = ("OPENROUTER_API_KEY", "MRA_OPENROUTER_KEY_FILE", "MRA_OPENROUTER_AUTH_FILE") if PROVIDER == "openrouter" else ("OPENAI_API_KEY", "MRA_OPENAI_KEY_FILE")
                        if not RUNTIME_KEY_FILE and not any(os.environ.get(name) for name in key_names):
                            key_path = read(f"Путь к файлу ключа {PROVIDER} (Enter — отмена): ").strip()
                            if not key_path:
                                continue
                            os.environ["MRA_OPENROUTER_KEY_FILE" if PROVIDER == "openrouter" else "MRA_OPENAI_KEY_FILE"] = key_path
                        from .config import positive_int
                        write(f"Один платный запрос для {mid}, максимум {positive_int('MRA_MAX_OUTPUT_TOKENS', 2000)} выходных токенов. Автоповторов нет.")
                        if read("Для запуска введите ВЫЗВАТЬ: ").strip() != "ВЫЗВАТЬ":
                            store.audit_event(mid, "model.call_cancelled", {"provider": PROVIDER, "model": MODEL}, actor=actor)
                            write("Вызов отменён.")
                            continue
                        store.audit_event(mid, "model.call_authorized", {"provider": PROVIDER, "model": MODEL, "calls": 1,
                            "max_output_tokens": positive_int('MRA_MAX_OUTPUT_TOKENS', 2000)}, actor=actor)
                        result = pipeline.process(mid, retry=row[11] != "new", allow_llm=True)
                        write(f"Результат: {result.state}; {result.reason}")
                        if result.draft_id:
                            review_message(store, mid, actor=actor, read=read, write=write)
                    elif choice == "7":
                        accounts = load_accounts(ACCOUNTS_FILE)
                        for index, account in enumerate(accounts, 1):
                            write(f"{index}. {account['marketplace']} · {account['name']} · {account['account_id']}")
                        selected = read("Номер кабинета (Enter — назад): ").strip()
                        if not selected:
                            continue
                        if not selected.isdigit() or not 1 <= int(selected) <= len(accounts):
                            raise ValueError("выберите кабинет из списка")
                        kind = read("1 — вопросы, 2 — отзывы: ").strip()
                        if kind not in {"1", "2"}:
                            raise ValueError("выберите вопросы или отзывы")
                        account = accounts[int(selected) - 1]
                        result = sync_inbox(store, account, kind="question" if kind == "1" else "review", limit=20)
                        write(f"Получено {result['received']} обращений. Модель не вызывалась.")
                        for mid in result["messages"]:
                            row = store.get_message(mid)
                            write(f"{mid} | {row[9] or 'нет связи с каноном'} | {row[5][:120]}")
                    elif choice == "8":
                        mid = read("ID проверенного и одобренного обращения: ").strip()
                        draft = store.latest_draft(mid)
                        if not draft:
                            raise ValueError("нет действующего черновика")
                        accounts = load_accounts(ACCOUNTS_FILE)
                        plan = delivery.prepare(store, accounts, mid, expected_draft_id=draft[0], actor=actor)
                        row, target = store.get_message(mid), plan["target"]
                        write(f"Куда: {target['marketplace']} · {plan['account_name']} · {target['account_id']}")
                        write(f"Обращение {target['external_id']} | артикул {row[9]}\n{row[5]}")
                        write("Окончательный ответ:\n" + plan["text"])
                        if read("Отправить этот ответ? Введите ОТПРАВИТЬ (Enter — назад): ").strip() != "ОТПРАВИТЬ":
                            store.audit_event(mid, "delivery.cancelled", {"job_id": plan["job_id"]}, actor=actor)
                            continue
                        result = delivery.send(store, accounts, plan["job_id"], expected_text_sha256=plan["text_sha256"], actor=actor, live=True)
                        write(f"Результат: {result['state']}. {result['note']}")
                    elif choice == "9":
                        rows = delivery.jobs(store)
                        for index, job in enumerate(rows, 1):
                            write(f"{index}. {job['job_id']} | {job['target']['account_id']} | {job['state']} | {job['note']}")
                        selected = read("Номер отправки для проверки (Enter — назад): ").strip()
                        if selected:
                            if not selected.isdigit() or not 1 <= int(selected) <= len(rows):
                                raise ValueError("выберите отправку из списка")
                            result = delivery.reconcile(store, load_accounts(ACCOUNTS_FILE), rows[int(selected) - 1]["job_id"], actor=actor)
                            write(f"Проверка: {result['state']}. {result['note']}")
                    else:
                        write("Выберите действие из списка.")
                except ValueError as exc:
                    write("Действие не выполнено: " + str(exc))
    except (EOFError, KeyboardInterrupt):
        write("\nРабота в терминале завершена.")
        return 0


def terminal(db_path, *, read=None, write=None, account_id=None):
    from .operator import terminal as run
    return run(db_path, read=read, write=write, account_id=account_id)
