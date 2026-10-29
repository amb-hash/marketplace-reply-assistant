"""Operator CLI: read incoming messages, review, explicitly send, and verify."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
import uuid
from contextlib import closing
from pathlib import Path

from . import __version__
from .config import ACCOUNTS_FILE, DATA_DIR, DB_PATH, RULES_FILE, MODEL, OPENAI_RESPONSES_URL, OPENROUTER_CHAT_URL, PROVIDER
from .generate import load_rules
from .importing import copy_database, import_legacy_messages, import_snapshot, read_snapshot
from .pipeline import Pipeline
from .store import SCHEMA_VERSION, open_store
from .knowledge import bundle_connection, import_bundle, read_bundle
from .catalog import apply_snapshot, matrix, refresh_for_message, register_accounts, sync_account
from .catalog_api import load_accounts
from .communications import sync_inbox
from . import delivery
from .market_api import CommunicationClient
from .rate_limits import AccountLimiter
from .config import RUNTIME_ERROR, RUNTIME_OPERATOR


def emit(value):
    print(json.dumps(value, ensure_ascii=False, indent=2))


def positive(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("требуется положительное число")
    return number


def build_parser():
    parser = argparse.ArgumentParser(description="Marketplace Reply Assistant: черновики → оператор → HOLD")
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("--db", type=Path, default=DB_PATH, help="путь рабочей SQLite БД")
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("init", "doctor", "demo", "outbox", "sources", "project", "terminal"):
        command_parser = commands.add_parser(command)
        if command == "terminal":
            command_parser.add_argument("--account")
            command_parser.add_argument("--advanced", action="store_true", help="расширенное техническое меню")
    session = commands.add_parser("review-session", help="автоматические пары вопрос–ответ и решения Да/Нет")
    session.add_argument("--account")
    session.add_argument("--category")
    session.add_argument("--first")
    session.add_argument("--limit", type=positive, default=20)
    session.add_argument("--live", action="store_true", required=True)
    readiness = commands.add_parser("readiness")
    readiness.add_argument("--accounts-file", type=Path, default=ACCOUNTS_FILE)
    for command in ("inbox-sync", "access-check"):
        remote = commands.add_parser(command)
        remote.add_argument("--accounts-file", type=Path, default=ACCOUNTS_FILE)
        scope = remote.add_mutually_exclusive_group(required=True)
        scope.add_argument("--account")
        scope.add_argument("--all", action="store_true")
        remote.add_argument("--live", required=True, action="store_true", help="прямое чтение API кабинета")
        remote.add_argument("--max-requests", type=positive, default=30)
        if command == "inbox-sync":
            remote.add_argument("--kind", choices=["question", "review"], default="question")
            remote.add_argument("--limit", type=positive, default=20)
    generate = commands.add_parser("generate")
    generate.add_argument("message_id")
    generate.add_argument("--live", required=True, action="store_true", help="один вызов модели для указанного обращения")
    generate.add_argument("--accounts-file", type=Path, default=ACCOUNTS_FILE)
    generate.add_argument("--operator", default=os.environ.get("MRA_OPERATOR") or RUNTIME_OPERATOR or "operator")
    prepare = commands.add_parser("delivery-preview")
    prepare.add_argument("message_id")
    prepare.add_argument("--draft-id", required=True)
    prepare.add_argument("--accounts-file", type=Path, default=ACCOUNTS_FILE)
    prepare.add_argument("--operator", required=True)
    commands.add_parser("delivery-status")
    for command in ("deliver", "delivery-reconcile", "delivery-recover"):
        send = commands.add_parser(command)
        send.add_argument("job_id")
        send.add_argument("--accounts-file", type=Path, default=ACCOUNTS_FILE)
        send.add_argument("--operator", required=True)
        if command != "delivery-recover":
            send.add_argument("--live", required=True, action="store_true")
        if command == "deliver":
            send.add_argument("--text-sha256", required=True)
    for command in ("accounts", "matrix", "catalog-sync", "catalog-import"):
        catalog = commands.add_parser(command)
        catalog.add_argument("--accounts-file", type=Path, default=ACCOUNTS_FILE)
        if command == "catalog-sync":
            scope = catalog.add_mutually_exclusive_group(required=True)
            scope.add_argument("--account")
            scope.add_argument("--all", action="store_true")
            catalog.add_argument("--live", required=True, action="store_true", help="читать каталоги напрямую у площадок")
            catalog.add_argument("--max-requests", type=positive, default=100, help="предел HTTP-запросов на кабинет")
        elif command == "matrix":
            catalog.add_argument("--account")
            catalog.add_argument("--full", action="store_true")
            catalog.add_argument("--gaps", action="store_true")
        elif command == "catalog-import":
            catalog.add_argument("path", type=Path)
    queue = commands.add_parser("queue")
    queue.add_argument("--state", choices=["all", "new", "processing", "drafted", "needs_data", "rejected", "approved", "done"], default="all")
    queue.add_argument("--limit", type=positive, default=100)
    for command in ("show", "recover"):
        commands.add_parser(command).add_argument("message_id")
    for command in ("review", "work", "approve", "revise", "reject"):
        operator = commands.add_parser(command)
        operator.add_argument("message_id")
        operator.add_argument("--operator", default=os.environ.get("MRA_OPERATOR") or RUNTIME_OPERATOR or "operator")
        if command == "review":
            operator.add_argument("--inspection-only", action="store_true", help="показать вопрос и ответ с возможностью отклонения, без одобрения или отправки")
        if command in {"approve", "revise"}:
            operator.add_argument("--draft-id", required=True, help="ID черновика, который вы проверили")
        if command == "revise":
            operator.add_argument("--text-file", required=True, type=Path)
        if command == "reject":
            operator.add_argument("--reason", required=True)
            operator.add_argument("--draft-id")
    demo_review = commands.add_parser("demo-review")
    demo_review.add_argument("--output-db", type=Path)
    demo_review.add_argument("--operator", default=os.environ.get("MRA_OPERATOR") or "operator")
    trace = commands.add_parser("trace")
    trace.add_argument("message_id", nargs="?")
    trace.add_argument("--all", action="store_true", help="все события рабочей базы")
    trace.add_argument("--full", action="store_true", help="включить сохранённые входы, запросы, ответы и источники")
    commands.add_parser("product").add_argument("article")
    preview = commands.add_parser("preview")
    preview.add_argument("--article", required=True)
    preview.add_argument("--question", required=True)
    preview.add_argument("--kind", choices=["question", "review"], default="question")
    preview.add_argument("--rating", type=int, choices=range(1, 6))
    bundled = commands.add_parser("import-bundled")
    bundled.add_argument("--replace-materials", action="store_true")
    run = commands.add_parser("run")
    run.add_argument("--limit", type=positive, default=10)
    run.add_argument("--operator", default=os.environ.get("MRA_OPERATOR") or "operator")
    modes = run.add_mutually_exclusive_group(required=True)
    modes.add_argument("--live", action="store_true", help="разрешить до limit вызовов модели")
    modes.add_argument("--dry-run", action="store_true", help="показать выбранные ID без обработки")
    retry = commands.add_parser("retry")
    retry.add_argument("message_id")
    retry.add_argument("--operator", default=os.environ.get("MRA_OPERATOR") or "operator")
    retry.add_argument("--live", action="store_true", required=True, help="разрешить один вызов модели")
    importer = commands.add_parser("import-json")
    importer.add_argument("path", type=Path)
    importer.add_argument("--replace-materials", action="store_true")
    legacy = commands.add_parser("import-legacy")
    legacy.add_argument("--source", required=True, type=Path)
    legacy.add_argument("--limit", type=positive, default=100)
    legacy.add_argument("--marketplace", choices=["wildberries", "ozon", "yandex_market"], default="")
    legacy.add_argument("--kind", choices=["question", "review"], default="")
    backup = commands.add_parser("backup")
    backup.add_argument("target", type=Path)
    migrate = commands.add_parser("migrate-db")
    migrate.add_argument("--source", required=True, type=Path)
    migrate.add_argument("--target", required=True, type=Path)
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "review-session":
            from .operator import review_session
            return review_session(args.db, account_id=args.account, category=args.category, limit=args.limit, first=args.first)
        if args.command == "terminal":
            from .terminal import terminal, advanced_terminal
            return advanced_terminal(args.db) if args.advanced else terminal(args.db, account_id=args.account)
        if args.command == "demo":
            from .demo import run_demo
            emit(run_demo())
            return 0
        if args.command == "demo-review":
            from .review import create_review_demo, review_message
            path = args.output_db or DATA_DIR / ("review-demo-" + uuid.uuid4().hex[:10] + ".sqlite3")
            with closing(create_review_demo(path)[0]) as store:
                message_id = store.messages_by_state("drafted")[0][0]
                print("Учебный прогон интерфейса: реальный канон, вручную подготовленный пример ответа. Модель не вызывается.")
                print(f"Отдельная база и полный журнал: {path}")
                code = review_message(store, message_id, actor=args.operator)
                emit({"demo_db": str(path), "message_id": message_id, "state": store.get_message(message_id)[11],
                      "review_events": store.review_history(message_id), "audit_events": len(store.audit_trace(message_id)), "model_calls": 0})
                return code
        if args.command == "sources":
            emit(read_bundle()[-1])
            return 0
        if args.command == "migrate-db":
            emit(copy_database(args.source, args.target, migrate=True))
            return 0
        if args.command not in {"init", "import-json", "import-legacy", "import-bundled"} and not args.db.is_file():
            raise ValueError("база не найдена; сначала выполните init, import-json или migrate-db")
        with closing(open_store(args.db)) as store:
            pipeline = Pipeline(store)
            if args.command == "readiness":
                from .readiness import report
                emit(report(store, load_accounts(args.accounts_file)))
            elif args.command in {"inbox-sync", "access-check"}:
                accounts = load_accounts(args.accounts_file)
                selected = [a for a in accounts if args.all or a["account_id"] == args.account]
                if not selected:
                    raise ValueError("кабинет не найден")
                failed = 0
                for account in selected:
                    if args.command == "inbox-sync":
                        try:
                            emit(sync_inbox(store, account, kind=args.kind, limit=args.limit, max_requests=args.max_requests))
                        except (OSError, ValueError) as exc:
                            failed += 1
                            emit({"account_id": account["account_id"], "status": "failed", "reason": str(exc)})
                    else:
                        for kind in ("question", "review"):
                            run_id = "access_" + uuid.uuid4().hex
                            client = CommunicationClient(account, max_requests=args.max_requests,
                                audit=lambda event, payload: store.audit_event(None, event, payload, run_id=run_id), limiter=AccountLimiter(store,account))
                            try:
                                result = {"account_id": account["account_id"], "kind": kind, **client.probe(kind),
                                          "write_access": "проверяется только реальной отправкой", "writes": 0}
                            except (OSError, ValueError) as exc:
                                failed += 1
                                result = {"account_id": account["account_id"], "kind": kind, "read_access": False, "reason": str(exc), "writes": 0}
                            store.audit_event(None, "access.checked", result, run_id=run_id)
                            emit(result)
                return 1 if failed else 0
            elif args.command == "generate":
                refresh_for_message(store, args.message_id, load_accounts(args.accounts_file))
                store.audit_event(args.message_id, "model.call_authorized", {"provider": PROVIDER, "model": MODEL, "calls": 1}, actor=args.operator)
                emit(pipeline.process(args.message_id, allow_llm=True).__dict__)
            elif args.command == "delivery-preview":
                emit(delivery.prepare(store, load_accounts(args.accounts_file), args.message_id,
                    expected_draft_id=args.draft_id, actor=args.operator))
            elif args.command == "delivery-status":
                emit(delivery.jobs(store))
            elif args.command == "deliver":
                emit(delivery.send(store, load_accounts(args.accounts_file), args.job_id,
                    expected_text_sha256=args.text_sha256, actor=args.operator, live=args.live))
            elif args.command == "delivery-reconcile":
                emit(delivery.reconcile(store, load_accounts(args.accounts_file), args.job_id, actor=args.operator))
            elif args.command == "delivery-recover":
                emit(delivery.recover(store, args.job_id, actor=args.operator))
            elif args.command == "init":
                emit({"version": __version__, "db": str(args.db), "schema": SCHEMA_VERSION, "knowledge": import_bundle(store)})
            elif args.command == "import-bundled":
                emit(import_bundle(store, replace_materials=args.replace_materials))
            elif args.command in {"accounts", "matrix", "catalog-sync", "catalog-import"}:
                accounts = load_accounts(args.accounts_file)
                register_accounts(store, accounts)
                if args.command == "accounts":
                    emit([{k: a[k] for k in ("marketplace", "account_id", "name")} for a in accounts])
                elif args.command == "matrix":
                    if args.account and not any(a["account_id"] == args.account for a in accounts):
                        raise ValueError("неизвестный кабинет")
                    emit(matrix(store, account_id=args.account, full=args.full, gaps_only=args.gaps))
                elif args.command == "catalog-import":
                    snapshot = read_snapshot(args.path)
                    account = next((a for a in accounts if a["account_id"] == snapshot.get("account_id") and a["marketplace"] == snapshot.get("marketplace")), None)
                    if not account:
                        raise ValueError("снимок не относится к настроенному кабинету")
                    emit(apply_snapshot(store, account, snapshot.get("listings"), observed_at=snapshot.get("observed_at"), complete=snapshot.get("complete")))
                else:
                    selected = [a for a in accounts if args.all or a["account_id"] == args.account]
                    if not selected:
                        raise ValueError("неизвестный кабинет")
                    failed = 0
                    for account in selected:
                        try:
                            emit(sync_account(store, account, max_pages=args.max_requests))
                        except (OSError, ValueError, sqlite3.Error) as exc:
                            failed += 1
                            emit({"account_id": account["account_id"], "status": "failed", "reason": str(exc)})
                    return 1 if failed else 0
            elif args.command == "product":
                material = store.material_for(args.article)
                if not material:
                    raise ValueError("для артикула нет материала")
                emit({"article": args.article, "name": store.product_name(args.article), "material": material})
            elif args.command == "preview":
                preview = pipeline.preview(args.article, args.question, kind=args.kind, rating=args.rating)
                store.audit_event(None, "request.previewed", {"preview_only": True, "preview": preview})
                emit(preview)
            elif args.command == "project":
                emit({"version": __version__, "knowledge": bundle_connection(store), "database": str(args.db),
                      "rules_file": str(RULES_FILE), "queue": {state: len(store.messages_by_state(state)) for state in
                      ["new", "processing", "drafted", "needs_data", "rejected", "approved"]},
                      "provider": {"name": PROVIDER, "model": MODEL, "endpoint": OPENROUTER_CHAT_URL if PROVIDER == "openrouter" else OPENAI_RESPONSES_URL},
                      "operator_interface": "terminal during human-in-the-loop testing",
                      "input": "direct API, local JSON or collector SQLite", "output": "reviewed HOLD and explicit delivery",
                      "marketplace_delivery": "explicit send with readback; live acceptance pending", "future_mode": "headless after staged acceptance"})
            elif args.command == "import-json":
                file_run = "file_" + uuid.uuid4().hex
                store.audit_event(None, "import.file_requested", {"source": str(args.path)}, run_id=file_run)
                try:
                    snapshot = read_snapshot(args.path)
                except (OSError, ValueError) as exc:
                    store.audit_event(None, "import.file_failed", {"source": str(args.path), "error_type": type(exc).__name__}, run_id=file_run)
                    raise
                emit(import_snapshot(store, snapshot, replace_materials=args.replace_materials,
                                     source_reference=str(args.path)))
            elif args.command == "import-legacy":
                emit(import_legacy_messages(store, args.source, limit=args.limit, marketplace=args.marketplace, kind=args.kind))
            elif args.command == "queue":
                states = [args.state] if args.state != "all" else ["new", "processing", "drafted", "needs_data", "rejected", "approved", "done"]
                rows = [{"message_id": r[0], "marketplace": r[1], "kind": r[2], "article": r[4], "state": state,
                         "note": store.message_note(r[0])} for state in states for r in store.messages_by_state(state)]
                emit({"total": len(rows), "messages": rows[:args.limit]})
            elif args.command == "show":
                row = store.get_message(args.message_id)
                if not row:
                    raise ValueError("нет сообщения")
                emit({"message": dict(zip(["id", "marketplace", "account", "external_id", "kind", "text", "pros", "cons", "rating", "article", "name", "state"], row)),
                      "revision": store.message_revision(args.message_id), "note": store.message_note(args.message_id),
                      "draft": store.latest_draft(args.message_id), "runs": store.runlog_for(args.message_id),
                      "review_history": store.review_history(args.message_id), "audit_events": len(store.audit_trace(args.message_id))})
            elif args.command == "work":
                from .review import work_message
                return work_message(store, args.message_id, actor=args.operator)
            elif args.command == "review":
                from .review import review_message
                return review_message(store, args.message_id, actor=args.operator, inspection_only=args.inspection_only)
            elif args.command == "approve":
                draft = pipeline.approve(args.message_id, expected_draft_id=args.draft_id, actor=args.operator)
                if not draft:
                    raise ValueError("нет актуального черновика")
                emit({"draft_id": draft, "outbox": "held"})
            elif args.command == "revise":
                text = args.text_file.read_text(encoding="utf-8")
                draft = pipeline.revise(args.message_id, text, expected_draft_id=args.draft_id, actor=args.operator)
                emit({"draft_id": draft, "state": "drafted", "requires_approval": True})
            elif args.command == "reject":
                pipeline.reject(args.message_id, args.reason, expected_draft_id=args.draft_id, actor=args.operator)
                emit({"message_id": args.message_id, "state": "rejected"})
            elif args.command == "trace":
                if bool(args.message_id) == bool(args.all):
                    raise ValueError("задайте message_id или --all")
                emit(store.audit_trace(args.message_id if not args.all else None, full=args.full))
            elif args.command == "outbox":
                rows = store._db.execute("""SELECT o.draft_id, d.message_id, o.state, o.hold_reason, d.text
                    FROM outbox o JOIN drafts d ON d.draft_id=o.draft_id ORDER BY o.created_at, o.draft_id""").fetchall()
                emit([dict(zip(["draft_id", "message_id", "state", "reason", "text"], r)) for r in rows])
            elif args.command == "recover":
                store.recover_message(args.message_id)
                emit({"message_id": args.message_id, "state": "new"})
            elif args.command == "run":
                rows = store.messages_by_state("new")[:args.limit]
                if args.dry_run:
                    emit({"selected": [r[0] for r in rows], "model_calls": 0})
                else:
                    accounts, checked = load_accounts(ACCOUNTS_FILE), set()
                    for row in rows:
                        try:
                            refresh_for_message(store, row[0], accounts, already_checked=checked)
                        except ValueError as exc:
                            store.audit_event(row[0], "catalog.refresh_required_failed", {"reason": str(exc)})
                        store.audit_event(row[0], "model.call_authorized", {"provider": PROVIDER, "model": MODEL, "calls": 1}, actor=args.operator)
                        outcome = pipeline.process(row[0], allow_llm=True)
                        emit({"message_id": outcome.message_id, "state": outcome.state, "reason": outcome.reason})
            elif args.command == "retry":
                try:
                    refresh_for_message(store, args.message_id, load_accounts(ACCOUNTS_FILE))
                except ValueError as exc:
                    store.audit_event(args.message_id, "catalog.refresh_required_failed", {"reason": str(exc)})
                store.audit_event(args.message_id, "model.call_authorized", {"provider": PROVIDER, "model": MODEL, "calls": 1, "retry": True}, actor=args.operator)
                outcome = pipeline.process(args.message_id, retry=True, allow_llm=True)
                emit({"message_id": outcome.message_id, "state": outcome.state, "reason": outcome.reason})
            elif args.command == "backup":
                emit(copy_database(args.db, args.target))
            elif args.command == "doctor":
                canon = store.verify_canon()
                bundled = bundle_connection(store)
                try:
                    load_rules()
                    rules_ok = True
                    expected = next(item["sha256"] for item in bundled["files"] if item["path"].endswith("ПРАВИЛА ОТВЕТОВ.md"))
                    bundled_rules = hashlib.sha256(RULES_FILE.read_bytes()).hexdigest() == expected
                except ValueError:
                    rules_ok = False
                    bundled_rules = False
                integrity = store._db.execute("PRAGMA integrity_check").fetchone() == ("ok",)
                audit_integrity = store.verify_audit()
                emit({"version": __version__, "database_ok": integrity, "rules_ok": rules_ok, "canon_ok": canon["ok"],
                      "audit": audit_integrity,
                      "bundled_rules_connected": bundled_rules, "knowledge": bundled,
                      "marketplace_delivery": "implemented; live acceptance checked separately", "runtime_error": RUNTIME_ERROR, "network_calls": 0})
                return 0 if rules_ok and canon["ok"] and integrity and audit_integrity["ok"] and bundled_rules and bundled["connected"] and not RUNTIME_ERROR else 1
        return 0
    except (OSError, ValueError, sqlite3.Error) as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
