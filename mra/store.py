"""Минимальное хранилище MRA (SQLite).

Три источника истины и рабочее состояние:
- products  — каталог товаров: артикул -> название/категория (ключ lookup);
- materials — материал канона по товару (факты: свойства, ограничения, совместимость);
- messages  — входящие вопросы/отзывы (только новые/неотвеченные);
- drafts / approvals / outbox — конвейер ответа; outbox всегда в HOLD;
- journal   — журнал обработанных (не ответить дважды).

Хранилище само не отправляет ответы. outbox хранит подтверждённый текст;
отдельный транспорт требует явного подтверждения и сохраняет устойчивое
состояние попытки в delivery_jobs.
"""

from __future__ import annotations

import sqlite3
import hashlib
import json
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple
from .audit import redact

SCHEMA_VERSION = 6

SCHEMA = """
CREATE TABLE IF NOT EXISTS products (
    article      TEXT PRIMARY KEY,
    name         TEXT,
    category     TEXT
);

-- Карта идентификаторов товара: внешний id площадки -> артикул Deppa.
-- WB: sellerArticle уже артикул; Yandex: offerId (срез -OZ); Ozon: sku -> offer_id.
CREATE TABLE IF NOT EXISTS product_map (
    marketplace  TEXT NOT NULL,
    account_id   TEXT NOT NULL DEFAULT '',
    external_id  TEXT NOT NULL,   -- sku (Ozon) | nmID | offerId
    article      TEXT NOT NULL,   -- артикул Deppa
    PRIMARY KEY (marketplace, account_id, external_id)
);

CREATE TABLE IF NOT EXISTS materials (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    article      TEXT NOT NULL,
    topic        TEXT NOT NULL,
    statement    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_materials_article ON materials(article);

CREATE TABLE IF NOT EXISTS messages (
    message_id   TEXT PRIMARY KEY,
    marketplace  TEXT NOT NULL,
    account_id   TEXT NOT NULL,
    external_id  TEXT NOT NULL,
    kind         TEXT NOT NULL,              -- question | review
    text         TEXT NOT NULL,
    pros         TEXT,
    cons         TEXT,
    rating       INTEGER,
    article      TEXT,                       -- извлечённый артикул
    product_name TEXT,                       -- извлечённое название
    state        TEXT NOT NULL DEFAULT 'new',-- new | drafted | needs_data | approved | done
    created_at   TEXT NOT NULL,
    revision     INTEGER NOT NULL DEFAULT 1,
    processing_token TEXT,
    last_error   TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_messages_state ON messages(state);

CREATE TABLE IF NOT EXISTS drafts (
    draft_id     TEXT PRIMARY KEY,
    message_id   TEXT NOT NULL,
    text         TEXT NOT NULL,
    source       TEXT NOT NULL,              -- luna | fallback
    created_at   TEXT NOT NULL,
    message_revision INTEGER NOT NULL DEFAULT 1,
    rules_sha256 TEXT NOT NULL DEFAULT '',
    material_sha256 TEXT NOT NULL DEFAULT '',
    FOREIGN KEY (message_id) REFERENCES messages(message_id)
);

CREATE TABLE IF NOT EXISTS approvals (
    draft_id     TEXT PRIMARY KEY,
    approved_at  TEXT NOT NULL,
    FOREIGN KEY (draft_id) REFERENCES drafts(draft_id)
);

CREATE TABLE IF NOT EXISTS outbox (
    draft_id     TEXT PRIMARY KEY,
    state        TEXT NOT NULL DEFAULT 'held', -- всегда held до явного разрешения
    hold_reason  TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    FOREIGN KEY (draft_id) REFERENCES drafts(draft_id)
);

CREATE TABLE IF NOT EXISTS journal (
    external_id  TEXT PRIMARY KEY,
    processed_at TEXT NOT NULL
);

-- Провенанс: версии канона и правил + факты-основания каждого ответа.
CREATE TABLE IF NOT EXISTS meta (
    key          TEXT PRIMARY KEY,
    value        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS runlog (
    log_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id    TEXT NOT NULL,
    article       TEXT,
    kind          TEXT,
    question      TEXT,
    answer        TEXT NOT NULL,
    basis         TEXT NOT NULL,   -- JSON: [{topic, statement}] — строки канона-основания
    canon_version TEXT NOT NULL,
    rules_version TEXT NOT NULL,
    model         TEXT,
    effort        TEXT,
    source        TEXT,            -- luna | fallback
    created_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_runlog_message ON runlog(message_id);

-- Подписанный манифест канона: происхождение + ответственность + неизменяемость.
-- Подпись = SHA-256 связки (версия|контент|подписант|источник). Любая мутация
-- базы меняет content_sha256 -> подпись не сойдётся -> фиксируем подмену.
CREATE TABLE IF NOT EXISTS canon_manifest (
    canon_version   TEXT PRIMARY KEY,
    content_sha256  TEXT NOT NULL,
    source          TEXT NOT NULL,
    built_by        TEXT NOT NULL,   -- кто составил базу — на нём ответственность
    built_at        TEXT NOT NULL,
    product_count   INTEGER NOT NULL,
    fact_count      INTEGER NOT NULL,
    signature       TEXT NOT NULL    -- SHA-256: версия|контент|подписант|источник
);
"""


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Store:
    def __init__(self, db_path: Path) -> None:
        self.path = db_path
        self._transaction_depth = 0
        self._db = sqlite3.connect(str(db_path), isolation_level=None, timeout=10)
        self._db.execute("PRAGMA foreign_keys = ON")
        try:
            if self._db.execute("PRAGMA user_version").fetchone()[0] > SCHEMA_VERSION:
                raise ValueError("БД создана более новой версией; откройте её соответствующим приложением")
            self._db.executescript(SCHEMA)
            self._migrate_schema()
        except Exception:
            self._db.close()
            raise

    @contextmanager
    def transaction(self):
        nested = self._db.in_transaction
        name = f"mra_{self._transaction_depth}"
        self._db.execute(f"SAVEPOINT {name}" if nested else "BEGIN IMMEDIATE")
        self._transaction_depth += 1
        try:
            yield
            self._db.execute(f"RELEASE SAVEPOINT {name}" if nested else "COMMIT")
        except BaseException:
            if nested:
                self._db.execute(f"ROLLBACK TO SAVEPOINT {name}")
                self._db.execute(f"RELEASE SAVEPOINT {name}")
            else:
                self._db.rollback()
            raise
        finally:
            self._transaction_depth -= 1

    def _migrate_schema(self) -> None:
        columns = {r[1] for r in self._db.execute("PRAGMA table_info(messages)")}
        legacy = "revision" not in columns
        additions = {
            "messages": {"revision": "INTEGER NOT NULL DEFAULT 1", "processing_token": "TEXT",
                         "last_error": "TEXT NOT NULL DEFAULT ''"},
            "drafts": {"message_revision": "INTEGER NOT NULL DEFAULT 0", "rules_sha256": "TEXT NOT NULL DEFAULT ''",
                       "material_sha256": "TEXT NOT NULL DEFAULT ''"},
            "canon_manifest": {"digest_format": "TEXT NOT NULL DEFAULT 'legacy'"},
            "runlog": {"message_revision": "INTEGER NOT NULL DEFAULT 0", "input_tokens": "INTEGER NOT NULL DEFAULT 0",
                       "output_tokens": "INTEGER NOT NULL DEFAULT 0"},
        }
        with self.transaction():
            for table, fields in additions.items():
                present = {r[1] for r in self._db.execute(f"PRAGMA table_info({table})")}
                for field, definition in fields.items():
                    if field not in present:
                        self._db.execute(f"ALTER TABLE {table} ADD COLUMN {field} {definition}")
            primary = [r[1] for r in sorted(self._db.execute("PRAGMA table_info(product_map)"), key=lambda x: x[5]) if r[5]]
            if primary != ["marketplace", "account_id", "external_id"]:
                self._db.execute("ALTER TABLE product_map RENAME TO product_map_legacy")
                self._db.execute("""CREATE TABLE product_map(marketplace TEXT NOT NULL, account_id TEXT NOT NULL DEFAULT '',
                    external_id TEXT NOT NULL, article TEXT NOT NULL, PRIMARY KEY(marketplace, account_id, external_id))""")
                self._db.execute("INSERT INTO product_map SELECT marketplace, account_id, external_id, article FROM product_map_legacy")
                self._db.execute("DROP TABLE product_map_legacy")
            self._db.execute("""CREATE TABLE IF NOT EXISTS processing_journal(
                marketplace TEXT NOT NULL, account_id TEXT NOT NULL, kind TEXT NOT NULL,
                external_id TEXT NOT NULL, revision INTEGER NOT NULL, processed_at TEXT NOT NULL,
                PRIMARY KEY(marketplace, account_id, kind, external_id, revision))""")
            self._db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_message_scope ON messages(marketplace, account_id, kind, external_id)")
            self._db.execute("""CREATE TABLE IF NOT EXISTS review_events(
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT NOT NULL REFERENCES messages(message_id),
                message_revision INTEGER NOT NULL,
                action TEXT NOT NULL,
                actor TEXT NOT NULL,
                draft_id TEXT REFERENCES drafts(draft_id),
                replacement_draft_id TEXT REFERENCES drafts(draft_id),
                note TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL)""")
            self._db.execute("CREATE INDEX IF NOT EXISTS idx_review_message ON review_events(message_id, event_id)")
            self._db.execute("""CREATE TABLE IF NOT EXISTS audit_documents(
                sha256 TEXT PRIMARY KEY, content_json TEXT NOT NULL)""")
            self._db.execute("""CREATE TABLE IF NOT EXISTS audit_events(
                event_id INTEGER PRIMARY KEY AUTOINCREMENT, message_id TEXT, run_id TEXT NOT NULL,
                event_type TEXT NOT NULL, actor TEXT NOT NULL, created_at TEXT NOT NULL,
                document_sha256 TEXT NOT NULL REFERENCES audit_documents(sha256))""")
            self._db.execute("CREATE INDEX IF NOT EXISTS idx_audit_message ON audit_events(message_id, event_id)")
            self._db.execute("CREATE INDEX IF NOT EXISTS idx_audit_run ON audit_events(run_id, event_id)")
            for statement in (
                """CREATE TABLE IF NOT EXISTS catalog_accounts(marketplace TEXT NOT NULL, account_id TEXT NOT NULL,
                    name TEXT NOT NULL, PRIMARY KEY(marketplace,account_id))""",
                """CREATE TABLE IF NOT EXISTS catalog_runs(run_id TEXT PRIMARY KEY, marketplace TEXT NOT NULL,
                    account_id TEXT NOT NULL, started_at TEXT NOT NULL, observed_at TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL, error TEXT NOT NULL DEFAULT '', item_count INTEGER NOT NULL DEFAULT 0)""",
                """CREATE TABLE IF NOT EXISTS catalog_listings(marketplace TEXT NOT NULL, account_id TEXT NOT NULL,
                    listing_id TEXT NOT NULL, seller_article TEXT NOT NULL, article TEXT NOT NULL, title TEXT NOT NULL,
                    status TEXT NOT NULL, first_seen TEXT NOT NULL, last_seen TEXT NOT NULL, run_id TEXT NOT NULL,
                    PRIMARY KEY(marketplace,account_id,listing_id))""",
                """CREATE TABLE IF NOT EXISTS catalog_aliases(marketplace TEXT NOT NULL, account_id TEXT NOT NULL,
                    id_type TEXT NOT NULL, external_id TEXT NOT NULL, listing_id TEXT NOT NULL, article TEXT NOT NULL,
                    PRIMARY KEY(marketplace,account_id,id_type,external_id))""",
                """CREATE TABLE IF NOT EXISTS message_inputs(message_id TEXT PRIMARY KEY REFERENCES messages(message_id),
                    hints_json TEXT NOT NULL, explicit_article TEXT NOT NULL DEFAULT '')""",
                "CREATE INDEX IF NOT EXISTS idx_catalog_runs_account ON catalog_runs(marketplace,account_id,started_at)",
                """CREATE TABLE IF NOT EXISTS remote_inputs(message_id TEXT PRIMARY KEY REFERENCES messages(message_id),
                    target_json TEXT NOT NULL, fingerprint TEXT NOT NULL, observed_at TEXT NOT NULL)""",
                """CREATE TABLE IF NOT EXISTS api_limits(scope TEXT PRIMARY KEY,next_at REAL NOT NULL,
                    lease_until REAL NOT NULL,lease_token TEXT NOT NULL)""",
                """CREATE TABLE IF NOT EXISTS delivery_jobs(job_id TEXT PRIMARY KEY,
                    message_id TEXT NOT NULL UNIQUE REFERENCES messages(message_id), draft_id TEXT NOT NULL REFERENCES drafts(draft_id),
                    message_revision INTEGER NOT NULL, target_json TEXT NOT NULL, input_fingerprint TEXT NOT NULL,
                    text TEXT NOT NULL, text_sha256 TEXT NOT NULL, state TEXT NOT NULL,
                    write_started INTEGER NOT NULL DEFAULT 0, remote_id TEXT NOT NULL DEFAULT '',
                    note TEXT NOT NULL DEFAULT '', actor TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)""",
            ):
                self._db.execute(statement)
            if legacy:
                self._db.execute("UPDATE messages SET state='needs_data', last_error='импорт из 1.1: требуется новая проверка' WHERE state IN ('drafted','approved')")
                self._db.execute("UPDATE outbox SET state='superseded', hold_reason='импорт из 1.1: старое подтверждение отменено' WHERE state='held'")
            limit_columns = {r[1] for r in self._db.execute("PRAGMA table_info(api_limits)")}
            for name, definition in {"blocked_until": "REAL NOT NULL DEFAULT 0", "streak": "INTEGER NOT NULL DEFAULT 0", "last_status": "INTEGER"}.items():
                if name not in limit_columns:
                    self._db.execute(f"ALTER TABLE api_limits ADD COLUMN {name} {definition}")
            self._db.execute("CREATE TABLE IF NOT EXISTS api_limit_events(scope TEXT NOT NULL,created_at REAL NOT NULL)")
            self._db.execute("CREATE INDEX IF NOT EXISTS idx_api_limit_events ON api_limit_events(scope,created_at)")
            self._db.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")

    def close(self) -> None:
        self._db.close()

    # --- каталог и материал ---

    def upsert_product(self, article: str, name: str = "", category: str = "") -> None:
        self._db.execute(
            """INSERT INTO products(article, name, category) VALUES (?, ?, ?)
               ON CONFLICT(article) DO UPDATE SET
                 name = COALESCE(NULLIF(excluded.name, ''), products.name),
                 category = COALESCE(NULLIF(excluded.category, ''), products.category)""",
            (article, name, category),
        )
        self.commit()

    def add_material(self, article: str, topic: str, statement: str) -> None:
        self._db.execute(
            "INSERT INTO materials(article, topic, statement) VALUES (?, ?, ?)",
            (article, topic, statement),
        )

    def replace_material(self, article: str, rows: Sequence[Tuple[str, str]]) -> None:
        with self.transaction():
            previous = self.material_for(article)
            self._db.execute("DELETE FROM materials WHERE article = ?", (article,))
            for topic, statement in rows:
                self.add_material(article, topic, statement)
            if previous and previous != self.material_for(article):
                affected = self._db.execute("SELECT message_id FROM messages WHERE article=? AND state IN ('drafted','approved')", (article,)).fetchall()
                self._db.execute("""UPDATE messages SET state='needs_data', last_error='изменились материалы товара'
                    WHERE article=? AND state IN ('drafted','approved')""", (article,))
                for (message_id,) in affected:
                    self.supersede_outbox(message_id, "изменились материалы товара")
                    self.audit_event(message_id, "sources.invalidated", {"article": article, "state": "needs_data"})

    def material_for(self, article: str) -> List[Tuple[str, str]]:
        return [
            (r[0], r[1])
            for r in self._db.execute(
                "SELECT topic, statement FROM materials WHERE article = ? ORDER BY topic, id",
                (article,),
            )
        ]

    def product_name(self, article: str) -> str:
        row = self._db.execute(
            "SELECT name FROM products WHERE article = ?", (article,)
        ).fetchone()
        return row[0] if row and row[0] else ""

    # --- карта идентификаторов товара (external_id -> артикул) ---

    def map_product(self, marketplace: str, external_id: str, article: str, account_id: str = "") -> None:
        self._db.execute(
            """INSERT INTO product_map(marketplace, account_id, external_id, article)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(marketplace, account_id, external_id) DO UPDATE SET article = excluded.article""",
            (marketplace, account_id, external_id, article),
        )

    def commit(self) -> None:
        if not self._transaction_depth:
            self._db.commit()

    def lookup_article(self, marketplace: str, external_id: str, account_id: str = "") -> str:
        """Внешний id площадки -> артикул Deppa (или пусто)."""
        row = self._db.execute(
            "SELECT article FROM product_map WHERE marketplace = ? AND account_id = ? AND external_id = ?",
            (marketplace, account_id, external_id),
        ).fetchone()
        return row[0] if row else ""

    # --- сообщения ---

    def upsert_message(
        self,
        message_id: str,
        marketplace: str,
        account_id: str,
        external_id: str,
        kind: str,
        text: str,
        *,
        pros: str = "",
        cons: str = "",
        rating: Optional[int] = None,
        article: str = "",
        product_name: str = "",
        created_at: str = "",
        audit_run_id: str = "",
        received_payload=None,
    ) -> str:
        text, pros, cons, product_name = (redact(value) for value in (text, pros, cons, product_name))
        self.audit_event(message_id, "message.received", received_payload if received_payload is not None else {
            "marketplace": marketplace, "account_id": account_id, "external_id": external_id, "kind": kind,
            "text": text, "pros": pros, "cons": cons, "rating": rating, "article": article,
            "product_name": product_name, "source_created_at": created_at or None}, run_id=audit_run_id)
        if marketplace not in {"wildberries", "ozon", "yandex_market"} or kind not in {"question", "review"}:
            raise ValueError("неизвестная площадка или тип обращения")
        if not all(isinstance(v, str) and v.strip() for v in (message_id, account_id, external_id)):
            raise ValueError("нужны message_id, account_id и external_id")
        if rating is not None and (type(rating) is not int or not 1 <= rating <= 5):
            raise ValueError("оценка должна быть целым числом от 1 до 5")
        with self.transaction():
            by_id = self.get_message(message_id)
            identity = (marketplace, account_id, external_id, kind)
            if by_id and tuple(by_id[1:5]) != identity:
                raise ValueError("message_id уже принадлежит другому обращению")
            existing = self._db.execute(
                "SELECT message_id FROM messages WHERE marketplace=? AND account_id=? AND kind=? AND external_id=?",
                (marketplace, account_id, kind, external_id),
            ).fetchone()
            if existing:
                message_id = existing[0]
                if message_id != (by_id[0] if by_id else None):
                    self.audit_event(message_id, "message.identity_resolved", {"received": received_payload if received_payload is not None else {
                        "text": text, "pros": pros, "cons": cons, "rating": rating, "article": article},
                        "marketplace": marketplace, "account_id": account_id, "external_id": external_id, "kind": kind}, run_id=audit_run_id)
            old = self.get_message(message_id)
            content = (text, pros, cons, rating, article, product_name)
            if old:
                if tuple(old[5:11]) == content:
                    self.audit_event(message_id, "message.unchanged", {"revision": self.message_revision(message_id)}, run_id=audit_run_id)
                    return message_id
                self._db.execute("""UPDATE messages SET text=?, pros=?, cons=?, rating=?, article=?, product_name=?,
                    state='new', revision=revision+1, processing_token=NULL, last_error='' WHERE message_id=?""",
                    (*content, message_id))
                self.supersede_outbox(message_id, "изменилось обращение")
            else:
                self._db.execute("""INSERT INTO messages(message_id, marketplace, account_id, external_id,
                    kind, text, pros, cons, rating, article, product_name, state, created_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'new', ?)""",
                    (message_id, marketplace, account_id, external_id, kind, *content, created_at or _now()))
            self.audit_event(message_id, "message.updated" if old else "message.created",
                {"revision": self.message_revision(message_id), "article": article,
                 "marketplace": marketplace, "account_id": account_id, "external_id": external_id, "kind": kind}, run_id=audit_run_id)
            return message_id

    def set_message_state(self, message_id: str, state: str) -> None:
        if state not in {"new", "processing", "drafted", "needs_data", "rejected", "approved", "done"}:
            raise ValueError("неизвестное состояние")
        self._db.execute(
            "UPDATE messages SET state = ? WHERE message_id = ?", (state, message_id)
        )
        self.commit()

    def get_message(self, message_id: str):
        return self._db.execute(
            """SELECT message_id, marketplace, account_id, external_id, kind, text,
                      pros, cons, rating, article, product_name, state
               FROM messages WHERE message_id = ?""",
            (message_id,),
        ).fetchone()

    def messages_by_state(self, state: str) -> List[tuple]:
        return self._db.execute(
            """SELECT message_id, marketplace, kind, text, article, product_name
               FROM messages WHERE state = ? ORDER BY created_at""",
            (state,),
        ).fetchall()

    # --- черновики / подтверждение / outbox ---

    def create_draft(self, draft_id: str, message_id: str, text: str, source: str,
                     rules_sha256: str = "", material_sha256: str = "") -> None:
        revision = self.message_revision(message_id)
        self._db.execute(
            """INSERT INTO drafts(draft_id, message_id, text, source, created_at,
                message_revision, rules_sha256, material_sha256) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (draft_id, message_id, redact(text), source, _now(), revision, rules_sha256, material_sha256),
        )
        self.commit()

    def latest_draft(self, message_id: str):
        return self._db.execute(
            """SELECT d.draft_id, d.text, d.source FROM drafts d JOIN messages m ON m.message_id=d.message_id
               WHERE d.message_id = ? AND d.message_revision=m.revision ORDER BY d.rowid DESC LIMIT 1""",
            (message_id,),
        ).fetchone()

    def approve(self, draft_id: str) -> None:
        with self.transaction():
            row = self._db.execute("""SELECT m.state, m.revision, d.message_revision, m.message_id FROM drafts d
                JOIN messages m ON m.message_id=d.message_id WHERE d.draft_id=?""", (draft_id,)).fetchone()
            if not row or row[0] not in {"drafted", "approved"} or row[1] != row[2]:
                raise ValueError("черновик устарел или не готов к подтверждению")
            if self.latest_draft(row[3])[0] != draft_id:
                raise ValueError("существует более новый черновик")
            self._db.execute("INSERT OR IGNORE INTO approvals(draft_id, approved_at) VALUES (?, ?)", (draft_id, _now()))
            added = self._db.execute("""INSERT OR IGNORE INTO outbox(draft_id, state, hold_reason, created_at)
                VALUES (?, 'held', ?, ?)""", (draft_id, "локальное удержание: ожидается отдельное подтверждение отправки", _now()))
            if added.rowcount:
                self.audit_event(row[3], "outbox.held", {"draft_id": draft_id, "state": "held", "marketplace_delivery": False})

    def outbox_count(self) -> int:
        return int(self._db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0])

    # --- журнал ---

    def mark_processed(self, external_id: str, marketplace: str = "", account_id: str = "", kind: str = "", revision: int = 0) -> None:
        self._db.execute(
            "INSERT OR IGNORE INTO processing_journal VALUES (?, ?, ?, ?, ?, ?)",
            (marketplace, account_id, kind, external_id, revision, _now()),
        )
        self.commit()

    def is_processed(self, external_id: str, marketplace: str = "", account_id: str = "", kind: str = "", revision: int = 0) -> bool:
        return self._db.execute(
            "SELECT 1 FROM processing_journal WHERE marketplace=? AND account_id=? AND kind=? AND external_id=? AND revision=?",
            (marketplace, account_id, kind, external_id, revision),
        ).fetchone() is not None

    def message_revision(self, message_id: str) -> int:
        row = self._db.execute("SELECT revision FROM messages WHERE message_id=?", (message_id,)).fetchone()
        if not row:
            raise ValueError("нет сообщения")
        return row[0]

    def material_digest_for(self, article: str) -> str:
        return hashlib.sha256(json.dumps(self.material_for(article), ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()

    def claim_message(self, message_id: str, retry: bool = False, *, expected_draft_id=None):
        with self.transaction():
            row = self.get_message(message_id)
            allowed = {"drafted", "approved", "needs_data", "rejected"} if expected_draft_id else ({"new", "needs_data", "rejected"} if retry else {"new"})
            if not row or row[11] not in allowed:
                raise ValueError("обращение уже обрабатывается, обработано или требует явного retry")
            if expected_draft_id:
                latest = self.latest_draft(message_id)
                delivery = self._db.execute("SELECT write_started FROM delivery_jobs WHERE message_id=?", (message_id,)).fetchone()
                if delivery and delivery[0]:
                    raise ValueError("отправка уже началась; перегенерация запрещена")
                if not latest or latest[0] != expected_draft_id:
                    raise ValueError("черновик изменился; откройте актуальный вариант")
            revision = self.message_revision(message_id)
            explicit_repeat = bool(expected_draft_id) or (retry and row[11] in {"needs_data", "rejected"})
            if self.is_processed(row[3], row[1], row[2], row[4], revision) and not explicit_repeat:
                raise ValueError("эта версия обращения уже подтверждена")
            if explicit_repeat:
                self.supersede_outbox(message_id, "явный повтор обработки")
            token = uuid.uuid4().hex
            self._db.execute("UPDATE messages SET state='processing', processing_token=?, last_error='' WHERE message_id=?", (token, message_id))
            return row, token, revision

    def assert_claim(self, message_id: str, token: str, revision: int) -> None:
        row = self._db.execute("SELECT state, processing_token, revision FROM messages WHERE message_id=?", (message_id,)).fetchone()
        if row != ("processing", token, revision):
            raise ValueError("обращение изменено или захват обработки отменён")

    def finish_claim(self, message_id: str, token: str, revision: int, state: str, reason: str = "") -> None:
        self.assert_claim(message_id, token, revision)
        self._db.execute("UPDATE messages SET state=?, processing_token=NULL, last_error=? WHERE message_id=?", (state, redact(reason), message_id))

    def recover_message(self, message_id: str) -> None:
        with self.transaction():
            row = self.get_message(message_id)
            if not row or row[11] != "processing":
                raise ValueError("восстанавливать можно только прерванную обработку")
            self._db.execute("UPDATE messages SET state='new', processing_token=NULL, last_error='обработка отменена оператором' WHERE message_id=?", (message_id,))
            self.audit_event(message_id, "processing.recovered", {"state": "new", "message_revision": self.message_revision(message_id)})

    def message_note(self, message_id: str) -> str:
        row = self._db.execute("SELECT last_error FROM messages WHERE message_id=?", (message_id,)).fetchone()
        return row[0] if row else ""

    def draft_context(self, draft_id: str):
        return self._db.execute("SELECT rules_sha256, material_sha256 FROM drafts WHERE draft_id=?", (draft_id,)).fetchone()

    def supersede_outbox(self, message_id: str, reason: str) -> None:
        rows = self._db.execute("""SELECT o.draft_id FROM outbox o JOIN drafts d ON d.draft_id=o.draft_id
            WHERE d.message_id=? AND o.state='held'""", (message_id,)).fetchall()
        self._db.execute("""UPDATE outbox SET state='superseded', hold_reason=?
            WHERE draft_id IN (SELECT draft_id FROM drafts WHERE message_id=?) AND state='held'""", (reason, message_id))
        if rows:
            self.audit_event(message_id, "outbox.superseded", {"draft_ids": [r[0] for r in rows], "reason": reason})

    def record_review(self, message_id: str, action: str, actor: str, draft_id=None,
                      replacement_draft_id=None, note: str = "") -> int:
        if action not in {"edited", "rejected", "approved", "regenerated"} or not isinstance(actor, str) or not actor.strip():
            raise ValueError("нужны действие и имя оператора")
        actor, note = redact(actor), redact(note)
        cursor = self._db.execute("""INSERT INTO review_events
            (message_id, message_revision, action, actor, draft_id, replacement_draft_id, note, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (message_id, self.message_revision(message_id), action, actor.strip(), draft_id, replacement_draft_id, note, _now()))
        self.audit_event(message_id, "review." + action,
            {"draft_id": draft_id, "replacement_draft_id": replacement_draft_id, "note": note,
             "message_revision": self.message_revision(message_id)}, actor=actor.strip())
        return cursor.lastrowid

    def review_history(self, message_id: str):
        rows = self._db.execute("""SELECT event_id, message_revision, action, actor, draft_id,
            replacement_draft_id, note, created_at FROM review_events WHERE message_id=? ORDER BY event_id""", (message_id,)).fetchall()
        names = ["event_id", "message_revision", "action", "actor", "draft_id", "replacement_draft_id", "note", "created_at"]
        return [dict(zip(names, row)) for row in rows]

    def audit_event(self, message_id, event_type: str, payload, *, run_id: str = "", actor: str = "system") -> int:
        content = json.dumps(redact(payload), ensure_ascii=False, sort_keys=True, separators=(',', ':'))
        digest = hashlib.sha256(content.encode()).hexdigest()
        timestamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        with self.transaction():
            self._db.execute("INSERT OR IGNORE INTO audit_documents VALUES (?, ?)", (digest, content))
            cursor = self._db.execute("""INSERT INTO audit_events
                (message_id, run_id, event_type, actor, created_at, document_sha256) VALUES (?, ?, ?, ?, ?, ?)""",
                (message_id if isinstance(message_id, str) else None, run_id, event_type, redact(actor), timestamp, digest))
            return cursor.lastrowid

    def audit_trace(self, message_id=None, *, full: bool = False):
        query = "SELECT event_id, message_id, run_id, event_type, actor, created_at, document_sha256 FROM audit_events"
        if message_id is None:
            rows = self._db.execute(query + " ORDER BY event_id").fetchall()
        else:
            rows = self._db.execute(query + """ WHERE message_id=? OR (message_id IS NULL AND run_id<>'' AND run_id IN
                (SELECT run_id FROM audit_events WHERE message_id=?)) ORDER BY event_id""", (message_id, message_id)).fetchall()
        names = ["event_id", "message_id", "run_id", "event_type", "actor", "created_at", "document_sha256"]
        result = []
        for row in rows:
            item = dict(zip(names, row))
            if full:
                stored = self._db.execute("SELECT content_json FROM audit_documents WHERE sha256=?", (row[6],)).fetchone()
                item["payload"] = json.loads(stored[0])
            result.append(item)
        return result

    def verify_audit(self):
        documents = self._db.execute("SELECT sha256, content_json FROM audit_documents").fetchall()
        mismatches = sum(hashlib.sha256(content.encode()).hexdigest() != digest for digest, content in documents)
        missing = self._db.execute("""SELECT count(*) FROM audit_events e LEFT JOIN audit_documents d
            ON d.sha256=e.document_sha256 WHERE d.sha256 IS NULL""").fetchone()[0]
        return {"ok": not mismatches and not missing, "documents": len(documents),
                "events": self._db.execute("SELECT count(*) FROM audit_events").fetchone()[0],
                "mismatched_documents": mismatches, "missing_documents": missing}

    # --- мета (версии) и журнал провенанса ---

    def set_meta(self, key: str, value: str) -> None:
        self._db.execute(
            "INSERT INTO meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )
        self.commit()

    def get_meta(self, key: str, default: str = "") -> str:
        row = self._db.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return row[0] if row else default

    def add_runlog(
        self,
        message_id: str,
        article: str,
        kind: str,
        question: str,
        answer: str,
        basis_json: str,
        canon_version: str,
        rules_version: str,
        *,
        model: str = "",
        effort: str = "",
        source: str = "",
        input_tokens: int = 0,
        output_tokens: int = 0,
    ) -> int:
        cur = self._db.execute(
            """INSERT INTO runlog(message_id, article, kind, question, answer, basis,
                 canon_version, rules_version, model, effort, source, created_at,
                 message_revision, input_tokens, output_tokens)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (message_id, article, kind, redact(question), redact(answer), redact(basis_json),
             canon_version, rules_version, model, effort, source, _now(),
             self.message_revision(message_id), input_tokens, output_tokens),
        )
        self.commit()
        return int(cur.lastrowid)

    def recent_runlog(self, limit: int = 20) -> List[tuple]:
        return self._db.execute(
            """SELECT log_id, message_id, article, kind, question, answer, basis,
                      canon_version, rules_version, model, source, created_at
               FROM runlog ORDER BY log_id DESC LIMIT ?""",
            (limit,),
        ).fetchall()

    def runlog_for(self, message_id: str, limit: int = 20) -> List[tuple]:
        return self._db.execute("""SELECT log_id, message_id, article, kind, question, answer, basis,
            canon_version, rules_version, model, source, created_at FROM runlog
            WHERE message_id=? ORDER BY log_id DESC LIMIT ?""", (message_id, limit)).fetchall()

    # --- подпись канона: происхождение, ответственность, неизменяемость ---

    def _material_digest(self, digest_format: str = "json-v1") -> Tuple[str, int, int]:
        """(content_sha256, product_count, fact_count) по каноничному содержимому."""
        import hashlib

        rows = self._db.execute(
            "SELECT article, topic, statement FROM materials ORDER BY article, topic, statement"
        ).fetchall()
        blob = ("".join("|".join(r) for r in rows) if digest_format == "legacy" else
                json.dumps(rows, ensure_ascii=False, separators=(',', ':'))).encode("utf-8")
        digest = hashlib.sha256(blob).hexdigest()
        products = len({r[0] for r in rows})
        return digest, products, len(rows)

    def sign_canon(self, built_by: str, source: str) -> dict:
        """Record an approved import's checksum and provenance; this is not a digital signature."""
        if not built_by.strip() or not source.strip():
            raise ValueError("нужны источник и ответственный за импорт")
        with self.transaction():
            content_sha, products, facts = self._material_digest()
            provenance_sha = hashlib.sha256(json.dumps([built_by, source]).encode()).hexdigest()[:12]
            version = f"canon-{content_sha}-{provenance_sha}"
            signature = hashlib.sha256(f"{version}|{content_sha}|{built_by}|{source}".encode()).hexdigest()
            self._db.execute("""INSERT OR IGNORE INTO canon_manifest
                (canon_version, content_sha256, source, built_by, built_at, product_count, fact_count, signature, digest_format)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'json-v1')""",
                (version, content_sha, source, built_by, _now(), products, facts, signature))
            self.set_meta("canon_version", version)
            self.set_meta("canon_signed_by", built_by)
        return {
            "canon_version": version, "content_sha256": content_sha,
            "signature": signature, "built_by": built_by, "source": source,
            "product_count": products, "fact_count": facts,
        }

    def verify_canon(self) -> dict:
        """Проверка неизменяемости: контент базы == подписанный контент?

        Если канон мутировал после подписи — content_sha256 не сойдётся
        с манифестом; это фиксируется как подмена (tampered).
        """
        version = self.get_meta("canon_version", "")
        row = self._db.execute(
            "SELECT content_sha256, signature, built_by, source, digest_format FROM canon_manifest WHERE canon_version = ?",
            (version,),
        ).fetchone()
        if row is None:
            return {"ok": False, "status": "не подписан", "canon_version": version}
        content_sha, signature, built_by, source, digest_format = row
        current_sha, _, _ = self._material_digest(digest_format)
        expected = hashlib.sha256(f"{version}|{content_sha}|{built_by}|{source}".encode()).hexdigest()
        intact = current_sha == content_sha and signature == expected
        return {
            "ok": intact,
            "status": "цел, без мутаций" if intact else "ИЗМЕНЁН после подписи (подмена!)",
            "canon_version": version,
            "signed_sha256": content_sha,
            "current_sha256": current_sha,
            "signature": signature,
            "built_by": built_by,
            "source": source,
        }

    def get_canon_manifest(self, canon_version: str = "") -> dict:
        version = canon_version or self.get_meta("canon_version", "")
        row = self._db.execute(
            """SELECT canon_version, content_sha256, source, built_by, built_at,
                      product_count, fact_count, signature
               FROM canon_manifest WHERE canon_version = ?""",
            (version,),
        ).fetchone()
        if row is None:
            return {}
        keys = ["canon_version", "content_sha256", "source", "built_by", "built_at",
                "product_count", "fact_count", "signature"]
        return dict(zip(keys, row))


def open_store(db_path: Path) -> Store:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return Store(db_path)
