
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
    PRIMARY KEY (marketplace, external_id)
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
    created_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_state ON messages(state);

CREATE TABLE IF NOT EXISTS drafts (
    draft_id     TEXT PRIMARY KEY,
    message_id   TEXT NOT NULL,
    text         TEXT NOT NULL,
    source       TEXT NOT NULL,              -- luna | fallback
    created_at   TEXT NOT NULL,
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
