-- PostgreSQL schema for Supabase
-- Run this in the Supabase SQL editor to initialise a fresh database.
-- Safe to re-run: all statements use IF NOT EXISTS / CREATE OR REPLACE.

-- ── Tables ────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS barcodes (
    id         BIGSERIAL PRIMARY KEY,
    barcode    TEXT UNIQUE NOT NULL,
    name       TEXT NOT NULL,
    active     INTEGER NOT NULL DEFAULT 1,
    created_at DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS alerts (
    id                 BIGSERIAL PRIMARY KEY,
    barcode            TEXT NOT NULL,
    product_name       TEXT NOT NULL,
    issue              TEXT NOT NULL,
    recommended_action TEXT NOT NULL,
    severity           TEXT NOT NULL CHECK(severity IN ('red','yellow','green')),
    urgency_score      INTEGER NOT NULL DEFAULT 5,
    alert_type         TEXT NOT NULL DEFAULT 'unknown',
    resolved           INTEGER NOT NULL DEFAULT 0,
    created_at         DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS system_status (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id                  BIGSERIAL PRIMARY KEY,
    started_at          DOUBLE PRECISION NOT NULL,
    finished_at         DOUBLE PRECISION,
    status              TEXT NOT NULL DEFAULT 'running',
    trigger             TEXT NOT NULL DEFAULT 'scheduled',
    shufersal_timestamp TEXT,
    new_data            INTEGER NOT NULL DEFAULT 0,
    products_before     INTEGER,
    products_after      INTEGER,
    products_added      INTEGER DEFAULT 0,
    products_removed    INTEGER DEFAULT 0,
    alerts_before       INTEGER,
    alerts_after        INTEGER,
    files_attempted     INTEGER DEFAULT 0,
    files_ok            INTEGER DEFAULT 0,
    files_failed        INTEGER DEFAULT 0,
    error_log           TEXT,
    duration_s          DOUBLE PRECISION
);

-- Raw tables mirror the Shufersal XML schema with a 30-day rolling window.

CREATE TABLE IF NOT EXISTS price_full (
    id                     BIGSERIAL PRIMARY KEY,
    chain_id               TEXT,
    store_id               TEXT,
    item_code              TEXT NOT NULL,
    item_name              TEXT,
    item_price             DOUBLE PRECISION,
    manufacturer_name      TEXT,
    manufacturer_item_desc TEXT,
    unit_of_measure        TEXT,
    quantity               DOUBLE PRECISION,
    allow_discount         INTEGER,
    item_status            INTEGER,
    format_name            TEXT,
    source_ts              TEXT,
    source_url             TEXT,
    scraped_at             DOUBLE PRECISION NOT NULL,
    UNIQUE(item_code, format_name, source_ts)
);

CREATE TABLE IF NOT EXISTS promo_full (
    id                    BIGSERIAL PRIMARY KEY,
    chain_id              TEXT,
    store_id              TEXT,
    promotion_id          TEXT NOT NULL DEFAULT '',
    promotion_description TEXT,
    start_date            TEXT,
    end_date              TEXT,
    discounted_price      DOUBLE PRECISION,
    min_qty               DOUBLE PRECISION,
    item_code             TEXT NOT NULL,
    format_name           TEXT,
    source_ts             TEXT,
    source_url            TEXT,
    scraped_at            DOUBLE PRECISION NOT NULL,
    UNIQUE(promotion_id, item_code, format_name, source_ts)
);

CREATE TABLE IF NOT EXISTS stores (
    id             BIGSERIAL PRIMARY KEY,
    chain_id       TEXT,
    chain_name     TEXT,
    sub_chain_name TEXT,
    sub_chain_code TEXT,
    store_id       TEXT,
    store_name     TEXT,
    city           TEXT,
    address        TEXT,
    store_type     TEXT,
    latitude       TEXT,
    longitude      TEXT,
    format_name    TEXT,
    scraped_at     DOUBLE PRECISION NOT NULL,
    UNIQUE(store_id, chain_id)
);

-- ── Indexes ───────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_alerts_resolved    ON alerts(resolved);
CREATE INDEX IF NOT EXISTS idx_runs_started       ON pipeline_runs(started_at);
CREATE INDEX IF NOT EXISTS idx_price_full_item    ON price_full(item_code);
CREATE INDEX IF NOT EXISTS idx_price_full_fmt_ts  ON price_full(format_name, source_ts);
CREATE INDEX IF NOT EXISTS idx_price_full_scraped ON price_full(scraped_at);
CREATE INDEX IF NOT EXISTS idx_promo_full_item    ON promo_full(item_code);
CREATE INDEX IF NOT EXISTS idx_promo_full_fmt_ts  ON promo_full(format_name, source_ts);
CREATE INDEX IF NOT EXISTS idx_promo_full_scraped ON promo_full(scraped_at);
CREATE INDEX IF NOT EXISTS idx_stores_format      ON stores(format_name);

-- ── Views ─────────────────────────────────────────────────────────────────────

-- Current snapshot: latest source_ts per barcode+format from price_full
CREATE OR REPLACE VIEW v_current_prices AS
SELECT pf.item_code, pf.item_name, pf.manufacturer_name, pf.format_name,
       pf.item_price, pf.source_ts, pf.source_url, pf.scraped_at
FROM price_full pf
JOIN (
    SELECT item_code, format_name, MAX(source_ts) AS max_ts
    FROM price_full GROUP BY item_code, format_name
) lat ON pf.item_code = lat.item_code
     AND pf.format_name = lat.format_name
     AND pf.source_ts   = lat.max_ts;

-- Current promotions: latest source_ts per barcode+format from promo_full
CREATE OR REPLACE VIEW v_current_promos AS
SELECT pr.item_code, pr.promotion_id, pr.promotion_description,
       pr.start_date, pr.end_date,
       pr.discounted_price, pr.min_qty,
       pr.format_name, pr.source_ts, pr.scraped_at
FROM promo_full pr
JOIN (
    SELECT item_code, format_name, MAX(source_ts) AS max_ts
    FROM promo_full GROUP BY item_code, format_name
) lat ON pr.item_code = lat.item_code
     AND pr.format_name = lat.format_name
     AND pr.source_ts   = lat.max_ts;
