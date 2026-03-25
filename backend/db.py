"""SQLite database layer using aiosqlite."""
import json
import time
import aiosqlite

DB_PATH = "data/prices.db"


async def get_db() -> aiosqlite.Connection:
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    return db


async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS barcodes (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                barcode    TEXT UNIQUE NOT NULL,
                name       TEXT NOT NULL,
                active     INTEGER NOT NULL DEFAULT 1,
                created_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS alerts (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                barcode            TEXT NOT NULL,
                product_name       TEXT NOT NULL,
                issue              TEXT NOT NULL,
                recommended_action TEXT NOT NULL,
                severity           TEXT NOT NULL CHECK(severity IN ('red','yellow','green')),
                urgency_score      INTEGER NOT NULL DEFAULT 5,
                alert_type         TEXT NOT NULL DEFAULT 'unknown',
                resolved           INTEGER NOT NULL DEFAULT 0,
                created_at         REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS system_status (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at          REAL NOT NULL,
                finished_at         REAL,
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
                duration_s          REAL
            );

            -- ── Raw tables (mirror Shufersal XML schema, 30-day rolling) ──────
            CREATE TABLE IF NOT EXISTS price_full (
                id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                chain_id              TEXT,
                store_id              TEXT,
                item_code             TEXT NOT NULL,
                item_name             TEXT,
                item_price            REAL,
                manufacturer_name     TEXT,
                manufacturer_item_desc TEXT,
                unit_of_measure       TEXT,
                quantity              REAL,
                allow_discount        INTEGER,
                item_status           INTEGER,
                format_name           TEXT,
                source_ts             TEXT,
                source_url            TEXT,
                scraped_at            REAL NOT NULL,
                UNIQUE(item_code, format_name, source_ts)
            );

            CREATE TABLE IF NOT EXISTS promo_full (
                id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                chain_id              TEXT,
                store_id              TEXT,
                promotion_id          TEXT NOT NULL DEFAULT '',
                promotion_description TEXT,
                start_date            TEXT,
                end_date              TEXT,
                discounted_price      REAL,
                min_qty               REAL,
                item_code             TEXT NOT NULL,
                format_name           TEXT,
                source_ts             TEXT,
                source_url            TEXT,
                scraped_at            REAL NOT NULL,
                UNIQUE(promotion_id, item_code, format_name, source_ts)
            );

            CREATE TABLE IF NOT EXISTS stores (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
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
                scraped_at     REAL NOT NULL,
                UNIQUE(store_id, chain_id)
            );

            -- ── Indexes ───────────────────────────────────────────────────────
            CREATE INDEX IF NOT EXISTS idx_alerts_resolved     ON alerts(resolved);
            CREATE INDEX IF NOT EXISTS idx_runs_started        ON pipeline_runs(started_at);
            CREATE INDEX IF NOT EXISTS idx_price_full_item     ON price_full(item_code);
            CREATE INDEX IF NOT EXISTS idx_price_full_fmt_ts   ON price_full(format_name, source_ts);
            CREATE INDEX IF NOT EXISTS idx_price_full_scraped  ON price_full(scraped_at);
            CREATE INDEX IF NOT EXISTS idx_promo_full_item     ON promo_full(item_code);
            CREATE INDEX IF NOT EXISTS idx_promo_full_fmt_ts   ON promo_full(format_name, source_ts);
            CREATE INDEX IF NOT EXISTS idx_promo_full_scraped  ON promo_full(scraped_at);
            CREATE INDEX IF NOT EXISTS idx_stores_format       ON stores(format_name);
        """)
        await db.commit()

        # ── Views: current snapshot (latest source_ts per barcode+format) ────
        # Recreate each time so changes take effect without manual DROP.
        await db.execute("DROP VIEW IF EXISTS v_current_prices")
        await db.execute("""
            CREATE VIEW v_current_prices AS
            SELECT pf.item_code, pf.item_name, pf.manufacturer_name, pf.format_name,
                   pf.item_price, pf.source_ts, pf.source_url, pf.scraped_at
            FROM price_full pf
            JOIN (
                SELECT item_code, format_name, MAX(source_ts) AS max_ts
                FROM price_full GROUP BY item_code, format_name
            ) lat ON pf.item_code = lat.item_code
                 AND pf.format_name = lat.format_name
                 AND pf.source_ts   = lat.max_ts
        """)
        await db.execute("DROP VIEW IF EXISTS v_current_promos")
        await db.execute("""
            CREATE VIEW v_current_promos AS
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
                 AND pr.source_ts   = lat.max_ts
        """)
        await db.commit()

        # Safe migration: alert_type column (legacy DBs)
        try:
            await db.execute("ALTER TABLE alerts ADD COLUMN alert_type TEXT NOT NULL DEFAULT 'unknown'")
            await db.commit()
        except Exception:
            pass


# ── Key-value status store ─────────────────────────────────────────────────────

async def get_status(key: str, default: str = "") -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT value FROM system_status WHERE key = ?", (key,)
        ) as cur:
            row = await cur.fetchone()
            return row[0] if row else default


async def set_status(key: str, value: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO system_status(key, value) VALUES(?, ?)",
            (key, value),
        )
        await db.commit()


# ── Barcodes ──────────────────────────────────────────────────────────────────

async def upsert_barcode(barcode: str, name: str, active: bool = True) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO barcodes(barcode, name, active, created_at)
               VALUES(?, ?, ?, ?)
               ON CONFLICT(barcode) DO UPDATE SET name=excluded.name, active=excluded.active""",
            (barcode, name, int(active), time.time()),
        )
        await db.commit()


async def get_active_barcodes() -> list[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT barcode FROM barcodes WHERE active = 1") as cur:
            return [r[0] for r in await cur.fetchall()]


async def get_barcode_names() -> dict[str, str]:
    """Return {barcode: name} for all active barcodes."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT barcode, name FROM barcodes WHERE active = 1") as cur:
            return {r[0]: r[1] for r in await cur.fetchall()}


# ── Cache helpers (price_full-based, replaces products-based cache) ───────────

async def get_price_full_source_timestamps() -> set[tuple[str, str]]:
    """Return set of (format_name, source_ts) already stored in price_full (for pipeline cache)."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT DISTINCT format_name, source_ts FROM price_full WHERE source_ts IS NOT NULL"
        ) as cur:
            return {(r[0], r[1]) for r in await cur.fetchall()}


async def get_records_by_format_ts(pairs: list[tuple[str, str]]) -> list[dict]:
    """
    Reconstruct merged product records from price_full + promo_full for given
    (format_name, source_ts) pairs. Used by pipeline to reuse cached data for
    the Analyzer without re-downloading PriceFull files.
    """
    if not pairs:
        return []
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        results: list[dict] = []
        for fmt, ts in set(pairs):
            async with db.execute("""
                SELECT
                    pf.item_code              AS barcode,
                    pf.item_name              AS name,
                    pf.manufacturer_name      AS manufacturer,
                    pf.format_name,
                    pf.item_price             AS price,
                    MIN(pr.discounted_price)  AS promo_price,
                    MIN(pr.promotion_description) AS promo_description,
                    CASE
                        WHEN pf.item_price > 0 AND MIN(pr.discounted_price) IS NOT NULL
                        THEN ROUND((pf.item_price - MIN(pr.discounted_price))
                                   / pf.item_price * 100, 1)
                        ELSE NULL
                    END                       AS discount_pct,
                    'PriceFull'               AS file_type,
                    pf.source_url,
                    pf.source_ts
                FROM price_full pf
                LEFT JOIN promo_full pr
                    ON pr.item_code   = pf.item_code
                   AND pr.format_name = pf.format_name
                   AND pr.source_ts   = (
                       SELECT MAX(source_ts) FROM promo_full
                       WHERE item_code = pf.item_code AND format_name = pf.format_name
                   )
                WHERE pf.format_name = ? AND pf.source_ts = ?
                GROUP BY pf.item_code, pf.format_name
            """, (fmt, ts)) as cur:
                results.extend(dict(r) for r in await cur.fetchall())
        return results


# ── Product / alert counts ────────────────────────────────────────────────────

async def count_products() -> int:
    """Count distinct barcodes present in the current price_full snapshot."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(DISTINCT item_code) FROM v_current_prices"
        ) as cur:
            row = await cur.fetchone()
            return row[0] if row else 0


async def count_alerts() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM alerts WHERE resolved = 0") as cur:
            row = await cur.fetchone()
            return row[0] if row else 0


# ── Alerts ────────────────────────────────────────────────────────────────────

async def replace_alerts(alerts: list[dict]) -> None:
    """Replace open alerts with new ones, keep resolved ones."""
    now = time.time()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM alerts WHERE resolved = 0")
        await db.executemany(
            """INSERT INTO alerts
               (barcode, product_name, issue, recommended_action,
                severity, urgency_score, alert_type, resolved, created_at)
               VALUES(:barcode, :product_name, :issue, :recommended_action,
                      :severity, :urgency_score, :alert_type, 0, :created_at)""",
            [{**a, "alert_type": a.get("alert_type", "unknown"), "created_at": now} for a in alerts],
        )
        await db.commit()


# ── Pipeline run audit ────────────────────────────────────────────────────────

async def start_pipeline_run(trigger: str = "scheduled") -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO pipeline_runs(started_at, status, trigger) VALUES(?,?,?)",
            (time.time(), "running", trigger),
        )
        await db.commit()
        return cur.lastrowid


async def finish_pipeline_run(run_id: int, data: dict) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """UPDATE pipeline_runs SET
                finished_at         = :finished_at,
                status              = :status,
                shufersal_timestamp = :shufersal_timestamp,
                new_data            = :new_data,
                products_before     = :products_before,
                products_after      = :products_after,
                products_added      = :products_added,
                products_removed    = :products_removed,
                alerts_before       = :alerts_before,
                alerts_after        = :alerts_after,
                files_attempted     = :files_attempted,
                files_ok            = :files_ok,
                files_failed        = :files_failed,
                error_log           = :error_log,
                duration_s          = :duration_s
               WHERE id = :id""",
            {**data, "finished_at": time.time(), "id": run_id},
        )
        await db.commit()


async def get_pipeline_runs(limit: int = 50) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT ?", (limit,)
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]


# ── Raw table persistence ─────────────────────────────────────────────────────

async def insert_price_full_batch(records: list[dict]) -> int:
    """Insert raw PriceFull records (skip duplicates by UNIQUE constraint). Enforces 30-day retention."""
    if not records:
        return 0
    now = time.time()
    cutoff = now - 30 * 86400
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.executemany(
            """INSERT OR IGNORE INTO price_full
               (chain_id, store_id, item_code, item_name, item_price,
                manufacturer_name, manufacturer_item_desc, unit_of_measure,
                quantity, allow_discount, item_status,
                format_name, source_ts, source_url, scraped_at)
               VALUES (:chain_id, :store_id, :item_code, :item_name, :item_price,
                       :manufacturer_name, :manufacturer_item_desc, :unit_of_measure,
                       :quantity, :allow_discount, :item_status,
                       :format_name, :source_ts, :source_url, :scraped_at)""",
            [{**r, "scraped_at": now} for r in records],
        )
        inserted = cur.rowcount
        await db.execute("DELETE FROM price_full WHERE scraped_at < ?", (cutoff,))
        await db.commit()
    return inserted


async def insert_promo_full_batch(records: list[dict]) -> int:
    """Insert raw PromoFull records (skip duplicates). Enforces 30-day retention."""
    if not records:
        return 0
    now = time.time()
    cutoff = now - 30 * 86400
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.executemany(
            """INSERT OR IGNORE INTO promo_full
               (chain_id, store_id, promotion_id, promotion_description,
                start_date, end_date, discounted_price, min_qty,
                item_code, format_name, source_ts, source_url, scraped_at)
               VALUES (:chain_id, :store_id, :promotion_id, :promotion_description,
                       :start_date, :end_date, :discounted_price, :min_qty,
                       :item_code, :format_name, :source_ts, :source_url, :scraped_at)""",
            [{**r, "scraped_at": now} for r in records],
        )
        inserted = cur.rowcount
        await db.execute("DELETE FROM promo_full WHERE scraped_at < ?", (cutoff,))
        await db.commit()
    return inserted


async def replace_stores(records: list[dict]) -> int:
    """Upsert stores (UNIQUE on store_id+chain_id = always latest snapshot)."""
    if not records:
        return 0
    now = time.time()
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.executemany(
            """INSERT OR REPLACE INTO stores
               (chain_id, chain_name, sub_chain_name, sub_chain_code,
                store_id, store_name, city, address, store_type,
                latitude, longitude, format_name, scraped_at)
               VALUES (:chain_id, :chain_name, :sub_chain_name, :sub_chain_code,
                       :store_id, :store_name, :city, :address, :store_type,
                       :latitude, :longitude, :format_name, :scraped_at)""",
            [{**r, "scraped_at": now} for r in records],
        )
        inserted = cur.rowcount
        await db.commit()
    return inserted


async def get_raw_table_stats() -> dict:
    """Return row counts and date ranges for all raw and meta tables."""
    async with aiosqlite.connect(DB_PATH) as db:
        stats = {}
        for table in ("price_full", "promo_full"):
            async with db.execute(f"""
                SELECT COUNT(*), COUNT(DISTINCT item_code),
                       COUNT(DISTINCT format_name), COUNT(DISTINCT source_ts),
                       MIN(scraped_at), MAX(scraped_at)
                FROM {table}
            """) as cur:
                row = await cur.fetchone()
                stats[table] = {
                    "rows": row[0], "unique_barcodes": row[1],
                    "formats": row[2], "snapshots": row[3],
                    "oldest_scraped": row[4], "newest_scraped": row[5],
                }
        async with db.execute(
            "SELECT COUNT(*), COUNT(DISTINCT format_name), COUNT(DISTINCT city) FROM stores"
        ) as cur:
            row = await cur.fetchone()
            stats["stores"] = {"rows": row[0], "formats": row[1], "cities": row[2]}
        async with db.execute(
            "SELECT COUNT(*), COUNT(DISTINCT barcode) FROM alerts"
        ) as cur:
            row = await cur.fetchone()
            stats["alerts"] = {"rows": row[0], "unique_barcodes": row[1]}
        return stats
