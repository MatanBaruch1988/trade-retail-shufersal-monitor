"""PostgreSQL database layer using asyncpg - drop-in replacement for db.py.

The public API is identical to db.py so main.py requires no changes.
Connection objects are wrapped to emulate the aiosqlite cursor-based
interface used throughout main.py:

    conn = await get_db()
    async with conn.execute(sql, params) as cur:
        rows = await cur.fetchall()
    await conn.close()

SQL dialect translation is applied automatically:
  - ? placeholders -> $1, $2, ...
  - DATE(col,'unixepoch','localtime') -> TO_TIMESTAMP(col)::DATE
  - DATE(col,'unixepoch') -> TO_TIMESTAMP(col)::DATE
  - strftime('%s','now') -> EXTRACT(EPOCH FROM NOW())::BIGINT
  - INTEGER PRIMARY KEY AUTOINCREMENT -> BIGSERIAL PRIMARY KEY (schema only)
"""

import os
import re
import time

import asyncpg

# ── Connection pool ────────────────────────────────────────────────────────────

_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            os.environ["DATABASE_URL"],
            min_size=1,
            max_size=5,
        )
    return _pool


# ── SQL dialect translation ───────────────────────────────────────────────────

_DATE_UNIXEPOCH_LOCAL = re.compile(
    r"DATE\s*\(\s*([^,)]+?)\s*,\s*'unixepoch'\s*,\s*'localtime'\s*\)",
    re.IGNORECASE,
)
_DATE_UNIXEPOCH = re.compile(
    r"DATE\s*\(\s*([^,)]+?)\s*,\s*'unixepoch'\s*\)",
    re.IGNORECASE,
)
_STRFTIME_S_NOW = re.compile(
    r"strftime\s*\(\s*'%s'\s*,\s*'now'\s*\)",
    re.IGNORECASE,
)


def _translate_sql(sql: str) -> str:
    """Translate SQLite-specific SQL to PostgreSQL."""
    # DATE(col,'unixepoch','localtime') -> TO_TIMESTAMP(col)::DATE
    sql = _DATE_UNIXEPOCH_LOCAL.sub(
        lambda m: f"TO_TIMESTAMP({m.group(1).strip()})::DATE",
        sql,
    )
    # DATE(col,'unixepoch') -> TO_TIMESTAMP(col)::DATE
    sql = _DATE_UNIXEPOCH.sub(
        lambda m: f"TO_TIMESTAMP({m.group(1).strip()})::DATE",
        sql,
    )
    # strftime('%s','now') -> EXTRACT(EPOCH FROM NOW())::BIGINT
    sql = _STRFTIME_S_NOW.sub("EXTRACT(EPOCH FROM NOW())::BIGINT", sql)
    return sql


def _to_pg(sql: str) -> str:
    """Translate SQLite SQL to PostgreSQL: dialect + ? -> $N placeholders."""
    sql = _translate_sql(sql)
    # Convert ? to $1, $2, ... (skip ? inside string literals is an edge case
    # not present in this codebase, so simple replacement is safe)
    idx = 0
    result = []
    for ch in sql:
        if ch == "?":
            idx += 1
            result.append(f"${idx}")
        else:
            result.append(ch)
    return "".join(result)


# ── Cursor / connection compatibility shims ───────────────────────────────────

class _PgCursor:
    """Mimics an aiosqlite cursor for use in `async with conn.execute(...) as cur`."""

    def __init__(self, rows: list) -> None:
        self._rows = rows

    async def fetchall(self) -> list:
        return self._rows

    async def fetchone(self):
        return self._rows[0] if self._rows else None

    def __aiter__(self):
        return self._aiter()

    async def _aiter(self):
        for row in self._rows:
            yield row


class _PgCursorContext:
    """Async context manager returned by _PgConn.execute().

    Supports both:
      - `async with conn.execute(sql) as cur:` (SELECT - returns rows)
      - `await conn.execute(sql, params)` (DML - fire and forget)
    """

    def __init__(self, conn: asyncpg.Connection, sql: str, params: tuple) -> None:
        self._conn = conn
        self._sql = _to_pg(sql)
        self._params = params

    async def __aenter__(self) -> _PgCursor:
        rows = await self._conn.fetch(self._sql, *self._params)
        return _PgCursor(rows)

    async def __aexit__(self, *_) -> None:
        pass

    def __await__(self):
        return self._run_write().__await__()

    async def _run_write(self):
        await self._conn.execute(self._sql, *self._params)


class _PgConn:
    """
    Wraps an asyncpg Connection and exposes an aiosqlite-compatible interface.

    Each _PgConn runs inside a single implicit transaction that is committed
    when commit() is called, matching the SQLite connection semantics used in
    main.py (multiple DML statements then conn.commit()).

    Supports:
        async with conn.execute(sql, params) as cur: ...   (SELECT)
        await conn.execute(sql, params)                    (DML)
        await conn.executemany(sql, list_of_tuples_or_dicts)
        await conn.commit()    -- commits the current transaction and starts a new one
        await conn.close()     -- releases the connection back to the pool
        conn.row_factory = ... -- accepted, no-op (asyncpg Records are dict-like)
    """

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn
        self._tr = conn.transaction()

    async def _start(self) -> None:
        await self._tr.start()

    @property
    def row_factory(self):
        return None

    @row_factory.setter
    def row_factory(self, _):
        pass

    def execute(self, sql: str, params: tuple = ()) -> _PgCursorContext:
        return _PgCursorContext(self._conn, sql, params)

    async def executemany(self, sql: str, seq_of_params) -> None:
        pg_sql = _to_pg(sql)
        param_list = _coerce_params(sql, seq_of_params)
        await self._conn.executemany(pg_sql, param_list)

    async def commit(self) -> None:
        """Commit the current transaction and start a fresh one."""
        await self._tr.commit()
        self._tr = self._conn.transaction()
        await self._tr.start()

    async def close(self) -> None:
        """Commit any pending work and release the connection back to the pool."""
        try:
            await self._tr.commit()
        except Exception:
            try:
                await self._tr.rollback()
            except Exception:
                pass
        pool = await get_pool()
        await pool.release(self._conn)


def _coerce_params(original_sql: str, seq_of_params) -> list[tuple]:
    """Convert named-dict params (:name) or bare dicts to positional tuples."""
    result = []
    names = re.findall(r":(\w+)", original_sql)
    for params in seq_of_params:
        if isinstance(params, dict):
            if names:
                result.append(tuple(params[n] for n in names))
            else:
                result.append(tuple(params.values()))
        else:
            result.append(tuple(params))
    return result


# ── Public get_db() ───────────────────────────────────────────────────────────

async def get_db() -> _PgConn:
    """Return a wrapped asyncpg connection with an active transaction.

    The caller owns the connection and must call:
        await conn.commit()   -- to persist DML changes
        await conn.close()    -- to release back to the pool (also commits pending work)
    """
    pool = await get_pool()
    raw = await pool.acquire()
    pg_conn = _PgConn(raw)
    await pg_conn._start()
    return pg_conn


# ── init_db ───────────────────────────────────────────────────────────────────

async def init_db() -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS barcodes (
                    id         BIGSERIAL PRIMARY KEY,
                    barcode    TEXT UNIQUE NOT NULL,
                    name       TEXT NOT NULL,
                    active     INTEGER NOT NULL DEFAULT 1,
                    created_at DOUBLE PRECISION NOT NULL
                )
            """)

            await conn.execute("""
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
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS system_status (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)

            await conn.execute("""
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
                )
            """)

            await conn.execute("""
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
                )
            """)

            await conn.execute("""
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
                )
            """)

            await conn.execute("""
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
                )
            """)

            for stmt in [
                "CREATE INDEX IF NOT EXISTS idx_alerts_resolved    ON alerts(resolved)",
                "CREATE INDEX IF NOT EXISTS idx_runs_started       ON pipeline_runs(started_at)",
                "CREATE INDEX IF NOT EXISTS idx_price_full_item    ON price_full(item_code)",
                "CREATE INDEX IF NOT EXISTS idx_price_full_fmt_ts  ON price_full(format_name, source_ts)",
                "CREATE INDEX IF NOT EXISTS idx_price_full_scraped ON price_full(scraped_at)",
                "CREATE INDEX IF NOT EXISTS idx_promo_full_item    ON promo_full(item_code)",
                "CREATE INDEX IF NOT EXISTS idx_promo_full_fmt_ts  ON promo_full(format_name, source_ts)",
                "CREATE INDEX IF NOT EXISTS idx_promo_full_scraped ON promo_full(scraped_at)",
                "CREATE INDEX IF NOT EXISTS idx_stores_format      ON stores(format_name)",
            ]:
                await conn.execute(stmt)

        # Views outside the transaction so CREATE OR REPLACE works
        await conn.execute("""
            CREATE OR REPLACE VIEW v_current_prices AS
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

        await conn.execute("""
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
                 AND pr.source_ts   = lat.max_ts
        """)

        # Safe migration: alert_type column (legacy DBs)
        try:
            await conn.execute(
                "ALTER TABLE alerts ADD COLUMN alert_type TEXT NOT NULL DEFAULT 'unknown'"
            )
        except Exception:
            pass


# ── Key-value status store ─────────────────────────────────────────────────────

async def get_status(key: str, default: str = "") -> str:
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT value FROM system_status WHERE key = $1", key
    )
    return row["value"] if row else default


async def set_status(key: str, value: str) -> None:
    pool = await get_pool()
    await pool.execute(
        """INSERT INTO system_status(key, value) VALUES($1, $2)
           ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value""",
        key, value,
    )


# ── Barcodes ──────────────────────────────────────────────────────────────────

async def upsert_barcode(barcode: str, name: str, active: bool = True) -> None:
    pool = await get_pool()
    await pool.execute(
        """INSERT INTO barcodes(barcode, name, active, created_at)
           VALUES($1, $2, $3, $4)
           ON CONFLICT(barcode) DO UPDATE SET name=EXCLUDED.name, active=EXCLUDED.active""",
        barcode, name, int(active), time.time(),
    )


async def get_active_barcodes() -> list[str]:
    pool = await get_pool()
    rows = await pool.fetch("SELECT barcode FROM barcodes WHERE active = 1")
    return [r["barcode"] for r in rows]


async def get_barcode_names() -> dict[str, str]:
    """Return {barcode: name} for all active barcodes."""
    pool = await get_pool()
    rows = await pool.fetch("SELECT barcode, name FROM barcodes WHERE active = 1")
    return {r["barcode"]: r["name"] for r in rows}


# ── Cache helpers ─────────────────────────────────────────────────────────────

async def get_price_full_source_timestamps() -> set[tuple[str, str]]:
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT DISTINCT format_name, source_ts FROM price_full WHERE source_ts IS NOT NULL"
    )
    return {(r["format_name"], r["source_ts"]) for r in rows}


async def get_records_by_format_ts(pairs: list[tuple[str, str]]) -> list[dict]:
    if not pairs:
        return []
    pool = await get_pool()
    results: list[dict] = []
    for fmt, ts in set(pairs):
        rows = await pool.fetch("""
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
                    THEN ROUND(CAST((pf.item_price - MIN(pr.discounted_price))
                               / pf.item_price * 100 AS NUMERIC), 1)
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
            WHERE pf.format_name = $1 AND pf.source_ts = $2
            GROUP BY pf.item_code, pf.format_name, pf.item_name,
                     pf.manufacturer_name, pf.item_price, pf.source_url, pf.source_ts
        """, fmt, ts)
        results.extend(dict(r) for r in rows)
    return results


# ── Product / alert counts ────────────────────────────────────────────────────

async def count_products() -> int:
    pool = await get_pool()
    val = await pool.fetchval("SELECT COUNT(DISTINCT item_code) FROM v_current_prices")
    return val or 0


async def count_alerts() -> int:
    pool = await get_pool()
    val = await pool.fetchval("SELECT COUNT(*) FROM alerts WHERE resolved = 0")
    return val or 0


# ── Alerts ────────────────────────────────────────────────────────────────────

async def replace_alerts(alerts: list[dict]) -> None:
    now = time.time()
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DELETE FROM alerts WHERE resolved = 0")
            if alerts:
                await conn.executemany(
                    """INSERT INTO alerts
                       (barcode, product_name, issue, recommended_action,
                        severity, urgency_score, alert_type, resolved, created_at)
                       VALUES($1, $2, $3, $4, $5, $6, $7, 0, $8)""",
                    [
                        (
                            a["barcode"],
                            a["product_name"],
                            a["issue"],
                            a["recommended_action"],
                            a["severity"],
                            a["urgency_score"],
                            a.get("alert_type", "unknown"),
                            now,
                        )
                        for a in alerts
                    ],
                )


# ── Pipeline run audit ────────────────────────────────────────────────────────

async def start_pipeline_run(trigger: str = "scheduled") -> int:
    pool = await get_pool()
    row = await pool.fetchrow(
        "INSERT INTO pipeline_runs(started_at, status, trigger) VALUES($1, $2, $3) RETURNING id",
        time.time(), "running", trigger,
    )
    return row["id"]


async def finish_pipeline_run(run_id: int, data: dict) -> None:
    pool = await get_pool()
    await pool.execute(
        """UPDATE pipeline_runs SET
            finished_at         = $1,
            status              = $2,
            shufersal_timestamp = $3,
            new_data            = $4,
            products_before     = $5,
            products_after      = $6,
            products_added      = $7,
            products_removed    = $8,
            alerts_before       = $9,
            alerts_after        = $10,
            files_attempted     = $11,
            files_ok            = $12,
            files_failed        = $13,
            error_log           = $14,
            duration_s          = $15
           WHERE id = $16""",
        time.time(),
        data.get("status"),
        data.get("shufersal_timestamp"),
        data.get("new_data"),
        data.get("products_before"),
        data.get("products_after"),
        data.get("products_added"),
        data.get("products_removed"),
        data.get("alerts_before"),
        data.get("alerts_after"),
        data.get("files_attempted"),
        data.get("files_ok"),
        data.get("files_failed"),
        data.get("error_log"),
        data.get("duration_s"),
        run_id,
    )


async def get_pipeline_runs(limit: int = 50) -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT $1", limit
    )
    return [dict(r) for r in rows]


# ── Raw table persistence ─────────────────────────────────────────────────────

async def insert_price_full_batch(records: list[dict]) -> int:
    if not records:
        return 0
    now = time.time()
    cutoff = now - 30 * 86400
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            rows_to_insert = [
                (
                    r.get("chain_id"), r.get("store_id"), r["item_code"],
                    r.get("item_name"), r.get("item_price"),
                    r.get("manufacturer_name"), r.get("manufacturer_item_desc"),
                    r.get("unit_of_measure"), r.get("quantity"),
                    r.get("allow_discount"), r.get("item_status"),
                    r.get("format_name"), r.get("source_ts"), r.get("source_url"),
                    now,
                )
                for r in records
            ]
            await conn.executemany(
                """INSERT INTO price_full
                   (chain_id, store_id, item_code, item_name, item_price,
                    manufacturer_name, manufacturer_item_desc, unit_of_measure,
                    quantity, allow_discount, item_status,
                    format_name, source_ts, source_url, scraped_at)
                   VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
                   ON CONFLICT (item_code, format_name, source_ts) DO NOTHING""",
                rows_to_insert,
            )
            await conn.execute(
                "DELETE FROM price_full WHERE scraped_at < $1", cutoff
            )
    return len(records)


async def insert_promo_full_batch(records: list[dict]) -> int:
    if not records:
        return 0
    now = time.time()
    cutoff = now - 30 * 86400
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            rows_to_insert = [
                (
                    r.get("chain_id"), r.get("store_id"),
                    r.get("promotion_id", ""), r.get("promotion_description"),
                    r.get("start_date"), r.get("end_date"),
                    r.get("discounted_price"), r.get("min_qty"),
                    r["item_code"], r.get("format_name"),
                    r.get("source_ts"), r.get("source_url"),
                    now,
                )
                for r in records
            ]
            await conn.executemany(
                """INSERT INTO promo_full
                   (chain_id, store_id, promotion_id, promotion_description,
                    start_date, end_date, discounted_price, min_qty,
                    item_code, format_name, source_ts, source_url, scraped_at)
                   VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
                   ON CONFLICT (promotion_id, item_code, format_name, source_ts) DO NOTHING""",
                rows_to_insert,
            )
            await conn.execute(
                "DELETE FROM promo_full WHERE scraped_at < $1", cutoff
            )
    return len(records)


async def replace_stores(records: list[dict]) -> int:
    if not records:
        return 0
    now = time.time()
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            rows_to_insert = [
                (
                    r.get("chain_id"), r.get("chain_name"),
                    r.get("sub_chain_name"), r.get("sub_chain_code"),
                    r.get("store_id"), r.get("store_name"),
                    r.get("city"), r.get("address"), r.get("store_type"),
                    r.get("latitude"), r.get("longitude"),
                    r.get("format_name"), now,
                )
                for r in records
            ]
            await conn.executemany(
                """INSERT INTO stores
                   (chain_id, chain_name, sub_chain_name, sub_chain_code,
                    store_id, store_name, city, address, store_type,
                    latitude, longitude, format_name, scraped_at)
                   VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
                   ON CONFLICT(store_id, chain_id)
                   DO UPDATE SET
                       chain_name=EXCLUDED.chain_name,
                       sub_chain_name=EXCLUDED.sub_chain_name,
                       sub_chain_code=EXCLUDED.sub_chain_code,
                       store_name=EXCLUDED.store_name,
                       city=EXCLUDED.city,
                       address=EXCLUDED.address,
                       store_type=EXCLUDED.store_type,
                       latitude=EXCLUDED.latitude,
                       longitude=EXCLUDED.longitude,
                       format_name=EXCLUDED.format_name,
                       scraped_at=EXCLUDED.scraped_at""",
                rows_to_insert,
            )
    return len(records)


async def get_raw_table_stats() -> dict:
    pool = await get_pool()
    stats = {}
    for table in ("price_full", "promo_full"):
        row = await pool.fetchrow(f"""
            SELECT COUNT(*) AS rows,
                   COUNT(DISTINCT item_code) AS unique_barcodes,
                   COUNT(DISTINCT format_name) AS formats,
                   COUNT(DISTINCT source_ts) AS snapshots,
                   MIN(scraped_at) AS oldest_scraped,
                   MAX(scraped_at) AS newest_scraped
            FROM {table}
        """)
        stats[table] = {
            "rows": row["rows"],
            "unique_barcodes": row["unique_barcodes"],
            "formats": row["formats"],
            "snapshots": row["snapshots"],
            "oldest_scraped": row["oldest_scraped"],
            "newest_scraped": row["newest_scraped"],
        }
    row = await pool.fetchrow(
        "SELECT COUNT(*) AS rows, COUNT(DISTINCT format_name) AS formats, "
        "COUNT(DISTINCT city) AS cities FROM stores"
    )
    stats["stores"] = {"rows": row["rows"], "formats": row["formats"], "cities": row["cities"]}
    row = await pool.fetchrow(
        "SELECT COUNT(*) AS rows, COUNT(DISTINCT barcode) AS unique_barcodes FROM alerts"
    )
    stats["alerts"] = {"rows": row["rows"], "unique_barcodes": row["unique_barcodes"]}
    return stats
