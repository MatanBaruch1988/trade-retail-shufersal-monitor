"""One-time migration: copy data/prices.db (SQLite) -> Supabase (PostgreSQL).

Usage:
    export DATABASE_URL="postgresql://user:pass@host:5432/dbname"
    python scripts/migrate_sqlite_to_supabase.py

    # Optional: specify a different SQLite file
    python scripts/migrate_sqlite_to_supabase.py --sqlite data/prices.db

The script:
  1. Reads every row from each table in the SQLite file.
  2. Inserts them into the target PostgreSQL database in batches.
  3. Uses ON CONFLICT DO NOTHING so re-running is idempotent.
  4. Prints per-table progress.

Requirements: aiosqlite, asyncpg (both in requirements.txt after update).
"""

import argparse
import asyncio
import os
import sys
import time

import aiosqlite
import asyncpg


BATCH_SIZE = 500

TABLES = [
    "barcodes",
    "alerts",
    "system_status",
    "pipeline_runs",
    "price_full",
    "promo_full",
    "stores",
]

# INSERT ... ON CONFLICT DO NOTHING statements for each table
INSERT_SQL: dict[str, str] = {
    "barcodes": """
        INSERT INTO barcodes (id, barcode, name, active, created_at)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (barcode) DO NOTHING
    """,
    "alerts": """
        INSERT INTO alerts (id, barcode, product_name, issue, recommended_action,
                            severity, urgency_score, alert_type, resolved, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT DO NOTHING
    """,
    "system_status": """
        INSERT INTO system_status (key, value)
        VALUES ($1, $2)
        ON CONFLICT (key) DO NOTHING
    """,
    "pipeline_runs": """
        INSERT INTO pipeline_runs (id, started_at, finished_at, status, trigger,
                                   shufersal_timestamp, new_data, products_before,
                                   products_after, products_added, products_removed,
                                   alerts_before, alerts_after, files_attempted,
                                   files_ok, files_failed, error_log, duration_s)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
        ON CONFLICT DO NOTHING
    """,
    "price_full": """
        INSERT INTO price_full (id, chain_id, store_id, item_code, item_name, item_price,
                                manufacturer_name, manufacturer_item_desc, unit_of_measure,
                                quantity, allow_discount, item_status, format_name,
                                source_ts, source_url, scraped_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
        ON CONFLICT (item_code, format_name, source_ts) DO NOTHING
    """,
    "promo_full": """
        INSERT INTO promo_full (id, chain_id, store_id, promotion_id, promotion_description,
                                start_date, end_date, discounted_price, min_qty, item_code,
                                format_name, source_ts, source_url, scraped_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
        ON CONFLICT (promotion_id, item_code, format_name, source_ts) DO NOTHING
    """,
    "stores": """
        INSERT INTO stores (id, chain_id, chain_name, sub_chain_name, sub_chain_code,
                            store_id, store_name, city, address, store_type,
                            latitude, longitude, format_name, scraped_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
        ON CONFLICT (store_id, chain_id) DO NOTHING
    """,
}

# Column lists in the same order as the INSERT $N params above
COLUMNS: dict[str, list[str]] = {
    "barcodes":      ["id", "barcode", "name", "active", "created_at"],
    "alerts":        ["id", "barcode", "product_name", "issue", "recommended_action",
                      "severity", "urgency_score", "alert_type", "resolved", "created_at"],
    "system_status": ["key", "value"],
    "pipeline_runs": ["id", "started_at", "finished_at", "status", "trigger",
                      "shufersal_timestamp", "new_data", "products_before",
                      "products_after", "products_added", "products_removed",
                      "alerts_before", "alerts_after", "files_attempted",
                      "files_ok", "files_failed", "error_log", "duration_s"],
    "price_full":    ["id", "chain_id", "store_id", "item_code", "item_name", "item_price",
                      "manufacturer_name", "manufacturer_item_desc", "unit_of_measure",
                      "quantity", "allow_discount", "item_status", "format_name",
                      "source_ts", "source_url", "scraped_at"],
    "promo_full":    ["id", "chain_id", "store_id", "promotion_id", "promotion_description",
                      "start_date", "end_date", "discounted_price", "min_qty", "item_code",
                      "format_name", "source_ts", "source_url", "scraped_at"],
    "stores":        ["id", "chain_id", "chain_name", "sub_chain_name", "sub_chain_code",
                      "store_id", "store_name", "city", "address", "store_type",
                      "latitude", "longitude", "format_name", "scraped_at"],
}


async def count_rows(sqlite_db: aiosqlite.Connection, table: str) -> int:
    try:
        async with sqlite_db.execute(f"SELECT COUNT(*) FROM {table}") as cur:
            row = await cur.fetchone()
            return row[0] if row else 0
    except Exception:
        return 0


async def migrate_table(
    sqlite_db: aiosqlite.Connection,
    pg_pool: asyncpg.Pool,
    table: str,
) -> int:
    cols = COLUMNS[table]
    sql  = INSERT_SQL[table].strip()
    col_list = ", ".join(cols)

    total_rows = await count_rows(sqlite_db, table)
    if total_rows == 0:
        print(f"  {table}: empty - skipping")
        return 0

    inserted = 0
    batch: list[tuple] = []
    t0 = time.time()

    async with sqlite_db.execute(f"SELECT {col_list} FROM {table}") as cur:
        async for row in cur:
            batch.append(tuple(row))
            if len(batch) >= BATCH_SIZE:
                await pg_pool.executemany(sql, batch)
                inserted += len(batch)
                elapsed = time.time() - t0
                pct = inserted / total_rows * 100 if total_rows else 0
                print(f"  {table}: {inserted}/{total_rows} rows ({pct:.0f}%) - {elapsed:.1f}s", end="\r")
                batch = []

        if batch:
            await pg_pool.executemany(sql, batch)
            inserted += len(batch)

    elapsed = time.time() - t0
    print(f"  {table}: {inserted}/{total_rows} rows - done in {elapsed:.1f}s          ")
    return inserted


async def main(sqlite_path: str, database_url: str) -> None:
    if not os.path.exists(sqlite_path):
        print(f"ERROR: SQLite file not found: {sqlite_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Source : {sqlite_path}")
    print(f"Target : {database_url[:40]}...")
    print()

    print("Connecting to PostgreSQL...")
    pg_pool = await asyncpg.create_pool(database_url, min_size=1, max_size=5)

    print("Opening SQLite database...")
    async with aiosqlite.connect(sqlite_path) as sqlite_db:
        sqlite_db.row_factory = aiosqlite.Row

        total_inserted = 0
        t_start = time.time()

        for table in TABLES:
            n = await migrate_table(sqlite_db, pg_pool, table)
            total_inserted += n

    await pg_pool.close()

    elapsed = time.time() - t_start
    print()
    print(f"Migration complete: {total_inserted} rows total in {elapsed:.1f}s")
    print()
    print("Next steps:")
    print("  1. Refresh the views in Supabase SQL editor (or run init_db() via the app).")
    print("  2. Reset sequences so future inserts don't collide with migrated IDs:")
    for table in ["barcodes", "alerts", "pipeline_runs", "price_full", "promo_full", "stores"]:
        print(f"     SELECT setval(pg_get_serial_sequence('{table}','id'), COALESCE(MAX(id),1)) FROM {table};")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate SQLite -> Supabase PostgreSQL")
    parser.add_argument(
        "--sqlite",
        default=os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "data", "prices.db",
        ),
        help="Path to the SQLite database file (default: data/prices.db)",
    )
    args = parser.parse_args()

    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url.startswith("postgresql"):
        print("ERROR: Set DATABASE_URL to a postgresql:// connection string.", file=sys.stderr)
        sys.exit(1)

    asyncio.run(main(args.sqlite, db_url))
