"""FastAPI main application — API routes + static frontend serving."""
import io
import json
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path

import aiosqlite
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Body, Header, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from backend import db
from backend.agents import parser_agent, scraper_agent
from backend.agents.orchestrator import run_pipeline
from backend.constants import FORMAT_KEYWORDS, map_consumer_format
# APScheduler removed — scheduling handled by Vercel Cron (vercel.json)

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
logger = logging.getLogger(__name__)

_pipeline_running = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init_db()
    yield


app = FastAPI(title="Tempo Price Monitor", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ──────────────────────────────────────────────────────────────────────────────
# System status
# ──────────────────────────────────────────────────────────────────────────────

BUILD_VERSION = "6eaa0c8"  # updated per deploy for version tracking


@app.get("/api/status")
async def get_status():
    last_refresh = await db.get_status("last_refresh_at", "")
    last_ts      = await db.get_status("last_shufersal_timestamp", "")

    conn = await db.get_db()
    try:
        async with conn.execute("SELECT COUNT(*) FROM barcodes WHERE active=1") as cur:
            barcode_count = (await cur.fetchone())[0]
        # Current snapshot: distinct barcodes in v_current_prices
        async with conn.execute("SELECT COUNT(DISTINCT item_code) FROM v_current_prices") as cur:
            product_count = (await cur.fetchone())[0]
        async with conn.execute("SELECT COUNT(*) FROM alerts WHERE resolved=0") as cur:
            alert_count = (await cur.fetchone())[0]
    finally:
        await conn.close()

    stale = True
    if last_refresh:
        stale = (time.time() - float(last_refresh)) > 86400

    return {
        "last_refresh_at": last_refresh,
        "last_shufersal_timestamp": last_ts,
        "is_stale": stale,
        "barcode_count": barcode_count,
        "product_count": product_count,
        "open_alert_count": alert_count,
        "next_cron": "21:00 Israel time (Vercel Cron)",
        "pipeline_running": _pipeline_running,
        "build_version": BUILD_VERSION,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Manual refresh
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/api/debug/promo-sample")
async def debug_promo_sample():
    """Return sample promo_full rows with discounted_price=10 to inspect promotion_description."""
    conn = await db.get_db()
    try:
        async with conn.execute("""
            SELECT item_code, format_name, promotion_id, promotion_description,
                   discounted_price, start_date, end_date, source_ts
            FROM promo_full
            WHERE discounted_price = 10
            LIMIT 20
        """) as cur:
            rows = [dict(r) for r in await cur.fetchall()]
        async with conn.execute("""
            SELECT COUNT(*) AS cnt, promotion_description, promotion_id
            FROM promo_full WHERE discounted_price = 10
            GROUP BY promotion_description, promotion_id
            ORDER BY cnt DESC LIMIT 10
        """) as cur:
            groups = [dict(r) for r in await cur.fetchall()]
        return {"sample": rows, "by_description": groups}
    finally:
        await conn.close()


@app.delete("/api/debug/promo-sbox")
async def delete_sbox_promos():
    """Delete all SBOX / credit-card wallet promos from promo_full."""
    pool = await db.get_pool()
    result = await pool.execute("""
        DELETE FROM promo_full
        WHERE promotion_description ILIKE '%SBOX%'
           OR promotion_description ILIKE '%כ.אשראי%'
    """)
    deleted = int(result.split()[-1]) if result else -1
    return {"status": "ok", "deleted": deleted}


@app.post("/api/refresh")
async def manual_refresh(force: bool = True):
    global _pipeline_running
    if _pipeline_running:
        return {"status": "already_running"}
    _pipeline_running = True
    try:
        return await run_pipeline(force=force, trigger="manual")
    finally:
        _pipeline_running = False


@app.post("/api/cron/run-pipeline")
async def cron_run_pipeline(authorization: str = Header(default="")):
    """Called by Vercel Cron Jobs at 08:00 / 15:00 / 22:00 Israel time.
    Vercel passes Authorization: Bearer <CRON_SECRET> automatically.
    """
    import os
    cron_secret = os.environ.get("CRON_SECRET", "")
    if cron_secret and authorization != f"Bearer {cron_secret}":
        raise HTTPException(status_code=401, detail="Unauthorized")

    global _pipeline_running
    if _pipeline_running:
        return {"status": "already_running"}
    _pipeline_running = True
    try:
        result = await run_pipeline(force=True, trigger="cron")
        return {"status": "ok", "result": result}
    finally:
        _pipeline_running = False


# ──────────────────────────────────────────────────────────────────────────────
# Shared filter helper
# ──────────────────────────────────────────────────────────────────────────────

async def _active_filters() -> tuple[set[str], set[str]]:
    """Return (disabled_formats, active_barcodes)."""
    disabled  = set(json.loads(await db.get_status("disabled_formats", "[]")))
    active_bc = set(await db.get_active_barcodes())
    return disabled, active_bc


# ──────────────────────────────────────────────────────────────────────────────
# Action Queue (alerts)
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/api/action-queue")
async def get_action_queue():
    _, active_bc = await _active_filters()
    conn = await db.get_db()
    try:
        async with conn.execute(
            """SELECT id, barcode, product_name, issue, recommended_action,
                      severity, urgency_score, alert_type, resolved, created_at
               FROM alerts WHERE resolved = 0
               ORDER BY urgency_score DESC"""
        ) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows if r["barcode"] in active_bc]
    finally:
        await conn.close()


@app.post("/api/action/{alert_id}/resolve")
async def resolve_alert(alert_id: int):
    conn = await db.get_db()
    try:
        await conn.execute("UPDATE alerts SET resolved=1 WHERE id=?", (alert_id,))
        await conn.commit()
        return {"ok": True}
    finally:
        await conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# Insights
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/api/insights")
async def get_insights():
    return {
        "insights": json.loads(await db.get_status("insights", "{}")),
        "kpis":     json.loads(await db.get_status("kpis",     "{}")),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Presence matrix  (price_full ⋈ promo_full)
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/api/presence")
async def get_presence():
    import statistics as _stats
    disabled_fmt, active_bc = await _active_filters()
    conn = await db.get_db()
    try:
        # Per-store latest price for each (barcode, format)
        async with conn.execute("""
            SELECT
                pf.item_code,
                COALESCE(b.name, pf.item_name, pf.item_code) AS name,
                pf.format_name,
                pf.store_id,
                pf.item_price,
                COALESCE(s.store_name, '') AS store_name,
                COALESCE(s.city, '')       AS city
            FROM price_full pf
            JOIN (
                SELECT item_code, format_name, store_id, MAX(source_ts) AS max_ts
                FROM price_full
                WHERE scraped_at > strftime('%s','now') - 86400
                GROUP BY item_code, format_name, store_id
            ) latest ON pf.item_code   = latest.item_code
                    AND pf.format_name = latest.format_name
                    AND pf.store_id    = latest.store_id
                    AND pf.source_ts   = latest.max_ts
            LEFT JOIN barcodes b ON b.barcode = pf.item_code
            LEFT JOIN stores s ON s.store_id = LTRIM(pf.store_id, '0')
            ORDER BY name, pf.format_name, pf.item_price DESC
        """) as cur:
            price_rows = await cur.fetchall()

        # Latest promo per (barcode, format, store) — per-store promo prices
        async with conn.execute("""
            SELECT
                pf.item_code, pf.format_name, pf.store_id,
                pf.discounted_price, pf.min_qty,
                pf.promotion_description,
                pf.start_date, pf.end_date,
                COALESCE(s.store_name, '') AS store_name,
                COALESCE(s.city, '')       AS city,
                COALESCE(s.format_name, s.sub_chain_name) AS store_true_format
            FROM promo_full pf
            JOIN (
                SELECT item_code, format_name, store_id, MAX(source_ts) AS max_ts
                FROM promo_full
                WHERE scraped_at > strftime('%s','now') - 86400
                  AND discounted_price IS NOT NULL
                  AND (promotion_description IS NULL
                       OR (promotion_description NOT ILIKE '%SBOX%'
                           AND promotion_description NOT ILIKE '%כ.אשראי%'))
                GROUP BY item_code, format_name, store_id
            ) latest ON pf.item_code   = latest.item_code
                    AND pf.format_name = latest.format_name
                    AND pf.store_id    = latest.store_id
                    AND pf.source_ts   = latest.max_ts
            LEFT JOIN stores s ON s.store_id = LTRIM(pf.store_id, '0')
            ORDER BY pf.item_code, pf.format_name, pf.discounted_price DESC
        """) as cur:
            promo_rows = await cur.fetchall()
    finally:
        await conn.close()

    # Index promos — keep best (lowest unit price) per store + collect all distinct promos
    from collections import defaultdict
    promo_store_best: dict[tuple, dict] = {}          # (bc, fmt, store_id) → best row
    promo_all_types: dict[tuple, list] = defaultdict(list)  # (bc, fmt) → distinct promos
    seen_promos: set = set()

    for r in promo_rows:
        # Skip credit-card / loyalty promos (no min_qty = not a per-unit price deal)
        if not r["min_qty"]:
            continue
        # Skip cross-chain promos: derive true chain from store_name and compare to promo format
        if r["store_name"]:
            derived_fmt = map_consumer_format(r["store_name"])
            if derived_fmt in FORMAT_KEYWORDS and derived_fmt != r["format_name"]:
                continue
        key2 = (r["item_code"], r["format_name"])
        key3 = (r["item_code"], r["format_name"], r["store_id"])
        qty  = r["min_qty"] if r["min_qty"] and r["min_qty"] > 1 else 1
        unit = round(r["discounted_price"] / qty, 2)

        # Best promo per store
        existing = promo_store_best.get(key3)
        if existing is None or unit < existing["price"]:
            promo_store_best[key3] = {
                "store_id":    r["store_id"],
                "store_name":  r["store_name"],
                "city":        r["city"],
                "price":       unit,
                "total_price": r["discounted_price"],
                "min_qty":     qty if qty > 1 else None,
            }

        # All distinct promos (deduplicated by description+price+qty)
        dedup = (key2, r["promotion_description"], r["discounted_price"], qty)
        if dedup not in seen_promos:
            seen_promos.add(dedup)
            promo_all_types[key2].append({
                "desc":       r["promotion_description"],
                "start_date": r["start_date"],
                "end_date":   r["end_date"],
                "total_price": r["discounted_price"],
                "min_qty":    qty if qty > 1 else None,
                "unit_price": unit,
            })

    # Build per-(barcode, format) store lists from best-per-store
    promo_buckets: dict[tuple, list] = defaultdict(list)
    for (bc, fmt, _sid), store_data in promo_store_best.items():
        promo_buckets[(bc, fmt)].append(store_data)

    # Collect per-store catalog rows into (barcode, format) buckets
    buckets: dict[tuple, list] = defaultdict(list)
    names: dict[str, str] = {}
    for r in price_rows:
        bc, fmt = r["item_code"], r["format_name"]
        if bc not in active_bc or fmt in disabled_fmt:
            continue
        names[bc] = r["name"]
        buckets[(bc, fmt)].append({
            "store_id":   r["store_id"],
            "store_name": r["store_name"],
            "city":       r["city"],
            "price":      r["item_price"],
        })

    formats_set: set[str] = set()
    products: dict[str, dict] = {}
    for (bc, fmt), store_list in buckets.items():
        prices = [s["price"] for s in store_list if s["price"] is not None]
        if not prices:
            continue
        formats_set.add(fmt)
        if bc not in products:
            products[bc] = {"barcode": bc, "name": names[bc], "formats": {}}
        sorted_stores = sorted(store_list, key=lambda s: (s["price"] or 0), reverse=True)
        n = len(prices)
        median = round(_stats.median(prices), 2) if prices else None

        # Promo statistics (separate from catalog)
        p_stores = sorted(promo_buckets.get((bc, fmt), []), key=lambda s: (s["price"] or 0), reverse=True)
        p_prices = [s["price"] for s in p_stores if s["price"] is not None]
        # All distinct promos sorted by unit price
        p_list = sorted(promo_all_types.get((bc, fmt), []), key=lambda p: p["unit_price"])
        promo_stats: dict = {}
        if p_prices:
            pn = len(p_prices)
            promo_stats = {
                "promo_min":          round(min(p_prices), 2),
                "promo_max":          round(max(p_prices), 2),
                "promo_avg":          round(sum(p_prices) / pn, 2),
                "promo_median":       round(_stats.median(p_prices), 2),
                "promo_store_count":  pn,
                "promo_stores":       p_stores,
            }

        products[bc]["formats"][fmt] = {
            "price_min":    round(min(prices), 2),
            "price_max":    round(max(prices), 2),
            "price_avg":    round(sum(prices) / n, 2),
            "price_median": median,
            "store_count":  n,
            "stores":       sorted_stores,
            "promo":        p_prices[0] if p_prices else None,
            "promo_list":   p_list,
            **promo_stats,
        }

    return {"formats": sorted(formats_set), "products": list(products.values())}


# ──────────────────────────────────────────────────────────────────────────────
# Price gaps  (price_full ⋈ promo_full)
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/api/price-gaps")
async def get_price_gaps():
    disabled_fmt, active_bc = await _active_filters()
    conn = await db.get_db()
    try:
        async with conn.execute("""
            SELECT
                cp.item_code               AS barcode,
                COALESCE(b.name, cp.item_name, cp.item_code) AS name,
                cp.format_name,
                cp.item_price              AS price,
                MIN(
                    cpr.discounted_price / CASE WHEN cpr.min_qty IS NOT NULL AND cpr.min_qty > 1
                                                THEN cpr.min_qty ELSE 1 END
                ) AS promo_price
            FROM v_current_prices cp
            LEFT JOIN v_current_promos cpr
                   ON cpr.item_code   = cp.item_code
                  AND cpr.format_name = cp.format_name
                  AND (cpr.promotion_description IS NULL
                       OR (cpr.promotion_description NOT ILIKE '%SBOX%'
                           AND cpr.promotion_description NOT ILIKE '%כ.אשראי%'))
            LEFT JOIN barcodes b ON b.barcode = cp.item_code
            GROUP BY cp.item_code, cp.format_name, cp.item_price, cp.item_name, b.name
        """) as cur:
            rows = await cur.fetchall()
    finally:
        await conn.close()

    from collections import defaultdict
    catalog_map: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    promo_map:   dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    names: dict[str, str] = {}

    for r in rows:
        bc = r["barcode"]
        if bc not in active_bc or r["format_name"] in disabled_fmt:
            continue
        names[bc] = r["name"]
        if r["price"] is not None:
            catalog_map[bc][r["format_name"]].append(r["price"])
        if r["promo_price"] is not None:
            promo_map[bc][r["format_name"]].append(r["promo_price"])

    def build_gaps(price_map):
        result = []
        for bc, fmt_map in price_map.items():
            if len(fmt_map) < 2:
                continue
            fmt_avg  = {fmt: round(sum(ps) / len(ps), 2) for fmt, ps in fmt_map.items()}
            min_fmt  = min(fmt_avg, key=fmt_avg.get)
            max_fmt  = max(fmt_avg, key=fmt_avg.get)
            min_p, max_p = fmt_avg[min_fmt], fmt_avg[max_fmt]
            gap_ils  = round(max_p - min_p, 2)
            gap_pct  = round(gap_ils / max_p * 100, 1) if max_p else 0
            result.append({
                "barcode": bc, "name": names[bc],
                "min_price": min_p, "max_price": max_p,
                "min_format": min_fmt, "max_format": max_fmt,
                "gap_ils": gap_ils, "gap_pct": gap_pct,
            })
        result.sort(key=lambda x: x["gap_ils"], reverse=True)
        return result

    return {"catalog": build_gaps(catalog_map), "promo": build_gaps(promo_map)}


# ──────────────────────────────────────────────────────────────────────────────
# Promotions depth  (promo_full current)
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/api/promotions")
async def get_promotions():
    disabled_fmt, active_bc = await _active_filters()
    conn = await db.get_db()
    try:
        async with conn.execute("""
            SELECT
                cpr.item_code              AS barcode,
                COALESCE(b.name, cpr.item_code) AS name,
                cpr.format_name,
                cp.item_price              AS price,
                MIN(
                    cpr.discounted_price / CASE WHEN cpr.min_qty IS NOT NULL AND cpr.min_qty > 1
                                                THEN cpr.min_qty ELSE 1 END
                ) AS promo_price
            FROM v_current_promos cpr
            LEFT JOIN v_current_prices cp
                   ON cp.item_code   = cpr.item_code
                  AND cp.format_name = cpr.format_name
            LEFT JOIN barcodes b ON b.barcode = cpr.item_code
            WHERE cpr.discounted_price IS NOT NULL
              AND (cpr.promotion_description IS NULL
                   OR (cpr.promotion_description NOT ILIKE '%SBOX%'
                       AND cpr.promotion_description NOT ILIKE '%כ.אשראי%'))
            GROUP BY cpr.item_code, cpr.format_name, b.name, cp.item_price
        """) as cur:
            rows = await cur.fetchall()
    finally:
        await conn.close()
    result = []
    for r in rows:
        rd = dict(r)
        if rd["barcode"] not in active_bc or rd["format_name"] in disabled_fmt:
            continue
        price, promo = rd.get("price"), rd.get("promo_price")
        rd["discount_pct"] = round((price - promo) / price * 100, 1) if price and promo else None
        result.append(rd)
    return sorted(result, key=lambda x: x["discount_pct"] or 0, reverse=True)


# ──────────────────────────────────────────────────────────────────────────────
# Barcode management
# ──────────────────────────────────────────────────────────────────────────────

class BarcodeIn(BaseModel):
    barcode: str
    name: str
    active: bool = True


@app.get("/api/barcodes")
async def list_barcodes():
    conn = await db.get_db()
    try:
        async with conn.execute(
            "SELECT id, barcode, name, active, created_at FROM barcodes ORDER BY name"
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]
    finally:
        await conn.close()


@app.put("/api/barcodes/{barcode_id}")
async def update_barcode(barcode_id: int, payload: BarcodeIn):
    conn = await db.get_db()
    try:
        await conn.execute(
            "UPDATE barcodes SET name=?, active=? WHERE id=?",
            (payload.name, int(payload.active), barcode_id),
        )
        await conn.commit()
        return {"ok": True}
    finally:
        await conn.close()


@app.delete("/api/barcodes/{barcode_id}")
async def delete_barcode(barcode_id: int):
    conn = await db.get_db()
    try:
        await conn.execute("DELETE FROM barcodes WHERE id=?", (barcode_id,))
        await conn.commit()
        return {"ok": True}
    finally:
        await conn.close()


@app.post("/api/barcodes")
async def add_barcode(payload: BarcodeIn):
    """Add or update a single barcode. Auto-fills name from price_full if not provided."""
    name = payload.name.strip()
    if not name or name == payload.barcode:
        conn = await db.get_db()
        try:
            async with conn.execute(
                "SELECT item_name FROM price_full WHERE item_code=? AND item_name != '' LIMIT 1",
                (payload.barcode,)
            ) as cur:
                row = await cur.fetchone()
                if row and row[0]:
                    name = row[0]
        finally:
            await conn.close()
    await db.upsert_barcode(payload.barcode, name or payload.barcode, payload.active)
    return {"ok": True, "barcode": payload.barcode, "name": name or payload.barcode}


@app.post("/api/barcodes/import")
async def import_barcodes(file: UploadFile = File(...)):
    """
    Import barcodes from Excel (.xlsx). Replaces ALL existing barcodes.
    Expected columns: A=barcode, B=name (optional).
    """
    content = await file.read()
    try:
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
        ws = wb.active
        raw_rows = []
        for row in ws.iter_rows(min_row=1, values_only=True):
            bc_cell   = str(row[0]).strip() if row[0] is not None else ""
            name_cell = str(row[1]).strip() if len(row) > 1 and row[1] is not None else ""
            if bc_cell:
                raw_rows.append((bc_cell, name_cell))
        wb.close()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"שגיאה בקריאת קובץ Excel: {e}")

    if not raw_rows:
        raise HTTPException(status_code=400, detail="לא נמצאו ברקודים בקובץ")

    if raw_rows and not raw_rows[0][0].replace("-", "").isdigit():
        raw_rows = raw_rows[1:]

    if not raw_rows:
        raise HTTPException(status_code=400, detail="לא נמצאו ברקודים בקובץ לאחר דילוג כותרת")

    conn = await db.get_db()
    try:
        # Name lookup: price_full first, then barcodes table
        async with conn.execute(
            "SELECT item_code, item_name FROM price_full WHERE item_name != '' GROUP BY item_code"
        ) as cur:
            product_names = {r[0]: r[1] for r in await cur.fetchall()}
        async with conn.execute("SELECT barcode, name FROM barcodes") as cur:
            existing_names = {r[0]: r[1] for r in await cur.fetchall()}

        final_rows, no_name_barcodes = [], []
        for barcode, name in raw_rows:
            if not name or name == barcode:
                name = product_names.get(barcode) or existing_names.get(barcode, "")
            if not name:
                no_name_barcodes.append(barcode)
                name = barcode
            final_rows.append((barcode, name))

        now = time.time()
        await conn.execute("DELETE FROM barcodes")
        await conn.executemany(
            "INSERT INTO barcodes(barcode, name, active, created_at) VALUES(?, ?, 1, ?)",
            [(b, n, now) for b, n in final_rows],
        )
        await conn.commit()
    finally:
        await conn.close()

    return {
        "ok": True,
        "imported": len(final_rows),
        "names_found": len(final_rows) - len(no_name_barcodes),
        "no_name": no_name_barcodes,
    }


@app.get("/api/formats")
async def list_formats():
    known       = json.loads(await db.get_status("known_formats",  "[]"))
    disabled    = set(json.loads(await db.get_status("disabled_formats", "[]")))
    store_counts = json.loads(await db.get_status("store_counts",  "{}"))
    return [
        {"name": fmt, "active": fmt not in disabled, "store_count": store_counts.get(fmt, 0)}
        for fmt in known
    ]


@app.put("/api/formats/{fmt_name}/toggle")
async def toggle_format(fmt_name: str):
    disabled = set(json.loads(await db.get_status("disabled_formats", "[]")))
    if fmt_name in disabled:
        disabled.discard(fmt_name)
        active = True
    else:
        disabled.add(fmt_name)
        active = False
    await db.set_status("disabled_formats", json.dumps(sorted(disabled), ensure_ascii=False))
    return {"ok": True, "name": fmt_name, "active": active}


@app.get("/api/barcodes/lookup/{barcode}")
async def lookup_barcode(barcode: str):
    """Look up product name from price_full (Shufersal data) or barcodes table."""
    conn = await db.get_db()
    try:
        async with conn.execute(
            "SELECT item_name FROM price_full WHERE item_code=? AND item_name != '' LIMIT 1",
            (barcode,)
        ) as cur:
            row = await cur.fetchone()
            if row and row[0]:
                return {"barcode": barcode, "name": row[0]}
        async with conn.execute(
            "SELECT name FROM barcodes WHERE barcode=? LIMIT 1", (barcode,)
        ) as cur:
            row = await cur.fetchone()
            if row and row[0] and row[0] != barcode:
                return {"barcode": barcode, "name": row[0]}
        return {"barcode": barcode, "name": ""}
    finally:
        await conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# Data quality  (price_full)
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/api/data-quality")
async def get_data_quality():
    conn = await db.get_db()
    try:
        async with conn.execute("SELECT barcode, name FROM barcodes WHERE active=1") as cur:
            user_barcodes = {r["barcode"]: r["name"] for r in await cur.fetchall()}
        async with conn.execute(
            "SELECT DISTINCT item_code FROM v_current_prices"
        ) as cur:
            found_barcodes = {r[0] for r in await cur.fetchall()}
        async with conn.execute(
            "SELECT DISTINCT format_name FROM v_current_prices ORDER BY format_name"
        ) as cur:
            fetched_formats = [r[0] for r in await cur.fetchall()]
        async with conn.execute("""
            SELECT format_name,
                   MAX(source_ts)              AS latest_ts,
                   COUNT(DISTINCT source_url)  AS branch_count,
                   COUNT(DISTINCT item_code)   AS bc_count
            FROM v_current_prices
            GROUP BY format_name
        """) as cur:
            fmt_rows = {r["format_name"]: dict(r) for r in await cur.fetchall()}
    finally:
        await conn.close()

    store_counts = json.loads(await db.get_status("store_counts",  "{}"))
    known_raw    = await db.get_status("known_formats",  "[]")
    disabled_raw = await db.get_status("disabled_formats", "[]")
    disabled_fmt = set(json.loads(disabled_raw))
    all_known    = json.loads(known_raw) or list(FORMAT_KEYWORDS.keys())
    known_formats = [f for f in all_known if f not in disabled_fmt]

    covered, missing = [], []
    for fmt in known_formats:
        keywords = FORMAT_KEYWORDS.get(fmt, [fmt.lower()])
        matched  = any(any(kw.lower() in lf.lower() for kw in keywords) for lf in fetched_formats)
        (covered if matched else missing).append(fmt)

    missing_barcodes = [
        {"barcode": bc, "name": name}
        for bc, name in user_barcodes.items()
        if bc not in found_barcodes
    ]

    _tz           = ZoneInfo("Asia/Jerusalem")
    _now          = datetime.now(tz=_tz)
    today_str     = _now.strftime("%Y%m%d")
    yesterday_str = (_now - timedelta(days=1)).strftime("%Y%m%d")
    total_user_bc = len(user_barcodes)

    format_freshness = []
    for fmt in known_formats:
        row        = fmt_rows.get(fmt)
        branch_db  = row["branch_count"] if row else 0
        branch_site = store_counts.get(fmt, 0)
        branch_pct = round(branch_db / branch_site * 100) if branch_site else 0
        if row:
            ts      = row["latest_ts"] or ""
            ts_date = ts[:8] if len(ts) >= 8 else ""
            if ts_date == today_str:       status = "today"
            elif ts_date == yesterday_str: status = "yesterday"
            elif ts_date:                  status = "stale"
            else:                          status = "missing"
            display_date = (f"{ts_date[6:8]}/{ts_date[4:6]}/{ts_date[:4]}"
                            if len(ts_date) == 8 else "—")
            format_freshness.append({
                "format_name": fmt, "display_date": display_date,
                "bc_count": row["bc_count"],
                "coverage_pct": round(row["bc_count"] / total_user_bc * 100) if total_user_bc else 0,
                "bc_count_site": total_user_bc,
                "branch_count_db": branch_db,
                "branch_count_site": branch_site,
                "branch_coverage_pct": branch_pct,
                "status": status,
            })
        else:
            format_freshness.append({
                "format_name": fmt, "display_date": "—",
                "bc_count": 0, "coverage_pct": 0, "bc_count_site": total_user_bc,
                "branch_count_db": 0, "branch_count_site": branch_site,
                "branch_coverage_pct": 0, "status": "missing",
            })

    formats_today       = sum(1 for f in format_freshness if f["status"] == "today")
    total_known         = len(known_formats)
    day_completeness_pct = round(formats_today / total_known * 100) if total_known else 0

    return {
        "known_formats": known_formats,
        "covered_formats": covered,
        "missing_formats": missing,
        "fetched_formats": fetched_formats,
        "total_user_barcodes": total_user_bc,
        "found_barcodes_count": len(found_barcodes),
        "missing_barcodes_count": len(missing_barcodes),
        "missing_barcodes": missing_barcodes,
        "store_counts": store_counts,
        "format_freshness": format_freshness,
        "formats_today": formats_today,
        "total_known_formats": total_known,
        "day_completeness_pct": day_completeness_pct,
        "today_display": _now.strftime("%d/%m/%Y"),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Gap report  (price_full)
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/api/gap-report")
async def get_gap_report():
    conn = await db.get_db()
    try:
        async with conn.execute("SELECT barcode, name FROM barcodes WHERE active=1") as cur:
            all_barcodes = {r["barcode"]: r["name"] for r in await cur.fetchall()}
        async with conn.execute(
            "SELECT DISTINCT item_code FROM v_current_prices"
        ) as cur:
            found_barcodes = {r[0] for r in await cur.fetchall()}
        async with conn.execute(
            "SELECT DISTINCT format_name FROM v_current_prices ORDER BY format_name"
        ) as cur:
            live_formats = [r[0] for r in await cur.fetchall()]
    finally:
        await conn.close()

    excel_formats = json.loads(await db.get_status("excel_formats", "[]"))
    missing_barcodes = [
        {"barcode": bc, "name": name}
        for bc, name in all_barcodes.items()
        if bc not in found_barcodes
    ]

    formats_to_check = excel_formats or list(FORMAT_KEYWORDS.keys())
    covered_formats, missing_formats = [], []
    for fmt in formats_to_check:
        keywords = FORMAT_KEYWORDS.get(fmt, [fmt.lower()])
        matched  = any(any(kw.lower() in lf.lower() for kw in keywords) for lf in live_formats)
        (covered_formats if matched else missing_formats).append(fmt)

    return {
        "total_excel_products": len(all_barcodes),
        "found_in_live": len(found_barcodes),
        "missing_count": len(missing_barcodes),
        "coverage_pct": round(len(found_barcodes) / len(all_barcodes) * 100, 1) if all_barcodes else 0,
        "excel_formats": formats_to_check,
        "covered_formats": covered_formats,
        "missing_formats": missing_formats,
        "live_formats": live_formats,
        "missing_products": missing_barcodes,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Pipeline audit log  (calendar from price_full)
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/api/audit")
async def get_audit(limit: int = 50):
    runs = await db.get_pipeline_runs(limit=limit)
    for run in runs:
        try:
            run["errors"] = json.loads(run["error_log"]) if run.get("error_log") else []
        except Exception:
            run["errors"] = []
        ts = run.get("shufersal_timestamp") or ""
        run["shufersal_date"] = f"{ts[6:8]}/{ts[4:6]}/{ts[:4]}" if len(ts) >= 8 else "—"

    # Calendar grid: last 10 days × formats from price_full
    cutoff_10d = time.time() - 10 * 86400
    conn = await db.get_db()
    try:
        async with conn.execute("""
            SELECT DATE(scraped_at,'unixepoch','localtime') AS snap_date,
                   format_name,
                   COUNT(DISTINCT item_code)               AS bc_count,
                   MAX(scraped_at)                         AS max_snap
            FROM price_full
            WHERE scraped_at > ?
            GROUP BY snap_date, format_name
        """, (cutoff_10d,)) as cur:
            cal_rows = [dict(r) for r in await cur.fetchall()]
    finally:
        await conn.close()

    known_raw       = await db.get_status("known_formats", "[]")
    calendar_formats = json.loads(known_raw) or list(FORMAT_KEYWORDS.keys())

    _tz    = ZoneInfo("Asia/Jerusalem")
    _today = datetime.now(tz=_tz).date()

    calendar_grid:       dict[str, dict[str, int]] = {fmt: {} for fmt in calendar_formats}
    format_norm:         dict[str, int] = {}
    calendar_timestamps: dict[str, dict[str, str]] = {}

    for row in cal_rows:
        d, fmt, bc, snap = row["snap_date"], row["format_name"], row["bc_count"], row["max_snap"]
        calendar_grid.setdefault(fmt, {})[d] = bc
        if fmt not in format_norm or bc > format_norm[fmt]:
            format_norm[fmt] = bc
        ts_str = datetime.fromtimestamp(snap, tz=_tz).strftime("%H:%M") if snap else ""
        calendar_timestamps.setdefault(fmt, {})[d] = ts_str

    calendar_dates = [(_today - timedelta(days=i)).isoformat() for i in range(0, 10)]

    return {
        "runs": runs,
        "calendar_grid": calendar_grid,
        "calendar_dates": calendar_dates,
        "calendar_formats": calendar_formats,
        "calendar_format_norm": format_norm,
        "calendar_timestamps": calendar_timestamps,
        "today": _today.isoformat(),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Price history per barcode  (price_full ⋈ promo_full — 30-day rolling)
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/api/history/{barcode}/chart")
async def get_history_chart(barcode: str, days: int = 30):
    """Aggregated price history per format per day, with per-day detail for tooltips."""
    cutoff    = time.time() - days * 86400
    _tz       = ZoneInfo("Asia/Jerusalem")
    _today    = datetime.now(tz=_tz).date()
    all_dates = [(_today - timedelta(days=i)).isoformat() for i in range(days - 1, -1, -1)]

    conn = await db.get_db()
    try:
        # Per-day promo sub-query (best promo per barcode+format+day, exclude credit-card promos)
        # GROUP BY uses the expression (not alias) for PostgreSQL compatibility
        promo_sub = """
            SELECT item_code, format_name,
                   DATE(scraped_at,'unixepoch','localtime') AS scrape_date,
                   MIN(discounted_price / CASE WHEN min_qty > 1 THEN min_qty ELSE 1 END) AS best_promo
            FROM promo_full
            WHERE item_code = ? AND scraped_at > ? AND min_qty IS NOT NULL
              AND (promotion_description IS NULL
                   OR (promotion_description NOT ILIKE '%SBOX%'
                       AND promotion_description NOT ILIKE '%כ.אשראי%'))
            GROUP BY item_code, format_name, DATE(scraped_at,'unixepoch','localtime')
        """
        # Aggregated: one row per (format, day)
        # Non-aggregate columns use MIN/MAX/AVG for PostgreSQL GROUP BY compatibility
        async with conn.execute(f"""
            SELECT
                DATE(pf.scraped_at,'unixepoch','localtime') AS snap_date,
                pf.format_name,
                MIN(pf.item_price)  AS min_price,
                MAX(pf.item_price)  AS max_price,
                AVG(pf.item_price)  AS avg_price,
                MIN(pr.best_promo)  AS promo_min,
                1                   AS branch_count
            FROM price_full pf
            LEFT JOIN ({promo_sub}) pr
                   ON pr.item_code   = pf.item_code
                  AND pr.format_name = pf.format_name
                  AND pr.scrape_date = DATE(pf.scraped_at,'unixepoch','localtime')
            WHERE pf.item_code = ? AND pf.scraped_at > ? AND pf.item_price IS NOT NULL
            GROUP BY DATE(pf.scraped_at,'unixepoch','localtime'), pf.format_name
            ORDER BY pf.format_name, DATE(pf.scraped_at,'unixepoch','localtime')
        """, (barcode, cutoff, barcode, cutoff)) as cur:
            agg_rows = [dict(r) for r in await cur.fetchall()]

        # Daily detail for tooltip — discount_pct computed in Python (ROUND(float,n) PostgreSQL compat)
        async with conn.execute(f"""
            SELECT
                DATE(pf.scraped_at,'unixepoch','localtime') AS snap_date,
                pf.format_name,
                MIN(pf.item_price)  AS price,
                MIN(pr.best_promo)  AS promo_price
            FROM price_full pf
            LEFT JOIN ({promo_sub}) pr
                   ON pr.item_code   = pf.item_code
                  AND pr.format_name = pf.format_name
                  AND pr.scrape_date = DATE(pf.scraped_at,'unixepoch','localtime')
            WHERE pf.item_code = ? AND pf.scraped_at > ? AND pf.item_price IS NOT NULL
            GROUP BY DATE(pf.scraped_at,'unixepoch','localtime'), pf.format_name
            ORDER BY pf.format_name, DATE(pf.scraped_at,'unixepoch','localtime')
        """, (barcode, cutoff, barcode, cutoff)) as cur:
            raw_detail = [dict(r) for r in await cur.fetchall()]
        # Compute discount_pct in Python (avoids ROUND(float,n) PostgreSQL incompatibility)
        detail_rows = []
        for r in raw_detail:
            p, pr_p = r.get("price"), r.get("promo_price")
            r["discount_pct"] = round((p - pr_p) / p * 100, 1) if p and pr_p else None
            detail_rows.append(r)

        async with conn.execute(
            "SELECT name FROM barcodes WHERE barcode=? LIMIT 1", (barcode,)
        ) as cur:
            row  = await cur.fetchone()
            name = row[0] if row else barcode
    finally:
        await conn.close()

    disabled_fmt, _ = await _active_filters()

    agg_by_fmt:    dict[str, dict[str, dict]] = {}
    formats_seen:  list[str] = []
    detail_by_fmt: dict[str, dict[str, list]] = {}

    for r in agg_rows:
        fmt = r["format_name"]
        if fmt in disabled_fmt:
            continue
        if fmt not in agg_by_fmt:
            agg_by_fmt[fmt] = {}
            formats_seen.append(fmt)
        agg_by_fmt[fmt][str(r["snap_date"])] = r

    for r in detail_rows:
        fmt, d = r["format_name"], str(r["snap_date"])
        if fmt in disabled_fmt:
            continue
        detail_by_fmt.setdefault(fmt, {}).setdefault(d, []).append({
            "price": r["price"], "promo_price": r["promo_price"],
            "discount_pct": r["discount_pct"],
        })

    formats_out = []
    for fmt in sorted(formats_seen):
        day_data = agg_by_fmt.get(fmt, {})
        formats_out.append({
            "format_name":  fmt,
            "dates":        all_dates,
            "min_price":    [day_data[d]["min_price"]    if d in day_data else None for d in all_dates],
            "max_price":    [day_data[d]["max_price"]    if d in day_data else None for d in all_dates],
            "avg_price":    [day_data[d]["avg_price"]    if d in day_data else None for d in all_dates],
            "promo_min":    [day_data[d]["promo_min"]    if d in day_data else None for d in all_dates],
            "branch_count": [day_data[d]["branch_count"] if d in day_data else None for d in all_dates],
            "daily_detail": detail_by_fmt.get(fmt, {}),
        })

    return {"barcode": barcode, "name": name, "formats": formats_out}


@app.get("/api/history/{barcode}")
async def get_history(barcode: str, days: int = 30):
    disabled_fmt, _ = await _active_filters()
    cutoff = time.time() - days * 86400
    conn   = await db.get_db()
    try:
        async with conn.execute("""
            SELECT
                pf.format_name,
                MIN(pf.item_price)       AS price,
                MIN(pr.discounted_price / CASE WHEN pr.min_qty > 1 THEN pr.min_qty ELSE 1 END) AS promo_price,
                MIN(pf.scraped_at)       AS snapshot_at
            FROM price_full pf
            LEFT JOIN promo_full pr
                   ON pr.item_code   = pf.item_code
                  AND pr.format_name = pf.format_name
                  AND DATE(pr.scraped_at,'unixepoch') = DATE(pf.scraped_at,'unixepoch')
                  AND (pr.promotion_description IS NULL
                       OR (pr.promotion_description NOT ILIKE '%SBOX%'
                           AND pr.promotion_description NOT ILIKE '%כ.אשראי%'))
            WHERE pf.item_code = ? AND pf.scraped_at > ?
            GROUP BY pf.format_name, DATE(pf.scraped_at,'unixepoch')
            ORDER BY MIN(pf.scraped_at)
        """, (barcode, cutoff)) as cur:
            raw_rows = [dict(r) for r in await cur.fetchall()]
        # Compute discount_pct in Python (avoids ROUND(float,n) PostgreSQL incompatibility)
        rows = []
        for r in raw_rows:
            p, pr_p = r.get("price"), r.get("promo_price")
            r["discount_pct"] = round((p - pr_p) / p * 100, 1) if p and pr_p else None
            rows.append(r)
        return [r for r in rows if r["format_name"] not in disabled_fmt]
    finally:
        await conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# Serve frontend
# ──────────────────────────────────────────────────────────────────────────────

frontend_path = Path(__file__).parent.parent / "frontend"
if frontend_path.exists():
    app.mount("/", StaticFiles(directory=str(frontend_path), html=True), name="frontend")
