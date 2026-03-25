"""Orchestrator — runs the full scrape → parse → analyze → alert → insight pipeline."""
import json
import logging
import time
from decimal import Decimal


class _DecimalEncoder(json.JSONEncoder):
    """JSON encoder that converts Decimal to float (asyncpg NUMERIC columns)."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def _dumps(obj) -> str:
    return json.dumps(obj, cls=_DecimalEncoder, ensure_ascii=False)

from backend import db
from backend.agents import scraper_agent, parser_agent, analyzer_agent, alert_agent, insight_agent
from backend.constants import map_consumer_format

logger = logging.getLogger(__name__)


async def run_pipeline(force: bool = False, trigger: str = "scheduled") -> dict:
    """Execute full data pipeline. Returns summary dict with counts and status."""
    start = time.time()
    logger.info("Pipeline started (force=%s, trigger=%s)", force, trigger)

    run_id = await db.start_pipeline_run(trigger=trigger)

    products_before = await db.count_products()
    alerts_before   = await db.count_alerts()

    failed_files: list[str] = []
    errors: list[dict] = []

    try:
        # ── 1. Scraper ──────────────────────────────────────────────────────
        last_ts = await db.get_status("last_shufersal_timestamp", "")
        scraper_result = await scraper_agent.run(last_timestamp="" if force else last_ts)

        if not scraper_result["new_data"] and not force:
            logger.info("No new data — pipeline skipped")
            await db.finish_pipeline_run(run_id, {
                "status": "skipped", "shufersal_timestamp": last_ts,
                "new_data": 0,
                "products_before": products_before, "products_after": products_before,
                "products_added": 0, "products_removed": 0,
                "alerts_before": alerts_before, "alerts_after": alerts_before,
                "files_attempted": 0, "files_ok": 0, "files_failed": 0,
                "error_log": "[]", "duration_s": round(time.time() - start, 1),
            })
            return {"status": "skipped", "reason": "no_new_data",
                    "duration_s": round(time.time() - start, 1)}

        # ── 2. Active barcodes ──────────────────────────────────────────────
        active_barcodes = await db.get_active_barcodes()
        if not active_barcodes:
            logger.warning("No active barcodes — pipeline aborted")
            await db.finish_pipeline_run(run_id, {
                "status": "failed",
                "shufersal_timestamp": scraper_result.get("latest_timestamp", ""),
                "new_data": 1,
                "products_before": products_before, "products_after": products_before,
                "products_added": 0, "products_removed": 0,
                "alerts_before": alerts_before, "alerts_after": alerts_before,
                "files_attempted": 0, "files_ok": 0, "files_failed": 0,
                "error_log": json.dumps([{"file": "n/a", "error": "no_active_barcodes"}]),
                "duration_s": round(time.time() - start, 1),
            })
            return {"status": "aborted", "reason": "no_barcodes",
                    "duration_s": round(time.time() - start, 1)}

        barcode_names = await db.get_barcode_names()

        # ── 2b. Stores file ─────────────────────────────────────────────────
        store_file = scraper_result.get("store_file")
        if store_file:
            known_formats, store_counts, raw_stores = \
                await parser_agent.download_and_parse_stores(store_file.url)
            if known_formats:
                await db.set_status("known_formats", _dumps(known_formats))
                await db.set_status("store_counts",   _dumps(store_counts))
                logger.info("Stores: %d known formats, store_counts=%s",
                            len(known_formats), store_counts)
            if raw_stores:
                n = await db.replace_stores(raw_stores)
                logger.info("Stores table: %d rows upserted", n)

        # ── 3. Parser ───────────────────────────────────────────────────────
        promo_files = scraper_result["promo_files"]
        price_files = scraper_result.get("price_files", [])

        # Cache: skip PriceFull files whose (format, source_ts) already exist
        # in price_full, unless barcode list changed.
        active_bc_count   = len(active_barcodes)
        last_bc_count     = int(await db.get_status("last_bc_count", "0"))
        barcodes_changed  = active_bc_count != last_bc_count

        existing_ts = (set() if barcodes_changed
                       else await db.get_price_full_source_timestamps())

        price_files_new, cached_pairs = [], []
        for f in price_files:
            fmt = map_consumer_format(f.store_type) if f.store_type else ""
            if (fmt, f.timestamp) in existing_ts:
                cached_pairs.append((fmt, f.timestamp))
            else:
                price_files_new.append(f)

        cached_records = await db.get_records_by_format_ts(cached_pairs)
        if cached_pairs:
            logger.info("PriceFull cache: %d files reused, %d to download",
                        len(price_files) - len(price_files_new), len(price_files_new))

        files_attempted = len(promo_files) + len(price_files)
        parse_result = await parser_agent.run(
            promo_files=promo_files,
            price_files=price_files_new,
            active_barcodes=active_barcodes,
        )
        records      = parse_result["records"] + cached_records
        failed_files = parse_result["failed"]
        raw_price    = parse_result.get("raw_price", [])
        raw_promo    = parse_result.get("raw_promo", [])
        errors       = [{"file": f, "error": "download_or_parse_failed"}
                        for f in failed_files]
        files_ok     = files_attempted - len(failed_files)

        # Fill missing names from barcodes table
        for r in records:
            if not r.get("name") and r["barcode"] in barcode_names:
                r["name"] = barcode_names[r["barcode"]]

        if not records:
            logger.warning("Parser returned 0 records")
            await db.finish_pipeline_run(run_id, {
                "status": "failed",
                "shufersal_timestamp": scraper_result.get("latest_timestamp", ""),
                "new_data": 1,
                "products_before": products_before, "products_after": 0,
                "products_added": 0, "products_removed": products_before,
                "alerts_before": alerts_before, "alerts_after": alerts_before,
                "files_attempted": files_attempted, "files_ok": files_ok,
                "files_failed": len(failed_files),
                "error_log": json.dumps(errors),
                "duration_s": round(time.time() - start, 1),
            })
            return {"status": "aborted", "reason": "no_records",
                    "duration_s": round(time.time() - start, 1)}

        # ── 4. Analyzer ─────────────────────────────────────────────────────
        analysis = analyzer_agent.run(records)

        # ── 5. Persist raw tables ────────────────────────────────────────────
        pf_ins    = await db.insert_price_full_batch(raw_price)
        pf_promo  = await db.insert_promo_full_batch(raw_promo)
        logger.info("Raw tables: price_full +%d rows, promo_full +%d rows",
                    pf_ins, pf_promo)
        products_after = await db.count_products()

        # ── 6. Alert Agent ──────────────────────────────────────────────────
        alerts = await alert_agent.run(
            outliers=analysis["outliers"],
            price_gaps=analysis["price_gaps"],
        )
        await db.replace_alerts(alerts)
        alerts_after = await db.count_alerts()

        # ── 7. Insight Agent ────────────────────────────────────────────────
        insights = await insight_agent.run(
            kpis=analysis["kpis"],
            top_alerts=alerts[:10],
        )
        await db.set_status("insights", _dumps(insights))
        await db.set_status("kpis",     _dumps(analysis["kpis"]))

        # ── 8. Update timestamps ────────────────────────────────────────────
        new_ts = scraper_result.get("latest_timestamp", "")
        if new_ts:
            await db.set_status("last_shufersal_timestamp", new_ts)
        await db.set_status("last_refresh_at", str(time.time()))
        await db.set_status("last_bc_count",   str(active_bc_count))

        duration     = round(time.time() - start, 1)
        final_status = "partial" if failed_files else "ok"

        await db.finish_pipeline_run(run_id, {
            "status": final_status,
            "shufersal_timestamp": new_ts,
            "new_data": 1,
            "products_before": products_before,
            "products_after":  products_after,
            "products_added":   max(0, products_after - products_before),
            "products_removed": max(0, products_before - products_after),
            "alerts_before": alerts_before,
            "alerts_after":  alerts_after,
            "files_attempted": files_attempted,
            "files_ok":        files_ok,
            "files_failed":    len(failed_files),
            "error_log":       json.dumps(errors),
            "duration_s":      duration,
        })

        summary = {
            "status": final_status,
            "products": len(records),
            "alerts":   len(alerts),
            "timestamp": new_ts,
            "duration_s": duration,
            "files_failed": len(failed_files),
        }
        logger.info("Pipeline complete: %s", summary)
        return summary

    except Exception as exc:
        duration = round(time.time() - start, 1)
        logger.exception("Pipeline crashed: %s", exc)
        await db.finish_pipeline_run(run_id, {
            "status": "failed", "shufersal_timestamp": "",
            "new_data": 0,
            "products_before": products_before, "products_after": products_before,
            "products_added": 0, "products_removed": 0,
            "alerts_before": alerts_before, "alerts_after": alerts_before,
            "files_attempted": 0, "files_ok": 0, "files_failed": 0,
            "error_log": json.dumps([{"file": "pipeline", "error": str(exc)}]),
            "duration_s": duration,
        })
        return {"status": "failed", "error": str(exc), "duration_s": duration}
