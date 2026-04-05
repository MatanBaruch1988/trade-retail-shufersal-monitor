"""Analyzer Agent — computes KPIs, price gaps, presence matrix, outliers."""
import logging
from collections import defaultdict
from typing import Optional

logger = logging.getLogger(__name__)

OUTLIER_GAP_PCT_THRESHOLD = 30.0  # flag if gap % > this
OUTLIER_PRICE_RATIO = 0.35        # מחיר < 35% מהחציון = bundle promo שגוי
MISSING_PROMO_FORMATS_THRESHOLD = 4  # flag if missing promo in >= this many formats

# Prefer catalog files over delta files when computing per-format price
TYPE_PRIORITY = {"PromoFull": 1, "PriceFull": 2, "Promo": 3, "Price": 4}


def _group_by_barcode(records: list[dict]) -> dict[str, list[dict]]:
    groups: dict[str, list[dict]] = defaultdict(list)
    for r in records:
        groups[r["barcode"]].append(r)
    return groups


def run(records: list[dict]) -> dict:
    """
    Main entry point for Analyzer Agent.

    Input: flat list of product records from Parser Agent.
    Returns: { kpis, price_gaps, presence_matrix, outliers }
    """
    if not records:
        return {"kpis": {}, "price_gaps": [], "presence_matrix": {}, "outliers": []}

    groups = _group_by_barcode(records)
    all_formats = sorted({r["format_name"] for r in records})

    # ── KPIs ──────────────────────────────────────────────────────────────
    total_products = len(groups)
    products_with_promo = sum(
        1 for recs in groups.values()
        if any(r["promo_price"] is not None for r in recs)
    )
    avg_discount = 0.0
    discount_values = [
        r["discount_pct"] for r in records
        if r["discount_pct"] is not None
    ]
    if discount_values:
        avg_discount = round(sum(discount_values) / len(discount_values), 1)

    kpis = {
        "total_products": total_products,
        "products_with_promo": products_with_promo,
        "products_without_promo": total_products - products_with_promo,
        "promo_rate_pct": round(products_with_promo / total_products * 100, 1) if total_products else 0,
        "avg_discount_pct": avg_discount,
        "total_formats": len(all_formats),
        "formats": all_formats,
    }

    # ── Presence matrix ───────────────────────────────────────────────────
    presence_matrix: dict[str, dict[str, bool]] = {}
    for barcode, recs in groups.items():
        name = recs[0]["name"]
        formats_present = {r["format_name"] for r in recs}
        presence_matrix[barcode] = {
            "name": name,
            "formats": {fmt: (fmt in formats_present) for fmt in all_formats},
            "count": len(formats_present),
        }

    # ── Price gaps (catalog prices only) ─────────────────────────────────
    price_gaps = []
    for barcode, recs in groups.items():
        # Per format: pick best-priority record (PromoFull > PriceFull > Promo > Price)
        # Catalog prices only — promo mismatches are reported separately
        fmt_best: dict[str, dict] = {}
        for r in recs:
            p = r["price"]  # catalog price only
            if p is None:
                continue
            fmt = r["format_name"]
            priority = TYPE_PRIORITY.get(r.get("file_type", ""), 9)
            if fmt not in fmt_best or priority < fmt_best[fmt]["priority"]:
                fmt_best[fmt] = {"price": p, "priority": priority}

        if len(fmt_best) < 2:
            continue

        fmt_avg = {fmt: v["price"] for fmt, v in fmt_best.items()}
        sorted_prices = sorted(fmt_avg.values())
        median = sorted_prices[len(sorted_prices) // 2]
        if median > 0:
            fmt_avg = {fmt: p for fmt, p in fmt_avg.items() if p >= median * OUTLIER_PRICE_RATIO}
        if len(fmt_avg) < 2:
            continue

        min_price = min(fmt_avg.values())
        max_price = max(fmt_avg.values())
        min_format = min(fmt_avg, key=fmt_avg.get)
        max_format = max(fmt_avg, key=fmt_avg.get)
        gap_ils = round(max_price - min_price, 2)
        gap_pct = round((max_price - min_price) / max_price * 100, 1) if max_price else 0

        price_gaps.append({
            "barcode": barcode,
            "name": recs[0]["name"],
            "min_price": min_price,
            "max_price": max_price,
            "min_format": min_format,
            "max_format": max_format,
            "gap_ils": gap_ils,
            "gap_pct": gap_pct,
        })

    price_gaps.sort(key=lambda x: x["gap_pct"], reverse=True)

    # ── Outliers ──────────────────────────────────────────────────────────
    outliers = []

    # High price gap
    for pg in price_gaps:
        if pg["gap_pct"] >= OUTLIER_GAP_PCT_THRESHOLD:
            outliers.append({
                "type": "high_gap",
                "barcode": pg["barcode"],
                "name": pg["name"],
                "detail": f"פער {pg['gap_pct']}% — {pg['min_format']}: {pg['min_price']}₪ לעומת {pg['max_format']}: {pg['max_price']}₪",
                "gap_pct": pg["gap_pct"],
            })

    # No promo in any format
    for barcode, recs in groups.items():
        has_promo = any(r["promo_price"] is not None for r in recs)
        if not has_promo:
            outliers.append({
                "type": "no_promo",
                "barcode": barcode,
                "name": recs[0]["name"],
                "detail": "ללא מבצע בשום פורמט",
                "gap_pct": None,
            })

    # Present in only one format
    for barcode, info in presence_matrix.items():
        if info["count"] == 1:
            fmt = next(f for f, v in info["formats"].items() if v)
            outliers.append({
                "type": "single_format",
                "barcode": barcode,
                "name": info["name"],
                "detail": f"מופיע רק ב-{fmt}",
                "gap_pct": None,
            })

    # Promo mismatch: promo exists in some formats but not in others
    for barcode, recs in groups.items():
        fmt_promos: dict[str, Optional[float]] = {}
        for r in recs:
            fmt = r["format_name"]
            promo = r["promo_price"]
            if fmt not in fmt_promos or (promo is not None and (fmt_promos[fmt] is None or promo < fmt_promos[fmt])):
                fmt_promos[fmt] = promo

        formats_with_promo = {f: p for f, p in fmt_promos.items() if p is not None}
        formats_without_promo = sorted(f for f, p in fmt_promos.items() if p is None)

        if formats_with_promo and formats_without_promo:
            best_fmt = min(formats_with_promo, key=formats_with_promo.get)
            best_price = formats_with_promo[best_fmt]
            missing = ", ".join(formats_without_promo)
            outliers.append({
                "type": "promo_mismatch",
                "barcode": barcode,
                "name": recs[0]["name"],
                "detail": f"מבצע ב-{best_fmt} (₪{best_price}) - חסר מבצע ב: {missing}",
                "gap_pct": None,
            })

    logger.info(
        "Analysis complete: %d products, %d price gaps, %d outliers",
        total_products, len(price_gaps), len(outliers)
    )
    return {
        "kpis": kpis,
        "price_gaps": price_gaps,
        "presence_matrix": presence_matrix,
        "outliers": outliers,
    }
