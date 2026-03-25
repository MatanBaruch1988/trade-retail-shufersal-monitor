"""Insight Agent - rule-based narrative summary and top actions."""
import logging

logger = logging.getLogger(__name__)


async def run(kpis: dict, top_alerts: list[dict]) -> dict:
    """
    Main entry point for Insight Agent.

    Returns { narrative_summary, trend_note, top_3_actions }
    """
    if not kpis:
        return {
            "narrative_summary": "אין נתונים זמינים.",
            "trend_note": "",
            "top_3_actions": [],
        }
    return _static_fallback(kpis, top_alerts)


def _static_fallback(kpis: dict, top_alerts: list[dict]) -> dict:
    total = kpis.get("total_products", 0)
    with_promo = kpis.get("products_with_promo", 0)
    without_promo = kpis.get("products_without_promo", 0)
    avg_disc = kpis.get("avg_discount_pct", 0)

    summary = (
        f"נמצאו {total} מוצרים בידיעון שופרסל. "
        f"{with_promo} מוצרים ({kpis.get('promo_rate_pct', 0)}%) מופיעים במבצע "
        f"עם הנחה ממוצעת של {avg_disc}%. "
        f"{without_promo} מוצרים אינם במבצע כלל."
    )

    top_3 = []
    for a in top_alerts[:3]:
        top_3.append(f"{a['product_name']}: {a['recommended_action']}")
    if not top_3:
        top_3 = ["בדוק נתוני מחירים עדכניים", "עדכן רשימת ברקודים", "צור קשר עם מנהל KA"]

    return {
        "narrative_summary": summary,
        "trend_note": "",
        "top_3_actions": top_3,
    }
