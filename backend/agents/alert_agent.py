"""Alert Agent - rule-based classification and prioritization of pricing issues."""
import logging

logger = logging.getLogger(__name__)


async def run(outliers: list[dict], price_gaps: list[dict]) -> list[dict]:
    """
    Main entry point for Alert Agent.

    Returns list of alert dicts sorted by urgency_score desc.
    """
    if not outliers and not price_gaps:
        return []
    return _static_fallback(outliers, price_gaps)


def _static_fallback(outliers: list[dict], price_gaps: list[dict]) -> list[dict]:
    """Simple rule-based fallback when API is unavailable."""
    alerts = []

    for o in outliers:
        if o["type"] == "high_gap":
            severity = "red"
            urgency = min(10, int((o.get("gap_pct", 30)) / 3))
            action = f"בדוק מחיר עם מנהל KA של {o['detail'].split('לעומת')[-1].strip().split(':')[0].strip()}"
        elif o["type"] == "no_promo":
            severity = "yellow"
            urgency = 5
            action = "שלח בקשה להכנסת מבצע לידיעון הבא"
        elif o["type"] == "promo_mismatch":
            severity = "yellow"
            urgency = 3
            missing_fmt = o["detail"].split("חסר מבצע ב:")[-1].strip() if "חסר מבצע ב:" in o["detail"] else ""
            action = f"בדוק עם KA של {missing_fmt} אפשרות להכניס מבצע מקביל"[:120]
        else:  # single_format
            severity = "yellow"
            urgency = 4
            action = "בדוק חוזה הפצה לרשתות נוספות"

        alerts.append({
            "barcode": o["barcode"],
            "product_name": o["name"],
            "issue": o["detail"][:80],
            "recommended_action": action[:120],
            "severity": severity,
            "urgency_score": urgency,
            "alert_type": o["type"],
        })

    alerts.sort(key=lambda x: x["urgency_score"], reverse=True)
    return alerts
