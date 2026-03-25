"""Shared constants for consumer format mapping."""
import re

FORMAT_KEYWORDS: dict[str, list[str]] = {
    "שופרסל שלי":    ["שלי"],
    "שופרסל דיל":    ["דיל", "deal"],
    "שופרסל אקספרס": ["אקספרס", "express"],
    "יוניברס":       ["יוניברס", "universe"],
    "גוד מרקט":      ["גוד מרקט", "good market"],
    "יש בשכונה":     ["יש בשכונה"],
    "יש חסד":        ["יש חסד"],
    "שערי רווחה":    ["שערי רווחה"],
}


def map_consumer_format(store_type: str) -> str:
    """Map a raw store_type/branch name to its consumer format name."""
    # Normalize: collapse whitespace variants (spaces, non-breaking, RTL/LTR marks)
    normalized = re.sub(r'[\s\u00a0\u200e\u200f\u200b]+', ' ', store_type).strip()
    s = normalized.lower()
    for fmt, keywords in FORMAT_KEYWORDS.items():
        if any(kw.lower() in s for kw in keywords):
            return fmt
    return normalized  # return normalized fallback (not raw original)
