"""Parser Agent — downloads and parses GZ/XML promo files from Shufersal."""
import gzip
import io
import logging
import re
import xml.etree.ElementTree as ET
from typing import Optional

import httpx

from backend.constants import map_consumer_format, FORMAT_KEYWORDS

logger = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; tempo-price-monitor/1.0)"}


def _format_name(entry) -> str:
    """Derive a consumer format name from a FileEntry."""
    if entry.store_type:
        return map_consumer_format(entry.store_type)
    # Extract store ID from filename: PromoFull7290027600007-666-... → "סניף 666"
    parts = entry.filename.split("-")
    if len(parts) >= 2:
        return f"סניף {parts[1]}"
    return "שופרסל"


def _safe_float(val: Optional[str]) -> Optional[float]:
    try:
        return float(val) if val and val.strip() not in ("-", "", "N/A") else None
    except (ValueError, TypeError):
        return None


def _text(el: Optional[ET.Element]) -> str:
    return el.text.strip() if el is not None and el.text else ""


def _find(parent: ET.Element, *tags: str) -> Optional[ET.Element]:
    """Find first matching tag using explicit None checks (avoids Element bool deprecation)."""
    for tag in tags:
        el = parent.find(tag)
        if el is not None:
            return el
    return None


def _parse_xml_promos(xml_bytes: bytes, format_name: str, active_barcodes: set, file_type: str = "Promo") -> tuple[list[dict], list[dict]]:
    """Returns (processed_records, raw_promo_records)."""
    records, raw = [], []
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        logger.error("XML parse error in %s: %s", format_name, e)
        return records, raw

    chain_id = _text(_find(root, "ChainId"))
    store_id  = _text(_find(root, "StoreId"))

    for promo in root.iter("Promotion"):
        promo_id   = _text(_find(promo, "PromotionId"))
        promo_desc = _text(_find(promo, "PromotionDescription", "Description"))
        start_date = _text(_find(promo, "StartDate", "PromotionStartDate"))
        end_date   = _text(_find(promo, "EndDate",   "PromotionEndDate"))
        promo_price_raw = _safe_float(_text(_find(promo, "DiscountedPrice", "PromotionPrice")))

        # Normalize bundle price to per-unit price
        # e.g. "4 יחידות ב-12 ₪" → DiscountedPrice=12, MinQty=4 → promo_price=3.0
        min_qty_str = _text(_find(promo, "MinQty", "MinNoOfItemOffered", "PromotionItemQty"))
        try:
            min_qty = max(1, int(float(min_qty_str))) if min_qty_str else 1
        except (ValueError, TypeError):
            min_qty = 1
        promo_price = round(promo_price_raw / min_qty, 2) if (promo_price_raw and min_qty > 1) else promo_price_raw

        for item in promo.iter("Item"):
            barcode = _text(_find(item, "ItemCode", "Barcode"))
            if not barcode or (active_barcodes and barcode not in active_barcodes):
                continue

            name = _text(_find(item, "ItemName", "ProductName"))
            price = _safe_float(_text(_find(item, "ItemPrice", "RegularPrice")))
            discount_pct = None
            if price and promo_price and price > 0:
                discount_pct = round((price - promo_price) / price * 100, 1)

            records.append({
                "barcode": barcode, "name": name,
                "manufacturer": "", "format_name": format_name,
                "price": price, "promo_price": promo_price,
                "promo_description": promo_desc or None,
                "fixed_price": None, "discount_pct": discount_pct,
                "file_type": file_type,
            })
            raw.append({
                "chain_id": chain_id, "store_id": store_id,
                "promotion_id": promo_id or "",
                "promotion_description": promo_desc,
                "start_date": start_date, "end_date": end_date,
                "discounted_price": promo_price_raw,
                "min_qty": _safe_float(min_qty_str),
                "item_code": barcode, "format_name": format_name,
            })
    return records, raw


def _parse_xml_prices(xml_bytes: bytes, format_name: str, active_barcodes: set, file_type: str = "Price") -> tuple[list[dict], list[dict]]:
    """Returns (processed_records, raw_price_records)."""
    records, raw = [], []
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        logger.error("XML parse error in %s: %s", format_name, e)
        return records, raw

    chain_id = _text(_find(root, "ChainId"))
    store_id  = _text(_find(root, "StoreId"))

    for item in root.iter("Item"):
        barcode = _text(_find(item, "ItemCode", "Barcode"))
        if not barcode or (active_barcodes and barcode not in active_barcodes):
            continue

        name         = _text(_find(item, "ItemName", "ProductName"))
        price        = _safe_float(_text(_find(item, "ItemPrice", "UnitOfMeasurePrice")))
        manufacturer = _text(_find(item, "ManufacturerName"))

        records.append({
            "barcode": barcode, "name": name,
            "manufacturer": manufacturer, "format_name": format_name,
            "price": price, "promo_price": None,
            "fixed_price": None, "discount_pct": None,
            "file_type": file_type,
        })
        raw.append({
            "chain_id": chain_id, "store_id": store_id,
            "item_code": barcode, "item_name": name,
            "item_price": price, "manufacturer_name": manufacturer,
            "manufacturer_item_desc": _text(_find(item, "ManufacturerItemDescription")),
            "unit_of_measure": _text(_find(item, "UnitOfMeasure")),
            "quantity":       _safe_float(_text(_find(item, "Quantity"))),
            "allow_discount": _safe_float(_text(_find(item, "AllowDiscount"))),
            "item_status":    _safe_float(_text(_find(item, "ItemStatus"))),
            "format_name": format_name,
        })
    return records, raw


async def download_and_parse(url: str, file_type: str, format_name: str, active_barcodes: set) -> tuple[list[dict], list[dict]]:
    """Returns (processed_records, raw_records)."""
    async with httpx.AsyncClient(timeout=60) as client:
        logger.info("Downloading %s...", url.split("/")[-1].split("?")[0])
        resp = await client.get(url, headers=HEADERS, follow_redirects=True)
        resp.raise_for_status()
        gz_data = resp.content

    try:
        with gzip.open(io.BytesIO(gz_data)) as f:
            xml_bytes = f.read()
    except Exception as e:
        logger.error("Failed to decompress %s: %s", url, e)
        return [], []

    if file_type in ("Promo", "PromoFull"):
        return _parse_xml_promos(xml_bytes, format_name, active_barcodes, file_type=file_type)
    return _parse_xml_prices(xml_bytes, format_name, active_barcodes, file_type=file_type)


def _parse_xml_stores(xml_bytes: bytes) -> tuple[list[str], dict[str, int], list[dict]]:
    """Extract unique SubChainName values, branch counts, and full raw store records."""
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        logger.error("XML parse error in stores file: %s", e)
        return [], {}, []
    formats: set[str] = set()
    store_counts: dict[str, int] = {}
    raw_stores: list[dict] = []
    # Support both standard format (<Branch>) and SAP ABAP format (<STORE>)
    branch_iter = list(root.iter("Branch")) or list(root.iter("STORE"))
    for branch in branch_iter:
        name_el = _find(branch, "SubChainName", "SUBCHAINNAME", "StoreType", "STORETYPE", "ChainName")
        raw_name = _text(name_el).strip() if name_el is not None else ""
        fmt = map_consumer_format(raw_name) if raw_name else ""
        if raw_name:
            formats.add(fmt)
            if fmt in FORMAT_KEYWORDS:
                store_counts[fmt] = store_counts.get(fmt, 0) + 1
        raw_stores.append({
            "chain_id":       _text(_find(branch, "ChainId",       "CHAINID")),
            "chain_name":     _text(_find(branch, "ChainName",     "CHAINNAME")),
            "sub_chain_name": _text(_find(branch, "SubChainName",  "SUBCHAINNAME")),
            "sub_chain_code": _text(_find(branch, "SubChainCode",  "SUBCHAINCODE")),
            "store_id":       _text(_find(branch, "StoreId",       "STOREID")),
            "store_name":     _text(_find(branch, "StoreName",     "STORENAME")),
            "city":           _text(_find(branch, "City",          "CITY")),
            "address":        _text(_find(branch, "Address",       "ADDRESS")),
            "store_type":     _text(_find(branch, "StoreType",     "STORETYPE")),
            "latitude":       _text(_find(branch, "Latitude",      "LATITUDE")),
            "longitude":      _text(_find(branch, "Longitude",     "LONGITUDE")),
            "format_name":    fmt,
        })
    logger.info("Stores XML: %d branches, %d unique formats, store_counts=%s",
                len(raw_stores), len(formats), store_counts)
    return sorted(formats), store_counts, raw_stores


async def download_and_parse_stores(url: str) -> tuple[list[str], dict[str, int], list[dict]]:
    """Download a Stores GZ file and return (formats, store_counts, raw_store_records)."""
    async with httpx.AsyncClient(timeout=60) as client:
        logger.info("Downloading stores file: %s", url.split("/")[-1].split("?")[0])
        resp = await client.get(url, headers=HEADERS, follow_redirects=True)
        resp.raise_for_status()
    try:
        with gzip.open(io.BytesIO(resp.content)) as f:
            xml_bytes = f.read()
    except Exception as e:
        logger.error("Failed to decompress stores file: %s", e)
        return [], {}, []
    return _parse_xml_stores(xml_bytes)



def _merge_price_promo(price_recs: list[dict], promo_recs: list[dict]) -> list[dict]:
    """
    Merge PriceFull (catalog) and PromoFull (promo) records.

    PriceFull records are the base — they contain all products with their
    catalog shelf prices.  PromoFull records overlay promo_price on matching
    (barcode, format_name) pairs.  Items that appear only in PromoFull (edge
    case where PriceFull is missing) are included as-is.
    """
    merged: dict[tuple, dict] = {}
    for r in price_recs:
        merged[(r["barcode"], r["format_name"])] = r.copy()
    for r in promo_recs:
        key = (r["barcode"], r["format_name"])
        if key in merged:
            merged[key]["promo_price"] = r["promo_price"]
        else:
            merged[key] = r.copy()
    return list(merged.values())


async def run(promo_files: list, active_barcodes: list[str], price_files: list | None = None) -> dict:
    """
    Download and parse promo and/or price files, then merge.

    - promo_files: PromoFull / Promo entries  → sets promo_price
    - price_files: PriceFull / Price entries  → sets price (catalog)

    Returns {"records": list[dict], "failed": list[str]}.
    """
    if not promo_files and not price_files:
        return {"records": [], "failed": []}

    barcode_set = set(active_barcodes)
    import asyncio

    # Limit concurrent downloads to avoid exhausting file descriptors on Windows
    sem = asyncio.Semaphore(50)

    async def _bounded(entry):
        async with sem:
            return await download_and_parse(entry.url, entry.file_type, _format_name(entry), barcode_set)

    failed: list[str] = []

    # ── Parse promo files ──────────────────────────────────────────────────
    promo_records: list[dict] = []
    raw_promo: list[dict] = []
    if promo_files:
        promo_results = await asyncio.gather(*[_bounded(e) for e in promo_files], return_exceptions=True)
        for result, entry in zip(promo_results, promo_files):
            if isinstance(result, Exception):
                logger.error("Parser task failed for %s: %s", entry.filename, result)
                failed.append(entry.filename)
            else:
                processed, raw = result
                ts = getattr(entry, "timestamp", None)
                for r in processed:
                    r["source_url"] = entry.url
                    r["source_ts"] = ts
                for r in raw:
                    r["source_url"] = entry.url
                    r["source_ts"] = ts
                promo_records.extend(processed)
                raw_promo.extend(raw)

    # ── Parse price files ──────────────────────────────────────────────────
    price_records: list[dict] = []
    raw_price: list[dict] = []
    if price_files:
        price_results = await asyncio.gather(*[_bounded(e) for e in price_files], return_exceptions=True)
        for result, entry in zip(price_results, price_files):
            if isinstance(result, Exception):
                logger.error("Parser task failed for %s: %s", entry.filename, result)
                failed.append(entry.filename)
            else:
                processed, raw = result
                ts = getattr(entry, "timestamp", None)
                for r in processed:
                    r["source_url"] = entry.url
                    r["source_ts"] = ts
                for r in raw:
                    r["source_url"] = entry.url
                    r["source_ts"] = ts
                price_records.extend(processed)
                raw_price.extend(raw)

    # ── Merge ──────────────────────────────────────────────────────────────
    all_records = _merge_price_promo(price_records, promo_records)

    total_files = len(promo_files or []) + len(price_files or [])
    if failed:
        logger.warning("Partial data: %d/%d files failed: %s", len(failed), total_files, failed)

    logger.info(
        "Parsed %d promo records + %d price records → %d merged records (%d/%d files ok)",
        len(promo_records), len(price_records), len(all_records),
        total_files - len(failed), total_files,
    )
    return {"records": all_records, "failed": failed, "raw_price": raw_price, "raw_promo": raw_promo}
