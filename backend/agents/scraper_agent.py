"""Scraper Agent — fetches GZ file index from prices.shufersal.co.il."""
import html as html_mod
import re
import logging
from dataclasses import dataclass
from urllib.parse import urlparse

import httpx

from backend.constants import map_consumer_format as _map_fmt_module, FORMAT_KEYWORDS as _FMT_MODULE

logger = logging.getLogger(__name__)

SHUFERSAL_INDEX = "https://prices.shufersal.co.il/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; tempo-price-monitor/1.0)",
    "Accept": "text/html,application/xhtml+xml",
}


@dataclass
class FileEntry:
    url: str
    filename: str
    file_type: str   # "PromoFull" | "PriceFull" | "Promo" | "Price" | "Stores"
    store_type: str
    timestamp: str   # YYYYMMDDHHMM


def _get_file_type(filename: str) -> str:
    fn_lower = filename.lower()
    base = fn_lower.split("-")[0].split(".")[0]
    if base.startswith("promofull"):
        return "PromoFull"
    if base.startswith("pricefull"):
        return "PriceFull"
    if base.startswith("promo"):
        return "Promo"
    if base.startswith("price"):
        return "Price"
    if base.startswith("store"):
        return "Stores"
    return "Unknown"


def _make_absolute(href: str, base: str) -> str:
    if href.startswith("http"):
        return href
    parsed = urlparse(base)
    if href.startswith("/"):
        return f"{parsed.scheme}://{parsed.netloc}{href}"
    return base.rstrip("/") + "/" + href


def _extract_entries(html_text: str, base_url: str) -> list[FileEntry]:
    """Extract all GZ file entries from Shufersal index HTML using regex."""
    entries = []
    seen = set()

    link_pattern = re.compile(
        r'<a[^>]+href=["\']([^"\']*\.gz[^"\']*)["\'][^>]*>([^<]*)</a>',
        re.IGNORECASE,
    )
    row_pattern = re.compile(r'<tr[^>]*>(.*?)</tr>', re.IGNORECASE | re.DOTALL)
    td_pattern = re.compile(r'<td[^>]*>(.*?)</td>', re.IGNORECASE | re.DOTALL)

    for row_match in row_pattern.finditer(html_text):
        row_html = row_match.group(1)
        gz_links = link_pattern.findall(row_html)
        if not gz_links:
            continue

        tds = td_pattern.findall(row_html)
        # TD layout: [0]=button [1]=date [2]=size [3]=format [4]=category [5]=branch [6]=filename [7]=id
        raw_branch = html_mod.unescape(re.sub(r'<[^>]+>', '', tds[5])).strip() if len(tds) > 5 else ""
        raw_format = html_mod.unescape(re.sub(r'<[^>]+>', '', tds[3])).strip() if len(tds) > 3 else ""
        # Strip numeric prefix e.g. "842 - אקספרס מטודלה" → "אקספרס מטודלה"
        branch_clean = re.sub(r'^\d+\s*-\s*', '', raw_branch).strip()
        # Prefer format column (tds[3]) if it maps to a known consumer format, else use branch (tds[5])
        store_type = raw_format if _map_fmt_module(raw_format) in _FMT_MODULE else branch_clean

        for href, _link_text in gz_links:
            href = html_mod.unescape(href)  # decode &amp; → & in SAS token params
            url = _make_absolute(href, base_url)
            filename = href.split("/")[-1].split("?")[0]

            if filename in seen:
                continue
            seen.add(filename)

            # Use 20XXXXXXXXXX pattern to avoid matching chain IDs like 7290027600007
            ts_match = re.search(r"(20\d{10})", filename)
            timestamp = ts_match.group(1) if ts_match else ""
            file_type = _get_file_type(filename)

            entries.append(FileEntry(
                url=url,
                filename=filename,
                file_type=file_type,
                store_type=store_type,
                timestamp=timestamp,
            ))

    return entries


def _get_last_page(html_text: str) -> int:
    """Extract the last page number from the pagination HTML."""
    match = re.search(r'page=(\d+)[^"\']*["\'][^>]*>»|page=(\d+)[^"\']*["\'][^>]*>&gt;&gt;', html_text)
    if match:
        return int(match.group(1) or match.group(2))
    # Fallback: find highest page= number in pagination
    pages = re.findall(r'[?&]page=(\d+)', html_text)
    if pages:
        return max(int(p) for p in pages)
    return 1


async def fetch_index(page: int = 1) -> list[FileEntry]:
    params = {"page": page, "per_page": 100}
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        resp = await client.get(SHUFERSAL_INDEX, params=params, headers=HEADERS)
        resp.raise_for_status()
        return _extract_entries(resp.text, str(resp.url))


async def fetch_index_with_html(page: int = 1) -> tuple[list[FileEntry], str]:
    params = {"page": page, "per_page": 100}
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        resp = await client.get(SHUFERSAL_INDEX, params=params, headers=HEADERS)
        resp.raise_for_status()
        return _extract_entries(resp.text, str(resp.url)), resp.text



async def run(last_timestamp: str = "") -> dict:
    # ── Step 1: Fetch page 1 to check freshness and find total pages ──────────
    logger.info("Fetching Shufersal index page 1...")
    try:
        page1_entries, page1_html = await fetch_index_with_html(page=1)
    except Exception as e:
        logger.error("Failed to fetch Shufersal index: %s", e)
        return {"promo_files": [], "store_file": None, "latest_timestamp": last_timestamp, "new_data": False}

    last_page = _get_last_page(page1_html)
    logger.info("Found %d entries on page 1, last page = %d", len(page1_entries), last_page)

    # Use Price/Promo delta files on page 1 to detect freshness
    all_timestamps = [e.timestamp for e in page1_entries if e.timestamp]
    latest_delta_ts = max(all_timestamps) if all_timestamps else ""

    if latest_delta_ts and latest_delta_ts <= last_timestamp:
        logger.info("No new data (latest=%s, last=%s)", latest_delta_ts, last_timestamp)
        return {"promo_files": [], "store_file": None, "latest_timestamp": latest_delta_ts, "new_data": False}

    logger.info("New data detected: delta timestamp %s", latest_delta_ts)

    # ── Step 2: Fetch last pages to find PromoFull / PriceFull files ──────────
    full_files: list[FileEntry] = []
    store_file = None

    # Scan ALL pages to capture every PromoFull/PriceFull file.
    # PriceFull files (published daily) can appear anywhere in the index —
    # some branches publish early (pages 1-x) others later (higher pages).
    # Fetching all pages in parallel has minimal overhead vs. partial scan.
    pages_to_fetch = list(range(1, last_page + 1))
    logger.info("Fetching pages %s-%s (%d pages) for full catalog files...",
                pages_to_fetch[0], pages_to_fetch[-1], len(pages_to_fetch))

    import asyncio
    page_results = await asyncio.gather(
        *[fetch_index(p) for p in pages_to_fetch],
        return_exceptions=True,
    )

    all_full_entries: list[FileEntry] = []
    for result in page_results:
        if isinstance(result, Exception):
            logger.warning("Failed to fetch a page: %s", result)
        else:
            all_full_entries.extend(result)

    for ft in ("PromoFull", "PriceFull", "Promo", "Price", "Stores", "Unknown"):
        count = sum(1 for e in all_full_entries if e.file_type == ft)
        if count:
            logger.info("  %s: %d files on last pages", ft, count)

    store_entries = [e for e in all_full_entries if e.file_type == "Stores"]
    if not store_entries:
        store_entries = [e for e in page1_entries if e.file_type == "Stores"]
    store_file = store_entries[0] if store_entries else None

    # ── Select PromoFull and PriceFull separately ─────────────────────────
    # PriceFull = all products with catalog shelf prices (required by Israeli
    #   Price Transparency Law, published daily)
    # PromoFull = only items currently on promotion (published daily)
    # Both are needed: PriceFull is the base, PromoFull overlays promo prices.
    from backend.constants import map_consumer_format as _map_fmt, FORMAT_KEYWORDS as _FMT

    def _select_by_format(entries: list) -> list:
        """Select ALL files per recognized consumer format from the given entries."""
        selected: list = []
        for e in entries:
            fmt = _map_fmt(e.store_type) if e.store_type else ""
            if fmt in _FMT:
                selected.append(e)
        unrecognized = {e.store_type for e in entries if e.store_type and _map_fmt(e.store_type) not in _FMT}
        if unrecognized:
            logger.warning("Unrecognized store_types dropped: %s", sorted(unrecognized))
        return selected

    promo_candidates = [e for e in all_full_entries if e.file_type in ("PromoFull", "Promo")]
    price_candidates = [e for e in all_full_entries if e.file_type in ("PriceFull", "Price")]

    # Fallback: price candidates from page1 if not found in last 60 pages
    if not price_candidates:
        price_candidates = [e for e in page1_entries if e.file_type in ("PriceFull", "Price")]

    promo_files = _select_by_format(promo_candidates) or promo_candidates[:20]
    price_files = _select_by_format(price_candidates) or price_candidates[:20]

    logger.info("Selected %d promo files, %d price files", len(promo_files), len(price_files))

    # Best timestamp: max across both file sets
    full_timestamps = [e.timestamp for e in promo_files + price_files if e.timestamp]
    best_ts = max(full_timestamps) if full_timestamps else latest_delta_ts

    logger.info("Pipeline ready, best_ts=%s", best_ts)
    return {
        "promo_files": promo_files,
        "price_files": price_files,
        "store_file": store_file,
        "latest_timestamp": best_ts or latest_delta_ts,
        "new_data": True,
    }
