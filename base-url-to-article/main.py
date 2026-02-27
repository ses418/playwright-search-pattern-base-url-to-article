"""
Single URL Article Scraper — Supabase Edition (v4 fixed)
=========================================================
CHANGE in this version vs previous:
  - Search method/pattern is now fetched from Supabase table
    `public.base_url_search_patterns` instead of a local CSV file.
  - No local CSV files needed at all. Everything comes from Supabase.

FIX also included:
  - JWT role validator: detects anon key at startup and exits immediately
    with clear instructions to use the service_role key instead.

HOW TO RUN:
  pip install playwright pandas supabase python-dotenv
  playwright install chromium
  python scraper_v4.py --base_url "https://example.com"

.env file (same folder as script):
  SUPABASE_URL=https://supabase.sesai.in
  SUPABASE_KEY=<your SERVICE_ROLE key — NOT the anon key>
"""

import argparse
import asyncio
import base64
import io
import json
import logging
import os
import re
import sys
import time
import uuid
from datetime import datetime, timezone, timedelta
from urllib.parse import quote_plus, urljoin, urlparse

import pandas as pd
from playwright.async_api import async_playwright

# ─── Load .env if present ────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ─── Supabase client ─────────────────────────────────────────────────────────
try:
    from supabase import create_client, Client as SupabaseClient
except ImportError:
    print("[FATAL] supabase-py not installed. Run: pip install supabase")
    sys.exit(1)

# ─── UTF-8 stdout/stderr (Windows CP1252 fix) ────────────────────────────────
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("scraper_v4.log", mode="w", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ─── Date filter ─────────────────────────────────────────────────────────────
TWO_YEAR_CUTOFF = datetime.now(tz=timezone.utc) - timedelta(days=365 * 2)


# ==============================================================================
# JWT KEY ROLE VALIDATOR
# ==============================================================================

def decode_jwt_role(jwt_token: str) -> str:
    """Decode Supabase JWT payload and return the 'role' field."""
    try:
        parts = jwt_token.strip().split(".")
        if len(parts) != 3:
            return "unknown"
        payload = parts[1]
        payload += "=" * (4 - len(payload) % 4)
        decoded = json.loads(base64.b64decode(payload).decode("utf-8"))
        return decoded.get("role", "unknown")
    except Exception:
        return "unknown"


def validate_supabase_key(supabase_key: str):
    """Hard-stops with a clear message if the anon key is used instead of service_role."""
    role = decode_jwt_role(supabase_key)
    log.info(f"[KEY CHECK] JWT role decoded: '{role}'")

    if role == "service_role":
        log.info("[KEY CHECK] PASS — service_role key detected. RLS will be bypassed.")
        return

    if role == "anon":
        log.error(
            "\n" + "=" * 70 + "\n"
            "[FATAL] You are using the ANON key — inserts will ALWAYS fail with:\n"
            "  'new row violates row-level security policy' (code 42501)\n\n"
            "HOW TO FIX:\n"
            "  1. Open Supabase Dashboard → Project Settings → API\n"
            "  2. Copy the 'service_role' key (marked 'secret')\n"
            "  3. Update your .env:\n"
            "       SUPABASE_KEY=<service_role key here>\n"
            + "=" * 70
        )
        sys.exit(1)

    log.warning(
        f"[KEY CHECK] Could not confirm role (got: '{role}'). "
        "Proceeding, but inserts may fail if this is not a service_role key."
    )


def explain_insert_error(err_str: str, article_url: str) -> str:
    if "42501" in err_str or "row-level security" in err_str.lower():
        return (
            f"[RLS BLOCK] '{article_url[:60]}'\n"
            "  Cause: Using anon key. Use service_role key to fix."
        )
    if "23505" in err_str or "duplicate" in err_str.lower() or "unique" in err_str.lower():
        return f"[DUPLICATE] '{article_url[:60]}' already exists"
    if "23503" in err_str or "foreign key" in err_str.lower():
        return f"[FK ERROR] '{article_url[:60]}' — referenced ID not found"
    if "connection" in err_str.lower() or "timeout" in err_str.lower():
        return f"[NETWORK] '{article_url[:60]}': {err_str[:100]}"
    return f"[INSERT ERROR] '{article_url[:60]}': {err_str[:150]}"


# ==============================================================================
# DATE FILTER
# ==============================================================================

def is_within_2_years(date_str) -> tuple:
    if not date_str:
        return True, None
    raw = str(date_str).strip()
    parse_fmts = [
        "%Y-%m-%d %H:%M:%S%z", "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",   "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",             "%B %d, %Y",
        "%b %d, %Y",            "%d %B %Y",
        "%d %b %Y",             "%m/%d/%Y", "%d/%m/%Y",
    ]
    article_dt = None
    for fmt in parse_fmts:
        try:
            parsed = datetime.strptime(raw[:25], fmt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            article_dt = parsed
            break
        except ValueError:
            continue
    if article_dt is None:
        return True, None
    if article_dt >= TWO_YEAR_CUTOFF:
        return True, None
    return (
        False,
        f"Date {article_dt.strftime('%Y-%m-%d')} older than cutoff "
        f"{TWO_YEAR_CUTOFF.strftime('%Y-%m-%d')}"
    )


# ─── Search fallback patterns ────────────────────────────────────────────────
FALLBACK_PATTERNS = [
    ("url_search_param",    "url",   "{base}?s={keyword}"),
    ("url_query_param",     "url",   "{base}?q={keyword}"),
    ("url_search_path",     "url",   "{base}/search?q={keyword}"),
    ("url_search_path_s",   "url",   "{base}/search?s={keyword}"),
    ("url_search_slash",    "url",   "{base}/search/{keyword}/"),
    ("url_tag_path",        "url",   "{base}/?tag={keyword}"),
    ("input_name_search",   "input", "input[name='search']"),
    ("input_name_q",        "input", "input[name='q']"),
    ("input_name_s",        "input", "input[name='s']"),
    ("input_type_search",   "input", "input[type='search']"),
    ("input_placeholder",   "input", "input[placeholder*='earch' i]"),
    ("input_class_search",  "input", "input[class*='search' i]"),
    ("input_id_search",     "input", "input[id*='search' i]"),
    ("icon_aria_search",    "icon",  "button[aria-label*='Search' i]"),
    ("icon_class_search",   "icon",  "button[class*='search' i]"),
    ("icon_i_search",       "icon",  "i[class*='search' i]"),
    ("icon_svg_search",     "icon",  "svg[class*='search' i]"),
    ("icon_class_srch_btn", "icon",  ".search-button"),
    ("icon_class_srch_icn", "icon",  ".search-icon"),
    ("icon_class_srch_tog", "icon",  ".search-toggle"),
]

POST_ICON_INPUT_SELECTORS = [
    "input[type='search']", "input[name='q']", "input[name='s']",
    "input[placeholder*='earch' i]", "input[class*='search' i]", "input:visible",
]

ARTICLE_LINK_SELECTOR_GROUPS = [
    ["article h1 a[href]", "article h2 a[href]", "article h3 a[href]",
     "article .entry-title a[href]", "article .post-title a[href]",
     "article .article-title a[href]", "article .news-title a[href]",
     "article a[href]"],
    ["h2 a[href]", "h3 a[href]", ".entry-title a[href]", ".post-title a[href]",
     ".news-title a[href]", ".article-title a[href]"],
    [".result a[href]", ".search-result a[href]", "li.result a[href]",
     "a.result-title[href]", "[class*='search-result'] a[href]"],
    [".post a[href]", ".article a[href]", "[class*='article'] a[href]",
     "[class*='post-item'] a[href]", "[class*='news-item'] a[href]"],
    ["main a[href]", "#content a[href]", ".content a[href]", "[role='main'] a[href]"],
]

NON_ARTICLE_PATH_SEGMENTS = {
    "topics", "topic", "category", "categories", "cat", "tag", "tags",
    "label", "labels", "explore", "browse", "author", "authors",
    "contributor", "contributors", "page", "archive", "archives", "search",
    "feed", "rss", "newsletter", "subscribe", "subscription",
    "about", "contact", "advertise", "careers", "events", "webinar",
    "webinars", "conference", "podcast", "podcasts", "video", "videos",
    "gallery", "product", "products", "shop", "store",
    "login", "register", "signup", "account",
}
MIN_ANCHOR_TEXT_LEN = 20

TITLE_SELECTORS = [
    "h1.entry-title", "h1.post-title", "h1.article-title",
    "h1[class*='title']", "h1[itemprop='headline']",
    ".headline", "article h1", "h1",
]
DATE_SELECTORS = [
    "time[datetime]", "[itemprop='datePublished']", ".published",
    ".post-date", ".entry-date", "[class*='date']", "[class*='time']",
    "meta[property='article:published_time']",
]
TEXT_SELECTORS = [
    "article .entry-content", "article .post-content",
    "article .article-body", "[itemprop='articleBody']",
    ".article-content", ".post-body", "article", "main",
]

COMPANY_RE = re.compile(
    r'\b([A-Z][A-Za-z0-9&\.\-]+(?: [A-Z][A-Za-z0-9&\.\-]+)*'
    r'\s*(?:Inc\.?|Corp\.?|Ltd\.?|LLC|PLC|GmbH|Co\.?|Group|Holdings?'
    r'|Technologies?|Solutions?|Services?))\b'
)
LOCATION_RE = re.compile(
    r'\b([A-Z][a-z]+(?: [A-Z][a-z]+)*),\s*'
    r'([A-Z]{2}|[A-Z][a-z]+(?: [A-Z][a-z]+)*)\b'
)


# ==============================================================================
# SUPABASE INIT
# ==============================================================================

def init_supabase(supabase_url: str, supabase_key: str) -> SupabaseClient:
    if not supabase_url or not supabase_key:
        log.error(
            "[FATAL] Supabase credentials missing.\n"
            "  Set SUPABASE_URL and SUPABASE_KEY in your .env file."
        )
        sys.exit(1)
    validate_supabase_key(supabase_key)
    client = create_client(supabase_url, supabase_key)
    log.info(f"[SUPABASE] Connected to {supabase_url}")
    return client


# ==============================================================================
# SUPABASE DATA LOADING
# ==============================================================================

def fetch_base_url_row(sb: SupabaseClient, base_url_raw: str, base_url_id_arg) -> dict:
    base_url = base_url_raw.rstrip("/")
    if base_url_id_arg:
        log.info(f"[SUPABASE] Fetching base_url row by id={base_url_id_arg}")
        resp = (
            sb.table("ses_base_url")
            .select("base_url_id, base_url, subsegment_id")
            .eq("base_url_id", base_url_id_arg)
            .limit(1).execute()
        )
    else:
        log.info(f"[SUPABASE] Fetching base_url row by URL: {base_url}")
        resp = (
            sb.table("ses_base_url")
            .select("base_url_id, base_url, subsegment_id")
            .eq("base_url", base_url)
            .limit(1).execute()
        )
        if not resp.data:
            resp = (
                sb.table("ses_base_url")
                .select("base_url_id, base_url, subsegment_id")
                .eq("base_url", base_url + "/")
                .limit(1).execute()
            )

    if not resp.data:
        log.error(f"[FATAL] '{base_url}' not found in ses_base_url table.")
        sys.exit(1)

    row = resp.data[0]
    log.info(
        f"[SUPABASE] base_url row: "
        f"base_url_id={row['base_url_id']}  subsegment_id={row.get('subsegment_id')}"
    )
    return row


def fetch_subsegment_and_segment(sb: SupabaseClient, subsegment_id) -> tuple:
    if not subsegment_id:
        return None, None, None
    resp = (
        sb.table("ses_subsegments")
        .select("subsegment_id, subsegment_name, segment_id")
        .eq("subsegment_id", subsegment_id)
        .limit(1).execute()
    )
    if not resp.data:
        log.warning(f"[SUPABASE] subsegment_id={subsegment_id} not found")
        return None, None, None
    sub         = resp.data[0]
    subseg_name = sub.get("subsegment_name")
    segment_id  = sub.get("segment_id")
    seg_name    = None
    if segment_id:
        seg_resp = (
            sb.table("ses_segments")
            .select("segment_id, segment_name")
            .eq("segment_id", segment_id)
            .limit(1).execute()
        )
        if seg_resp.data:
            seg_name = seg_resp.data[0].get("segment_name")
    log.info(f"[SUPABASE] subsegment='{subseg_name}'  segment='{seg_name}'")
    return subseg_name, seg_name, segment_id


def fetch_keywords(sb: SupabaseClient, subsegment_id) -> list:
    if not subsegment_id:
        return []
    resp = (
        sb.table("ses_keywords")
        .select("keyword")
        .eq("subsegment_id", subsegment_id)
        .execute()
    )
    keywords = [r["keyword"] for r in resp.data if r.get("keyword")]
    log.info(f"[SUPABASE] {len(keywords)} keywords for subsegment_id={subsegment_id}: {keywords}")
    return keywords


def resolve_search_terms(keywords, subsegment_name, segment_name) -> tuple:
    if keywords:
        log.info(f"[TERMS] Using {len(keywords)} keywords from ses_keywords")
        return keywords, "keywords"
    if subsegment_name:
        log.info(f"[TERMS] No keywords → falling back to subsegment: '{subsegment_name}'")
        return [subsegment_name], "subsegment"
    if segment_name:
        log.info(f"[TERMS] No subsegment → falling back to segment: '{segment_name}'")
        return [segment_name], "segment"
    log.error("[FATAL] No keywords, subsegment, or segment name available.")
    sys.exit(1)


# ==============================================================================
# ▼▼▼ CHANGED: load search pattern from Supabase instead of local CSV ▼▼▼
# ==============================================================================

def load_search_pattern_from_supabase(sb: SupabaseClient, base_url_id: str, base_url_raw: str) -> dict:
    """
    Fetch method/pattern for this base URL from Supabase table:
        public.base_url_search_patterns

    Lookup priority:
      1. Match by base_url_id (exact, using the FK — fastest and most reliable)
      2. Match by base_url string (fallback if base_url_id somehow differs)

    Returns: { method, pattern, confidence, result_type }
    Exits with a clear error if no row is found.

    Table schema used:
        base_url_search_patterns (
            id, base_url_id (FK → ses_base_url), base_url,
            method, pattern, confidence, result_type, created_at
        )
    """
    log.info(
        f"[SUPABASE] Fetching search pattern from base_url_search_patterns\n"
        f"  base_url_id = {base_url_id}\n"
        f"  base_url    = {base_url_raw}"
    )

    # ── Lookup 1: by base_url_id (FK, unique constraint guaranteed) ───────
    resp = (
        sb.table("base_url_search_patterns")
        .select("method, pattern, confidence, result_type, base_url")
        .eq("base_url_id", base_url_id)
        .limit(1)
        .execute()
    )

    # ── Lookup 2 (fallback): by base_url string if id lookup returned nothing
    if not resp.data:
        log.warning(
            f"[SUPABASE] No pattern found by base_url_id={base_url_id}, "
            f"trying base_url string match..."
        )
        base_url_norm = base_url_raw.rstrip("/")
        resp = (
            sb.table("base_url_search_patterns")
            .select("method, pattern, confidence, result_type, base_url")
            .eq("base_url", base_url_norm)
            .limit(1)
            .execute()
        )
        if not resp.data:
            # Try with trailing slash
            resp = (
                sb.table("base_url_search_patterns")
                .select("method, pattern, confidence, result_type, base_url")
                .eq("base_url", base_url_norm + "/")
                .limit(1)
                .execute()
            )

    if not resp.data:
        log.error(
            f"\n{'='*70}\n"
            f"[FATAL] No search pattern found in base_url_search_patterns for:\n"
            f"  base_url_id = {base_url_id}\n"
            f"  base_url    = {base_url_raw}\n\n"
            "  This URL needs to be added to the base_url_search_patterns table first.\n"
            "  Required columns: base_url_id, base_url, method, pattern, confidence, result_type\n"
            f"{'='*70}"
        )
        sys.exit(1)

    row = resp.data[0]
    pattern = {
        "method":      row.get("method"),
        "pattern":     row.get("pattern"),
        "confidence":  int(row.get("confidence") or 0),
        "result_type": row.get("result_type") or "unknown",
    }

    log.info(
        f"[SUPABASE] Search pattern loaded:\n"
        f"  method      = {pattern['method']}\n"
        f"  pattern     = {pattern['pattern']}\n"
        f"  confidence  = {pattern['confidence']}\n"
        f"  result_type = {pattern['result_type']}"
    )
    return pattern

# ==============================================================================
# ▲▲▲ END OF CHANGED SECTION ▲▲▲
# ==============================================================================


# ==============================================================================
# SUPABASE INSERT
# ==============================================================================

def insert_articles_to_supabase(sb: SupabaseClient, rows: list) -> tuple:
    inserted = 0
    skipped  = 0
    for row in rows:
        record = {
            "unfiltered_article_id": row["unfiltered_article_id"],
            "search_url_id":         None,
            "article_link":          row["article_link"],
            "article_title":         row.get("article_title"),
            "article_date":          row.get("article_date"),
            "companies_mentioned":   row.get("companies_mentioned"),
            "location":              row.get("location"),
            "extracted_text":        row.get("extracted_text"),
            "is_valid":              True,
            "drop_reason":           None,
            "filter_article_status": "pending",
            "subsegment_name":       row.get("subsegment_name"),
            "base_url_id":           row.get("base_url_id"),
        }
        try:
            sb.table("ses_unfiltered_articles").insert(record).execute()
            inserted += 1
            log.debug(f"  [INSERT] OK: {row['article_link'][:80]}")
        except Exception as e:
            err_str = str(e)
            msg     = explain_insert_error(err_str, row["article_link"])
            if "23505" in err_str or "duplicate" in err_str.lower() or "unique" in err_str.lower():
                skipped += 1
                log.debug(f"  {msg}")
            elif "42501" in err_str or "row-level security" in err_str.lower():
                log.error(f"\n{'='*70}\n  {msg}\n{'='*70}")
                skipped += 1
            else:
                log.error(f"  {msg}")
                skipped += 1
    log.info(f"  [SUPABASE BATCH] inserted={inserted}  skipped={skipped}")
    return inserted, skipped


def maybe_save_local_csv(rows: list, path, write_header: bool):
    if not path or not rows:
        return
    col_order = [
        "unfiltered_article_id", "search_url_id", "article_link",
        "article_title", "article_date", "companies_mentioned", "location",
        "extracted_text", "is_valid", "drop_reason", "filter_article_status",
        "created_at", "subsegment_name", "base_url_id",
        "keyword_used", "search_url", "method_used", "search_term_source",
    ]
    df = pd.DataFrame(rows)
    for c in col_order:
        if c not in df.columns:
            df[c] = None
    df[col_order].to_csv(path, mode="w" if write_header else "a",
                         header=write_header, index=False, encoding="utf-8")
    log.info(f"  [CSV BACKUP] {len(rows)} rows → {path}")


# ==============================================================================
# PLAYWRIGHT HELPERS — unchanged
# ==============================================================================

async def try_url_pattern(page, base_url, keyword, url_template):
    base   = base_url.rstrip("/")
    kw_enc = quote_plus(keyword)
    url    = url_template.replace("{base}", base).replace("{keyword}", kw_enc)
    log.debug(f"    [url] {url}")
    try:
        resp = await page.goto(url, timeout=20000, wait_until="domcontentloaded")
        return bool(resp and resp.status < 400)
    except Exception as e:
        log.debug(f"    [url] ERROR: {e}")
        return False


async def try_input_pattern(page, selector, keyword):
    log.debug(f"    [input] {selector}")
    try:
        el = page.locator(selector).first
        if await el.count() == 0:
            return False
        await el.scroll_into_view_if_needed(timeout=5000)
        await el.click(timeout=5000)
        await el.fill(keyword)
        await el.press("Enter")
        await page.wait_for_load_state("domcontentloaded", timeout=15000)
        return True
    except Exception as e:
        log.debug(f"    [input] ERROR: {e}")
        return False


async def try_icon_pattern(page, selector, keyword):
    log.debug(f"    [icon] {selector}")
    try:
        el = page.locator(selector).first
        if await el.count() == 0:
            return False
        await el.click(timeout=5000)
        await page.wait_for_timeout(800)
        for inp_sel in POST_ICON_INPUT_SELECTORS:
            inp = page.locator(inp_sel).first
            if await inp.count() > 0 and await inp.is_visible():
                await inp.fill(keyword)
                await inp.press("Enter")
                await page.wait_for_load_state("domcontentloaded", timeout=15000)
                return True
        return False
    except Exception as e:
        log.debug(f"    [icon] ERROR: {e}")
        return False


def is_likely_article_url(url: str, anchor_text: str, base_url: str) -> tuple:
    parsed     = urlparse(url)
    path       = parsed.path.rstrip("/")
    text       = (anchor_text or "").strip()
    if text and len(text) < MIN_ANCHOR_TEXT_LEN:
        return False, f"anchor text too short ({len(text)} chars)"
    lower_path = path.lower()
    last_seg   = lower_path.split("/")[-1]
    ext        = ("." + last_seg.rsplit(".", 1)[1]) if "." in last_seg else ""
    if ext in {".pdf", ".jpg", ".jpeg", ".png", ".gif", ".svg",
               ".zip", ".xml", ".json", ".css", ".js"}:
        return False, f"non-article extension: {ext}"
    if ext in (".html", ".htm") and len([s for s in path.split("/") if s]) < 4:
        return False, ".html shallow path"
    for seg in [s.lower() for s in path.split("/") if s]:
        if seg.split("?")[0].split("#")[0] in NON_ARTICLE_PATH_SEGMENTS:
            return False, f"non-article path segment: '{seg}'"
    if len([s for s in path.split("/") if s]) < 2:
        return False, "path too shallow"
    if path == urlparse(base_url).path.rstrip("/") or path == "":
        return False, "is base URL"
    return True, "ok"


async def extract_article_links(page, base_url):
    base_domain = urlparse(base_url).netloc
    accepted    = {}
    rejected    = []
    for group_idx, selectors in enumerate(ARTICLE_LINK_SELECTOR_GROUPS, 1):
        group_accepted = {}
        group_rejected = []
        for sel in selectors:
            try:
                for el in await page.locator(sel).all():
                    try:
                        href = await el.get_attribute("href")
                        if not href or href.startswith(("#", "javascript", "mailto", "tel")):
                            continue
                        abs_url     = urljoin(base_url, href)
                        link_domain = urlparse(abs_url).netloc
                        if base_domain not in link_domain and link_domain not in base_domain:
                            continue
                        if abs_url in accepted or abs_url in group_accepted:
                            continue
                        try:
                            anchor_text = (await el.inner_text()).strip()
                        except Exception:
                            anchor_text = ""
                        keep, reason = is_likely_article_url(abs_url, anchor_text, base_url)
                        if keep:
                            group_accepted[abs_url] = anchor_text
                        else:
                            group_rejected.append((abs_url, reason))
                    except Exception:
                        continue
            except Exception:
                continue
        log.debug(f"    [extract] group {group_idx}: {len(group_accepted)} ok, {len(group_rejected)} skip")
        if group_accepted:
            accepted.update(group_accepted)
            rejected.extend(group_rejected)
            log.info(f"    [extract] Stopped at group {group_idx} — {len(accepted)} links")
            break
        else:
            rejected.extend(group_rejected)
    for url, reason in rejected[:10]:
        log.debug(f"      SKIP [{reason}] {url}")
    log.info(f"    [extract] Final: {len(accepted)} article links")
    return list(accepted.keys())


async def search_and_extract(page, base_url, keyword, known_pattern):
    result = {
        "base_url": base_url, "keyword": keyword, "method_used": None,
        "pattern_used": None, "search_url": None, "articles_found": 0,
        "article_links": [], "status": "failed", "error": None,
    }
    log.info(f"  [step1] Navigating to: {base_url}")
    try:
        resp = await page.goto(base_url, timeout=25000, wait_until="domcontentloaded")
        if resp and resp.status >= 400:
            log.warning(f"  [step1] HTTP {resp.status}")
    except Exception as e:
        log.error(f"  [step1] UNREACHABLE: {e}")
        result["error"]  = str(e)
        result["status"] = "unreachable"
        return result

    search_succeeded = False
    if known_pattern and known_pattern["method"] != "fallback":
        method, pattern = known_pattern["method"], known_pattern["pattern"]
        log.info(f"  [step2] pattern: method={method}  pattern={pattern}")
        if method == "url":
            search_succeeded = await try_url_pattern(page, base_url, keyword, pattern)
        elif method == "input":
            search_succeeded = await try_input_pattern(page, pattern, keyword)
        elif method == "icon":
            search_succeeded = await try_icon_pattern(page, pattern, keyword)
        if search_succeeded:
            result["method_used"]  = method
            result["pattern_used"] = pattern
            log.info("  [step2] pattern SUCCESS")
        else:
            log.warning("  [step2] pattern FAILED → fallbacks")
    elif known_pattern and known_pattern["method"] == "fallback":
        kw_enc  = quote_plus(keyword)
        fb_url  = known_pattern["pattern"]
        nav_url = fb_url + kw_enc if "?" in fb_url else f"{fb_url}?q={kw_enc}"
        log.info(f"  [step2] fallback URL: {nav_url}")
        try:
            resp = await page.goto(nav_url, timeout=20000, wait_until="domcontentloaded")
            if resp and resp.status < 400:
                search_succeeded       = True
                result["method_used"]  = "fallback_url"
                result["pattern_used"] = nav_url
        except Exception as e:
            log.debug(f"  [step2] fallback URL ERROR: {e}")

    if not search_succeeded:
        log.info(f"  [step3] Trying {len(FALLBACK_PATTERNS)} fallback patterns...")
        for label, ftype, fpat in FALLBACK_PATTERNS:
            if ftype in ("input", "icon"):
                try:
                    await page.goto(base_url, timeout=20000, wait_until="domcontentloaded")
                    await page.wait_for_timeout(500)
                except Exception:
                    continue
            ok = False
            if ftype == "url":
                ok = await try_url_pattern(page, base_url, keyword, fpat)
            elif ftype == "input":
                ok = await try_input_pattern(page, fpat, keyword)
            elif ftype == "icon":
                ok = await try_icon_pattern(page, fpat, keyword)
            if ok:
                search_succeeded       = True
                result["method_used"]  = ftype
                result["pattern_used"] = fpat
                log.info(f"  [step3] FALLBACK SUCCESS: [{label}] {fpat}")
                break

    if not search_succeeded:
        log.warning(f"  [step3] ALL patterns failed for '{keyword}'")
        result["status"] = "no_pattern_worked"
        return result

    result["search_url"] = page.url
    log.info(f"  [step4] Results page: {page.url}")
    await page.wait_for_timeout(1500)
    links = await extract_article_links(page, base_url)
    if not links:
        title = await page.title()
        log.warning(f"  [step4] 0 links. Page: '{title}'")
        result["status"] = "no_links_found"
    else:
        result["status"]         = "success"
        result["articles_found"] = len(links)
        result["article_links"]  = links
        log.info(f"  [step4] SUCCESS — {len(links)} links")
    return result


# ==============================================================================
# ARTICLE DETAIL EXTRACTION — unchanged
# ==============================================================================

def _parse_date(raw: str):
    if not raw:
        return None
    raw = raw.strip()
    for fmt in [
        "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d", "%B %d, %Y", "%b %d, %Y",
        "%d %B %Y", "%d %b %Y", "%m/%d/%Y", "%d/%m/%Y",
    ]:
        try:
            dt = datetime.strptime(raw[:25], fmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S+05:30")
        except ValueError:
            continue
    return raw


async def extract_article_details(page, article_url: str) -> dict:
    d = {
        "article_title": None, "article_date": None,
        "extracted_text": None, "companies_mentioned": None, "location": None,
    }
    try:
        resp = await page.goto(article_url, timeout=25000, wait_until="domcontentloaded")
        if resp and resp.status >= 400:
            return d
        await page.wait_for_timeout(1000)
        for sel in TITLE_SELECTORS:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    t = (await el.inner_text()).strip()
                    if t:
                        d["article_title"] = t[:500]
                        break
            except Exception:
                continue
        if not d["article_title"]:
            try:
                d["article_title"] = (await page.title()).strip()[:500]
            except Exception:
                pass
        for sel in DATE_SELECTORS:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    raw = (await el.get_attribute("content") or
                           await el.get_attribute("datetime") or
                           (await el.inner_text()).strip())
                    if raw:
                        d["article_date"] = _parse_date(raw)
                        break
            except Exception:
                continue
        for sel in TEXT_SELECTORS:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    text = (await el.inner_text()).strip()
                    if len(text) > 100:
                        d["extracted_text"] = text[:5000]
                        break
            except Exception:
                continue
        src = d["extracted_text"] or ""
        if not src:
            try:
                src = (await page.inner_text("body"))[:3000]
            except Exception:
                pass
        companies = COMPANY_RE.findall(src)
        d["companies_mentioned"] = "; ".join(list(dict.fromkeys(companies))[:10]) or None
        locs = LOCATION_RE.findall(src[:500])
        d["location"] = "; ".join(
            list(dict.fromkeys(f"{c}, {s}" for c, s in locs))[:5]
        ) or None
    except Exception as e:
        log.warning(f"    [art-detail] Failed for {article_url}: {e}")
    return d


# ==============================================================================
# MAIN
# ==============================================================================

async def main(args):
    base_url = args.base_url.rstrip("/")

    log.info(
        f"\n{'='*70}\n"
        f"  Single URL Article Scraper — Supabase Edition (v4 fixed)\n"
        f"  Target       : {base_url}\n"
        f"  2-year cutoff: {TWO_YEAR_CUTOFF.strftime('%Y-%m-%d')}\n"
        f"{'='*70}"
    )

    supabase_url = args.supabase_url or os.getenv("SUPABASE_URL", "")
    supabase_key = args.supabase_key or os.getenv("SUPABASE_KEY", "")
    sb = init_supabase(supabase_url, supabase_key)

    # Fetch base_url row (gives us base_url_id + subsegment_id)
    base_row      = fetch_base_url_row(sb, base_url, args.base_url_id)
    base_url_id   = str(base_row["base_url_id"])
    subsegment_id = base_row.get("subsegment_id")

    # Fetch segment / subsegment names
    subseg_name, seg_name, segment_id = fetch_subsegment_and_segment(sb, subsegment_id)

    # Fetch keywords
    keywords = fetch_keywords(sb, subsegment_id)
    search_terms, term_source = resolve_search_terms(keywords, subseg_name, seg_name)

    # ── CHANGED: fetch search pattern from Supabase (not local CSV) ───────
    known_pattern = load_search_pattern_from_supabase(sb, base_url_id, base_url)

    log.info(
        f"\n[CONFIG]\n"
        f"  base_url_id  : {base_url_id}\n"
        f"  segment      : {seg_name}\n"
        f"  subsegment   : {subseg_name}\n"
        f"  terms [{term_source}] ({len(search_terms)}): {search_terms}\n"
        f"  method       : {known_pattern['method']}\n"
        f"  pattern      : {known_pattern['pattern']}\n"
        f"  visit pages  : {not args.skip_article_visit}\n"
        f"  CSV backup   : {args.output_csv or 'disabled'}\n"
        f"{'='*70}"
    )

    all_links      = {}
    output_rows    = []
    total_inserted = 0
    total_skipped  = 0
    first_csv_write = True

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
            java_script_enabled=True,
        )
        await context.route(
            "**/*.{png,jpg,jpeg,gif,svg,woff,woff2,ttf,eot,mp4,webm}",
            lambda route: route.abort(),
        )

        # PHASE 1: search each term → collect links
        page = await context.new_page()
        log.info(f"\n[PHASE 1] Searching {len(search_terms)} terms on {base_url}")

        for kw_idx, term in enumerate(search_terms, 1):
            log.info(f"\n  --- term [{kw_idx}/{len(search_terms)}]: '{term}' ---")
            res = await search_and_extract(page, base_url, term, known_pattern)
            await page.wait_for_timeout(1000)
            new_count = 0
            for link in res["article_links"]:
                if link not in all_links:
                    all_links[link] = {
                        "keyword":     term,
                        "search_url":  res["search_url"],
                        "method_used": res["method_used"],
                    }
                    new_count += 1
            log.info(
                f"  [term done] status={res['status']} | "
                f"links={res['articles_found']} | new={new_count} | total={len(all_links)}"
            )

        log.info(f"\n[PHASE 1 DONE] {len(all_links)} unique article links\n{'='*70}")

        # PHASE 2: visit articles + insert to Supabase
        article_urls = list(all_links.keys())

        if not args.skip_article_visit and article_urls:
            log.info(f"[PHASE 2] Visiting {len(article_urls)} article pages...")
            art_page = await context.new_page()
            batch    = []

            for art_idx, art_url in enumerate(article_urls, 1):
                log.info(f"  [article {art_idx}/{len(article_urls)}] {art_url}")
                src = all_links[art_url]
                t0  = time.time()

                details = await extract_article_details(art_page, art_url)
                await art_page.wait_for_timeout(800)

                keep, drop_reason = is_within_2_years(details["article_date"])
                if not keep:
                    log.info(f"    [DATE FILTER] Dropped: {drop_reason}")
                    continue

                log.info(
                    f"    title={str(details['article_title'])[:60]!r}  "
                    f"date={details['article_date']}  "
                    f"chars={len(details['extracted_text'] or '')}  "
                    f"{round(time.time()-t0, 2)}s"
                )

                row = {
                    "unfiltered_article_id": str(uuid.uuid4()),
                    "search_url_id":         None,
                    "article_link":          art_url,
                    "article_title":         details["article_title"],
                    "article_date":          details["article_date"],
                    "companies_mentioned":   details["companies_mentioned"],
                    "location":              details["location"],
                    "extracted_text":        details["extracted_text"],
                    "is_valid":              True,
                    "drop_reason":           None,
                    "filter_article_status": "pending",
                    "created_at":            datetime.now().strftime("%Y-%m-%d %H:%M:%S+05:30"),
                    "subsegment_name":       subseg_name,
                    "base_url_id":           base_url_id,
                    "keyword_used":          src["keyword"],
                    "search_url":            src["search_url"],
                    "method_used":           src["method_used"],
                    "search_term_source":    term_source,
                }
                batch.append(row)
                output_rows.append(row)

                if len(batch) >= 20:
                    ins, skip = insert_articles_to_supabase(sb, batch)
                    total_inserted += ins
                    total_skipped  += skip
                    maybe_save_local_csv(batch, args.output_csv, first_csv_write)
                    first_csv_write = False
                    batch.clear()
                    log.info(f"  [checkpoint] after article {art_idx}")

            if batch:
                ins, skip = insert_articles_to_supabase(sb, batch)
                total_inserted += ins
                total_skipped  += skip
                maybe_save_local_csv(batch, args.output_csv, first_csv_write)
                first_csv_write = False

        else:
            log.info("[PHASE 2] Skipped (--skip_article_visit)")
            batch = []
            for art_url in article_urls:
                src = all_links[art_url]
                row = {
                    "unfiltered_article_id": str(uuid.uuid4()),
                    "search_url_id":         None,
                    "article_link":          art_url,
                    "article_title":         None, "article_date":        None,
                    "companies_mentioned":   None, "location":            None,
                    "extracted_text":        None, "is_valid":            True,
                    "drop_reason":           None,
                    "filter_article_status": "pending",
                    "created_at":            datetime.now().strftime("%Y-%m-%d %H:%M:%S+05:30"),
                    "subsegment_name":       subseg_name,
                    "base_url_id":           base_url_id,
                    "keyword_used":          src["keyword"],
                    "search_url":            src["search_url"],
                    "method_used":           src["method_used"],
                    "search_term_source":    term_source,
                }
                batch.append(row)
                output_rows.append(row)
            if batch:
                ins, skip = insert_articles_to_supabase(sb, batch)
                total_inserted += ins
                total_skipped  += skip
                maybe_save_local_csv(batch, args.output_csv, True)

        await browser.close()

    log.info(
        f"\n{'='*70}\n"
        f"[FINAL SUMMARY]  {base_url}\n"
        f"  Search term source         : {term_source}\n"
        f"  Search terms used          : {search_terms}\n"
        f"  Unique article links       : {len(all_links)}\n"
        f"  After date filter          : {len(output_rows)}\n"
        f"  Inserted to Supabase       : {total_inserted}\n"
        f"  Skipped (duplicates/errors): {total_skipped}\n"
        f"  With title                 : {sum(1 for r in output_rows if r['article_title'])}\n"
        f"  With date                  : {sum(1 for r in output_rows if r['article_date'])}\n"
        f"  With body text             : {sum(1 for r in output_rows if r['extracted_text'])}\n"
        f"  CSV backup                 : {args.output_csv or 'not saved'}\n"
        f"  Debug log                  : scraper_v4.log\n"
        f"{'='*70}"
    )


def parse_args():
    p = argparse.ArgumentParser(
        description="Single URL scraper → Supabase ses_unfiltered_articles"
    )
    p.add_argument("--base_url",    required=True,
                   help="Target base URL, e.g. https://example.com")
    p.add_argument("--base_url_id", default=None,
                   help="UUID of base URL in ses_base_url (optional, auto-resolved if omitted)")
    p.add_argument("--supabase_url", default=None,
                   help="Supabase project URL (or set SUPABASE_URL in .env)")
    p.add_argument("--supabase_key", default=None,
                   help="Supabase SERVICE_ROLE key (or set SUPABASE_KEY in .env)")
    p.add_argument("--output_csv",  default=None,
                   help="Optional local CSV backup path")
    p.add_argument("--skip_article_visit", action="store_true",
                   help="Collect links only, skip visiting article pages")
    return p.parse_args()


if __name__ == "__main__":
    asyncio.run(main(parse_args()))