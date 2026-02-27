"""
Single URL Article Scraper — FastAPI Server Edition  v5.0
=========================================================
Converted from scraper_v4.py (CLI) to a production FastAPI service.

Endpoints:
    POST /scrape          — Start a scrape job (background, returns job_id)
    GET  /job/{job_id}    — Poll job status + result
    GET  /jobs            — List all jobs (last 100)
    GET  /health          — Health check
    GET  /                — API info

Run:
    uvicorn main:app --host 0.0.0.0 --port 5060
    or via Docker (see Dockerfile at repo root)

Environment (.env or Docker env):
    SUPABASE_URL=https://supabase.sesai.in
    SUPABASE_KEY=<service_role key — NOT anon>
"""

import asyncio
import base64
import json
import logging
import os
import re
import sys
import time
import uuid
from collections import OrderedDict
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import Optional
from urllib.parse import quote_plus, urljoin, urlparse

from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from playwright.async_api import async_playwright

# ─── Windows asyncio fix (required for Playwright on Python 3.12+) ───────────
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# ─── Load .env if present ────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ─── Supabase ─────────────────────────────────────────────────────────────────
try:
    from supabase import create_client, Client as SupabaseClient
except ImportError:
    print("[FATAL] supabase-py not installed.")
    sys.exit(1)

# ─── UTF-8 stdout (Windows fix) ───────────────────────────────────────────────

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ─── Date filter ──────────────────────────────────────────────────────────────
TWO_YEAR_CUTOFF = datetime.now(tz=timezone.utc) - timedelta(days=365 * 2)


# ==============================================================================
# IN-MEMORY JOB STORE  (last 200 jobs, thread-safe via asyncio single-thread)
# ==============================================================================

_jobs: OrderedDict = OrderedDict()   # job_id → dict
MAX_JOBS = 200


def _new_job(job_id: str, payload: dict) -> dict:
    job = {
        "job_id":      job_id,
        "status":      "queued",    # queued | running | done | error
        "created_at":  datetime.utcnow().isoformat(),
        "started_at":  None,
        "finished_at": None,
        "payload":     payload,
        "progress":    {},
        "result":      None,
        "error":       None,
    }
    _jobs[job_id] = job
    if len(_jobs) > MAX_JOBS:
        _jobs.popitem(last=False)   # drop oldest
    return job


# ==============================================================================
# SHARED PLAYWRIGHT BROWSER
# ==============================================================================

_playwright_inst = None
_browser         = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _playwright_inst, _browser
    log.info("[BOOT] Starting Playwright Chromium...")
    _playwright_inst = await async_playwright().start()
    _browser = await _playwright_inst.chromium.launch(
        headless=True,
        args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
    )
    log.info("[BOOT] Playwright ready ✓")
    yield
    log.info("[SHUTDOWN] Closing browser...")
    if _browser:
        await _browser.close()
    if _playwright_inst:
        await _playwright_inst.stop()


# ==============================================================================
# FASTAPI APP
# ==============================================================================

app = FastAPI(
    title="Article Scraper API — Supabase Edition v5.0",
    description="Scrape articles from a URL using Playwright + save to Supabase.",
    version="5.0.0",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)


# ==============================================================================
# REQUEST / RESPONSE MODELS
# ==============================================================================

class ScrapeRequest(BaseModel):
    base_url:            str           = Field(...,  description="Target URL, e.g. https://example.com")
    base_url_id:         Optional[str] = Field(None, description="UUID in ses_base_url (auto-resolved if omitted)")
    skip_article_visit:  bool          = Field(False, description="Collect links only, skip article detail pages")
    output_csv:          Optional[str] = Field(None,  description="Optional local CSV backup path (server-side)")


class ScrapeAccepted(BaseModel):
    job_id:   str
    status:   str
    message:  str


# ==============================================================================
# JWT KEY VALIDATOR
# ==============================================================================

def decode_jwt_role(token: str) -> str:
    try:
        parts = token.strip().split(".")
        if len(parts) != 3:
            return "unknown"
        payload = parts[1] + "=" * (4 - len(parts[1]) % 4)
        return json.loads(base64.b64decode(payload).decode()).get("role", "unknown")
    except Exception:
        return "unknown"


def get_supabase_client() -> SupabaseClient:
    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_KEY", "")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_KEY must be set in environment.")
    role = decode_jwt_role(key)
    if role == "anon":
        raise RuntimeError(
            "SUPABASE_KEY is the anon key — inserts will fail with RLS error (42501). "
            "Use the service_role key instead."
        )
    if role != "service_role":
        log.warning(f"[KEY] JWT role='{role}' — not service_role, inserts may fail.")
    return create_client(url, key)


# ==============================================================================
# SUPABASE DATA LOADING  (unchanged logic from scraper_v4)
# ==============================================================================

def fetch_base_url_row(sb: SupabaseClient, base_url: str, base_url_id_arg) -> dict:
    base_url = base_url.rstrip("/")
    if base_url_id_arg:
        resp = (sb.table("ses_base_url")
                .select("base_url_id,base_url,subsegment_id")
                .eq("base_url_id", base_url_id_arg).limit(1).execute())
    else:
        resp = (sb.table("ses_base_url")
                .select("base_url_id,base_url,subsegment_id")
                .eq("base_url", base_url).limit(1).execute())
        if not resp.data:
            resp = (sb.table("ses_base_url")
                    .select("base_url_id,base_url,subsegment_id")
                    .eq("base_url", base_url + "/").limit(1).execute())
    if not resp.data:
        raise ValueError(f"'{base_url}' not found in ses_base_url table.")
    row = resp.data[0]
    log.info(f"[DB] base_url_id={row['base_url_id']}  subsegment_id={row.get('subsegment_id')}")
    return row


def fetch_subsegment_and_segment(sb: SupabaseClient, subsegment_id) -> tuple:
    if not subsegment_id:
        return None, None, None
    resp = (sb.table("ses_subsegments")
            .select("subsegment_id,subsegment_name,segment_id")
            .eq("subsegment_id", subsegment_id).limit(1).execute())
    if not resp.data:
        return None, None, None
    sub       = resp.data[0]
    seg_name  = None
    segment_id = sub.get("segment_id")
    if segment_id:
        sr = (sb.table("ses_segments")
              .select("segment_name")
              .eq("segment_id", segment_id).limit(1).execute())
        if sr.data:
            seg_name = sr.data[0].get("segment_name")
    return sub.get("subsegment_name"), seg_name, segment_id


def fetch_keywords(sb: SupabaseClient, subsegment_id) -> list:
    if not subsegment_id:
        return []
    resp = (sb.table("ses_keywords")
            .select("keyword")
            .eq("subsegment_id", subsegment_id).execute())
    return [r["keyword"] for r in resp.data if r.get("keyword")]


def load_search_pattern(sb: SupabaseClient, base_url_id: str, base_url: str) -> dict:
    resp = (sb.table("base_url_search_patterns")
            .select("method,pattern,confidence,result_type")
            .eq("base_url_id", base_url_id).limit(1).execute())
    if not resp.data:
        for url_try in [base_url.rstrip("/"), base_url.rstrip("/") + "/"]:
            resp = (sb.table("base_url_search_patterns")
                    .select("method,pattern,confidence,result_type")
                    .eq("base_url", url_try).limit(1).execute())
            if resp.data:
                break
    if not resp.data:
        raise ValueError(
            f"No search pattern in base_url_search_patterns for base_url_id={base_url_id}. "
            "Add a row there first."
        )
    row = resp.data[0]
    return {
        "method":      row.get("method"),
        "pattern":     row.get("pattern"),
        "confidence":  int(row.get("confidence") or 0),
        "result_type": row.get("result_type") or "unknown",
    }


def insert_articles(sb: SupabaseClient, rows: list) -> tuple:
    inserted = skipped = 0
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
        except Exception as e:
            skipped += 1
            err = str(e)
            if "42501" in err or "row-level security" in err.lower():
                log.error(f"  [RLS BLOCK] {row['article_link'][:70]} — use service_role key!")
            elif "23505" in err or "duplicate" in err.lower():
                log.debug(f"  [DUPLICATE] {row['article_link'][:70]}")
            else:
                log.error(f"  [INSERT ERROR] {row['article_link'][:70]}: {err[:100]}")
    return inserted, skipped


# ==============================================================================
# SEARCH / EXTRACTION CONSTANTS & HELPERS  (unchanged from scraper_v4)
# ==============================================================================

FALLBACK_PATTERNS = [
    ("url_search_param",  "url",   "{base}?s={keyword}"),
    ("url_query_param",   "url",   "{base}?q={keyword}"),
    ("url_search_path",   "url",   "{base}/search?q={keyword}"),
    ("url_search_path_s", "url",   "{base}/search?s={keyword}"),
    ("url_search_slash",  "url",   "{base}/search/{keyword}/"),
    ("url_tag_path",      "url",   "{base}/?tag={keyword}"),
    ("input_name_search", "input", "input[name='search']"),
    ("input_name_q",      "input", "input[name='q']"),
    ("input_name_s",      "input", "input[name='s']"),
    ("input_type_search", "input", "input[type='search']"),
    ("input_placeholder", "input", "input[placeholder*='earch' i]"),
    ("input_class_search","input", "input[class*='search' i]"),
    ("input_id_search",   "input", "input[id*='search' i]"),
    ("icon_aria_search",  "icon",  "button[aria-label*='Search' i]"),
    ("icon_class_search", "icon",  "button[class*='search' i]"),
    ("icon_i_search",     "icon",  "i[class*='search' i]"),
    ("icon_svg_search",   "icon",  "svg[class*='search' i]"),
    ("icon_srch_btn",     "icon",  ".search-button"),
    ("icon_srch_icn",     "icon",  ".search-icon"),
    ("icon_srch_tog",     "icon",  ".search-toggle"),
]
POST_ICON_INPUTS = [
    "input[type='search']", "input[name='q']", "input[name='s']",
    "input[placeholder*='earch' i]", "input[class*='search' i]", "input:visible",
]
ARTICLE_LINK_GROUPS = [
    ["article h1 a[href]","article h2 a[href]","article h3 a[href]",
     "article .entry-title a[href]","article a[href]"],
    ["h2 a[href]","h3 a[href]",".entry-title a[href]",".post-title a[href]",
     ".news-title a[href]",".article-title a[href]"],
    [".result a[href]",".search-result a[href]","[class*='search-result'] a[href]"],
    [".post a[href]",".article a[href]","[class*='article'] a[href]",
     "[class*='post-item'] a[href]","[class*='news-item'] a[href]"],
    ["main a[href]","#content a[href]",".content a[href]"],
]
NON_ARTICLE_SEGS = {
    "topics","topic","category","categories","cat","tag","tags","label","labels",
    "explore","browse","author","authors","contributor","page","archive","archives",
    "search","feed","rss","newsletter","subscribe","about","contact","advertise",
    "careers","events","webinar","conference","podcast","video","gallery",
    "product","products","shop","store","login","register","signup","account",
}
TITLE_SELS  = ["h1.entry-title","h1.post-title","h1[class*='title']","article h1","h1"]
DATE_SELS   = ["time[datetime]","[itemprop='datePublished']",".published",
               ".post-date",".entry-date","[class*='date']",
               "meta[property='article:published_time']"]
TEXT_SELS   = ["article .entry-content","article .post-content",
               "[itemprop='articleBody']",".article-content","article","main"]
COMPANY_RE  = re.compile(
    r'\b([A-Z][A-Za-z0-9&\.\-]+(?: [A-Z][A-Za-z0-9&\.\-]+)*'
    r'\s*(?:Inc\.?|Corp\.?|Ltd\.?|LLC|PLC|GmbH|Co\.?|Group|Holdings?'
    r'|Technologies?|Solutions?|Services?))\b'
)
LOCATION_RE = re.compile(
    r'\b([A-Z][a-z]+(?: [A-Z][a-z]+)*),\s*'
    r'([A-Z]{2}|[A-Z][a-z]+(?: [A-Z][a-z]+)*)\b'
)


def _is_article_url(url: str, anchor: str, base_url: str) -> tuple:
    path = urlparse(url).path.rstrip("/")
    if anchor and len(anchor.strip()) < 20:
        return False, "anchor too short"
    last = path.lower().split("/")[-1]
    ext  = ("." + last.rsplit(".", 1)[1]) if "." in last else ""
    if ext in {".pdf",".jpg",".jpeg",".png",".gif",".svg",".zip",".xml",".json",".css",".js"}:
        return False, f"bad ext {ext}"
    for seg in [s.lower() for s in path.split("/") if s]:
        if seg.split("?")[0].split("#")[0] in NON_ARTICLE_SEGS:
            return False, f"non-article seg '{seg}'"
    if len([s for s in path.split("/") if s]) < 2:
        return False, "path too shallow"
    if path == urlparse(base_url).path.rstrip("/") or path == "":
        return False, "is base url"
    return True, "ok"


def _is_within_2_years(date_str) -> tuple:
    if not date_str:
        return True, None
    raw = str(date_str).strip()
    for fmt in ["%Y-%m-%d %H:%M:%S%z","%Y-%m-%dT%H:%M:%S%z","%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S","%Y-%m-%d","%B %d, %Y","%b %d, %Y",
                "%d %B %Y","%d %b %Y","%m/%d/%Y","%d/%m/%Y"]:
        try:
            parsed = datetime.strptime(raw[:25], fmt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            if parsed >= TWO_YEAR_CUTOFF:
                return True, None
            return False, f"date {parsed.date()} older than cutoff {TWO_YEAR_CUTOFF.date()}"
        except ValueError:
            continue
    return True, None   # unparseable → keep


def _parse_date(raw: str):
    if not raw:
        return None
    for fmt in ["%Y-%m-%dT%H:%M:%S%z","%Y-%m-%dT%H:%M:%S","%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d","%B %d, %Y","%b %d, %Y","%d %B %Y","%d %b %Y",
                "%m/%d/%Y","%d/%m/%Y"]:
        try:
            dt = datetime.strptime(raw.strip()[:25], fmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S+05:30")
        except ValueError:
            continue
    return raw


# ==============================================================================
# PLAYWRIGHT SEARCH HELPERS
# ==============================================================================

async def _try_url(page, base_url, keyword, template):
    url = template.replace("{base}", base_url.rstrip("/")).replace("{keyword}", quote_plus(keyword))
    try:
        r = await page.goto(url, timeout=20000, wait_until="domcontentloaded")
        return bool(r and r.status < 400)
    except Exception:
        return False


async def _try_input(page, selector, keyword):
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
    except Exception:
        return False


async def _try_icon(page, selector, keyword):
    try:
        el = page.locator(selector).first
        if await el.count() == 0:
            return False
        await el.click(timeout=5000)
        await page.wait_for_timeout(800)
        for inp in POST_ICON_INPUTS:
            i = page.locator(inp).first
            if await i.count() > 0 and await i.is_visible():
                await i.fill(keyword)
                await i.press("Enter")
                await page.wait_for_load_state("domcontentloaded", timeout=15000)
                return True
        return False
    except Exception:
        return False


async def _extract_links(page, base_url) -> list:
    base_domain = urlparse(base_url).netloc
    accepted    = {}
    for group in ARTICLE_LINK_GROUPS:
        group_ok = {}
        for sel in group:
            try:
                for el in await page.locator(sel).all():
                    try:
                        href = await el.get_attribute("href")
                        if not href or href.startswith(("#","javascript","mailto","tel")):
                            continue
                        abs_url = urljoin(base_url, href)
                        if base_domain not in urlparse(abs_url).netloc:
                            continue
                        if abs_url in accepted or abs_url in group_ok:
                            continue
                        try:
                            text = (await el.inner_text()).strip()
                        except Exception:
                            text = ""
                        keep, _ = _is_article_url(abs_url, text, base_url)
                        if keep:
                            group_ok[abs_url] = text
                    except Exception:
                        continue
            except Exception:
                continue
        if group_ok:
            accepted.update(group_ok)
            break
    return list(accepted.keys())


async def _search_for_keyword(page, base_url: str, keyword: str, pattern: dict) -> dict:
    result = {"keyword": keyword, "status": "failed",
              "search_url": None, "links": [], "method": None}

    try:
        await page.goto(base_url, timeout=25000, wait_until="domcontentloaded")
    except Exception as e:
        result["status"] = "unreachable"
        result["error"]  = str(e)
        return result

    ok = False
    method, pat = pattern.get("method"), pattern.get("pattern")
    if method and method != "fallback":
        if   method == "url":   ok = await _try_url(page, base_url, keyword, pat)
        elif method == "input": ok = await _try_input(page, pat, keyword)
        elif method == "icon":  ok = await _try_icon(page, pat, keyword)
        if ok:
            result["method"] = method

    if not ok:
        for label, ftype, fpat in FALLBACK_PATTERNS:
            if ftype in ("input", "icon"):
                try:
                    await page.goto(base_url, timeout=20000, wait_until="domcontentloaded")
                    await page.wait_for_timeout(500)
                except Exception:
                    continue
            if   ftype == "url":   ok = await _try_url(page, base_url, keyword, fpat)
            elif ftype == "input": ok = await _try_input(page, fpat, keyword)
            elif ftype == "icon":  ok = await _try_icon(page, fpat, keyword)
            if ok:
                result["method"] = f"fallback_{ftype}:{label}"
                break

    if not ok:
        result["status"] = "no_pattern_worked"
        return result

    result["search_url"] = page.url
    await page.wait_for_timeout(1500)
    links = await _extract_links(page, base_url)
    result["links"]  = links
    result["status"] = "success" if links else "no_links"
    return result


async def _extract_article(page, url: str) -> dict:
    d = {"article_title": None, "article_date": None,
         "extracted_text": None, "companies_mentioned": None, "location": None}
    try:
        r = await page.goto(url, timeout=25000, wait_until="domcontentloaded")
        if r and r.status >= 400:
            return d
        await page.wait_for_timeout(1000)
        for sel in TITLE_SELS:
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
        for sel in DATE_SELS:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    raw = (await el.get_attribute("content")
                           or await el.get_attribute("datetime")
                           or (await el.inner_text()).strip())
                    if raw:
                        d["article_date"] = _parse_date(raw)
                        break
            except Exception:
                continue
        for sel in TEXT_SELS:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    txt = (await el.inner_text()).strip()
                    if len(txt) > 100:
                        d["extracted_text"] = txt[:5000]
                        break
            except Exception:
                continue
        src = d["extracted_text"] or ""
        companies = COMPANY_RE.findall(src)
        d["companies_mentioned"] = "; ".join(list(dict.fromkeys(companies))[:10]) or None
        locs = LOCATION_RE.findall(src[:500])
        d["location"] = "; ".join(
            list(dict.fromkeys(f"{c},{s}" for c, s in locs))[:5]) or None
    except Exception as e:
        log.warning(f"  [article] Failed {url}: {e}")
    return d


# ==============================================================================
# CORE SCRAPE TASK  (runs in background)
# ==============================================================================

async def _run_scrape(job_id: str, req: ScrapeRequest):
    job = _jobs[job_id]
    job["status"]     = "running"
    job["started_at"] = datetime.utcnow().isoformat()

    def progress(msg: str):
        job["progress"][datetime.utcnow().isoformat()] = msg
        log.info(f"[{job_id[:8]}] {msg}")

    try:
        # ── Supabase init ────────────────────────────────────────────
        sb = get_supabase_client()
        progress(f"Supabase connected | role=service_role")

        base_url = req.base_url.rstrip("/")

        # ── Load metadata from Supabase ──────────────────────────────
        base_row      = fetch_base_url_row(sb, base_url, req.base_url_id)
        base_url_id   = str(base_row["base_url_id"])
        subsegment_id = base_row.get("subsegment_id")

        subseg_name, seg_name, _ = fetch_subsegment_and_segment(sb, subsegment_id)
        keywords                  = fetch_keywords(sb, subsegment_id)

        if keywords:
            search_terms  = keywords
            term_source   = "keywords"
        elif subseg_name:
            search_terms  = [subseg_name]
            term_source   = "subsegment"
        elif seg_name:
            search_terms  = [seg_name]
            term_source   = "segment"
        else:
            raise ValueError("No keywords, subsegment, or segment found for this URL.")

        pattern = load_search_pattern(sb, base_url_id, base_url)
        progress(f"Pattern loaded: method={pattern['method']} | {len(search_terms)} terms")

        # ── Playwright Phase 1 — collect links ───────────────────────
        all_links: dict = {}
        context = await _browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
        )
        await context.route(
            "**/*.{png,jpg,jpeg,gif,svg,woff,woff2,ttf,eot,mp4,webm}",
            lambda route: route.abort(),
        )

        search_page = await context.new_page()
        for idx, term in enumerate(search_terms, 1):
            progress(f"[{idx}/{len(search_terms)}] Searching '{term}'")
            res = await _search_for_keyword(search_page, base_url, term, pattern)
            await search_page.wait_for_timeout(1000)
            new_count = 0
            for link in res["links"]:
                if link not in all_links:
                    all_links[link] = {
                        "keyword":    term,
                        "search_url": res["search_url"],
                        "method":     res["method"],
                    }
                    new_count += 1
            progress(f"  '{term}' → status={res['status']} links={len(res['links'])} new={new_count} total={len(all_links)}")

        progress(f"Phase 1 done — {len(all_links)} unique links")

        # ── Playwright Phase 2 — article details + insert ────────────
        output_rows    = []
        total_inserted = 0
        total_skipped  = 0

        if not req.skip_article_visit and all_links:
            art_page = await context.new_page()
            batch    = []
            urls     = list(all_links.keys())

            for idx, art_url in enumerate(urls, 1):
                if idx % 10 == 0:
                    progress(f"  Article {idx}/{len(urls)} ...")
                src     = all_links[art_url]
                details = await _extract_article(art_page, art_url)
                await art_page.wait_for_timeout(800)

                keep, drop_reason = _is_within_2_years(details["article_date"])
                if not keep:
                    log.info(f"  [DATE FILTER] {drop_reason} → {art_url[:60]}")
                    continue

                row = {
                    "unfiltered_article_id": str(uuid.uuid4()),
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
                    "method_used":           src["method"],
                    "search_term_source":    term_source,
                }
                batch.append(row)
                output_rows.append(row)

                if len(batch) >= 20:
                    ins, skip = insert_articles(sb, batch)
                    total_inserted += ins
                    total_skipped  += skip
                    progress(f"  Batch inserted={ins} skipped={skip}")
                    batch.clear()

            if batch:
                ins, skip = insert_articles(sb, batch)
                total_inserted += ins
                total_skipped  += skip

        else:
            progress("Phase 2 skipped (skip_article_visit=true)")
            for art_url, src in all_links.items():
                output_rows.append({
                    "unfiltered_article_id": str(uuid.uuid4()),
                    "article_link":    art_url,
                    "article_title":   None, "article_date":      None,
                    "extracted_text":  None, "subsegment_name":   subseg_name,
                    "base_url_id":     base_url_id,
                    "keyword_used":    src["keyword"],
                    "method_used":     src["method"],
                })
            ins, skip = insert_articles(sb, output_rows)
            total_inserted += ins
            total_skipped  += skip

        await context.close()

        # ── Final result ─────────────────────────────────────────────
        job["result"] = {
            "base_url":          base_url,
            "base_url_id":       base_url_id,
            "subsegment":        subseg_name,
            "segment":           seg_name,
            "term_source":       term_source,
            "search_terms":      search_terms,
            "unique_links":      len(all_links),
            "after_date_filter": len(output_rows),
            "inserted":          total_inserted,
            "skipped":           total_skipped,
        }
        job["status"]      = "done"
        job["finished_at"] = datetime.utcnow().isoformat()
        progress(f"DONE — inserted={total_inserted} skipped={total_skipped}")

    except Exception as e:
        log.error(f"[{job_id[:8]}] FAILED: {e}", exc_info=True)
        job["status"]      = "error"
        job["error"]       = str(e)
        job["finished_at"] = datetime.utcnow().isoformat()


# ==============================================================================
# API ENDPOINTS
# ==============================================================================

@app.get("/")
async def root():
    return {
        "service":    "Article Scraper API v5.0",
        "status":     "running",
        "endpoints": {
            "POST /scrape":       "Start a scrape job (returns job_id immediately)",
            "GET  /job/{job_id}": "Poll job status and result",
            "GET  /jobs":         "List recent jobs",
            "GET  /health":       "Health check",
        },
    }


@app.get("/health")
async def health():
    sb_ok = True
    try:
        get_supabase_client()
    except Exception:
        sb_ok = False
    return {
        "status":           "healthy",
        "browser_ready":    _browser is not None and _browser.is_connected(),
        "supabase_key_ok":  sb_ok,
        "active_jobs":      sum(1 for j in _jobs.values() if j["status"] == "running"),
        "total_jobs":       len(_jobs),
    }


@app.post("/scrape", response_model=ScrapeAccepted, status_code=202)
async def start_scrape(req: ScrapeRequest, bg: BackgroundTasks):
    """
    Fire-and-forget scrape.  Returns a job_id immediately.
    Poll GET /job/{job_id} to check progress and get results.
    """
    if _browser is None or not _browser.is_connected():
        raise HTTPException(503, detail="Browser not ready yet. Retry in a few seconds.")

    job_id = f"job_{int(time.time())}_{uuid.uuid4().hex[:6]}"
    _new_job(job_id, req.model_dump())
    bg.add_task(_run_scrape, job_id, req)
    return ScrapeAccepted(
        job_id=job_id,
        status="queued",
        message=f"Scrape job accepted for {req.base_url}. Poll GET /job/{job_id} for status.",
    )


@app.get("/job/{job_id}")
async def get_job(job_id: str):
    """Poll this endpoint after POSTing to /scrape."""
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(404, detail=f"Job '{job_id}' not found.")
    return job


@app.get("/jobs")
async def list_jobs(limit: int = 20):
    """List recent jobs (newest first)."""
    jobs = list(reversed(list(_jobs.values())))
    return {"total": len(_jobs), "jobs": jobs[:max(1, min(limit, 100))]}


# ==============================================================================
# ENTRY POINT
# ==============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=5060,
        reload=False,   # reload=True breaks Playwright on Windows — just restart manually
        log_level="info",
    )