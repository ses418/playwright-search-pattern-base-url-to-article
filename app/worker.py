import asyncio
import gc
import logging
from datetime import datetime
from playwright.async_api import async_playwright, Error as PlaywrightError
from app.detector import detect_search
from app.db import save_search_pattern

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


DOMAIN_CONCURRENCY = 1  # Keep at 1 to prevent memory exhaustion
NAVIGATION_TIMEOUT = 30000  # Increased timeout for slow sites
POST_LOAD_WAIT = 300  # small stabilization wait
DETECT_TIMEOUT = 10000  # safety guard
MAX_RETRIES = 3  # Number of retries for browser launch
BROWSER_ARGS = [
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-web-security",
    "--disable-features=IsolateOrigins,site-per-process",
    # NOTE: --single-process is intentionally removed: it causes the entire
    # browser to crash when any page renderer hits a fault in Docker/Linux.
    "--disable-extensions",
    "--disable-default-apps",
    "--disable-background-networking",
    "--disable-background-timer-throttling",
    "--disable-backgrounding-occluded-windows",
    "--disable-renderer-backgrounding",
    "--disable-accelerated-2d-canvas",
    "--disable-accelerated-video-decode",
    "--disable-blink-features=AutomationControlled",
    "--memory-pressure-off",
    "--max_old_space_size=512",
]


async def launch_browser(p):
    """Launch a fresh browser instance with retries."""
    for attempt in range(MAX_RETRIES):
        try:
            browser = await p.chromium.launch(
                headless=True,
                args=BROWSER_ARGS
            )
            return browser
        except Exception as e:
            logger.error(f"Browser launch attempt {attempt + 1} failed: {e}")
            if attempt == MAX_RETRIES - 1:
                raise
            await asyncio.sleep(2)
    return None


async def process_domain(p, domain, semaphore):
    """Process a single domain, launching its own browser if the shared one crashed."""

    base_url_id = domain["base_url_id"]
    base_url = domain["base_url"]

    async with semaphore:
        browser = None
        context = None
        page = None
        try:
            # Each domain gets its own fresh browser to avoid cross-domain crashes
            browser = await launch_browser(p)

            if not browser or not browser.is_connected():
                logger.error(f"Browser not connected for {base_url}, skipping.")
                return

            context = await browser.new_context(
                ignore_https_errors=True,
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            )

            await context.route(
                "**/*",
                lambda route: route.abort()
                if route.request.resource_type in ["image", "font", "media"]
                else route.continue_()
            )

            page = await context.new_page()

            logger.info(f"Processing: {base_url}")

            success = False

            for attempt in range(2):
                try:
                    await page.goto(
                        base_url,
                        timeout=NAVIGATION_TIMEOUT,
                        wait_until="domcontentloaded"
                    )

                    await page.wait_for_timeout(POST_LOAD_WAIT)

                    success = True
                    break

                except PlaywrightError as e:
                    if "Target page, context or browser has been closed" in str(e):
                        logger.error(f"Browser crashed while processing {base_url}")
                        raise
                    if attempt == 1:
                        logger.error(f"Playwright error for {base_url}: {e}")
                        return
                    logger.warning(f"Retrying {base_url} due to error: {e}")
                    await asyncio.sleep(1)

                except Exception as e:
                    if attempt == 1:
                        logger.error(f"ERROR: {base_url} -> {str(e)}")
                        return
                    logger.warning(f"Retrying {base_url} due to timeout...")
                    await asyncio.sleep(1)

            if not success:
                logger.warning(f"Not Found: {base_url}")
                return

            # Detection timeout safety
            result = await asyncio.wait_for(
                detect_search(page, base_url),
                timeout=DETECT_TIMEOUT
            )

            if result and result.get("confidence", 0) > 0:

                await save_search_pattern(
                    base_url_id=base_url_id,
                    base_url=base_url,
                    result=result
                )

                logger.info(f"✔ Saved: {base_url} -> {result}")

            else:
                logger.warning(f"Not Found: {base_url}")

        except PlaywrightError as e:
            logger.error(f"Playwright error for {base_url}: {e}")
        except Exception as e:
            logger.error(f"ERROR: {base_url} -> {str(e)}")

        finally:
            try:
                if page:
                    await page.close()
            except:
                pass
            try:
                if context:
                    await context.close()
            except:
                pass
            try:
                if browser and browser.is_connected():
                    await browser.close()
            except:
                pass
            gc.collect()


async def run_batch(domains, domains_since_restart=0):

    semaphore = asyncio.Semaphore(DOMAIN_CONCURRENCY)

    async with async_playwright() as p:
        tasks = [
            process_domain(p, domain, semaphore)
            for domain in domains
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Log unexpected task-level exceptions
        for r in results:
            if isinstance(r, Exception):
                logger.error("Unhandled task exception: %s", str(r))

    # Force garbage collection after all domains processed
    gc.collect()
    logger.info("Batch complete, garbage collected")