import asyncio
import gc
import logging
from playwright.async_api import async_playwright, Error as PlaywrightError
from app.detector import detect_search
from app.db import save_search_pattern, mark_as_not_found

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


DOMAIN_CONCURRENCY = 1
NAVIGATION_TIMEOUT = 30000  # ms
POST_LOAD_WAIT = 500  # ms
DETECT_TIMEOUT = 120  # seconds
MAX_RETRIES = 3
BROWSER_ARGS = [
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-web-security",
    "--disable-features=IsolateOrigins,site-per-process",
    "--disable-extensions",
    "--disable-setuid-sandbox",
    "--js-flags=--max-old-space-size=512",
    "--memory-pressure-off",
    "--disable-default-apps",
    "--disable-background-networking",
    "--disable-background-timer-throttling",
    "--disable-backgrounding-occluded-windows",
    "--disable-renderer-backgrounding",
    "--disable-accelerated-2d-canvas",
    "--disable-accelerated-video-decode",
    "--disable-blink-features=AutomationControlled",
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


async def process_domain(p, domain, semaphore, enhanced_mode=False):
    """
    Process a single domain.

    Args:
        enhanced_mode: If True, use enhanced detection (longer waits, networkidle).
                       Used when retrying previously not-found URLs.
    """
    base_url_id = domain["base_url_id"]
    base_url = domain["base_url"]

    async with semaphore:
        browser = None
        context = None
        page = None
        try:
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

            logger.info(f"Processing: {base_url} (enhanced={enhanced_mode})")

            success = False
            nav_timeout = NAVIGATION_TIMEOUT * 2 if enhanced_mode else NAVIGATION_TIMEOUT
            wait_until = "load" if enhanced_mode else "domcontentloaded"

            for attempt in range(2):
                try:
                    await page.goto(
                        base_url,
                        timeout=nav_timeout,
                        wait_until=wait_until
                    )
                    await page.wait_for_timeout(1000 if enhanced_mode else POST_LOAD_WAIT)
                    success = True
                    break

                except PlaywrightError as e:
                    error_msg = str(e)
                    # Dead domain — permanently skip, no point retrying
                    if "ERR_NAME_NOT_RESOLVED" in error_msg:
                        logger.error(f"Dead domain (DNS failed): {base_url} — permanently skipping")
                        await mark_as_not_found(base_url_id, base_url)
                        return
                    if "Target page, context or browser has been closed" in error_msg:
                        logger.error(f"Browser crashed while processing {base_url}")
                        raise
                    if attempt == 1:
                        logger.error(f"Playwright error for {base_url}: {e}")
                        return
                    logger.warning(f"Retrying {base_url} due to error: {e}")
                    await asyncio.sleep(2)

                except Exception as e:
                    if attempt == 1:
                        logger.error(f"ERROR: {base_url} -> {str(e)}")
                        return
                    logger.warning(f"Retrying {base_url} due to timeout...")
                    await asyncio.sleep(2)

            if not success:
                logger.warning(f"Navigation failed for {base_url}, marking as not-found")
                await mark_as_not_found(base_url_id, base_url)
                return

            # Detection
            detect_timeout = DETECT_TIMEOUT * 2 if enhanced_mode else DETECT_TIMEOUT
            try:
                result = await asyncio.wait_for(
                    detect_search(page, base_url, enhanced_mode=enhanced_mode),
                    timeout=detect_timeout
                )
            except asyncio.TimeoutError:
                logger.warning(f"Detection timed out for {base_url} after {detect_timeout}s")
                await mark_as_not_found(base_url_id, base_url)
                return

            if result and result.get("confidence", 0) > 0:
                await save_search_pattern(
                    base_url_id=base_url_id,
                    base_url=base_url,
                    result=result
                )
                logger.info(f"✔ Saved: {base_url} -> {result}")
            else:
                logger.warning(f"Not Found: {base_url} (no strategy matched)")
                await mark_as_not_found(base_url_id, base_url)

        except PlaywrightError as e:
            logger.error(f"Playwright error for {base_url}: {e}")
        except Exception as e:
            logger.error(f"ERROR: {base_url} -> {str(e)}")

        finally:
            try:
                if page:
                    await page.close()
            except Exception:
                pass
            try:
                if context:
                    await context.close()
            except Exception:
                pass
            try:
                if browser and browser.is_connected():
                    await browser.close()
            except Exception:
                pass
            gc.collect()


async def run_batch(domains, enhanced_mode=False):
    """
    Run a batch of domains through detection.

    Args:
        enhanced_mode: If True, use enhanced detection for all domains in batch.
    """
    semaphore = asyncio.Semaphore(DOMAIN_CONCURRENCY)

    async with async_playwright() as p:
        tasks = [
            process_domain(p, domain, semaphore, enhanced_mode=enhanced_mode)
            for domain in domains
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, Exception):
                logger.error("Unhandled task exception: %s", str(r))

    gc.collect()
    logger.info("Batch complete, garbage collected")