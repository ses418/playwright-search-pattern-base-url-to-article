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
NAVIGATION_TIMEOUT = 15000  # Reduced for faster failure detection
POST_LOAD_WAIT = 300  # small stabilization wait
DETECT_TIMEOUT = 10000  # safety guard
MAX_RETRIES = 1  # Number of retries for browser launch


async def process_domain(browser, domain, semaphore):

    base_url_id = domain["base_url_id"]
    base_url = domain["base_url"]

    async with semaphore:

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

        try:
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
                        raise
                    logger.warning(f"Retrying {base_url} due to error: {e}")

                except Exception as e:
                    if attempt == 1:
                        raise
                    logger.warning(f"Retrying {base_url} due to timeout...")

            if not success:
                logger.warning(f"Not Found: {base_url}")
                return

            # 🔥 Detection timeout safety
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
                await page.close()
            except:
                pass
            try:
                await context.close()
            except:
                pass


async def run_batch(domains, domains_since_restart=0):

    semaphore = asyncio.Semaphore(DOMAIN_CONCURRENCY)

    async with async_playwright() as p:
        browser = None
        for attempt in range(MAX_RETRIES):
            try:
                browser = await p.chromium.launch(
                    headless=True,
                    args=[
                        "--disable-blink-features=AutomationControlled",
                        "--no-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-gpu",
                        "--disable-web-security",
                        "--disable-features=IsolateOrigins,site-per-process",
                        "--single-process",
                        "--disable-extensions",
                        "--disable-default-apps",
                        "--disable-background-networking",
                        "--disable-background-timer-throttling",
                        "--disable-backgrounding-occluded-windows",
                        "--disable-renderer-backgrounding",
                        "--disable-accelerated-2d-canvas",
                        "--disable-accelerated-video-decode",
                    ]
                )
                break
            except Exception as e:
                logger.error(f"Browser launch attempt {attempt + 1} failed: {e}")
                if attempt == MAX_RETRIES - 1:
                    raise
                await asyncio.sleep(2)

        if not browser:
            logger.error("Failed to launch browser after retries")
            return

        try:
            tasks = [
                process_domain(browser, domain, semaphore)
                for domain in domains
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # 🔥 Log unexpected task-level exceptions
            for r in results:
                if isinstance(r, Exception):
                    logger.error("Unhandled task exception: %s", str(r))
        finally:
            try:
                await browser.close()
            except Exception as e:
                logger.warning(f"Error closing browser: {e}")

    # Force garbage collection after browser closes
    gc.collect()
    logger.info("Browser closed and garbage collected")