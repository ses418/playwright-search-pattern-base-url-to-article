import asyncio
import logging
from datetime import datetime
from playwright.async_api import async_playwright
from app.detector import detect_search
from app.db import save_search_pattern

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


DOMAIN_CONCURRENCY = 2  # increase in production
NAVIGATION_TIMEOUT = 30000
POST_LOAD_WAIT = 500  # small stabilization wait
DETECT_TIMEOUT = 20000  # safety guard


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

                except Exception:
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

        except Exception as e:
            logger.error(f"ERROR: {base_url} -> {str(e)}")

        finally:
            try:
                await page.close()
            except:
                pass
            await context.close()


async def run_batch(domains):

    semaphore = asyncio.Semaphore(DOMAIN_CONCURRENCY)

    async with async_playwright() as p:

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
            ]
        )

        tasks = [
            process_domain(browser, domain, semaphore)
            for domain in domains
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 🔥 Log unexpected task-level exceptions
        for r in results:
            if isinstance(r, Exception):
                print("Unhandled task exception:", str(r))

        await browser.close()