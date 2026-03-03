import asyncio
import logging
from app.strategies.base import BaseSearchStrategy

logger = logging.getLogger(__name__)

ICON_SELECTORS = [

    # Aria / title based
    "button[aria-label*='search']",
    "button[aria-label*='Search']",
    "button[title*='search']",
    "button[title*='Search']",

    # SVG / icon based
    "svg[class*='search']",
    "svg[aria-hidden='true'][class*='icon']",
    "svg[viewBox][class*='magnifier']",

    "i[class*='search']",
    "i[class*='icon-search']",
    "i[class*='fa-search']",
    "i[class*='magnifier']",

    # Data attributes
    "[data-testid*='search']",
    "[data-icon*='search']",

    # Class-based
    "button[class*='search']",
    "div[class*='search-icon']",
    "span[class*='search-icon']",
    "a[class*='search']",
    ".search-toggle",
    ".search-button",
    ".search-trigger",
    ".nav-search",
    ".navbar-search",
    ".mobile-search",
    ".icon-search",
    "[class*='magnifier']",

    # Header-specific
    "header button[class*='search']",
    "header svg[class*='search']",

    # Role-based
    "[role='search'] button",

    # Nested SVG inside button
    "button svg",
]

# Short list of high-signal input selectors to try after icon click
# (not the full 170+ list — that was causing O(n²) slowness)
ICON_INPUT_SELECTORS = [
    "input[type='search']",
    "input[name='q']",
    "input[name='query']",
    "input[name='s']",
    "input[name='search']",
    "input[placeholder*='Search']",
    "input[placeholder*='search']",
    "input[class*='search']",
    "input[id*='search']",
    "[role='search'] input",
    "form[action*='search'] input",
    "input[aria-label*='Search']",
    "input[aria-label*='search']",
    "input[name='keyword']",
    "input[name='keys']",
    "#search",
    "#search-input",
    "#search-field",
]


class IconSearchStrategy(BaseSearchStrategy):

    CONFIDENCE_THRESHOLD = 3

    async def execute(self):

        from app.executor import execute_search

        for selector in ICON_SELECTORS:

            try:
                element = await self.page.query_selector(selector)
                if not element:
                    continue

                if not await element.is_visible():
                    continue

                await element.click()

                # Wait briefly for input to appear (event-based)
                try:
                    await self.page.wait_for_selector(
                        "input",
                        timeout=3000
                    )
                except Exception:
                    pass

                # Try only the short high-signal input selectors
                for input_selector in ICON_INPUT_SELECTORS:

                    try:
                        input_el = await self.page.query_selector(input_selector)
                        if not input_el or not await input_el.is_visible():
                            continue

                        score, result_type = await execute_search(
                            self.page,
                            input_selector
                        )

                        if score >= self.CONFIDENCE_THRESHOLD:
                            return {
                                "method": "icon",
                                "pattern": selector,
                                "confidence": score,
                                "result_type": result_type
                            }
                    except Exception as e:
                        logger.debug(f"Icon input {input_selector} failed: {e}")
                        continue

            except Exception as e:
                logger.debug(f"Icon selector {selector} failed: {e}")
                continue

        return None