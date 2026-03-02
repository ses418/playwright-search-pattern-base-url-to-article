import asyncio
from app.strategies.base import BaseSearchStrategy


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


class IconSearchStrategy(BaseSearchStrategy):

    CONFIDENCE_THRESHOLD = 3

    async def execute(self):

        from app.strategies.input_strategy import INPUT_SELECTORS
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
                except:
                    pass

                # Try all input selectors after icon click
                for input_selector in INPUT_SELECTORS:

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

            except:
                continue

        return None