"""
JavaScript API Interception Strategy.

Intercepts network requests (fetch/XHR) triggered by typing in search inputs,
to catch SPA-style search endpoints that don't cause page navigation.
"""
import asyncio
import logging
from urllib.parse import urlparse, parse_qs
from app.strategies.base import BaseSearchStrategy

logger = logging.getLogger(__name__)

TEST_KEYWORD = "test"

# Input selectors to try (high-signal only)
API_INPUT_SELECTORS = [
    "input[type='search']",
    "input[name='q']",
    "input[name='query']",
    "input[name='s']",
    "input[name='search']",
    "input[placeholder*='Search']",
    "input[placeholder*='search']",
    "input[aria-label*='Search']",
    "input[aria-label*='search']",
    "input[class*='search']",
    "input[id*='search']",
    "[role='search'] input",
    "form[action*='search'] input",
]


class ApiInterceptStrategy(BaseSearchStrategy):
    """
    Strategy that intercepts network calls triggered by search input.
    Catches AJAX/fetch-based search APIs used by SPAs and modern sites.
    """

    async def execute(self):
        captured_urls = []

        def request_handler(request):
            url = request.url.lower()
            # Look for search-related API calls
            if any(kw in url for kw in [
                "search", "?q=", "?s=", "query=", "keyword=",
                "/api/", "autocomplete", "suggest", "typeahead",
                "find", "lookup"
            ]):
                # Filter out tracking/analytics
                if not any(skip in url for skip in [
                    "google-analytics", "gtag", "facebook", "pixel",
                    "doubleclick", "analytics", ".css", ".js", ".png",
                    ".jpg", ".gif", ".svg", "fonts"
                ]):
                    captured_urls.append(request.url)

        # Attach listener
        self.page.on("request", request_handler)

        try:
            for selector in API_INPUT_SELECTORS:
                try:
                    element = await self.page.query_selector(selector)
                    if not element:
                        continue
                    if not await element.is_visible():
                        continue

                    input_type = await element.get_attribute("type")
                    if input_type in ["email", "password", "tel", "number"]:
                        continue

                    # Clear captured URLs for this attempt
                    captured_urls.clear()

                    # Type the keyword character by character (triggers autocomplete)
                    await element.focus()
                    await element.fill("")
                    await self.page.keyboard.type(TEST_KEYWORD, delay=100)

                    # Wait for API calls to fire
                    await self.page.wait_for_timeout(2000)

                    # Also try pressing Enter
                    await self.page.keyboard.press("Enter")
                    await self.page.wait_for_timeout(2000)

                    if captured_urls:
                        # Take the most relevant URL
                        best_url = self._extract_pattern(captured_urls[0])
                        if best_url:
                            logger.info(
                                f"⚡ Intercepted search API: {captured_urls[0]} -> {best_url}"
                            )
                            return {
                                "method": "api",
                                "pattern": best_url,
                                "confidence": 5,
                                "result_type": "api-intercept"
                            }

                except Exception as e:
                    logger.debug(f"ApiIntercept input {selector} failed: {e}")
                    continue

        finally:
            # Remove listener
            try:
                self.page.remove_listener("request", request_handler)
            except Exception:
                pass

        return None

    def _extract_pattern(self, url):
        """
        Convert a captured API URL into a reusable search pattern.
        e.g. https://example.com/api/search?q=test -> /api/search?q={}
        """
        try:
            parsed = urlparse(url)
            path = parsed.path

            # Check query parameters for the keyword
            params = parse_qs(parsed.query)
            for key, values in params.items():
                for val in values:
                    if TEST_KEYWORD.lower() in val.lower():
                        # Replace the keyword with {} placeholder
                        return f"{path}?{key}={{}}"

            # Check if keyword is in the path
            if TEST_KEYWORD.lower() in path.lower():
                pattern_path = path.lower().replace(TEST_KEYWORD.lower(), "{}")
                return pattern_path

            # Fallback: just return the path with first query param as template
            if params:
                first_key = list(params.keys())[0]
                return f"{path}?{first_key}={{}}"

            return path

        except Exception as e:
            logger.debug(f"Pattern extraction error: {e}")
            return None
