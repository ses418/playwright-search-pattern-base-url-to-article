import asyncio
from urllib.parse import urljoin
from app.strategies.base import BaseSearchStrategy


SEARCH_URL_PATTERNS = [
    "/search?q={}",
    "/search?query={}",
    "/search?keyword={}",
    "/search?term={}",
    "/search?search={}",
    "/search?text={}",
    "/?q={}",
    "/?query={}",
    "/?s={}",
    "/?search={}",
    "/search/{}",
    "/search/{}/",
    "/search/results?q={}",
    "/search/results?query={}",
    "/api/search?q={}",
    "/api/search?query={}",
    "/api/v1/search?q={}",
    "/content/search?q={}",
    "/site/search?q={}",
    "/find?q={}",
    "/lookup?q={}",
    "/articles/search?q={}",
    "/news/search?q={}",
    "/search.aspx?q={}",
    "/search.php?q={}",
    "/search.jsp?q={}",
    "/en/search?q={}",
    "/global/search?q={}",
    "search/?k={}",
]


class UrlSearchStrategy(BaseSearchStrategy):

    TEST_KEYWORD = "automationtest123"
    CONFIDENCE_THRESHOLD = 2
    FAST_TIMEOUT = 8000
    SLOW_TIMEOUT = 15000

    async def safe_goto(self, page, url):
        """
        Fast attempt + retry with extended timeout.
        Prevents long blocking while handling slow sites.
        """
        for timeout in [self.FAST_TIMEOUT, self.SLOW_TIMEOUT]:
            try:
                response = await page.goto(
                    url,
                    timeout=timeout,
                    wait_until="domcontentloaded"
                )
                return response
            except:
                continue
        return None

    async def execute(self):

        from app.validator import validate_search

        context = self.page.context
        temp_page = await context.new_page()

        try:
            for pattern in SEARCH_URL_PATTERNS:

                test_url = urljoin(
                    self.base_url.rstrip("/") + "/",
                    pattern.format(self.TEST_KEYWORD)
                )

                response = await self.safe_goto(temp_page, test_url)

                if not response or response.status != 200:
                    continue

                try:
                    current_url = temp_page.url.lower()

                    # 1️⃣ URL contains search param
                    if "search" not in current_url and "q=" not in current_url:
                        continue

                    # 2️⃣ Page not same as homepage
                    if current_url.rstrip("/") == self.base_url.rstrip("/"):
                        continue

                    # 3️⃣ Page has multiple result links
                    links = await temp_page.query_selector_all("a")

                    if len(links) < 5:
                        continue

                    # Structured validation scoring
                    score = await validate_search(
                        temp_page,
                        original_url=self.base_url,
                        original_hash=None,
                        keyword=self.TEST_KEYWORD
                    )

                    if score >= self.CONFIDENCE_THRESHOLD:
                        return {
                            "method": "url",
                            "pattern": pattern,
                            "confidence": score,
                            "result_type": "url"
                        }

                except:
                    continue

        finally:
            await temp_page.close()

        return None