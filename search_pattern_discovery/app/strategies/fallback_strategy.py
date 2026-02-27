from app.strategies.base import BaseSearchStrategy


class FallbackStrategy(BaseSearchStrategy):

    async def execute(self):
        try:
            links = await self.page.query_selector_all("a")
            for link in links:
                href = await link.get_attribute("href")
                if href and "search" in href.lower():
                    return {
                        "method": "fallback",
                        "pattern": href
                    }
        except:
            pass

        return None