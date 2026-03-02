from app.strategies.icon_strategy import IconSearchStrategy
from app.strategies.input_strategy import InputSearchStrategy
from app.strategies.url_strategy import UrlSearchStrategy
from app.strategies.fallback_strategy import FallbackStrategy


async def detect_search(page, base_url):

    strategies = [
        InputSearchStrategy,
        IconSearchStrategy,
        UrlSearchStrategy,
        FallbackStrategy,
    ]
    for Strategy in strategies:
        strategy = Strategy(page, base_url)
        result = await strategy.execute()
        if result:
            return result

    return None