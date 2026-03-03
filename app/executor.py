import logging
from app.validator import validate_search, get_page_hash

logger = logging.getLogger(__name__)


async def execute_search(page, input_selector=None, keyword="test"):
    """
    Execute a search by filling an input field and pressing Enter.
    Returns (score, result_type) tuple from validation.
    """

    original_url = page.url
    original_hash = await get_page_hash(page)

    try:
        if input_selector:
            element = await page.query_selector(input_selector)
            if element:
                await element.focus()
                await element.fill(keyword)
                await element.press("Enter")
                await page.wait_for_timeout(3000)

                score, result_type = await validate_search(
                    page,
                    original_url,
                    original_hash,
                    keyword
                )

                return score, result_type

    except Exception as e:
        logger.debug(f"execute_search failed for {input_selector}: {e}")
        return 0, "none"

    return 0, "none"