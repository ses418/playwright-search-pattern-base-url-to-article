import hashlib
import logging

logger = logging.getLogger(__name__)


async def get_page_hash(page):
    """Get MD5 hash of current page content for change detection."""
    try:
        content = await page.content()
        if not content:
            return None
        return hashlib.md5(content.encode()).hexdigest()
    except Exception:
        return None


async def validate_search(page, original_url=None, original_hash=None, keyword=None):
    """
    Score how likely the current page is a search results page.
    Returns (score, result_type) tuple.

    Score breakdown:
        +2: URL changed from original
        +2: URL contains search indicators (?q=, ?s=, search, query=)
        +1: Page content changed (hash comparison)
        +1: Keyword visible in page content
        +1: Search result indicators in content
        +1: Page has 5+ links
        +2: AJAX-style search (URL same, content changed)

    Max capped at 6.
    """
    score = 0
    result_type = "unknown"

    try:
        new_url = page.url or ""
        content = await page.content() or ""
        content_lower = content.lower()

        # Empty / blocked page protection
        if len(content) < 500:
            return 0, "empty"

        # 1. URL changed (strong signal)
        if original_url and new_url.rstrip("/") != original_url.rstrip("/"):
            score += 2
            result_type = "redirect"

        # 2. URL contains search indicators
        if any(x in new_url.lower() for x in ["search", "?q=", "?s=", "query="]):
            score += 2
            if result_type == "unknown":
                result_type = "search-url"

        # 3. Content changed
        if original_hash:
            new_hash = await get_page_hash(page)
            if new_hash and new_hash != original_hash:
                score += 1
                if result_type == "unknown":
                    result_type = "same-page"

        # 4. Keyword visible
        if keyword and keyword.lower() in content_lower:
            score += 1

        # 5. Search result containers
        search_indicators = [
            "search result",
            "results for",
            "no results",
            "found",
        ]
        if any(indicator in content_lower for indicator in search_indicators):
            score += 1

        # 6. Multiple links (result pages have many)
        try:
            links = await page.query_selector_all("a")
            if len(links) >= 5:
                score += 1
        except Exception:
            pass

        # 7. AJAX-style search (URL same, content changed)
        if original_url and new_url.rstrip("/") == original_url.rstrip("/"):
            if original_hash:
                new_hash = await get_page_hash(page)
                if new_hash and new_hash != original_hash:
                    score += 2
                    result_type = "ajax"

        # 8. Modal detection
        if any(x in content_lower for x in ["modal", "popup", "overlay"]):
            result_type = "modal"

        # Cap score
        if score > 6:
            score = 6

        return score, result_type

    except Exception as e:
        logger.debug(f"Validation error: {e}")
        return 0, "error"