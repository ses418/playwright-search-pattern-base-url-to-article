import hashlib


async def get_page_hash(page):
    try:
        content = await page.content()
        if not content:
            return None
        return hashlib.md5(content.encode()).hexdigest()
    except:
        return None


async def validate_search(page, original_url=None, original_hash=None, keyword=None):

    score = 0
    result_type = "unknown"

    try:
        new_url = page.url or ""
        content = await page.content() or ""
        content_lower = content.lower()

        # 🚫 Empty / blocked page protection
        if len(content) < 500:
            return 0, "empty"

        # 1️⃣ URL changed (strong signal)
        if original_url and new_url.rstrip("/") != original_url.rstrip("/"):
            score += 2
            result_type = "redirect"

        # 2️⃣ URL contains search indicators
        if any(x in new_url.lower() for x in ["search", "?q=", "?s=", "query="]):
            score += 2
            if result_type == "unknown":
                result_type = "search-url"

        # 3️⃣ Content changed (only if original_hash provided)
        if original_hash:
            new_hash = await get_page_hash(page)
            if new_hash and new_hash != original_hash:
                score += 1
                if result_type == "unknown":
                    result_type = "same-page"

        # 4️⃣ Keyword visible (optional, weak signal)
        if keyword and keyword.lower() in content_lower:
            score += 1

        # 5️⃣ Detect search result containers
        search_indicators = [
            "search result",
            "results for",
            "no results",
            "found",
        ]

        if any(indicator in content_lower for indicator in search_indicators):
            score += 1

        # 6️⃣ Count internal links (result pages have many links)
        try:
            links = await page.query_selector_all("a")
            if len(links) >= 5:
                score += 1
        except:
            pass

        # 7️⃣ Detect AJAX-style search (URL unchanged but content changed)
        if original_url and new_url.rstrip("/") == original_url.rstrip("/"):
            if original_hash:
                new_hash = await get_page_hash(page)
                if new_hash and new_hash != original_hash:
                    score += 2
                    result_type = "ajax"

        # 8️⃣ Modal detection (more accurate)
        if any(x in content_lower for x in ["modal", "popup", "overlay"]):
            result_type = "modal"

        # 9️⃣ Cap score to avoid runaway
        if score > 6:
            score = 6

        return score, result_type

    except Exception:
        # Absolute fail-safe
        return 0, "error"

# import hashlib


# async def get_page_hash(page):
#     content = await page.content()
#     return hashlib.md5(content.encode()).hexdigest()


# async def validate_search(page, original_url, original_hash, keyword):

#     score = 0
#     result_type = "unknown"

#     new_url = page.url
#     new_hash = await get_page_hash(page)
#     content = await page.content()

#     # URL changed
#     if new_url != original_url:
#         score += 1
#         result_type = "redirect"

#     # Content changed
#     if new_hash != original_hash:
#         score += 1
#         if result_type == "unknown":
#             result_type = "same-page"

#     # Keyword visible
#     if keyword.lower() in content.lower():
#         score += 1

#     # Results word present
#     if "result" in content.lower():
#         score += 1

#     # Modal detection
#     if "modal" in content.lower():
#         result_type = "modal"

#     return score, result_type