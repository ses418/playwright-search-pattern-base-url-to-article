import asyncio
import logging
from app.cookie_consent import dismiss_cookie_consent
from app.strategies.input_strategy import InputSearchStrategy
from app.strategies.icon_strategy import IconSearchStrategy
from app.strategies.form_strategy import FormActionStrategy
from app.strategies.url_strategy import UrlSearchStrategy
from app.strategies.api_strategy import ApiInterceptStrategy
from app.strategies.fallback_strategy import FallbackStrategy

logger = logging.getLogger(__name__)


async def ensure_page_ready(page):
    """Check that the page is still alive and has a valid DOM."""
    try:
        await page.evaluate("() => document.readyState")
        return True
    except Exception as e:
        logger.warning(f"Page readiness check failed: {e}")
        return False


async def try_iframe_search(page, base_url, enhanced_mode=False):
    """
    If main page strategies fail, try searching inside iframes.
    Some sites put search functionality inside iframes.
    """
    try:
        frames = page.frames
        if len(frames) <= 1:  # Only main frame, no iframes
            return None

        logger.info(f"🖼️ Checking {len(frames) - 1} iframes for search on {base_url}")

        for frame in frames:
            if frame == page.main_frame:
                continue

            try:
                # Check if frame has a search input
                search_input = await frame.query_selector(
                    "input[type='search'], input[name='q'], input[name='query'], "
                    "input[name='s'], input[name='search'], "
                    "input[placeholder*='Search'], input[placeholder*='search'], "
                    "[role='search'] input"
                )

                if search_input and await search_input.is_visible():
                    logger.info(f"🖼️ Found search input in iframe: {frame.url}")

                    # Try to extract form action from the iframe
                    form = await frame.query_selector("form[action*='search'], form[role='search']")
                    if form:
                        action = await form.get_attribute("action")
                        if action:
                            name = await search_input.get_attribute("name") or "q"
                            if action.startswith("http"):
                                from urllib.parse import urlparse
                                parsed = urlparse(action)
                                action = parsed.path
                            pattern = f"{action}?{name}={{}}"
                            return {
                                "method": "iframe",
                                "pattern": pattern,
                                "confidence": 3,
                                "result_type": "iframe-form"
                            }

                    # Fallback: return the iframe input selector
                    selector = await search_input.get_attribute("name") or "search"
                    return {
                        "method": "iframe",
                        "pattern": f"iframe:input[name='{selector}']",
                        "confidence": 2,
                        "result_type": "iframe-input"
                    }

            except Exception as e:
                logger.debug(f"Iframe search error: {e}")
                continue

    except Exception as e:
        logger.debug(f"Iframe traversal error: {e}")

    return None


async def detect_search(page, base_url, enhanced_mode=False):
    """
    Run detection strategies in priority order.
    
    Flow:
    1. Dismiss cookie consent banners (GDPR)
    2. Run strategies: Input → FormAction → Icon → ApiIntercept → URL → Fallback
    3. If all fail, check inside iframes
    
    Args:
        page: Playwright page object
        base_url: The URL being analyzed
        enhanced_mode: If True, use longer timeouts (for retrying not-found URLs)
    """
    logger.info(f"Starting detection for {base_url} (enhanced={enhanced_mode})")

    if not await ensure_page_ready(page):
        logger.warning(f"Page not ready: {base_url}")
        return None

    # Step 1: Dismiss cookie consent banners
    await dismiss_cookie_consent(page)

    # Step 2: Run strategies in priority order
    strategies = [
        InputSearchStrategy,     # Direct input fields (most reliable)
        FormActionStrategy,      # Extract from <form action> (no interaction needed)
        IconSearchStrategy,      # Click search icons to reveal inputs
        ApiInterceptStrategy,    # Intercept XHR/fetch search API calls (SPAs)
        UrlSearchStrategy,       # Try common URL patterns directly
        FallbackStrategy,        # Find any link with "search" in href
    ]

    strategy_timeout = 60.0 if enhanced_mode else 30.0

    for Strategy in strategies:
        strategy_name = Strategy.__name__
        logger.info(f"Trying {strategy_name} for {base_url}")

        try:
            # Reset page to original URL before each strategy
            current_url = page.url
            if current_url.rstrip("/").lower() != base_url.rstrip("/").lower():
                logger.info(f"Resetting page to {base_url} before {strategy_name}")
                try:
                    wait_until = "load" if enhanced_mode else "domcontentloaded"
                    await page.goto(
                        base_url,
                        timeout=20000 if enhanced_mode else 15000,
                        wait_until=wait_until
                    )
                    await page.wait_for_timeout(1000 if enhanced_mode else 500)
                    # Re-dismiss cookie consent after page reset
                    await dismiss_cookie_consent(page)
                except Exception as nav_err:
                    logger.warning(f"Failed to reset page for {strategy_name}: {nav_err}")
                    break

            strategy = Strategy(page, base_url)
            result = await asyncio.wait_for(strategy.execute(), timeout=strategy_timeout)

            if result:
                logger.info(
                    f"✅ {strategy_name} succeeded for {base_url} "
                    f"with confidence {result.get('confidence', 0)}"
                )
                return result
            else:
                logger.info(f"❌ {strategy_name} returned no result for {base_url}")
        except asyncio.TimeoutError:
            logger.warning(f"⏱️ {strategy_name} timed out for {base_url}")
        except Exception as e:
            logger.error(f"💥 {strategy_name} error for {base_url}: {str(e)}")

    # Step 3: If all strategies failed, check inside iframes
    logger.info(f"🖼️ All strategies failed, checking iframes for {base_url}")
    iframe_result = await try_iframe_search(page, base_url, enhanced_mode)
    if iframe_result:
        logger.info(f"✅ Found search in iframe for {base_url}")
        return iframe_result

    return None