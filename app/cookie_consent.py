"""
Cookie Consent Dismissal Utility.

Attempts to dismiss GDPR / cookie consent banners that block search functionality.
Called once before running detection strategies.
"""
import logging

logger = logging.getLogger(__name__)

# Common cookie consent button selectors (ordered by specificity)
COOKIE_ACCEPT_SELECTORS = [
    # Text-based — strongest signals
    "button:has-text('Accept all')",
    "button:has-text('Accept All')",
    "button:has-text('Accept cookies')",
    "button:has-text('Accept Cookies')",
    "button:has-text('I agree')",
    "button:has-text('I Agree')",
    "button:has-text('Allow all')",
    "button:has-text('Allow All')",
    "button:has-text('Got it')",
    "button:has-text('OK')",
    "button:has-text('Agree')",
    "button:has-text('Consent')",
    "button:has-text('Continue')",
    "a:has-text('Accept all')",
    "a:has-text('Accept cookies')",
    "a:has-text('I agree')",

    # ID-based
    "#onetrust-accept-btn-handler",
    "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
    "#CybotCookiebotDialogBodyLevelButtonAccept",
    "#cookie-accept",
    "#accept-cookies",
    "#acceptAllCookies",
    "#cookieAcceptAll",
    "#gdpr-accept",
    "#consent-accept",
    "#cookie-consent-accept",
    "#hs-eu-confirmation-button",

    # Class-based
    ".cookie-accept",
    ".cookie-consent-accept",
    ".accept-cookies",
    ".cc-accept",
    ".cc-btn.cc-dismiss",
    ".gdpr-accept",
    ".consent-accept",
    ".js-cookie-accept",

    # Data attributes
    "[data-action='accept']",
    "[data-consent='accept']",
    "[data-cookie-accept]",
    "[data-testid='cookie-accept']",
    "[data-testid='accept-cookies']",

    # Aria
    "button[aria-label*='accept']",
    "button[aria-label*='Accept']",
    "button[aria-label*='consent']",
    "button[aria-label*='cookie']",

    # Common frameworks
    ".fc-cta-consent",  # FundingChoices (Google)
    ".qc-cmp2-summary-buttons button:first-child",  # Quantcast
    ".sp_choice_type_11",  # SourcePoint
]


async def dismiss_cookie_consent(page, timeout=5000):
    """
    Attempt to dismiss any visible cookie consent banner.
    Returns True if a banner was found and dismissed, False otherwise.

    Args:
        page: Playwright page object
        timeout: Max time to wait for banner element (ms)
    """
    try:
        for selector in COOKIE_ACCEPT_SELECTORS:
            try:
                element = await page.query_selector(selector)
                if element and await element.is_visible():
                    await element.click()
                    # Brief wait for banner to close
                    await page.wait_for_timeout(1000)
                    logger.info(f"🍪 Dismissed cookie consent via: {selector}")
                    return True
            except Exception:
                continue

        # If no button found, try pressing Escape (some banners close with Escape)
        try:
            await page.keyboard.press("Escape")
            await page.wait_for_timeout(500)
        except Exception:
            pass

        return False

    except Exception as e:
        logger.debug(f"Cookie consent dismissal error: {e}")
        return False
