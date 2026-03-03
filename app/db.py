import os
import logging
import time
from supabase import create_client, Client

# Safety net: load .env if present (for local dev and as Docker fallback)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logger = logging.getLogger(__name__)

# Load environment variables
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError(
        "Supabase credentials not found. Set SUPABASE_URL and SUPABASE_KEY."
    )

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Lock configuration
LOCK_KEY = "batch_processing_lock"
LOCK_EXPIRY_SECONDS = 1800  # 30 minutes

# The actual table name in Supabase
PATTERNS_TABLE = "base_url_search_patterns"
BASE_URL_TABLE = "ses_base_url"

# Max retries before permanently skipping a not-found URL
MAX_RETRY_COUNT = 3


async def fetch_unprocessed_base_urls(limit=50):
    """
    Fetches URLs that have NEVER been processed (search_processed=FALSE).
    These are the priority — always processed first.
    """
    try:
        response = (
            supabase.from_(BASE_URL_TABLE)
            .select("base_url_id, base_url")
            .eq("search_processed", False)
            .limit(limit)
            .execute()
        )
        return response.data or []
    except Exception as e:
        logger.error(f"Error fetching unprocessed URLs: {e}")
        return []


async def fetch_not_found_urls_for_retry(limit=20):
    """
    Fetches URLs that were processed but not found (search_not_found=TRUE).
    Only retries URLs with retry_count < MAX_RETRY_COUNT.
    These are retried with enhanced strategies AFTER all new URLs are done.
    """
    try:
        response = (
            supabase.from_(BASE_URL_TABLE)
            .select("base_url_id, base_url")
            .eq("search_not_found", True)
            .lt("retry_count", MAX_RETRY_COUNT)
            .limit(limit)
            .execute()
        )
        return response.data or []
    except Exception as e:
        error_str = str(e)
        # If retry_count column doesn't exist, just fetch without that filter
        if "retry_count" in error_str or "42703" in error_str:
            logger.warning(
                "retry_count column missing. Run: "
                "ALTER TABLE ses_base_url ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 0;"
            )
            try:
                response = (
                    supabase.from_(BASE_URL_TABLE)
                    .select("base_url_id, base_url")
                    .eq("search_not_found", True)
                    .limit(limit)
                    .execute()
                )
                return response.data or []
            except Exception as e2:
                logger.error(f"Error fetching not-found URLs: {e2}")
                return []
        logger.error(f"Error fetching not-found URLs: {e}")
        return []


async def save_search_pattern(base_url_id, base_url, result):
    """
    Saves the detected search pattern to BOTH:
    1. base_url_search_patterns table (full details)
    2. ses_base_url.search_pattern column (the pattern string)
    Also marks search_processed=TRUE, search_not_found=FALSE.
    """
    pattern = result.get("pattern")
    strategy_used = result.get("method")
    confidence = result.get("confidence", 0)
    result_type = result.get("result_type", "unknown")

    # 1. Save to base_url_search_patterns table
    #    Column is "method" (not "strategy_used") in the actual table
    try:
        supabase.from_(PATTERNS_TABLE).upsert(
            {
                "base_url_id": base_url_id,
                "base_url": base_url,
                "pattern": pattern,
                "method": strategy_used,
                "confidence": confidence,
                "result_type": result_type,
            },
            on_conflict="base_url_id"
        ).execute()
    except Exception as e:
        logger.error(f"Error saving to {PATTERNS_TABLE}: {e}")

    # 2. Update ses_base_url: mark as processed + store pattern string
    try:
        supabase.from_(BASE_URL_TABLE).update({
            "search_processed": True,
            "search_not_found": False,
            "search_pattern": pattern,
        }).eq("base_url_id", base_url_id).execute()

        logger.info(
            f"✅ Saved pattern for {base_url}: {pattern} "
            f"(strategy: {strategy_used}, confidence: {confidence})"
        )
    except Exception as e:
        logger.error(f"Error updating {BASE_URL_TABLE} for {base_url}: {e}")


async def mark_as_not_found(base_url_id, base_url):
    """
    Marks a URL as processed + not found.
    Increments retry_count so we don't retry forever.
    """
    try:
        # First get current retry_count
        current = supabase.from_(BASE_URL_TABLE).select(
            "retry_count"
        ).eq("base_url_id", base_url_id).execute()

        current_count = 0
        if current.data and current.data[0].get("retry_count") is not None:
            current_count = current.data[0]["retry_count"]

        supabase.from_(BASE_URL_TABLE).update({
            "search_processed": True,
            "search_not_found": True,
            "retry_count": current_count + 1,
        }).eq("base_url_id", base_url_id).execute()

        logger.info(f"Marked not-found: {base_url} (retry {current_count + 1}/{MAX_RETRY_COUNT})")

    except Exception as e:
        error_str = str(e)
        if "retry_count" in error_str or "42703" in error_str:
            # retry_count column doesn't exist — just update without it
            try:
                supabase.from_(BASE_URL_TABLE).update({
                    "search_processed": True,
                    "search_not_found": True,
                }).eq("base_url_id", base_url_id).execute()
                logger.info(f"Marked not-found: {base_url} (no retry_count column)")
            except Exception as e2:
                logger.error(f"Error marking {base_url} as not-found: {e2}")
        else:
            logger.error(f"Error marking {base_url} as not-found: {e}")


async def reset_not_found_for_retry(base_url_id):
    """
    Resets a not-found URL back to unprocessed state for retry.
    Called before enhanced retry processing.
    """
    try:
        supabase.from_(BASE_URL_TABLE).update({
            "search_processed": False,
            "search_not_found": False,
        }).eq("base_url_id", base_url_id).execute()
    except Exception as e:
        logger.error(f"Error resetting URL {base_url_id} for retry: {e}")


def acquire_batch_lock(max_retries=3, retry_delay=2):
    """Acquire batch processing lock."""
    for attempt in range(max_retries):
        try:
            response = supabase.table("batch_locks").upsert({
                "lock_key": LOCK_KEY,
                "locked_at": time.time(),
                "expires_at": time.time() + LOCK_EXPIRY_SECONDS
            }, on_conflict="lock_key").execute()
            if response.data:
                return True
        except Exception as e:
            logger.error(f"Lock attempt {attempt + 1} failed: {e}")
            time.sleep(retry_delay)
    return False


def release_batch_lock():
    """Release batch processing lock."""
    try:
        supabase.table("batch_locks").delete().eq("lock_key", LOCK_KEY).execute()
    except Exception as e:
        logger.error(f"Error releasing lock: {e}")