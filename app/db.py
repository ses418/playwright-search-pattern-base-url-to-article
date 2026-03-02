import os
import time
from supabase import create_client, Client


# Load environment variables
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# Enforce that environment variables are set
if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError(
        "Supabase credentials not found. Please set SUPABASE_URL and SUPABASE_KEY in your environment."
    )

# Initialize Supabase client
# This will now only execute if the credentials are valid
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Lock configuration
LOCK_KEY = "batch_processing_lock"
LOCK_EXPIRY_SECONDS = 1800  # 30 minutes


async def fetch_unprocessed_base_urls(limit=100):
    """
    Fetches a batch of base URLs that haven't been processed yet.
    """
    try:
        response = (
            supabase.from_("ses_base_url")
            .select("base_url_id, base_url")
            .eq("search_processed", "FALSE")
            .limit(limit)
            .execute()
        )

        if response.data:
            return response.data
        else:
            return []

    except Exception as e:
        print(f"Error fetching unprocessed base URLs: {e}")
        return []


async def save_search_pattern(base_url_id, base_url, result):
    """
    Saves the detected search pattern and marks the base_url as processed.
    """
    try:
        pattern = result.get("pattern")
        strategy_used = result.get("method")

        # Upsert the search pattern
        supabase.from_("search_patterns").upsert(
            {
                "base_url_id": base_url_id,
                "base_url": base_url,
                "pattern": pattern,
                "strategy_used": strategy_used,
            }
        ).execute()

        # Mark as processed
        supabase.from_("ses_base_url").update({"search_processed": "TRUE"}).eq(
            "base_url_id", base_url_id
        ).execute()

    except Exception as e:
        print(f"Error saving search pattern: {e}")


def acquire_batch_lock(max_retries=3, retry_delay=2):
    """
    Attempts to acquire a batch processing lock.
    Returns True if lock acquired, False otherwise.
    """
    for attempt in range(max_retries):
        try:
            # Try to insert a lock record
            response = supabase.table("batch_locks").upsert({
                "lock_key": LOCK_KEY,
                "locked_at": time.time(),
                "expires_at": time.time() + LOCK_EXPIRY_SECONDS
            }, on_conflict="lock_key").execute()

            # Check if we got the lock
            if response.data:
                return True

        except Exception as e:
            print(f"Lock acquisition attempt {attempt + 1} failed: {e}")
            time.sleep(retry_delay)

    return False


def release_batch_lock():
    """
    Releases the batch processing lock.
    """
    try:
        supabase.table("batch_locks").delete().eq("lock_key", LOCK_KEY).execute()
    except Exception as e:
        print(f"Error releasing lock: {e}")