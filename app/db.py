import os
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


async def save_search_pattern(base_url_id, pattern, strategy_used):
    """
    Saves the detected search pattern and marks the base_url as processed.
    """
    try:
        # Upsert the search pattern
        supabase.from_("search_patterns").upsert(
            {
                "base_url_id": base_url_id,
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