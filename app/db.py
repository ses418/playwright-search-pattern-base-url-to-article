import os
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in environment variables")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# 🔥 Fetch only unprocessed rows (NO OFFSET)
async def fetch_unprocessed_base_urls(limit=100):
    try:
        response = (
            supabase
            .table("ses_base_url")
            .select("base_url_id, base_url")
            .eq("search_processed", False)
            .limit(limit)
            .execute()
        )

        return response.data or []

    except Exception as e:
        print("Error fetching unprocessed base URLs:", str(e))
        return []


# 🔥 Save pattern AND mark as processed
async def save_search_pattern(base_url_id, base_url, result):

    if not result:
        return

    if result.get("confidence", 0) <= 0:
        return

    try:
        # Save or update search pattern
        supabase.table("base_url_search_patterns").upsert({
            "base_url_id": base_url_id,
            "base_url": base_url,
            "method": result.get("method"),
            "pattern": result.get("pattern"),
            "confidence": result.get("confidence"),
            "result_type": result.get("result_type")
        }, on_conflict="base_url_id").execute()

        # ✅ Mark this URL as processed
        supabase.table("ses_base_url") \
            .update({"search_processed": True}) \
            .eq("base_url_id", base_url_id) \
            .execute()

    except Exception as e:
        print(f"Error saving search pattern for {base_url}:", str(e))