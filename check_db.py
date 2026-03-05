import os
from dotenv import load_dotenv
load_dotenv()
from supabase import create_client

supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

r1 = supabase.from_("ses_base_url").select("base_url_id", count="exact").eq("search_processed", False).execute()
r2 = supabase.from_("ses_base_url").select("base_url_id", count="exact").eq("search_processed", True).eq("search_not_found", False).execute()
r3 = supabase.from_("ses_base_url").select("base_url_id", count="exact").eq("search_not_found", True).execute()
r4 = supabase.from_("base_url_search_patterns").select("id", count="exact").execute()
print(f"Unprocessed: {r1.count}")
print(f"Found: {r2.count}")
print(f"Not Found: {r3.count}")
print(f"Patterns in table: {r4.count}")
print(f"Gap: {r2.count - r4.count}")

# Latest 5 patterns
print("\nLatest patterns saved:")
r = supabase.from_("base_url_search_patterns").select("base_url, pattern, method, confidence").order("created_at", desc=True).limit(5).execute()
for p in r.data:
    print(f"  {p['base_url']} -> {p['pattern']} (method={p['method']}, conf={p['confidence']})")
