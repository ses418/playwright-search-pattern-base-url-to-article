# test_supabase.py

import asyncio
from db import fetch_base_urls

async def test():
    data = await fetch_base_urls(limit=5)
    print(data)

asyncio.run(test())