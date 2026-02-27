import asyncio
from fastapi import FastAPI, BackgroundTasks
from app.db import fetch_unprocessed_base_urls
from app.worker import run_batch


BATCH_SIZE = 100  # Adjust as needed

app = FastAPI(title="Search Pattern Discovery Service")

is_running = False  # Prevent parallel runs


async def main():

    print("Starting batch processing...")

    total_processed = 0

    while True:

        # 🔥 Fetch only unprocessed rows
        domains = await fetch_unprocessed_base_urls(limit=BATCH_SIZE)

        if not domains:
            break

        print(f"Fetched unprocessed batch: {len(domains)}")

        await run_batch(domains)

        total_processed += len(domains)

    print("\n✅ All batches completed.")
    print(f"Total processed in this run: {total_processed}")


async def background_runner():
    global is_running

    if is_running:
        print("Batch already running.")
        return

    is_running = True
    try:
        await main()
    finally:
        is_running = False


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/run-batch")
async def run_batch_endpoint(background_tasks: BackgroundTasks):
    background_tasks.add_task(background_runner)
    return {"message": "Batch started in background"}