import asyncio
import logging
import signal
from fastapi import FastAPI, BackgroundTasks
from app.db import fetch_unprocessed_base_urls
from app.worker import run_batch

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

BATCH_SIZE = 100  # Adjust as needed


async def lifespan(app: FastAPI):
    # Startup
    logger.info("Application starting up...")
    yield
    # Shutdown
    logger.info("Application shutting down...")
    shutdown_event.set()


app = FastAPI(title="Search Pattern Discovery Service", lifespan=lifespan)

is_running = False  # Prevent parallel runs
shutdown_event = asyncio.Event()


def handle_shutdown(signum, frame):
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_event.set()


# Register signal handlers for graceful shutdown
signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)


async def main():

    logger.info("Starting batch processing...")

    total_processed = 0

    while not shutdown_event.is_set():

        # Check for shutdown signal
        if shutdown_event.is_set():
            logger.info("Shutdown signal received, stopping batch...")
            break

        # 🔥 Fetch only unprocessed rows
        domains = await fetch_unprocessed_base_urls(limit=BATCH_SIZE)

        if not domains:
            logger.info("No more domains to process.")
            break

        logger.info(f"Fetched unprocessed batch: {len(domains)}")

        await run_batch(domains)

        total_processed += len(domains)

    logger.info(f"All batches completed. Total processed in this run: {total_processed}")


async def background_runner():
    global is_running

    if is_running:
        logger.warning("Batch already running.")
        return

    is_running = True
    try:
        await main()
    finally:
        is_running = False


@app.get("/health")
async def health_check():
    return {"status": "ok"}


@app.post("/run-batch")
async def trigger_batch(background_tasks: BackgroundTasks):
    """
    Triggers a new batch processing job in the background.
    """
    background_tasks.add_task(background_runner)
    return {"message": "Batch processing started in the background."}


@app.post("/shutdown")
async def shutdown_server():
    """
    Gracefully shuts down the server.
    """
    shutdown_event.set()
    return {"message": "Shutdown signal sent. Server will stop after current task."}


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=5070,
        reload=False
    )