import asyncio
import gc
import logging
import signal
import time
from fastapi import FastAPI, BackgroundTasks
from app.db import fetch_unprocessed_base_urls, acquire_batch_lock, release_batch_lock
from app.worker import run_batch

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

BATCH_SIZE = 50  # Reduced from 100 to lower memory pressure
DOMAINS_PER_BROWSER = 25  # Restart browser after this many domains
shutdown_event = asyncio.Event()
batch_status = {"running": False, "last_run": None, "total_processed": 0}


async def lifespan(app: FastAPI):
    # Startup
    logger.info("Application starting up...")
    yield
    # Shutdown
    logger.info("Application shutting down...")
    shutdown_event.set()
    release_batch_lock()  # Ensure lock is released on shutdown


app = FastAPI(title="Search Pattern Discovery Service", lifespan=lifespan)


def handle_shutdown(signum, frame):
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_event.set()
    release_batch_lock()


# Register signal handlers for graceful shutdown
signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)


async def main():

    logger.info("Starting batch processing...")

    total_processed = 0
    domains_since_restart = 0

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

        # Run batch with memory management
        await run_batch(domains, domains_since_restart)

        total_processed += len(domains)
        domains_since_restart += len(domains)

        # Force garbage collection after each batch
        gc.collect()

        # Check if we need to restart the browser (memory management)
        if domains_since_restart >= DOMAINS_PER_BROWSER:
            logger.info(f"Processed {domains_since_restart} domains, triggering browser restart...")
            domains_since_restart = 0

    logger.info(f"All batches completed. Total processed in this run: {total_processed}")
    batch_status["total_processed"] += total_processed


async def background_runner():

    if batch_status["running"]:
        logger.warning("Batch already running.")
        return

    # Try to acquire database lock
    if not acquire_batch_lock():
        logger.warning("Could not acquire batch lock. Another instance may be running.")
        return

    batch_status["running"] = True
    batch_status["last_run"] = time.time()

    try:
        await main()
    except Exception as e:
        logger.error(f"Batch processing failed: {e}")
    finally:
        batch_status["running"] = False
        release_batch_lock()


@app.get("/health")
async def health_check():
    return {
        "status": "ok",
        "batch_running": batch_status["running"],
        "last_run": batch_status["last_run"],
        "total_processed": batch_status["total_processed"]
    }


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