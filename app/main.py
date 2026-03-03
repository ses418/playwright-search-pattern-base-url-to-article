import asyncio
import gc
import logging
import os
import signal
import time
from fastapi import FastAPI, BackgroundTasks
from app.db import fetch_unprocessed_base_urls, fetch_not_found_urls_for_retry, acquire_batch_lock, release_batch_lock
from app.worker import run_batch
from app.scheduler import Scheduler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

BATCH_SIZE = 50
RETRY_BATCH_SIZE = 20
DOMAINS_PER_BROWSER = 25
shutdown_event = asyncio.Event()
batch_status = {
    "running": False,
    "last_run": None,
    "total_new_processed": 0,
    "total_retried": 0,
}

# Scheduler instance
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", "300"))
scheduler = Scheduler(poll_interval=POLL_INTERVAL, batch_size=BATCH_SIZE)


async def lifespan(app: FastAPI):
    logger.info("Application starting up...")
    logger.info(f"Auto-scheduler will poll every {POLL_INTERVAL}s for new URLs")
    await scheduler.start()
    yield
    logger.info("Application shutting down...")
    await scheduler.stop()
    shutdown_event.set()
    release_batch_lock()


app = FastAPI(title="Search Pattern Discovery Service", lifespan=lifespan)


def handle_shutdown(signum, frame):
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_event.set()
    release_batch_lock()


signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)


async def process_new_urls():
    """Phase 1: Process all NEW unprocessed URLs."""
    total = 0
    while not shutdown_event.is_set():
        domains = await fetch_unprocessed_base_urls(limit=BATCH_SIZE)
        if not domains:
            logger.info(f"Phase 1 complete. Total new URLs processed: {total}")
            break
        logger.info(f"Phase 1: Processing {len(domains)} new URLs")
        await run_batch(domains, enhanced_mode=False)
        total += len(domains)
        gc.collect()
    return total


async def process_retry_urls():
    """Phase 2: Retry not-found URLs with enhanced strategies."""
    domains = await fetch_not_found_urls_for_retry(limit=RETRY_BATCH_SIZE)
    if not domains:
        logger.info("Phase 2: No not-found URLs to retry")
        return 0
    logger.info(f"Phase 2: Retrying {len(domains)} not-found URLs (enhanced mode)")
    await run_batch(domains, enhanced_mode=True)
    gc.collect()
    return len(domains)


async def background_runner():
    """Two-phase background processing."""
    if batch_status["running"]:
        logger.warning("Batch already running.")
        return

    if not acquire_batch_lock():
        logger.warning("Could not acquire batch lock.")
        return

    batch_status["running"] = True
    batch_status["last_run"] = time.time()

    try:
        # Phase 1: New URLs
        new_count = await process_new_urls()
        batch_status["total_new_processed"] += new_count

        # Phase 2: Retry not-found
        retry_count = await process_retry_urls()
        batch_status["total_retried"] += retry_count

    except Exception as e:
        logger.error(f"Batch processing failed: {e}")
    finally:
        batch_status["running"] = False
        release_batch_lock()


async def retry_runner():
    """Only retry not-found URLs with enhanced mode."""
    if batch_status["running"]:
        logger.warning("Batch already running.")
        return

    if not acquire_batch_lock():
        logger.warning("Could not acquire batch lock.")
        return

    batch_status["running"] = True
    batch_status["last_run"] = time.time()

    try:
        retry_count = await process_retry_urls()
        batch_status["total_retried"] += retry_count
    except Exception as e:
        logger.error(f"Retry processing failed: {e}")
    finally:
        batch_status["running"] = False
        release_batch_lock()


@app.get("/health")
async def health_check():
    return {
        "status": "ok",
        "batch_running": batch_status["running"],
        "last_run": batch_status["last_run"],
        "total_new_processed": batch_status["total_new_processed"],
        "total_retried": batch_status["total_retried"],
    }


@app.get("/status")
async def detailed_status():
    return {
        "service": "Search Pattern Discovery Engine",
        "batch": batch_status,
        "scheduler": scheduler.get_stats(),
        "config": {
            "batch_size": BATCH_SIZE,
            "retry_batch_size": RETRY_BATCH_SIZE,
            "poll_interval_seconds": POLL_INTERVAL,
        }
    }


@app.post("/run-batch")
async def trigger_batch(background_tasks: BackgroundTasks):
    """Process NEW URLs first, then retry not-found."""
    background_tasks.add_task(background_runner)
    return {"message": "Two-phase batch processing started (new URLs first, then retries)."}


@app.post("/retry-not-found")
async def trigger_retry(background_tasks: BackgroundTasks):
    """Only retry not-found URLs with enhanced strategies."""
    background_tasks.add_task(retry_runner)
    return {"message": "Retrying not-found URLs with enhanced mode."}


@app.post("/scheduler/start")
async def start_scheduler():
    if scheduler.is_running:
        return {"message": "Scheduler already running", "stats": scheduler.get_stats()}
    await scheduler.start()
    return {"message": "Scheduler started", "stats": scheduler.get_stats()}


@app.post("/scheduler/stop")
async def stop_scheduler():
    await scheduler.stop()
    return {"message": "Scheduler stopped", "stats": scheduler.get_stats()}


@app.post("/shutdown")
async def shutdown_server():
    await scheduler.stop()
    shutdown_event.set()
    return {"message": "Shutdown signal sent."}


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=5070, reload=False)