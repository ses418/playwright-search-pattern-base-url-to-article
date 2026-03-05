import asyncio
import logging
from app.db import (
    fetch_unprocessed_base_urls,
    fetch_not_found_urls_for_retry,
    acquire_batch_lock,
    release_batch_lock,
)
from app.worker import run_batch

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL = 86400  # seconds (24 hours)
BATCH_SIZE = 50
RETRY_BATCH_SIZE = 20


class Scheduler:
    """
    Background scheduler with two-phase processing:
    Phase 1: Process NEW URLs (search_processed=FALSE)
    Phase 2: Retry NOT-FOUND URLs with enhanced strategies
    """

    def __init__(self, poll_interval=DEFAULT_POLL_INTERVAL, batch_size=BATCH_SIZE):
        self.poll_interval = poll_interval
        self.batch_size = batch_size
        self._task = None
        self._running = False
        self.stats = {
            "total_polls": 0,
            "total_new_processed": 0,
            "total_retried": 0,
            "last_poll_time": None,
            "last_batch_size": 0,
            "errors": 0,
        }

    @property
    def is_running(self):
        return self._running

    async def start(self):
        if self._running:
            logger.warning("Scheduler already running")
            return
        self._running = True
        self._task = asyncio.create_task(self._poll_loop())
        logger.info(f"Scheduler started (polling every {self.poll_interval}s)")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Scheduler stopped")

    async def _poll_loop(self):
        while self._running:
            try:
                await self._process_cycle()
            except Exception as e:
                logger.error(f"Scheduler cycle error: {e}")
                self.stats["errors"] += 1
            try:
                await asyncio.sleep(self.poll_interval)
            except asyncio.CancelledError:
                break

    async def _process_cycle(self):
        import time

        self.stats["total_polls"] += 1
        self.stats["last_poll_time"] = time.time()

        if not acquire_batch_lock():
            logger.info("Scheduler: lock held, skipping cycle")
            return

        try:
            # PHASE 1: Process NEW URLs first (priority)
            new_domains = await fetch_unprocessed_base_urls(limit=self.batch_size)
            if new_domains:
                self.stats["last_batch_size"] = len(new_domains)
                logger.info(f"Scheduler Phase 1: processing {len(new_domains)} NEW URLs")
                await run_batch(new_domains, enhanced_mode=False)
                self.stats["total_new_processed"] += len(new_domains)
            else:
                logger.info("Scheduler Phase 1: no new URLs to process")

            # PHASE 2: Retry NOT-FOUND URLs with enhanced strategies
            retry_domains = await fetch_not_found_urls_for_retry(limit=RETRY_BATCH_SIZE)
            if retry_domains:
                logger.info(f"Scheduler Phase 2: retrying {len(retry_domains)} not-found URLs with enhanced mode")
                await run_batch(retry_domains, enhanced_mode=True)
                self.stats["total_retried"] += len(retry_domains)
            else:
                logger.info("Scheduler Phase 2: no not-found URLs to retry")

        finally:
            release_batch_lock()

    def get_stats(self):
        return {
            "running": self._running,
            "poll_interval_seconds": self.poll_interval,
            **self.stats,
        }
