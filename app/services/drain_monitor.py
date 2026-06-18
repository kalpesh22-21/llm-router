"""
Simplified background drain monitoring service.

Polls clusters in DRAINING state and automatically transitions
them to DISABLED when in-flight tokens reach 0.
"""

import asyncio
import logging
from typing import Optional

from app.db.cluster_drain import (
    DrainState,
    complete_drain,
    get_drain_state,
    get_draining_clusters,
    get_inflight_tokens,
)

logger = logging.getLogger(__name__)


class DrainMonitor:
    """
    Background service for monitoring cluster drain operations.

    Polls every 5 seconds to check if draining clusters have
    completed (in-flight tokens == 0) and auto-completes them.
    """

    def __init__(self, poll_interval_seconds: float = 5.0) -> None:
        """
        Initialize the drain monitor service.

        Args:
            poll_interval_seconds: How often to poll (default 5s)
        """
        self._poll_interval = poll_interval_seconds
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    # ----------------------------------------------------------------------- #
    # Lifecycle
    # ----------------------------------------------------------------------- #

    async def start_monitoring(self) -> None:
        """Start the background drain monitoring task."""
        if self._task is not None:
            logger.warning("Drain monitor is already running")
            return

        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_monitor_loop())
        logger.info("Drain monitor started (poll_interval=%.1fs)", self._poll_interval)

    async def stop_monitoring(self) -> None:
        """Stop the background drain monitoring task."""
        if self._task is None:
            logger.warning("Drain monitor is not running")
            return

        logger.info("Stopping drain monitor...")
        self._stop_event.set()

        try:
            await asyncio.wait_for(self._task, timeout=10.0)
        except asyncio.TimeoutError:
            logger.warning("Drain monitor did not stop gracefully, cancelling...")
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        except asyncio.CancelledError:
            pass
        finally:
            self._task = None
            logger.info("Drain monitor stopped")

    def is_running(self) -> bool:
        """Check if the drain monitor is currently running."""
        return self._task is not None and not self._task.done()

    # ----------------------------------------------------------------------- #
    # Monitor Loop
    # ----------------------------------------------------------------------- #

    async def _run_monitor_loop(self) -> None:
        """Main monitoring loop."""
        logger.debug("Drain monitor loop started")

        while not self._stop_event.is_set():
            try:
                await self._check_draining_clusters()
            except Exception as e:
                logger.error("Error during drain monitor check: %s", e)

            # Wait for next poll interval or until stopped
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._poll_interval
                )
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                break

        logger.debug("Drain monitor loop ended")

    async def _check_draining_clusters(self) -> None:
        """Check all clusters currently in DRAINING state."""
        draining = await get_draining_clusters()

        if not draining:
            return

        logger.debug("Checking %d draining clusters", len(draining))

        for model_id, cluster_id in draining:
            try:
                await self._check_single_cluster(model_id, cluster_id)
            except Exception as e:
                logger.error("Error checking cluster %s/%s: %s", model_id, cluster_id, e)

    async def _check_single_cluster(self, model_id: str, cluster_id: str) -> None:
        """
        Check a single draining cluster.

        If in-flight tokens == 0, complete the drain (transition to DISABLED).
        """
        # Double-check state is still DRAINING
        state = await get_drain_state(model_id, cluster_id)
        if state != DrainState.DRAINING:
            return

        # Check in-flight tokens
        inflight = await get_inflight_tokens(model_id, cluster_id)

        if inflight == 0:
            logger.info("Drain complete for %s/%s (in-flight=0)", model_id, cluster_id)
            await complete_drain(model_id, cluster_id)
        else:
            logger.debug("Drain in progress for %s/%s (in-flight=%d)", model_id, cluster_id, inflight)


# Module-level singleton
drain_monitor = DrainMonitor()
