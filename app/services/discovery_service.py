"""
DiscoveryService — per-pod liveness discovery for static backends.

Backends for every configured model are registered at startup (even unreachable
ones) by build_registry_from_config(). This service then probes each
StaticBackend on an interval and flips its availability, so the proxy can return
503 for a down model and automatically resume serving it when it recovers — with
no restart.

Design (per the PoC decisions):
  - Per-pod, in-memory: each pod probes from its own network vantage; state lives
    on the StaticBackend object, not Redis.
  - Polling only: a model is marked down/up by the probe, not by live traffic.
  - Hysteresis: one success re-enables immediately; N consecutive failures
    (DISCOVERY_FAILURE_THRESHOLD) are required to disable.
  - Key re-resolution: a reachable-but-401 backend triggers find_working_api_key
    so a rotated/late key is picked up automatically.
  - Static backends only: MultiClusterBackend manages its own health.
"""

from __future__ import annotations

import asyncio
import logging
from typing import List, Optional

from app.backends.base import registry
from app.backends.static import StaticBackend
from app.core.config import get_settings
from app.metrics.collector import metrics_collector
from app.utils.api_key_validator import find_working_api_key, validate_api_key

logger = logging.getLogger(__name__)
settings = get_settings()


class DiscoveryService:
    """Background task that keeps static-backend availability in sync with reality."""

    def __init__(self) -> None:
        self._task: Optional[asyncio.Task] = None
        self._stop_event: Optional[asyncio.Event] = None

    # ------------------------------------------------------------------ #
    # Probing
    # ------------------------------------------------------------------ #

    def _static_backends(self) -> List[StaticBackend]:
        """All registered StaticBackends (skips MultiClusterBackend, etc.)."""
        backends: List[StaticBackend] = []
        for model_id in registry.list_model_ids():
            backend = registry.get(model_id)
            if isinstance(backend, StaticBackend):
                backends.append(backend)
        return backends

    async def probe_once(self, backend: StaticBackend, failure_threshold: int) -> None:
        """Probe a backend, apply the result with hysteresis, update the gauge."""
        success = await self._check_reachable(backend)
        backend.record_probe_result(success, failure_threshold)
        metrics_collector.set_model_available(backend.model_id, backend.is_available)

    async def _check_reachable(self, backend: StaticBackend) -> bool:
        """
        Probe one backend's upstream.

        - Connection error / timeout  → False (down).
        - 200 from /models             → True (up).
        - 401 (reachable, bad key)     → try to re-resolve a working key; True if
          one is found (and stored), otherwise False.
        """
        try:
            reachable_ok = await validate_api_key(backend.base_url, backend.api_key)
        except Exception:
            # Unreachable — no point trying other keys against a dead endpoint.
            return False

        if reachable_ok:
            return True

        # Reachable but the current key was rejected — try to find a working one.
        try:
            new_key = await find_working_api_key(
                base_url=backend.base_url, API_Keys=settings.VAULT_API_KEYs
            )
        except Exception:
            new_key = ""

        if new_key:
            backend.set_api_key(new_key)
            return True
        return False

    async def _probe_all(self, failure_threshold: int) -> None:
        backends = self._static_backends()
        if not backends:
            return
        await asyncio.gather(
            *(self.probe_once(b, failure_threshold) for b in backends),
            return_exceptions=True,
        )

    async def run_initial_probe(self) -> None:
        """
        One probe pass before the app starts serving, so availability reflects
        reality immediately. Uses threshold=1 (a single failed initial probe
        marks a model down) rather than waiting for steady-state hysteresis.
        """
        await self._probe_all(failure_threshold=1)

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    async def _loop(self) -> None:
        interval = settings.DISCOVERY_PROBE_INTERVAL_SECONDS
        threshold = settings.DISCOVERY_FAILURE_THRESHOLD
        assert self._stop_event is not None
        while not self._stop_event.is_set():
            try:
                await self._probe_all(failure_threshold=threshold)
            except Exception:
                # Never let a probe cycle kill the loop.
                logger.exception("Discovery probe cycle failed; retrying next interval")
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass

    async def start(self) -> None:
        """Run the initial probe pass, then start the background loop."""
        if not settings.DISCOVERY_ENABLED:
            logger.info("DiscoveryService disabled (DISCOVERY_ENABLED=false)")
            return
        if self._task is not None:
            logger.warning("DiscoveryService already started")
            return

        self._stop_event = asyncio.Event()
        await self.run_initial_probe()
        self._task = asyncio.create_task(self._loop())
        logger.info(
            "DiscoveryService started (interval=%ss, failure_threshold=%s)",
            settings.DISCOVERY_PROBE_INTERVAL_SECONDS,
            settings.DISCOVERY_FAILURE_THRESHOLD,
        )

    async def stop(self) -> None:
        """Signal and cancel the background loop."""
        if self._stop_event is not None:
            self._stop_event.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("Error awaiting DiscoveryService task during stop")
            self._task = None
        logger.info("DiscoveryService stopped")


# Module-level singleton, started/stopped from the app lifespan.
discovery_service = DiscoveryService()
