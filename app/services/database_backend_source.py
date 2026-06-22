"""
DatabaseBackendSource — register backends from the model_backends DB table.

A third discovery source (DB_BACKENDS_ENABLED=true): reads all enabled
model_backend rows with backend_type='static', decrypts their api_key_enc,
and registers StaticBackend instances into the global BackendRegistry.

Source precedence (highest → lowest):
  1. config/models.yaml   (registered at startup before any source starts)
  2. KubernetesServiceDiscovery
  3. DatabaseBackendSource  (this file)

This source tracks what it owns in ``self._managed`` (model_id -> row version).
It never overwrites a backend registered by a higher-priority source; it just
logs a warning and skips the row.

Change propagation:
  - Poll loop: checks ``llmr:models_version`` Redis counter every
    DB_BACKEND_POLL_INTERVAL_SECONDS; if the counter changes (or Redis is
    unreachable) it runs a full reconcile.
  - Pub/sub loop: subscribes to ``llmr:models_changed``; any publisher (Phase 3
    admin API) triggers an immediate reconcile with sub-1 s latency.

Security: api_key_enc is decrypted in-process; the plaintext is NEVER logged
and never appears in API responses.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Dict, List, Optional

from sqlalchemy import select

from app.backends.base import registry
from app.backends.static import StaticBackend
from app.core.config import get_settings
from app.db.mysql import AsyncSessionLocal, ModelBackend
from app.db.redis_client import get_redis_pool
from app.services.model_backend_crypto import decrypt_api_key

logger = logging.getLogger(__name__)

# Redis keys / channel names (namespace matches ADR-001)
_MODELS_VERSION_KEY = "llmr:models_version"
_MODELS_CHANGED_CHANNEL = "llmr:models_changed"


# ---------------------------------------------------------------------------
# Module-level helper: build a StaticBackend from a ModelBackend ORM row
# ---------------------------------------------------------------------------


def _build_backend(row: ModelBackend) -> Optional[StaticBackend]:
    """
    Construct a StaticBackend from a ModelBackend ORM row.

    Returns None if the api_key cannot be decrypted (bad/rotated token), so
    the caller can skip this row without crashing the whole reconcile.

    api_key fallback: if api_key_enc is NULL or empty, fall back to the global
    API_KEY setting (the same convention StaticBackend uses for YAML-sourced
    models where api_key was left empty).
    """
    settings = get_settings()

    if row.api_key_enc:
        try:
            api_key = decrypt_api_key(row.api_key_enc)
        except (ValueError, RuntimeError):
            logger.error(
                "DatabaseBackendSource: failed to decrypt api_key for model_id=%r "
                "(bad or rotated encryption token); skipping this backend",
                row.model_id,
            )
            return None
    else:
        # NULL / empty api_key_enc → fall back to global API_KEY, clearing the
        # sentinel "EMPTY" value the same way kubernetes_discovery.py does.
        api_key = settings.API_KEY if settings.API_KEY not in ("", "EMPTY") else ""

    return StaticBackend(
        model_id=row.model_id,
        base_url=row.base_url,
        api_key=api_key,
    )


# ---------------------------------------------------------------------------
# Module-level helper: publish change notification (Phase 3 calls this)
# ---------------------------------------------------------------------------


async def publish_models_changed() -> None:
    """
    Bump the version counter and notify all pods of a model_backends change.

    Designed to be called by the Phase 3 admin API after a successful DB
    write (create / update / disable / delete).  Fire-and-forget safe: a
    Redis hiccup logs a warning and returns — it must never abort the
    caller's DB transaction.

    Pods that miss the pub/sub message converge on the next poll cycle.
    """
    try:
        redis = get_redis_pool()
        await redis.incr(_MODELS_VERSION_KEY)
        await redis.publish(_MODELS_CHANGED_CHANNEL, "1")
    except Exception as exc:
        logger.warning(
            "publish_models_changed: Redis operation failed (%s); "
            "pods will converge via poll",
            exc,
        )


# ---------------------------------------------------------------------------
# Service class
# ---------------------------------------------------------------------------


class DatabaseBackendSource:
    """
    Reconciles the backend registry from the model_backends DB table.

    Mirrors the structure of KubernetesServiceDiscovery:
      - ``_managed``       dict[model_id, row_version] — backends owned by this source
      - ``_task``          asyncio.Task for the poll loop
      - ``_pubsub_task``   asyncio.Task for the pub/sub loop (best-effort)
      - ``_stop_event``    asyncio.Event to signal both loops to exit
      - ``_last_models_version``  last Redis counter value seen by the poll loop
    """

    def __init__(self) -> None:
        self._managed: Dict[str, int] = {}          # model_id -> row version
        self._task: Optional[asyncio.Task] = None
        self._pubsub_task: Optional[asyncio.Task] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._last_models_version: int = -1          # sentinel: force first reconcile

    # ------------------------------------------------------------------ #
    # Reconciliation
    # ------------------------------------------------------------------ #

    async def _fetch_rows(self) -> List[ModelBackend]:
        """Fetch all static ModelBackend rows from the DB. Isolated for testability."""
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(ModelBackend).where(
                    ModelBackend.backend_type == "static"
                )
            )
            return result.scalars().all()

    async def reconcile(self) -> None:
        """
        Read all enabled static backends from the DB and sync the registry.

        Ownership rules:
          - New model_id already in registry → skipped (higher-priority source owns it).
          - Managed model_id with changed version → backend rebuilt and upserted (or
            unregistered if now disabled).
          - Managed model_id no longer in DB → unregistered.
        """
        rows = await self._fetch_rows()

        # Build desired map: model_id -> row
        desired: Dict[str, ModelBackend] = {row.model_id: row for row in rows}

        # --- Additions / updates ---
        for mid, row in desired.items():
            if mid in self._managed:
                # We own this model_id. Check for version bump.
                if row.version == self._managed[mid]:
                    # No change — skip.
                    continue
                # Version changed: rebuild backend.
                if row.is_enabled:
                    backend = _build_backend(row)
                    if backend is not None:
                        registry.upsert(backend)
                        self._managed[mid] = row.version
                        logger.info(
                            "DatabaseBackendSource: updated %r -> %s (version=%d)",
                            mid,
                            row.base_url,
                            row.version,
                        )
                else:
                    # Disabled — remove from registry and stop managing.
                    registry.unregister(mid)
                    del self._managed[mid]
                    logger.info(
                        "DatabaseBackendSource: unregistered %r (is_enabled=False)",
                        mid,
                    )
            else:
                # Not yet managed by us.
                if registry.contains(mid):
                    # A higher-priority source (YAML/k8s) already owns this model_id.
                    logger.warning(
                        "DatabaseBackendSource: model_id %r already registered by a "
                        "higher-priority source; skipping DB row",
                        mid,
                    )
                    continue
                if not row.is_enabled:
                    # Disabled and not yet registered — nothing to do.
                    continue
                backend = _build_backend(row)
                if backend is not None:
                    registry.upsert(backend)
                    self._managed[mid] = row.version
                    logger.info(
                        "DatabaseBackendSource: registered %r -> %s",
                        mid,
                        row.base_url,
                    )

        # --- Removals: rows deleted from DB ---
        for mid in list(self._managed.keys()):
            if mid not in desired:
                registry.unregister(mid)
                del self._managed[mid]
                logger.info(
                    "DatabaseBackendSource: unregistered %r (row deleted from DB)",
                    mid,
                )

        logger.info(
            "DatabaseBackendSource reconcile complete: %d managed backend(s)",
            len(self._managed),
        )

    # ------------------------------------------------------------------ #
    # Poll loop
    # ------------------------------------------------------------------ #

    async def _poll_loop(self) -> None:
        """
        Periodically check the Redis version counter and reconcile on change.

        If Redis is unreachable the poll still triggers a DB reconcile so that
        changes propagate even during a Redis outage.
        """
        assert self._stop_event is not None
        settings = get_settings()
        interval = settings.DB_BACKEND_POLL_INTERVAL_SECONDS

        while not self._stop_event.is_set():
            try:
                current_version: int = -1
                redis_failed = False
                try:
                    redis = get_redis_pool()
                    raw = await redis.get(_MODELS_VERSION_KEY)
                    current_version = int(raw) if raw is not None else 0
                except Exception as redis_exc:
                    logger.warning(
                        "DatabaseBackendSource: Redis GET failed (%s); "
                        "running reconcile anyway",
                        redis_exc,
                    )
                    redis_failed = True

                if redis_failed or current_version != self._last_models_version:
                    await self.reconcile()
                    if not redis_failed:
                        self._last_models_version = current_version

            except Exception:
                logger.exception(
                    "DatabaseBackendSource: poll loop iteration failed; "
                    "will retry after sleep"
                )

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass

    # ------------------------------------------------------------------ #
    # Pub/sub loop
    # ------------------------------------------------------------------ #

    async def _pubsub_loop(self) -> None:
        """
        Subscribe to llmr:models_changed and reconcile on receipt.

        This is the fast-path: when Phase 3 publishes after a write, all pods
        reconcile within < 1 s.  The poll loop is the safety net.

        If Redis is unavailable at entry we log a warning and exit immediately
        (the poll loop remains the sole propagation mechanism in that case).
        """
        assert self._stop_event is not None

        try:
            redis = get_redis_pool()
        except Exception as exc:
            logger.warning(
                "DatabaseBackendSource: cannot get Redis pool for pub/sub (%s); "
                "falling back to poll-only mode",
                exc,
            )
            return

        pubsub = redis.pubsub()
        try:
            await pubsub.subscribe(_MODELS_CHANGED_CHANNEL)
        except Exception as exc:
            logger.warning(
                "DatabaseBackendSource: failed to subscribe to %r (%s); "
                "falling back to poll-only mode",
                _MODELS_CHANGED_CHANNEL,
                exc,
            )
            await pubsub.close()
            return

        logger.debug(
            "DatabaseBackendSource: subscribed to %r", _MODELS_CHANGED_CHANNEL
        )

        _backoff_base = 1.0   # seconds; doubles each consecutive error
        _backoff_cap  = 30.0  # seconds max
        _consec_errors = 0

        try:
            while not self._stop_event.is_set():
                try:
                    message = await asyncio.wait_for(
                        pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0),
                        timeout=2.0,
                    )
                except asyncio.TimeoutError:
                    # Normal "nothing arrived yet" — reset error counter.
                    _consec_errors = 0
                    continue
                except Exception as exc:
                    _consec_errors += 1
                    backoff = min(_backoff_base * (2 ** (_consec_errors - 1)), _backoff_cap)
                    logger.warning(
                        "DatabaseBackendSource: pub/sub receive error (%s); "
                        "backing off %.1fs (consecutive_errors=%d)",
                        exc,
                        backoff,
                        _consec_errors,
                    )
                    try:
                        await asyncio.wait_for(
                            self._stop_event.wait(), timeout=backoff
                        )
                    except asyncio.TimeoutError:
                        pass
                    continue

                # Successful receive (even if message is None) — reset error counter.
                _consec_errors = 0

                if message is None:
                    continue

                if message.get("type") == "message":
                    logger.debug(
                        "DatabaseBackendSource: received %r signal; reconciling",
                        _MODELS_CHANGED_CHANNEL,
                    )
                    try:
                        await self.reconcile()
                        # Sync the poll loop's version counter so it doesn't
                        # immediately reconcile again on the next tick.
                        try:
                            redis2 = get_redis_pool()
                            raw = await redis2.get(_MODELS_VERSION_KEY)
                            if raw is not None:
                                self._last_models_version = int(raw)
                        except Exception:
                            pass  # Non-critical; poll loop handles it
                    except Exception:
                        logger.exception(
                            "DatabaseBackendSource: reconcile triggered by pub/sub failed"
                        )
        finally:
            try:
                await pubsub.unsubscribe(_MODELS_CHANGED_CHANNEL)
            except Exception:
                pass
            try:
                await pubsub.close()
            except Exception:
                pass

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        """
        Run an initial reconcile, then start the background loops.

        Ordering mirrors KubernetesServiceDiscovery.start():
          1. Create stop event.
          2. Run initial reconcile (errors are non-fatal — loop retries).
          3. Spawn poll loop (always).
          4. Spawn pub/sub loop (best-effort: skipped if Redis unavailable).
        """
        if self._task is not None:
            logger.warning("DatabaseBackendSource already started")
            return

        self._stop_event = asyncio.Event()
        self._last_models_version = -1  # force reconcile on first poll tick

        try:
            await self.reconcile()
        except Exception:
            logger.exception(
                "DatabaseBackendSource: initial reconcile failed; loop will retry"
            )

        self._task = asyncio.create_task(self._poll_loop())

        # Best-effort pub/sub: Redis may not be reachable in all environments.
        # create_task() itself cannot raise; failures are handled inside the loop.
        self._pubsub_task = asyncio.create_task(self._pubsub_loop())

        logger.info(
            "DatabaseBackendSource started (poll_interval=%ds)",
            get_settings().DB_BACKEND_POLL_INTERVAL_SECONDS,
        )

    async def stop(self) -> None:
        """Signal loops to stop, cancel tasks, and clean up."""
        if self._stop_event is not None:
            self._stop_event.set()

        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception(
                    "DatabaseBackendSource: error awaiting poll loop task during stop"
                )
            self._task = None

        if self._pubsub_task is not None:
            self._pubsub_task.cancel()
            try:
                await self._pubsub_task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception(
                    "DatabaseBackendSource: error awaiting pub/sub task during stop"
                )
            self._pubsub_task = None

        # Reset state so a stop-then-start on the same instance performs a clean
        # full reconcile rather than skipping rows whose version matches stale
        # _managed entries.
        self._managed.clear()
        self._last_models_version = -1

        logger.info("DatabaseBackendSource stopped")


# ---------------------------------------------------------------------------
# Module-level singleton — started/stopped from the app lifespan.
# ---------------------------------------------------------------------------

database_backend_source = DatabaseBackendSource()
