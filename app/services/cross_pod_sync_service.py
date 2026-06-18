"""
Background cross-pod synchronization service.

This service provides unified cluster configuration management across pods:
1. One-time YAML-to-DB sync on startup (initialize_from_yaml)
2. Continuous Redis polling for runtime updates (start_sync/stop_sync)

It ensures all pods in a deployment have consistent:
- Infrastructure configuration (base_url, replicas, api_key)
- Operational state (is_enabled, auto_adapt_capacity)

Features:
- Syncs YAML configurations to database on startup
- Polls Redis for config timestamp changes (every 5 seconds by default)
- Polls Redis for operational state changes (is_enabled, auto_adapt_capacity)
- Updates local endpoints when configuration or state changes
- Supports pub/sub for real-time notifications (optional)
- Graceful degradation when Redis is unavailable

Usage:
    # Initialize from YAML (one-time, call before start_sync)
    await cross_pod_sync_service.initialize_from_yaml()

    # Start the sync service (continuous polling)
    await cross_pod_sync_service.start_sync()

    # Stop the sync service
    await cross_pod_sync_service.stop_sync()
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.cluster_config import (
    ClusterConfig,
    get_all_cluster_configs_for_model,
    get_cluster_config,
)
from app.db.cluster_operational_state import get_all_cluster_states
from app.db.mysql import ModelCluster, ModelClusterAuditLog, AsyncSessionLocal
from app.backends.multi_cluster import ClusterEndpoint, MultiClusterBackend
from app.backends.base import registry as backend_registry
from app.core.config import get_settings

logger = logging.getLogger(__name__)


@dataclass
class ClusterDiff:
    """Result of comparing YAML vs DB cluster configurations."""

    added: List[Tuple[str, str]] = field(
        default_factory=list
    )  # (model_id, cluster_id) to add
    removed: List[Tuple[str, str]] = field(
        default_factory=list
    )  # (model_id, cluster_id) to remove
    modified: List[Tuple[str, str]] = field(
        default_factory=list
    )  # (model_id, cluster_id) with base_url change
    unchanged: List[Tuple[str, str]] = field(
        default_factory=list
    )  # (model_id, cluster_id) no changes needed

    def has_changes(self) -> bool:
        """Check if any changes are needed."""
        return bool(self.added or self.removed or self.modified)

    def summary(self) -> Dict[str, int]:
        """Return a summary of changes."""
        return {
            "added": len(self.added),
            "removed": len(self.removed),
            "modified": len(self.modified),
            "unchanged": len(self.unchanged),
        }


class CrossPodSyncService:
    """
    Background service for synchronizing cluster configuration and state across pods.

    This service periodically polls Redis for:
    1. Infrastructure configuration changes (base_url, replicas, api_key)
    2. Operational state changes (is_enabled, auto_adapt_capacity)

    It updates local ClusterEndpoint instances to maintain consistency across
    all pods in a deployment.
    """

    def __init__(
        self,
        poll_interval_seconds: float = 5.0,
        enable_pubsub: bool = False,
    ) -> None:
        """
        Initialize the cross-pod sync service.

        Args:
            poll_interval_seconds: How often to poll for changes (default 5s)
            enable_pubsub: Whether to use pub/sub for real-time updates (future)
        """
        self._poll_interval = poll_interval_seconds
        self._enable_pubsub = enable_pubsub
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._timestamp_cache: dict[str, datetime] = {}
        self._update_callbacks: list[Callable[[str, str, ClusterConfig], None]] = []
        self._last_db_load_time: Optional[datetime] = (
            None  # Track when pod loaded from DB
        )
        self._db_sync_completed = False  # Track if initial DB sync done

    # ----------------------------------------------------------------------- #
    # Lifecycle
    # ----------------------------------------------------------------------- #

    async def start_sync(self) -> None:
        """Start the background cross-pod synchronization task."""
        if self._task is not None:
            logger.warning("Cross-pod sync service is already running")
            return

        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_sync_loop())
        logger.info(
            "Cross-pod sync service started (poll_interval=%.1fs, pubsub=%s)",
            self._poll_interval,
            self._enable_pubsub,
        )

    async def stop_sync(self) -> None:
        """Stop the background cross-pod synchronization task."""
        if self._task is None:
            logger.warning("Cross-pod sync service is not running")
            return

        logger.info("Stopping cross-pod sync service...")
        self._stop_event.set()

        try:
            # Wait for the task to complete with timeout
            await asyncio.wait_for(self._task, timeout=10.0)
        except asyncio.TimeoutError:
            logger.warning("Cross-pod sync service did not stop gracefully, cancelling...")
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        except asyncio.CancelledError:
            pass
        finally:
            self._task = None
            logger.info("Cross-pod sync service stopped")

    def is_running(self) -> bool:
        """Check if the sync service is currently running."""
        return self._task is not None and not self._task.done()

    def is_db_sync_completed(self) -> bool:
        """Check if the initial YAML-to-DB sync has been completed."""
        return self._db_sync_completed

    def set_db_load_time(self, load_time: datetime) -> None:
        """
        Set the timestamp when this pod loaded cluster configuration from DB.

        This is used to filter Redis sync - only changes made after this time
        will be applied to prevent stale data from overriding fresh DB state.
        """
        self._last_db_load_time = load_time
        logger.info("Config sync DB load time set to: %s", load_time.isoformat())

    # ----------------------------------------------------------------------- #
    # Callbacks
    # ----------------------------------------------------------------------- #

    def add_update_callback(
        self, callback: Callable[[str, str, ClusterConfig], None]
    ) -> None:
        """
        Add a callback to be invoked when a config is updated.

        Args:
            callback: Function(model_id, cluster_id, config) called on update
        """
        self._update_callbacks.append(callback)

    def remove_update_callback(
        self, callback: Callable[[str, str, ClusterConfig], None]
    ) -> None:
        """Remove a previously registered update callback."""
        if callback in self._update_callbacks:
            self._update_callbacks.remove(callback)

    def _notify_update(
        self, model_id: str, cluster_id: str, config: ClusterConfig
    ) -> None:
        """Notify all registered callbacks of a config update."""
        for callback in self._update_callbacks:
            try:
                callback(model_id, cluster_id, config)
            except Exception as e:
                logger.error("Error in config update callback: %s", e)

    # ----------------------------------------------------------------------- #
    # Sync Loop
    # ----------------------------------------------------------------------- #

    async def _run_sync_loop(self) -> None:
        """Main synchronization loop."""
        logger.debug("Config sync loop started")

        while not self._stop_event.is_set():
            try:
                await self._poll_for_changes()
            except Exception as e:
                logger.error("Error during config sync poll: %s", e)

            # Wait for next poll interval or until stopped
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._poll_interval
                )
            except asyncio.TimeoutError:
                # Normal timeout, continue to next poll
                pass
            except asyncio.CancelledError:
                # Task was cancelled
                break

        logger.debug("Config sync loop ended")

    async def _poll_for_changes(self) -> None:
        """
        Poll Redis for configuration and operational state changes.

        Checks all registered models and their clusters for:
        1. Infrastructure config changes (base_url, replicas, etc.)
        2. Operational state changes (is_enabled, auto_adapt_capacity)

        Only applies config updates that are newer than the last YAML config load time.
        """
        # Get all registered models
        model_ids = backend_registry.list_model_ids()

        for model_id in model_ids:
            backend = backend_registry.get(model_id)
            if not isinstance(backend, MultiClusterBackend):
                continue

            # Sync infrastructure configuration
            await self._check_backend_configs(backend)
            # Sync operational state (is_enabled, auto_adapt_capacity)
            await self._sync_operational_state(backend)

    async def _check_backend_configs(self, backend: MultiClusterBackend) -> None:
        """
        Check for config changes for a specific backend.

        Args:
            backend: The MultiClusterBackend to check
        """
        model_id = backend.model_id

        # Get all cluster configs from Redis for this model
        try:
            remote_configs = await get_all_cluster_configs_for_model(model_id)
        except Exception as e:
            logger.warning("Failed to get remote configs for model %s: %s", model_id, e)
            return

        # Build map of remote configs
        remote_config_map = {c.cluster_id: c for c in remote_configs}

        # Check each cluster in the backend
        for cluster_id, endpoint in backend._clusters.items():
            remote_config = remote_config_map.get(cluster_id)

            if remote_config is None:
                continue

            # Apply the update
            await self._apply_config_update(
                model_id, cluster_id, endpoint, remote_config
            )

            # Notify callbacks
            self._notify_update(model_id, cluster_id, remote_config)

        # Check for new clusters in Redis that aren't in the backend
        local_cluster_ids = set(backend._clusters.keys())
        for remote_config in remote_configs:
            if remote_config.cluster_id not in local_cluster_ids:

                logger.info(
                    "New cluster discovered in Redis: model=%s cluster=%s",
                    model_id,
                    remote_config.cluster_id,
                )

                # Add the new cluster to the backend
                await self._add_new_cluster(backend, remote_config)

    async def _apply_config_update(
        self,
        model_id: str,
        cluster_id: str,
        endpoint: ClusterEndpoint,
        config: ClusterConfig,
    ) -> None:
        """
        Apply a configuration update to a local endpoint with two-step staleness check.

        TIMESTAMP CHECK ORDERING (CRITICAL):
        ====================================

        This method implements a two-step timestamp comparison to prevent stale
        Redis configs from overriding fresh runtime updates:

        Step 1: Compare Redis config timestamp vs backend's endpoint.updated_at
          - If config.updated_at > endpoint.updated_at: Apply immediately (Redis is newer)
          - If config.updated_at <= endpoint.updated_at: Proceed to Step 2

        Step 2: Compare Redis config vs DB's config_updated_at (only if Step 1 fails)
          - If config.updated_at > DB.config_updated_at: Apply (Redis newer than DB)
          - If config.updated_at <= DB.config_updated_at: PURGE Redis (stale cache)

        WHY THIS ORDERING MATTERS:
        ==========================

        Scenario: Backend updated via API after YAML was loaded into DB

        Timeline:
          T0: YAML loaded into DB (DB.config_updated_at = T0)
          T1: API updates backend (endpoint.updated_at = T1, where T1 > T0)
          T2: Redis config arrives (config.updated_at = T2, where T0 < T2 < T1)

        With OLD ordering (check DB first):
          - T2 > T0, so we'd apply Redis config
          - But T2 < T1, so we'd override API update with stale data! ❌

        With NEW ordering (check backend first):
          - T2 <= T1, so we check DB
          - T2 > T0, so we apply Redis config (correctly)

        Scenario: Truly stale Redis config
          - T2 < T1 (Redis older than backend) AND T2 < T0 (Redis older than DB)
          - We correctly purge the stale Redis entry ✅

        PURGING STALE CONFIGS:
        ======================

        When Redis data is determined to be stale (older than BOTH backend AND DB), we skip.

        Args:
            model_id: The model identifier
            cluster_id: The cluster identifier
            endpoint: The local ClusterEndpoint to update (has updated_at timestamp)
            config: The new configuration from Redis (has updated_at timestamp)
        """
        # STEP 1: Compare Redis config with backend's endpoint.updated_at
        # This is the PRIMARY check - if Redis is newer than the running backend,
        # we apply it immediately without checking the database.
        if endpoint.updated_at and hasattr(config, "updated_at") and config.updated_at:
            if config.updated_at > endpoint.updated_at:
                # Redis is newer than backend - apply it immediately
                # This handles the normal case where another pod updated the config
                logger.debug(
                    "Redis config newer than backend, applying update: model=%s cluster=%s "
                    "(config_time=%s > backend_time=%s)",
                    model_id,
                    cluster_id,
                    config.updated_at.isoformat(),
                    endpoint.updated_at.isoformat(),
                )
                # Continue to apply the update below
            else:
                # Redis is older than backend - proceed to Step 2
                # This can happen if the backend was updated via API after YAML load
                logger.debug(
                    "Redis config older than backend, checking against DB: model=%s cluster=%s "
                    "(config_time=%s <= backend_time=%s)",
                    model_id,
                    cluster_id,
                    config.updated_at.isoformat(),
                    endpoint.updated_at.isoformat(),
                )

                # STEP 2: Compare against DB config_updated_at
                # Only executed when Redis is older than backend. This check determines
                # if the Redis data is truly stale or if the DB just has older YAML data.
                try:
                    async with AsyncSessionLocal() as db:
                        result = await db.execute(
                            select(ModelCluster).where(
                                ModelCluster.model_id == model_id,
                                ModelCluster.cluster_id == cluster_id,
                            )
                        )
                        db_cluster = result.scalar_one_or_none()

                        if db_cluster and db_cluster.config_updated_at:
                            if config.updated_at <= db_cluster.config_updated_at:
                                # Redis is older than BOTH backend AND DB - PURGE IT
                                # This is truly stale data that should not be used by any pod
                                logger.debug(
                                    "Ignoring Redis config older than DB: model=%s cluster=%s "
                                    "(config_time=%s <= db_time=%s)",
                                    model_id,
                                    cluster_id,
                                    config.updated_at.isoformat(),
                                    db_cluster.config_updated_at.isoformat(),
                                )
                                return
                            else:
                                # Redis is newer than DB but older than backend
                                # Edge case: API update happened after YAML load but before Redis sync
                                # Apply the Redis config as it's fresher than the DB
                                logger.debug(
                                    "Redis config newer than DB but older than backend, applying: model=%s cluster=%s "
                                    "(config_time=%s > db_time=%s, config_time=%s <= backend_time=%s)",
                                    model_id,
                                    cluster_id,
                                    config.updated_at.isoformat(),
                                    db_cluster.config_updated_at.isoformat(),
                                    config.updated_at.isoformat(),
                                    endpoint.updated_at.isoformat(),
                                )
                                # Continue to apply the update below
                except Exception as e:
                    # DB check failed - be permissive and allow the update rather than
                    # blocking valid configuration changes due to transient DB issues
                    logger.warning("Could not check config timestamp against DB: %s", e)

        try:
            # Update endpoint properties
            old_url = endpoint.base_url
            old_replicas = endpoint.number_of_replicas
            old_api_key = endpoint.api_key

            endpoint.base_url = config.base_url.rstrip("/")
            endpoint.api_key = config.api_key or ""
            endpoint.number_of_replicas = config.number_of_replicas
            endpoint.seed_capacity_tokens_per_pod = (
                40000  # Default, could be added to config
            )
            # Note: is_enabled and auto_adapt_capacity are operational state,
            # managed separately via cluster_operational_state.py, not infrastructure config

            # Update the last update timestamp to track when config was applied
            endpoint.updated_at = datetime.utcnow()

            # Log significant changes
            changes = []
            if old_url != endpoint.base_url:
                changes.append(f"base_url={old_url}->{endpoint.base_url}")
            if old_replicas != endpoint.number_of_replicas:
                changes.append(
                    f"replicas={old_replicas}->{endpoint.number_of_replicas}"
                )
            if old_api_key != endpoint.api_key:
                changes.append("api_key=***updated***")

            if changes:
                logger.info(
                    "Applied config update to %s/%s: %s",
                    model_id,
                    cluster_id,
                    ", ".join(changes),
                )
            else:
                logger.debug(
                    "Config update applied (no significant changes): %s/%s",
                    model_id,
                    cluster_id,
                )

        except Exception as e:
            logger.error(
                "Failed to apply config update for %s/%s: %s",
                model_id,
                cluster_id,
                e,
            )

    async def _add_new_cluster(
        self,
        backend: MultiClusterBackend,
        config: ClusterConfig,
    ) -> None:
        """
        Add a new cluster discovered from Redis.

        Args:
            backend: The MultiClusterBackend to add the cluster to
            config: The cluster configuration from Redis
        """
        try:
            # Note: is_enabled and auto_adapt_capacity are operational state,
            # managed separately via cluster_operational_state.py
            # New clusters start with defaults (enabled=True, auto_adapt=True)
            new_endpoint = ClusterEndpoint(
                cluster_id=config.cluster_id,
                base_url=config.base_url,
                api_key=config.api_key or "",
                model_id=config.model_id,
                number_of_replicas=config.number_of_replicas,
                seed_capacity_tokens_per_pod=40000,
                is_enabled=True,  # Operational state default
                auto_adapt_capacity=True,  # Operational state default
                updated_at=datetime.utcnow(),  # Initialize with current time
            )

            backend.enable_cluster(new_endpoint)
            logger.info(
                "Added new cluster %s/%s from Redis config",
                config.model_id,
                config.cluster_id,
            )

        except Exception as e:
            logger.error(
                "Failed to add new cluster %s/%s: %s",
                config.model_id,
                config.cluster_id,
                e,
            )

    async def _sync_operational_state(self, backend: MultiClusterBackend) -> None:
        """
        Sync operational state (is_enabled, auto_adapt_capacity) from Redis.

        This method polls the cluster_state:* keys from Redis and updates
        local ClusterEndpoint instances to ensure all pods have consistent
        operational state for routing decisions.

        Args:
            backend: The MultiClusterBackend to sync operational state for
        """
        model_id = backend.model_id

        try:
            # Get all cluster states from Redis for this model
            states = await get_all_cluster_states(model_id)

            if not states:
                # No state in Redis yet, nothing to sync
                return

            # Update each cluster's operational state
            for cluster_id, state in states.items():
                endpoint = backend.get_cluster(cluster_id)
                if endpoint is None:
                    # Cluster exists in Redis state but not in local runtime
                    # This can happen during startup or if cluster was added elsewhere
                    continue

                # Get the new values with defaults
                new_is_enabled = state.get("is_enabled", True)
                new_auto_adapt = state.get("auto_adapt_capacity", True)

                # Only update if there's a change (to avoid unnecessary logging)
                if (
                    endpoint.is_enabled != new_is_enabled
                    or endpoint.auto_adapt_capacity != new_auto_adapt
                ):  
                    async with endpoint.update_lock:
                        old_enabled = endpoint.is_enabled
                        old_auto_adapt = endpoint.auto_adapt_capacity

                        endpoint.is_enabled = new_is_enabled
                        endpoint.auto_adapt_capacity = new_auto_adapt

                    logger.info(
                        "Synced operational state for %s/%s: is_enabled=%s->%s, "
                        "auto_adapt_capacity=%s->%s",
                        model_id,
                        cluster_id,
                        old_enabled,
                        new_is_enabled,
                        old_auto_adapt,
                        new_auto_adapt,
                    )

        except Exception as e:
            logger.warning(
                "Failed to sync operational state for model %s: %s",
                model_id,
                e,
            )

    # ----------------------------------------------------------------------- #
    # Manual Operations
    # ----------------------------------------------------------------------- #

    async def force_sync_cluster(
        self, model_id: str, cluster_id: str
    ) -> Optional[ClusterConfig]:
        """
        Force an immediate sync for a specific cluster.

        Args:
            model_id: The model identifier
            cluster_id: The cluster identifier

        Returns:
            The updated ClusterConfig, or None if not found
        """
        backend = backend_registry.get(model_id)
        if not isinstance(backend, MultiClusterBackend):
            logger.warning(
                "Cannot sync: backend for %s is not MultiClusterBackend", model_id
            )
            return None

        endpoint = backend.get_cluster(cluster_id)
        if endpoint is None:
            logger.warning(
                "Cannot sync: cluster %s not found in backend for %s",
                cluster_id,
                model_id,
            )
            return None

        config = await get_cluster_config(model_id, cluster_id)
        if config is None:
            logger.warning(
                "Cannot sync: no config found in Redis for %s/%s",
                model_id,
                cluster_id,
            )
            return None

        cache_key = f"{model_id}:{cluster_id}"

        await self._apply_config_update(model_id, cluster_id, endpoint, config)
        self._timestamp_cache[cache_key] = config.updated_at
        self._notify_update(model_id, cluster_id, config)

        return config

    async def get_cached_timestamp(
        self, model_id: str, cluster_id: str
    ) -> Optional[datetime]:
        """
        Get the cached timestamp for a cluster.

        Args:
            model_id: The model identifier
            cluster_id: The cluster identifier

        Returns:
            The cached timestamp, or None if not cached
        """
        cache_key = f"{model_id}:{cluster_id}"
        return self._timestamp_cache.get(cache_key)

    def clear_cache(self) -> None:
        """Clear the timestamp cache (useful for testing)."""
        self._timestamp_cache.clear()
        logger.debug("Config sync timestamp cache cleared")

    # ----------------------------------------------------------------------- #
    # YAML-to-DB Initialization (One-time sync on startup)
    # ----------------------------------------------------------------------- #

    async def initialize_from_yaml(self) -> ClusterDiff:
        """
        One-time initialization: Sync YAML to DB on startup.

        Compares YAML configs with DB state and reconciles:
        - Adds new clusters from YAML
        - Removes clusters not in YAML
        - Updates config while preserving is_enabled

        This runs ONCE at startup before polling begins. Sets _db_sync_completed
        flag on success to prevent duplicate runs.

        Audit Logging:
            All changes (create, update, delete) are logged to the audit table.
            Changes are attributed to the superuser configured via the
            SUPERUSER_USERNAME environment variable (defaults to "admin").
            This ensures consistent audit attribution for all YAML-initiated changes.

        Returns:
            ClusterDiff indicating what changes were made
        """
        if self._db_sync_completed:
            logger.warning("YAML-to-DB sync already completed, skipping")
            return ClusterDiff()

        settings = get_settings()
        yaml_configs = await settings.load_model_configs()

        # Convert to lookup dict keyed by (model_id, cluster_id)
        yaml_lookup: Dict[Tuple[str, str], Any] = {}
        for model_cfg in yaml_configs:
            if hasattr(model_cfg, "clusters"):
                for cluster_cfg in model_cfg.clusters:
                    yaml_lookup[(model_cfg.model_id, cluster_cfg.cluster_id)] = (
                        cluster_cfg
                    )

        async with AsyncSessionLocal() as db:
            try:
                # Get superuser ID for audit logging
                # Uses the configured superuser (SUPERUSER_USERNAME env var) rather than
                # any admin user to ensure consistent audit attribution for YAML-initiated changes.
                # This ensures that all cluster changes made during YAML initialization are
                # attributed to the designated superuser account.
                from app.db.mysql import User

                result = await db.execute(
                    select(User).where(User.username == settings.SUPERUSER_USERNAME)
                )
                admin_user = result.scalar_one_or_none()
                updated_by = admin_user.id if admin_user else None

                # Perform the sync
                diff = await self._atomic_yaml_sync(db, yaml_lookup, updated_by)

                if diff.has_changes():
                    logger.info(
                        "Cluster configuration synchronized from YAML: "
                        "added=%d, removed=%d, modified=%d, unchanged=%d",
                        len(diff.added),
                        len(diff.removed),
                        len(diff.modified),
                        len(diff.unchanged),
                    )
                    for model_id, cluster_id in diff.added:
                        logger.info("  + Added cluster: %s/%s", model_id, cluster_id)
                    for model_id, cluster_id in diff.removed:
                        logger.info("  - Removed cluster: %s/%s", model_id, cluster_id)
                    for model_id, cluster_id in diff.modified:
                        logger.info("  ~ Modified cluster: %s/%s", model_id, cluster_id)
                else:
                    logger.info("Cluster configuration matches YAML, using DB state")

                await db.commit()

                # Mark sync as completed
                self._db_sync_completed = True
                logger.debug("YAML-to-DB sync completed successfully")

                return diff

            except Exception as e:
                await db.rollback()
                logger.error("Failed to synchronize clusters from YAML: %s", e)
                # Don't mark as completed on failure - allow retry
                raise

    async def _atomic_yaml_sync(
        self,
        db: AsyncSession,
        yaml_configs: Dict[Tuple[str, str], Any],
        updated_by: Optional[int] = None,
    ) -> ClusterDiff:
        """
        Perform atomic cluster synchronization from YAML to database.

        This is the internal implementation that works within an existing
        database session/transaction.

        Args:
            db: Database session (must be in a transaction)
            yaml_configs: Dict of YAML configurations keyed by (model_id, cluster_id)
            updated_by: User ID for audit logging

        Returns:
            ClusterDiff indicating what changes were made
        """
        if not yaml_configs:
            logger.warning("No YAML configurations provided for sync")
            return ClusterDiff()

        # Extract all model_ids from yaml_configs
        model_ids = set(model_id for model_id, _ in yaml_configs.keys())

        # Load all clusters from DB for the models in YAML
        result = await db.execute(
            select(ModelCluster).where(ModelCluster.model_id.in_(model_ids))
        )
        db_clusters = list(result.scalars().all())

        # Build DB lookup
        db_lookup: Dict[Tuple[str, str], ModelCluster] = {}
        for cluster in db_clusters:
            key = (cluster.model_id, cluster.cluster_id)
            db_lookup[key] = cluster

        # Compare and get diff
        diff = self._compare_yaml_to_db(yaml_configs, db_lookup)

        # Apply changes if any
        if diff.has_changes() or diff.unchanged:
            await self._sync_yaml_to_db(
                db=db,
                diff=diff,
                yaml_configs=yaml_configs,
                db_lookup=db_lookup,
                updated_by=updated_by,
            )

        return diff

    def _compare_yaml_to_db(
        self,
        yaml_configs: Dict[Tuple[str, str], Any],
        db_lookup: Dict[Tuple[str, str], ModelCluster],
    ) -> ClusterDiff:
        """
        Compare YAML configurations against database state.

        Uses (model_id, cluster_id) as the unique identifier. Clusters are categorized
        based on their presence in YAML vs DB and whether their base_url has changed.

        Args:
            yaml_configs: Dict with (model_id, cluster_id) keys from YAML
            db_clusters: List of ModelCluster objects from DB

        Returns:
            ClusterDiff indicating what changes are needed
        """
        diff = ClusterDiff()

        # Identify added clusters (in YAML, not in DB)
        yaml_keys = set(yaml_configs.keys())
        db_keys = set(db_lookup.keys())

        for key in yaml_keys - db_keys:
            diff.added.append(key)

        # Identify removed clusters (in DB, not in YAML)
        for key in db_keys - yaml_keys:
            diff.removed.append(key)

        # Check for modified and unchanged clusters (in both)
        for key in yaml_keys & db_keys:
            yaml_config = yaml_configs[key]
            db_cluster = db_lookup[key]

            # base_url is treated as immutable - if it changes, cluster is "modified"
            yaml_base_url = getattr(yaml_config, "base_url", None)
            db_base_url = db_cluster.base_url

            if yaml_base_url != db_base_url:
                diff.modified.append(key)
            else:
                diff.unchanged.append(key)

        return diff

    async def _sync_yaml_to_db(
        self,
        db: AsyncSession,
        diff: ClusterDiff,
        yaml_configs: Dict[Tuple[str, str], Any],
        db_lookup: Dict[Tuple[str, str], ModelCluster],
        updated_by: Optional[int] = None,
    ) -> None:
        """
        Synchronize database to match YAML while preserving is_enabled state.

        Strategy:
        1. ADDED: Insert new clusters from YAML with is_enabled=True (default)
        2. REMOVED: Delete clusters from DB (with audit logging)
        3. MODIFIED: Update base_url and other mutable fields, preserve is_enabled

        Args:
            db: Database session (transaction already started)
            diff: ClusterDiff indicating what changes to apply
            yaml_configs: Dict of YAML configurations keyed by (model_id, cluster_id)
            db_lookup: Dict of existing DB clusters keyed by (model_id, cluster_id)
            updated_by: User ID for audit logging
        """
        # Process additions
        for model_id, cluster_id in diff.added:
            yaml_config = yaml_configs[(model_id, cluster_id)]
            await self._create_cluster_from_yaml(
                db=db,
                model_id=model_id,
                cluster_id=cluster_id,
                yaml_config=yaml_config,
                updated_by=updated_by,
            )
            logger.debug("Created cluster from YAML: %s/%s", model_id, cluster_id)

        # Process removals
        for model_id, cluster_id in diff.removed:
            cluster = db_lookup[(model_id, cluster_id)]
            await self._delete_cluster(db=db, cluster=cluster, updated_by=updated_by)
            logger.debug("Deleted cluster (not in YAML): %s/%s", model_id, cluster_id)

        # Process modifications (base_url changed)
        for model_id, cluster_id in diff.modified:
            yaml_config = yaml_configs[(model_id, cluster_id)]
            cluster = db_lookup[(model_id, cluster_id)]
            await self._update_cluster_from_yaml(
                db=db,
                cluster=cluster,
                yaml_config=yaml_config,
                preserve_is_enabled=True,
                updated_by=updated_by,
            )
            logger.debug(
                "Modified cluster (base_url changed): %s/%s", model_id, cluster_id
            )

        return diff

    async def _create_cluster_from_yaml(
        self,
        db: AsyncSession,
        model_id: str,
        cluster_id: str,
        yaml_config: Any,
        updated_by: Optional[int] = None,
    ) -> ModelCluster:
        """
        Create a new cluster from YAML configuration.

        New clusters are enabled by default (is_enabled=True).

        Args:
            db: Database session
            model_id: The model identifier
            cluster_id: The cluster identifier
            yaml_config: YAML cluster configuration object
            updated_by: User ID performing the creation

        Returns:
            Created ModelCluster object
        """
        cluster = ModelCluster(
            model_id=model_id,
            cluster_id=cluster_id,
            base_url=getattr(yaml_config, "base_url", None),
            api_key=getattr(yaml_config, "api_key", None),
            number_of_replicas=getattr(yaml_config, "number_of_replicas", 1),
            seed_capacity_tokens_per_pod=getattr(
                yaml_config, "seed_capacity_tokens_per_pod", 40000
            ),
            is_enabled=True,  # New clusters are enabled by default
            auto_adapt_capacity=getattr(yaml_config, "auto_adapt_capacity", True),
            priority=getattr(yaml_config, "priority", 0),
            updated_by=updated_by,
            config_updated_at=datetime.utcnow(),  # Track YAML sync timestamp
        )
        db.add(cluster)
        await db.flush()

        # Log the creation
        await self._log_audit_entry(
            db=db,
            model_id=model_id,
            cluster_id=cluster_id,
            action="sync_create",
            new_values={
                "base_url": cluster.base_url,
                "api_key": "***" if cluster.api_key else None,
                "number_of_replicas": cluster.number_of_replicas,
                "seed_capacity_tokens_per_pod": cluster.seed_capacity_tokens_per_pod,
                "is_enabled": cluster.is_enabled,
                "auto_adapt_capacity": cluster.auto_adapt_capacity,
                "priority": cluster.priority,
            },
            performed_by=updated_by,
        )

        return cluster

    async def _delete_cluster(
        self, db: AsyncSession, cluster: ModelCluster, updated_by: Optional[int] = None
    ) -> None:
        """
        Delete a cluster from the database.

        Args:
            db: Database session
            cluster: ModelCluster to delete
            updated_by: User ID performing the deletion
        """
        # Log the deletion before removing
        await self._log_audit_entry(
            db=db,
            model_id=cluster.model_id,
            cluster_id=cluster.cluster_id,
            action="sync_delete",
            old_values={
                "base_url": cluster.base_url,
                "api_key": "***" if cluster.api_key else None,
                "number_of_replicas": cluster.number_of_replicas,
                "seed_capacity_tokens_per_pod": cluster.seed_capacity_tokens_per_pod,
                "is_enabled": cluster.is_enabled,
                "auto_adapt_capacity": cluster.auto_adapt_capacity,
                "priority": cluster.priority,
            },
            performed_by=updated_by,
        )

        await db.delete(cluster)

    async def _update_cluster_from_yaml(
        self,
        db: AsyncSession,
        cluster: ModelCluster,
        yaml_config: Any,
        preserve_is_enabled: bool = True,
        updated_by: Optional[int] = None,
    ) -> None:
        """
        Update cluster from YAML, preserving is_enabled state.

        Args:
            db: Database session
            cluster: Existing ModelCluster to update
            yaml_config: YAML cluster configuration object
            preserve_is_enabled: If True, preserve DB's is_enabled value
            updated_by: User ID performing the update
        """
        # Capture old values for audit
        old_values = {
            "base_url": cluster.base_url,
            "api_key": "***" if cluster.api_key else None,
            "number_of_replicas": cluster.number_of_replicas,
            "seed_capacity_tokens_per_pod": cluster.seed_capacity_tokens_per_pod,
            "is_enabled": cluster.is_enabled,
            "auto_adapt_capacity": cluster.auto_adapt_capacity,
            "priority": cluster.priority,
        }

        # Track which fields changed
        new_values = {}

        # Update mutable fields from YAML
        new_base_url = getattr(yaml_config, "base_url", None)
        if cluster.base_url != new_base_url:
            cluster.base_url = new_base_url
            new_values["base_url"] = new_base_url

        new_api_key = getattr(yaml_config, "api_key", None)
        if cluster.api_key != new_api_key:
            cluster.api_key = new_api_key
            new_values["api_key"] = "***" if new_api_key else None

        new_replicas = getattr(yaml_config, "number_of_replicas", 1)
        if cluster.number_of_replicas != new_replicas:
            cluster.number_of_replicas = new_replicas
            new_values["number_of_replicas"] = new_replicas

        new_capacity = getattr(yaml_config, "seed_capacity_tokens_per_pod", 40000)
        if cluster.seed_capacity_tokens_per_pod != new_capacity:
            cluster.seed_capacity_tokens_per_pod = new_capacity
            new_values["seed_capacity_tokens_per_pod"] = new_capacity

        new_auto_adapt = getattr(yaml_config, "auto_adapt_capacity", True)
        if cluster.auto_adapt_capacity != new_auto_adapt:
            cluster.auto_adapt_capacity = new_auto_adapt
            new_values["auto_adapt_capacity"] = new_auto_adapt

        new_priority = getattr(yaml_config, "priority", 0)
        if cluster.priority != new_priority:
            cluster.priority = new_priority
            new_values["priority"] = new_priority

        # is_enabled is preserved from DB (not overwritten from YAML)
        if not preserve_is_enabled:
            new_is_enabled = getattr(yaml_config, "is_enabled", True)
            if cluster.is_enabled != new_is_enabled:
                cluster.is_enabled = new_is_enabled
                new_values["is_enabled"] = new_is_enabled

        # Update timestamp and user
        cluster.updated_by = updated_by
        cluster.config_updated_at = datetime.utcnow()  # Track YAML sync timestamp

        # Only log if something actually changed
        if new_values:
            await self._log_audit_entry(
                db=db,
                model_id=cluster.model_id,
                cluster_id=cluster.cluster_id,
                action="sync_update",
                old_values=old_values,
                new_values=new_values,
                performed_by=updated_by,
            )

    async def _log_audit_entry(
        self,
        db: AsyncSession,
        model_id: str,
        cluster_id: str,
        action: str,
        old_values: Optional[Dict[str, Any]] = None,
        new_values: Optional[Dict[str, Any]] = None,
        performed_by: Optional[int] = None,
    ) -> None:
        """
        Create an audit log entry for cluster changes.

        Args:
            db: Database session
            model_id: The model identifier
            cluster_id: The cluster identifier
            action: The action performed (create, delete, update, sync)
            old_values: Previous values (for updates/deletes)
            new_values: New values (for creates/updates)
            performed_by: User ID who performed the action
        """
        audit_entry = ModelClusterAuditLog(
            model_id=model_id,
            cluster_id=cluster_id,
            action=action,
            old_values=old_values,
            new_values=new_values,
            performed_by=performed_by,
        )
        db.add(audit_entry)


# Module-level singleton
cross_pod_sync_service = CrossPodSyncService()
