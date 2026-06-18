"""
ClusterManager — service for managing model cluster configurations.

Provides CRUD operations for ModelCluster entities with audit logging
and runtime synchronization capabilities.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import List, Optional

from fastapi import HTTPException, status
from pydantic import BaseModel, field_validator
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.cluster_operational_state import set_cluster_state
from app.db.cluster_drain import start_drain, get_drain_state, DrainState
from app.db.mysql import ModelCluster, ModelClusterAuditLog, User, get_db

logger = logging.getLogger(__name__)


async def _get_username_for_log(db: AsyncSession, user_id: int) -> str:
    """Fetch username for logging purposes. Returns id=X if user not found."""
    result = await db.execute(select(User.username).where(User.id == user_id))
    username = result.scalar_one_or_none()
    return username if username else f"id={user_id}"


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------


class ClusterCreate(BaseModel):
    """Schema for creating a new model cluster."""

    cluster_id: str
    base_url: str
    api_key: Optional[str] = None
    number_of_replicas: int = 1
    seed_capacity_tokens_per_pod: int = 40000
    is_enabled: bool = True
    auto_adapt_capacity: bool = True
    priority: int = 0

    # Performance tuning knobs
    ttft_budget_ms: float = 500.0
    itl_budget_ms: float = 50.0
    ttft_alpha: float = 0.3
    itl_alpha: float = 0.3
    capacity_alpha: float = 0.1

    @field_validator("cluster_id")
    @classmethod
    def validate_cluster_id(cls, v: str) -> str:
        """Validate cluster_id format."""
        if not v or not v.strip():
            raise ValueError("cluster_id cannot be empty")
        if len(v) > 255:
            raise ValueError("cluster_id must be at most 255 characters long")
        return v.strip()

    @field_validator("base_url")
    @classmethod
    def validate_base_url(cls, v: str) -> str:
        """Validate base_url format."""
        if not v or not v.strip():
            raise ValueError("base_url cannot be empty")
        if len(v) > 2048:
            raise ValueError("base_url must be at most 2048 characters long")
        # Ensure URL starts with http:// or https://
        v = v.strip()
        if not (v.startswith("http://") or v.startswith("https://")):
            raise ValueError("base_url must start with http:// or https://")
        return v.rstrip("/")

    @field_validator("number_of_replicas")
    @classmethod
    def validate_replicas(cls, v: int) -> int:
        """Validate number_of_replicas is positive."""
        if v < 1:
            raise ValueError("number_of_replicas must be at least 1")
        if v > 1000:
            raise ValueError("number_of_replicas must be at most 1000")
        return v

    @field_validator("seed_capacity_tokens_per_pod")
    @classmethod
    def validate_capacity(cls, v: int) -> int:
        """Validate seed_capacity_tokens_per_pod is positive."""
        if v < 1:
            raise ValueError("seed_capacity_tokens_per_pod must be at least 1")
        if v > 10000000:
            raise ValueError("seed_capacity_tokens_per_pod must be at most 10000000")
        return v

    @field_validator("ttft_budget_ms")
    @classmethod
    def validate_ttft_budget(cls, v: float) -> float:
        """Validate TTFT budget is positive."""
        if v <= 0:
            raise ValueError("ttft_budget_ms must be greater than 0")
        if v > 10000:
            raise ValueError("ttft_budget_ms must be at most 10000")
        return v

    @field_validator("itl_budget_ms")
    @classmethod
    def validate_itl_budget(cls, v: float) -> float:
        """Validate ITL budget is positive."""
        if v <= 0:
            raise ValueError("itl_budget_ms must be greater than 0")
        if v > 1000:
            raise ValueError("itl_budget_ms must be at most 1000")
        return v

    @field_validator("ttft_alpha", "itl_alpha", "capacity_alpha")
    @classmethod
    def validate_alpha(cls, v: float) -> float:
        """Validate EWMA alpha is between 0 and 1."""
        if v < 0.0:
            raise ValueError("alpha value must be at least 0.0")
        if v > 1.0:
            raise ValueError("alpha value must be at most 1.0")
        return v


class ClusterUpdate(BaseModel):
    """Schema for updating an existing model cluster."""

    number_of_replicas: Optional[int] = None
    seed_capacity_tokens_per_pod: Optional[int] = None
    is_enabled: Optional[bool] = None
    auto_adapt_capacity: Optional[bool] = None
    priority: Optional[int] = None
    base_url: Optional[str] = None

    # Performance tuning knobs
    ttft_budget_ms: Optional[float] = None
    itl_budget_ms: Optional[float] = None
    ttft_alpha: Optional[float] = None
    itl_alpha: Optional[float] = None
    capacity_alpha: Optional[float] = None

    @field_validator("number_of_replicas")
    @classmethod
    def validate_replicas(cls, v: Optional[int]) -> Optional[int]:
        """Validate number_of_replicas is positive if provided."""
        if v is None:
            return v
        if v < 1:
            raise ValueError("number_of_replicas must be at least 1")
        if v > 1000:
            raise ValueError("number_of_replicas must be at most 1000")
        return v

    @field_validator("seed_capacity_tokens_per_pod")
    @classmethod
    def validate_capacity(cls, v: Optional[int]) -> Optional[int]:
        """Validate seed_capacity_tokens_per_pod is positive if provided."""
        if v is None:
            return v
        if v < 1:
            raise ValueError("seed_capacity_tokens_per_pod must be at least 1")
        if v > 10000000:
            raise ValueError("seed_capacity_tokens_per_pod must be at most 10000000")
        return v

    @field_validator("base_url")
    @classmethod
    def validate_base_url(cls, v: Optional[str]) -> Optional[str]:
        """Validate base_url format if provided."""
        if v is None:
            return v
        if not v.strip():
            raise ValueError("base_url cannot be empty")
        if len(v) > 2048:
            raise ValueError("base_url must be at most 2048 characters long")
        v = v.strip()
        if not (v.startswith("http://") or v.startswith("https://")):
            raise ValueError("base_url must start with http:// or https://")
        return v.rstrip("/")

    @field_validator("ttft_budget_ms")
    @classmethod
    def validate_ttft_budget(cls, v: Optional[float]) -> Optional[float]:
        """Validate TTFT budget is positive if provided."""
        if v is None:
            return v
        if v <= 0:
            raise ValueError("ttft_budget_ms must be greater than 0")
        if v > 10000:
            raise ValueError("ttft_budget_ms must be at most 10000")
        return v

    @field_validator("itl_budget_ms")
    @classmethod
    def validate_itl_budget(cls, v: Optional[float]) -> Optional[float]:
        """Validate ITL budget is positive if provided."""
        if v is None:
            return v
        if v <= 0:
            raise ValueError("itl_budget_ms must be greater than 0")
        if v > 1000:
            raise ValueError("itl_budget_ms must be at most 1000")
        return v

    @field_validator("ttft_alpha", "itl_alpha", "capacity_alpha")
    @classmethod
    def validate_alpha(cls, v: Optional[float]) -> Optional[float]:
        """Validate EWMA alpha is between 0 and 1 if provided."""
        if v is None:
            return v
        if v < 0.0:
            raise ValueError("alpha value must be at least 0.0")
        if v > 1.0:
            raise ValueError("alpha value must be at most 1.0")
        return v


# ---------------------------------------------------------------------------
# ClusterManager service
# ---------------------------------------------------------------------------


class ClusterManager:
    """Async service for managing model cluster configurations."""

    async def get_clusters_for_model(
        self, db: AsyncSession, model_id: str
    ) -> List[ModelCluster]:
        """Get all clusters for a model."""
        result = await db.execute(
            select(ModelCluster).where(ModelCluster.model_id == model_id)
        )
        return list(result.scalars().all())

    async def get_clusters(self, db: AsyncSession, model_id: str) -> List[ModelCluster]:
        """
        Get all clusters for a model.

        Args:
            db: Database session
            model_id: The model identifier

        Returns:
            List of ModelCluster objects ordered by priority (desc), then cluster_id
        """
        result = await db.execute(
            select(ModelCluster)
            .where(ModelCluster.model_id == model_id)
            .order_by(ModelCluster.priority.desc(), ModelCluster.cluster_id)
        )
        return list(result.scalars().all())

    async def get_cluster(
        self, db: AsyncSession, model_id: str, cluster_id: str
    ) -> Optional[ModelCluster]:
        """
        Get a specific cluster for a model.

        Args:
            db: Database session
            model_id: The model identifier
            cluster_id: The cluster identifier

        Returns:
            ModelCluster object or None if not found
        """
        result = await db.execute(
            select(ModelCluster).where(
                ModelCluster.model_id == model_id,
                ModelCluster.cluster_id == cluster_id,
            )
        )
        return result.scalar_one_or_none()

    async def _get_cluster_or_raise(
        self, db: AsyncSession, model_id: str, cluster_id: str
    ) -> ModelCluster:
        """
        Get a cluster or raise HTTPException 404 if not found.

        Args:
            db: Database session
            model_id: The model identifier
            cluster_id: The cluster identifier

        Returns:
            ModelCluster object

        Raises:
            HTTPException(404) if cluster not found
        """
        cluster = await self.get_cluster(db, model_id, cluster_id)
        if cluster is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Cluster '{cluster_id}' not found for model '{model_id}'",
            )
        return cluster

    async def _log_audit(
        self,
        db: AsyncSession,
        model_id: str,
        cluster_id: str,
        action: str,
        old_values: Optional[dict] = None,
        new_values: Optional[dict] = None,
        performed_by: Optional[int] = None,
    ) -> None:
        """
        Create an audit log entry for cluster changes.

        Args:
            db: Database session
            model_id: The model identifier
            cluster_id: The cluster identifier
            action: The action performed (e.g., 'create', 'update', 'delete')
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
        await db.flush()
        logger.debug(
            "Audit log created: model=%s cluster=%s action=%s by_user=%s",
            model_id,
            cluster_id,
            action,
            performed_by,
        )

    async def create_cluster(
        self, db: AsyncSession, model_id: str, data: ClusterCreate, updated_by: int
    ) -> ModelCluster:
        """
        Create a new cluster for a model.

        Args:
            db: Database session
            model_id: The model identifier
            data: Cluster creation data
            updated_by: User ID performing the creation

        Returns:
            Created ModelCluster object

        Raises:
            HTTPException(409) if cluster already exists
        """
        cluster = ModelCluster(
            model_id=model_id,
            cluster_id=data.cluster_id,
            base_url=data.base_url,
            api_key=data.api_key,
            number_of_replicas=data.number_of_replicas,
            seed_capacity_tokens_per_pod=data.seed_capacity_tokens_per_pod,
            is_enabled=data.is_enabled,
            auto_adapt_capacity=data.auto_adapt_capacity,
            priority=data.priority,
            ttft_budget_ms=data.ttft_budget_ms,
            itl_budget_ms=data.itl_budget_ms,
            ttft_alpha=data.ttft_alpha,
            itl_alpha=data.itl_alpha,
            capacity_alpha=data.capacity_alpha,
            updated_by=updated_by,
        )
        db.add(cluster)

        try:
            await db.flush()
        except IntegrityError:
            await db.rollback()
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Cluster '{data.cluster_id}' already exists for model '{model_id}'",
            )

        # Log the creation
        await self._log_audit(
            db=db,
            model_id=model_id,
            cluster_id=data.cluster_id,
            action="create",
            new_values=data.model_dump(exclude_none=True),
            performed_by=updated_by,
        )

        updated_by_username = await _get_username_for_log(db, updated_by)
        logger.info(
            "Created cluster '%s' for model '%s' by user %s",
            data.cluster_id,
            model_id,
            updated_by_username,
        )

        # Sync to runtime and Redis (propagates to other pods via ConfigSyncService)
        await self.sync_to_runtime_and_redis(db, model_id, data.cluster_id)

        return cluster

    async def update_cluster(
        self,
        db: AsyncSession,
        model_id: str,
        cluster_id: str,
        data: ClusterUpdate,
        updated_by: int,
    ) -> ModelCluster:
        """
        Update an existing cluster.

        Args:
            db: Database session
            model_id: The model identifier
            cluster_id: The cluster identifier
            data: Cluster update data
            updated_by: User ID performing the update

        Returns:
            Updated ModelCluster object

        Raises:
            HTTPException(404) if cluster not found
        """
        cluster = await self._get_cluster_or_raise(db, model_id, cluster_id)

        # Capture old values for audit
        old_values = {
            "number_of_replicas": cluster.number_of_replicas,
            "seed_capacity_tokens_per_pod": cluster.seed_capacity_tokens_per_pod,
            "is_enabled": cluster.is_enabled,
            "auto_adapt_capacity": cluster.auto_adapt_capacity,
            "priority": cluster.priority,
            "base_url": cluster.base_url,
            "ttft_budget_ms": cluster.ttft_budget_ms,
            "itl_budget_ms": cluster.itl_budget_ms,
            "ttft_alpha": cluster.ttft_alpha,
            "itl_alpha": cluster.itl_alpha,
            "capacity_alpha": cluster.capacity_alpha,
        }

        # Track which fields were updated
        new_values = {}
        update_data = data.model_dump(exclude_none=True)

        if "number_of_replicas" in update_data:
            cluster.number_of_replicas = update_data["number_of_replicas"]
            new_values["number_of_replicas"] = update_data["number_of_replicas"]

        if "seed_capacity_tokens_per_pod" in update_data:
            cluster.seed_capacity_tokens_per_pod = update_data[
                "seed_capacity_tokens_per_pod"
            ]
            new_values["seed_capacity_tokens_per_pod"] = update_data[
                "seed_capacity_tokens_per_pod"
            ]

        if "is_enabled" in update_data:
            cluster.is_enabled = update_data["is_enabled"]
            new_values["is_enabled"] = update_data["is_enabled"]

        if "auto_adapt_capacity" in update_data:
            cluster.auto_adapt_capacity = update_data["auto_adapt_capacity"]
            new_values["auto_adapt_capacity"] = update_data["auto_adapt_capacity"]

        if "priority" in update_data:
            cluster.priority = update_data["priority"]
            new_values["priority"] = update_data["priority"]

        if "base_url" in update_data:
            cluster.base_url = update_data["base_url"]
            new_values["base_url"] = update_data["base_url"]

        if "ttft_budget_ms" in update_data:
            cluster.ttft_budget_ms = update_data["ttft_budget_ms"]
            new_values["ttft_budget_ms"] = update_data["ttft_budget_ms"]

        if "itl_budget_ms" in update_data:
            cluster.itl_budget_ms = update_data["itl_budget_ms"]
            new_values["itl_budget_ms"] = update_data["itl_budget_ms"]

        if "ttft_alpha" in update_data:
            cluster.ttft_alpha = update_data["ttft_alpha"]
            new_values["ttft_alpha"] = update_data["ttft_alpha"]

        if "itl_alpha" in update_data:
            cluster.itl_alpha = update_data["itl_alpha"]
            new_values["itl_alpha"] = update_data["itl_alpha"]

        if "capacity_alpha" in update_data:
            cluster.capacity_alpha = update_data["capacity_alpha"]
            new_values["capacity_alpha"] = update_data["capacity_alpha"]

        cluster.updated_by = updated_by
        await db.flush()

        # Log the update
        await self._log_audit(
            db=db,
            model_id=model_id,
            cluster_id=cluster_id,
            action="update",
            old_values=old_values,
            new_values=new_values,
            performed_by=updated_by,
        )

        updated_by_username = await _get_username_for_log(db, updated_by)
        logger.info(
            "Updated cluster '%s' for model '%s' by user %s",
            cluster_id,
            model_id,
            updated_by_username,
        )
        return cluster

    async def enable_cluster(
        self, db: AsyncSession, model_id: str, cluster_id: str, updated_by: int
    ) -> ModelCluster:
        """
        Enable a cluster.

        Also resumes the cluster from DRAINED or DISABLED drain state if needed.

        Args:
            db: Database session
            model_id: The model identifier
            cluster_id: The cluster identifier
            updated_by: User ID performing the enable

        Returns:
            Updated ModelCluster object

        Raises:
            HTTPException(404) if cluster not found
            HTTPException(400) if cluster is already enabled
        """
        cluster = await self._get_cluster_or_raise(db, model_id, cluster_id)

        if cluster.is_enabled:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Cluster '{cluster_id}' is already enabled",
            )

        old_values = {"is_enabled": cluster.is_enabled}
        cluster.is_enabled = True
        cluster.config_updated_at = datetime.utcnow()
        cluster.updated_by = updated_by
        await db.flush()

        # Write operational state to Redis for cross-pod synchronization
        # This ensures other pods see the enabled state change
        auto_adapt_capacity = (
            cluster.auto_adapt_capacity
            if cluster.auto_adapt_capacity is not None
            else True
        )
        await set_cluster_state(
            model_id, cluster_id, is_enabled=True, auto_adapt_capacity=auto_adapt_capacity
        )
        logger.debug(
            "Synced cluster state to Redis on enable: model=%s cluster=%s "
            "is_enabled=True auto_adapt_capacity=%s",
            model_id,
            cluster_id,
            auto_adapt_capacity,
        )

        # Check and clear drain state if needed
        drain_state = await get_drain_state(model_id, cluster_id)
        if drain_state in (DrainState.DRAINING, DrainState.DISABLED):
            from app.db.cluster_drain import resume_cluster

            await resume_cluster(model_id, cluster_id)
            logger.info(
                "Resumed cluster '%s' for model '%s' from %s state",
                cluster_id,
                model_id,
                drain_state.value,
            )

        # Log the enable action
        await self._log_audit(
            db=db,
            model_id=model_id,
            cluster_id=cluster_id,
            action="enable",
            old_values=old_values,
            new_values={"is_enabled": True},
            performed_by=updated_by,
        )

        updated_by_username = await _get_username_for_log(db, updated_by)
        logger.info(
            "Enabled cluster '%s' for model '%s' by user %s",
            cluster_id,
            model_id,
            updated_by_username,
        )
        return cluster

    async def disable_cluster(
        self, db: AsyncSession, model_id: str, cluster_id: str, updated_by: int
    ) -> ModelCluster:
        """
        Disable a cluster with mandatory drain.

        Always drains the cluster first to allow in-flight requests to complete.
        The cluster transitions: ENABLED -> DRAINING -> DISABLED (automatically).
        This is mandatory and cannot be skipped.

        Args:
            db: Database session
            model_id: The model identifier
            cluster_id: The cluster identifier
            updated_by: User ID performing the disable

        Returns:
            Updated ModelCluster object

        Raises:
            HTTPException(404) if cluster not found
            HTTPException(400) if cluster is already disabled
        """
        cluster = await self._get_cluster_or_raise(db, model_id, cluster_id)

        if not cluster.is_enabled:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Cluster '{cluster_id}' is already disabled",
            )

        old_values = {"is_enabled": cluster.is_enabled}
        cluster.is_enabled = False
        cluster.config_updated_at = datetime.utcnow()
        cluster.updated_by = updated_by
        await db.flush()

        # Log the disable action
        await self._log_audit(
            db=db,
            model_id=model_id,
            cluster_id=cluster_id,
            action="disable",
            old_values=old_values,
            new_values={"is_enabled": False},
            performed_by=updated_by,
        )

        # Start drain (mandatory - always happens)
        drain_started = await start_drain(model_id, cluster_id)
        if drain_started:
            logger.info(
                "Started drain for cluster '%s' for model '%s'",
                cluster_id,
                model_id,
            )
        else:
            logger.warning(
                "Failed to start drain for cluster '%s' for model '%s'",
                cluster_id,
                model_id,
            )

        updated_by_username = await _get_username_for_log(db, updated_by)
        logger.info(
            "Disabled cluster '%s' for model '%s' by user %s (drain started)",
            cluster_id,
            model_id,
            updated_by_username,
        )
        return cluster

    async def sync_to_runtime_and_redis(
        self, db: AsyncSession, model_id: str, cluster_id: str
    ) -> None:
        """
        Sync database changes to both Redis and local runtime.

        This implements a two-layer synchronization strategy for cluster configuration:

        1. **Immediate Local Update**: Updates the local MultiClusterBackend's in-memory
           ClusterEndpoint directly. This provides immediate feedback on the current pod
           for better UX and faster routing decisions.

        2. **Redis Propagation (for Cross-Pod Sync)**: Writes operational state and full
           configuration to Redis. The CrossPodSyncService (polling every 5s) reads from
           Redis and propagates infrastructure changes to other pods in the deployment.

        Architecture Flow:
        ```
        Admin Operation (this pod)
               |
               v
        sync_to_runtime_and_redis()
               |
               +---> Redis (cluster_operational_state + cluster_config)
               |          ^
               |          | (polls every 5s)
               |          v
               +---> Local Runtime <----- CrossPodSyncService (other pods)

        Other Pods: CrossPodSyncService reads Redis -> updates local runtime
        ```

        This ensures:
        - Strong consistency on the current pod (immediate local update)
        - Eventual consistency across all pods (via CrossPodSyncService polling Redis)
        - Survives pod restarts (Redis persists state)

        Args:
            db: Database session
            model_id: The model identifier
            cluster_id: The cluster identifier

        Raises:
            HTTPException(404) if cluster not found in database
        """
        # 1. Get cluster from DB (raises 404 if not found)
        cluster = await self._get_cluster_or_raise(db, model_id, cluster_id)

        # 2. Write basic state to Redis (CRITICAL for routing decisions)
        # This is used by routing logic across all pods
        auto_adapt_capacity = (
            cluster.auto_adapt_capacity
            if cluster.auto_adapt_capacity is not None
            else True
        )
        await set_cluster_state(
            model_id, cluster_id, cluster.is_enabled, auto_adapt_capacity
        )
        logger.debug(
            "Synced cluster state to Redis: model=%s cluster=%s "
            "is_enabled=%s auto_adapt_capacity=%s",
            model_id,
            cluster_id,
            cluster.is_enabled,
            auto_adapt_capacity,
        )

        # 3. Write infrastructure config to Redis (for CrossPodSyncService to propagate)
        # Operational state (is_enabled, auto_adapt_capacity) is stored separately
        from app.db.cluster_config import set_cluster_config

        await set_cluster_config(
            model_id,
            cluster_id,
            {
                "base_url": str(cluster.base_url) if cluster.base_url else "",
                "api_key": cluster.api_key,
                "weight": getattr(cluster, "weight", 1),
                "number_of_replicas": cluster.number_of_replicas or 1,
                "seed_capacity_tokens_per_pod": cluster.seed_capacity_tokens_per_pod
                or 40000,
                "timeout": getattr(cluster, "timeout", 30.0),
            },
        )
        logger.debug(
            "Synced full cluster config to Redis: model=%s cluster=%s",
            model_id,
            cluster_id,
        )

        # 4. Update local runtime (immediate feedback on this pod)
        from app.backends.base import registry as backend_registry
        from app.backends.multi_cluster import MultiClusterBackend, ClusterEndpoint

        backend = backend_registry.get(model_id)
        if backend is None:
            logger.warning(
                "No backend found for model '%s'. "
                "Cluster '%s' exists in database but runtime is not configured.",
                model_id,
                cluster_id,
            )
            return

        if not isinstance(backend, MultiClusterBackend):
            logger.warning(
                "Backend for model '%s' is not MultiClusterBackend (type=%s). "
                "Cannot sync cluster '%s'.",
                model_id,
                type(backend).__name__,
                cluster_id,
            )
            return

        # Check if cluster exists in runtime
        existing_endpoint = backend.get_cluster(cluster_id)

        if cluster.is_enabled:
            # Cluster should be enabled in runtime
            # Create/update the cluster endpoint
            new_endpoint = ClusterEndpoint(
                cluster_id=cluster.cluster_id,
                base_url=cluster.base_url or "",
                api_key=cluster.api_key or "",
                model_id=model_id,
                number_of_replicas=cluster.number_of_replicas or 1,
                seed_capacity_tokens_per_pod=cluster.seed_capacity_tokens_per_pod
                or 40000,
                is_enabled=True,
                auto_adapt_capacity=auto_adapt_capacity,
                ttft_budget_ms=cluster.ttft_budget_ms or 500.0,
                itl_budget_ms=cluster.itl_budget_ms or 50.0,
                ttft_alpha=cluster.ttft_alpha or 0.3,
                itl_alpha=cluster.itl_alpha or 0.3,
                capacity_alpha=cluster.capacity_alpha or 0.1,
            )
            is_new = backend.enable_cluster(new_endpoint)

            if existing_endpoint is not None:
                logger.info(
                    "Synced enabled cluster '%s' for model '%s' to local runtime: "
                    "replicas=%d, capacity_per_pod=%d, auto_adapt=%s, "
                    "ttft_budget=%.1fms, itl_budget=%.1fms, base_url=%s",
                    cluster_id,
                    model_id,
                    new_endpoint.number_of_replicas,
                    new_endpoint.seed_capacity_tokens_per_pod,
                    new_endpoint.auto_adapt_capacity,
                    new_endpoint._ttft_budget_ms,
                    new_endpoint._itl_budget_ms,
                    new_endpoint.base_url,
                )
            else:
                logger.info(
                    "Added new cluster '%s' to local runtime for model '%s': "
                    "replicas=%d, capacity_per_pod=%d, auto_adapt=%s, "
                    "ttft_budget=%.1fms, itl_budget=%.1fms",
                    cluster_id,
                    model_id,
                    new_endpoint.number_of_replicas,
                    new_endpoint.seed_capacity_tokens_per_pod,
                    new_endpoint.auto_adapt_capacity,
                    new_endpoint._ttft_budget_ms,
                    new_endpoint._itl_budget_ms,
                )
        else:
            # Cluster should be disabled
            was_disabled = backend.disable_cluster(cluster_id)
            if was_disabled:
                logger.info(
                    "Disabled cluster '%s' for model '%s' in local runtime "
                    "(in-flight requests will complete)",
                    cluster_id,
                    model_id,
                )
            else:
                logger.debug(
                    "Cluster '%s' for model '%s' was not found in local runtime",
                    cluster_id,
                    model_id,
                )


# Module-level singleton
cluster_manager = ClusterManager()
