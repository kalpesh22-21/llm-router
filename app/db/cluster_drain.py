"""
Simplified cluster drain state management using Redis.

This module provides a minimal state machine for gracefully draining clusters:
- ENABLED: Cluster is active and accepting new requests
- DRAINING: Not accepting new requests, waiting for in-flight to complete
- DISABLED: Fully disabled

Key pattern: cluster_drain:{model_id}:{cluster_id} -> stores state string

Uses existing inflight_tokens from cluster_operational_state.py for tracking.
"""

from __future__ import annotations

import logging
from enum import Enum
from typing import Optional

from app.db.redis_client import get_redis_pool
from app.db.cluster_operational_state import cluster_metrics_store, CLUSTER_STATE_TTL_SECONDS

logger = logging.getLogger(__name__)

# ============================================================================
# Constants
# ============================================================================

CLUSTER_DRAIN_PREFIX = "cluster_drain:"
DEFAULT_DRAIN_TIMEOUT_SECONDS = 300


# ============================================================================
# Enums
# ============================================================================

class DrainState(Enum):
    """Simplified 3-state drain state machine."""

    ENABLED = "enabled"
    DRAINING = "draining"
    DISABLED = "disabled"


# ============================================================================
# Key Generation
# ============================================================================

def _make_drain_key(model_id: str, cluster_id: str) -> str:
    """Generate the Redis key for cluster drain state."""
    return f"{CLUSTER_DRAIN_PREFIX}{model_id}:{cluster_id}"


# ============================================================================
# Core Operations
# ============================================================================

async def get_drain_state(model_id: str, cluster_id: str) -> DrainState:
    """
    Get the current drain state for a cluster.

    Returns:
        DrainState (defaults to ENABLED if not found)
    """
    try:
        redis = get_redis_pool()
        drain_key = _make_drain_key(model_id, cluster_id)
        value = await redis.get(drain_key)

        if value is None:
            return DrainState.ENABLED

        return DrainState(value.decode("utf-8") if isinstance(value, bytes) else value)

    except Exception as e:
        logger.warning(
            "Failed to get drain state: model=%s cluster=%s error=%s",
            model_id,
            cluster_id,
            e,
        )
        return DrainState.ENABLED


async def start_drain(
    model_id: str,
    cluster_id: str,
    timeout_seconds: int = DEFAULT_DRAIN_TIMEOUT_SECONDS,
    ttl: int = CLUSTER_STATE_TTL_SECONDS,
) -> bool:
    """
    Start draining a cluster.

    Transitions state from ENABLED -> DRAINING and disables the cluster
    so no new requests are routed to it.

    Args:
        model_id: The model identifier
        cluster_id: The cluster identifier
        timeout_seconds: Maximum time to wait for drain completion
        ttl: Time-to-live for the drain state key

    Returns:
        True if drain was started successfully
    """
    try:
        redis = get_redis_pool()
        drain_key = _make_drain_key(model_id, cluster_id)

        # Check current state
        current = await get_drain_state(model_id, cluster_id)

        if current == DrainState.DRAINING:
            logger.debug("Cluster %s/%s is already draining", model_id, cluster_id)
            return True

        if current == DrainState.DISABLED:
            logger.warning(
                "Cannot start drain for %s/%s: already disabled", model_id, cluster_id
            )
            return False

        # Set state to DRAINING
        await redis.set(drain_key, DrainState.DRAINING.value, ex=ttl)

        # Disable the cluster in cluster_operational_state
        from app.db.cluster_operational_state import set_cluster_enabled

        await set_cluster_enabled(model_id, cluster_id, False)

        logger.info(
            "Started draining cluster %s/%s (timeout=%ds)",
            model_id,
            cluster_id,
            timeout_seconds,
        )
        return True

    except Exception as e:
        logger.error("Failed to start drain for %s/%s: %s", model_id, cluster_id, e)
        return False


async def complete_drain(
    model_id: str,
    cluster_id: str,
    ttl: int = CLUSTER_STATE_TTL_SECONDS,
) -> bool:
    """
    Mark a drain as complete.

    Transitions state from DRAINING -> DISABLED.

    Args:
        model_id: The model identifier
        cluster_id: The cluster identifier
        ttl: Time-to-live for the drain state key

    Returns:
        True if successful
    """
    try:
        redis = get_redis_pool()
        drain_key = _make_drain_key(model_id, cluster_id)

        current = await get_drain_state(model_id, cluster_id)

        if current != DrainState.DRAINING:
            logger.warning(
                "Cannot complete drain for %s/%s: not in DRAINING state (current=%s)",
                model_id,
                cluster_id,
                current.value,
            )
            return False

        # Set state to DISABLED
        await redis.set(drain_key, DrainState.DISABLED.value, ex=ttl)

        logger.info("Drain complete: %s/%s is now DISABLED", model_id, cluster_id)
        return True

    except Exception as e:
        logger.error("Failed to complete drain for %s/%s: %s", model_id, cluster_id, e)
        return False


async def force_disable(
    model_id: str,
    cluster_id: str,
    ttl: int = CLUSTER_STATE_TTL_SECONDS,
) -> bool:
    """
    Force disable a cluster immediately.

    Transitions state from any state -> DISABLED.
    This is used for emergency shutdowns or when drain timeout is exceeded.

    Args:
        model_id: The model identifier
        cluster_id: The cluster identifier
        ttl: Time-to-live for the drain state key

    Returns:
        True if successful
    """
    try:
        redis = get_redis_pool()
        drain_key = _make_drain_key(model_id, cluster_id)

        # Set state to DISABLED directly
        await redis.set(drain_key, DrainState.DISABLED.value, ex=ttl)

        # Disable the cluster in cluster_operational_state
        from app.db.cluster_operational_state import set_cluster_enabled

        await set_cluster_enabled(model_id, cluster_id, False)

        logger.warning("Force disabled cluster %s/%s", model_id, cluster_id)
        return True

    except Exception as e:
        logger.error("Failed to force disable %s/%s: %s", model_id, cluster_id, e)
        return False


async def resume_cluster(
    model_id: str,
    cluster_id: str,
    ttl: int = CLUSTER_STATE_TTL_SECONDS,
) -> bool:
    """
    Resume a cluster from DRAINING or DISABLED state back to ENABLED.

    Args:
        model_id: The model identifier
        cluster_id: The cluster identifier
        ttl: Time-to-live for the drain state key

    Returns:
        True if successful
    """
    try:
        redis = get_redis_pool()
        drain_key = _make_drain_key(model_id, cluster_id)

        current = await get_drain_state(model_id, cluster_id)

        if current == DrainState.ENABLED:
            logger.debug("Cluster %s/%s is already enabled", model_id, cluster_id)
            return True

        # Set state to ENABLED
        await redis.set(drain_key, DrainState.ENABLED.value, ex=ttl)

        # Enable the cluster in cluster_operational_state
        from app.db.cluster_operational_state import set_cluster_enabled

        await set_cluster_enabled(model_id, cluster_id, True)

        logger.info("Resumed cluster %s/%s to ENABLED state", model_id, cluster_id)
        return True

    except Exception as e:
        logger.error("Failed to resume cluster %s/%s: %s", model_id, cluster_id, e)
        return False


# ============================================================================
# In-Flight Tracking (uses existing cluster_operational_state metrics)
# ============================================================================

async def get_inflight_tokens(model_id: str, cluster_id: str) -> int:
    """
    Get the count of in-flight tokens for a cluster.

    Uses existing inflight_tokens from cluster_operational_state.py.

    Args:
        model_id: The model identifier
        cluster_id: The cluster identifier

    Returns:
        Number of in-flight tokens (0 if error)
    """
    try:
        return await cluster_metrics_store.get_inflight_tokens(model_id, cluster_id)
    except Exception as e:
        logger.warning(
            "Failed to get inflight tokens for %s/%s: %s", model_id, cluster_id, e
        )
        return 0


async def is_drain_complete(model_id: str, cluster_id: str) -> bool:
    """
    Check if a cluster has completed draining (in-flight tokens == 0).

    Args:
        model_id: The model identifier
        cluster_id: The cluster identifier

    Returns:
        True if cluster is DRAINING and in-flight tokens == 0
    """
    state = await get_drain_state(model_id, cluster_id)

    if state != DrainState.DRAINING:
        return False

    inflight = await get_inflight_tokens(model_id, cluster_id)
    return inflight == 0


# ============================================================================
# Query Operations
# ============================================================================

async def get_draining_clusters(model_id: Optional[str] = None) -> list[tuple[str, str]]:
    """
    Get all clusters currently in DRAINING state.

    Args:
        model_id: Optional model ID to filter by

    Returns:
        List of (model_id, cluster_id) tuples
    """
    try:
        redis = get_redis_pool()

        if model_id:
            pattern = f"{CLUSTER_DRAIN_PREFIX}{model_id}:*"
        else:
            pattern = f"{CLUSTER_DRAIN_PREFIX}*"

        draining = []

        async for key in redis.scan_iter(match=pattern):
            key_str = key.decode("utf-8") if isinstance(key, bytes) else key

            try:
                value = await redis.get(key)
                if not value:
                    continue

                state_value = value.decode("utf-8") if isinstance(value, bytes) else value

                if state_value == DrainState.DRAINING.value:
                    # Extract model_id and cluster_id from key
                    # Format: cluster_drain:{model_id}:{cluster_id}
                    parts = key_str.split(":")
                    if len(parts) >= 3:
                        m_id = parts[1]
                        c_id = ":".join(parts[2:])  # Handle model_id with colons
                        draining.append((m_id, c_id))

            except Exception:
                continue

        return draining

    except Exception as e:
        logger.warning("Failed to get draining clusters: %s", e)
        return []


async def delete_drain_state(model_id: str, cluster_id: str) -> bool:
    """
    Delete the drain state for a cluster.

    Args:
        model_id: The model identifier
        cluster_id: The cluster identifier

    Returns:
        True if successful
    """
    try:
        redis = get_redis_pool()
        drain_key = _make_drain_key(model_id, cluster_id)
        await redis.delete(drain_key)

        logger.debug("Deleted drain state for %s/%s", model_id, cluster_id)
        return True

    except Exception as e:
        logger.warning("Failed to delete drain state for %s/%s: %s", model_id, cluster_id, e)
        return False
