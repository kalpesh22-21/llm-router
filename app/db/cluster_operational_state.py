"""
OPERATIONAL State and Metrics Management

This module stores RUNTIME operational state:
- is_enabled: Whether cluster accepts traffic
- auto_adapt_capacity: Whether to dynamically adjust capacity

And runtime metrics:
- inflight_tokens, ewma latencies, adaptive_capacity

This is the SOLE source of truth for operational flags.

Atomic Operations:
    - check_and_reserve_tokens: Atomically checks cluster state (is_enabled) and
      reserves tokens if enabled. Prevents race conditions between enable/disable
      and token reservation.

The state is stored in Redis with a TTL to handle pod restarts gracefully.
If Redis is unavailable, the system falls back to in-memory state.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Optional

import redis.asyncio as aioredis

from app.db.redis_client import get_redis_pool
from app.core.config import settings

logger = logging.getLogger(__name__)

# ============================================================================
# Constants
# ============================================================================

# Key prefixes
CLUSTER_STATE_PREFIX = "cluster_state:"
CLUSTER_METRICS_PREFIX = "cluster_metrics:"

# Key suffixes for metrics
INFLIGHT_TOKENS_SUFFIX = ":inflight_tokens"
EWMA_TTFT_SUFFIX = ":ewma_ttft_ms"
EWMA_ITL_SUFFIX = ":ewma_itl_ms"
ADAPTIVE_CAPACITY_SUFFIX = ":adaptive_capacity"
HAS_SAMPLES_SUFFIX = ":has_samples"

# TTL for cluster state keys - configurable via settings
CLUSTER_STATE_TTL_SECONDS = settings.CLUSTER_STATE_TTL_SECONDS

# Default TTL for metrics - configurable via settings
METRICS_TTL_SECONDS = settings.METRICS_TTL_SECONDS


# ============================================================================
# Dataclasses
# ============================================================================

@dataclass
class ClusterRuntimeState:
    """Complete runtime state for a cluster."""

    is_enabled: bool
    auto_adapt_capacity: bool
    inflight_tokens: int
    ewma_latency: float
    adaptive_capacity: float


# ============================================================================
# Key Generation Helpers
# ============================================================================

def _make_state_key(model_id: str, cluster_id: str) -> str:
    """Generate the Redis key for cluster state."""
    return f"{CLUSTER_STATE_PREFIX}{model_id}:{cluster_id}"


def _make_metrics_key(model_id: str, cluster_id: str, suffix: str) -> str:
    """Build a Redis key for cluster metrics."""
    return f"{CLUSTER_METRICS_PREFIX}{model_id}:{cluster_id}{suffix}"


# ============================================================================
# Cluster State Operations
# ============================================================================

async def set_cluster_enabled(
    model_id: str,
    cluster_id: str,
    is_enabled: bool,
    auto_adapt_capacity: bool = True,
    ttl: int = CLUSTER_STATE_TTL_SECONDS,
) -> bool:
    """
    Set the enabled state for a cluster in Redis.

    Args:
        model_id: The model identifier
        cluster_id: The cluster identifier
        is_enabled: Whether the cluster is enabled for routing
        auto_adapt_capacity: Whether to dynamically adjust capacity (default True)
        ttl: Time-to-live for the state key in seconds

    Returns:
        True if successful, False if Redis is unavailable
    """
    try:
        redis = get_redis_pool()
        key = _make_state_key(model_id, cluster_id)
        state = {"is_enabled": is_enabled, "auto_adapt_capacity": auto_adapt_capacity}
        await redis.set(key, json.dumps(state), ex=ttl)
        logger.debug(
            "Set cluster state in Redis: model=%s cluster=%s is_enabled=%s auto_adapt_capacity=%s",
            model_id,
            cluster_id,
            is_enabled,
            auto_adapt_capacity,
        )
        return True
    except Exception as e:
        logger.warning(
            "Failed to set cluster state in Redis: model=%s cluster=%s error=%s",
            model_id,
            cluster_id,
            e,
        )
        return False


async def get_cluster_enabled(model_id: str, cluster_id: str) -> Optional[bool]:
    """
    Get the enabled state for a cluster from Redis.

    Args:
        model_id: The model identifier
        cluster_id: The cluster identifier

    Returns:
        True/False if state exists in Redis, None if not found or Redis unavailable
    """
    try:
        redis = get_redis_pool()
        key = _make_state_key(model_id, cluster_id)
        value = await redis.get(key)
        if value is None:
            return None
        state = json.loads(value)
        return state.get("is_enabled")
    except Exception as e:
        logger.warning(
            "Failed to get cluster state from Redis: model=%s cluster=%s error=%s",
            model_id,
            cluster_id,
            e,
        )
        return None


async def get_cluster_state(
    model_id: str, cluster_id: str
) -> Optional[dict[str, any]]:
    """
    Get the full state for a cluster from Redis.

    Args:
        model_id: The model identifier
        cluster_id: The cluster identifier

    Returns:
        Dictionary with 'is_enabled' and 'auto_adapt_capacity' if state exists,
        None if not found or Redis unavailable
    """
    try:
        redis = get_redis_pool()
        key = _make_state_key(model_id, cluster_id)
        value = await redis.get(key)
        if value is None:
            return None
        state = json.loads(value)
        return {
            "is_enabled": state.get("is_enabled", True),
            "auto_adapt_capacity": state.get("auto_adapt_capacity", True),
        }
    except Exception as e:
        logger.warning(
            "Failed to get cluster state from Redis: model=%s cluster=%s error=%s",
            model_id,
            cluster_id,
            e,
        )
        return None


async def set_cluster_state(
    model_id: str,
    cluster_id: str,
    is_enabled: bool,
    auto_adapt_capacity: bool = True,
    ttl: int = CLUSTER_STATE_TTL_SECONDS,
) -> bool:
    """
    Set the full state for a cluster in Redis.

    Args:
        model_id: The model identifier
        cluster_id: The cluster identifier
        is_enabled: Whether the cluster is enabled for routing
        auto_adapt_capacity: Whether to dynamically adjust capacity (default True)
        ttl: Time-to-live for the state key in seconds

    Returns:
        True if successful, False if Redis is unavailable
    """
    try:
        redis = get_redis_pool()
        key = _make_state_key(model_id, cluster_id)
        state = {"is_enabled": is_enabled, "auto_adapt_capacity": auto_adapt_capacity}
        await redis.set(key, json.dumps(state), ex=ttl)
        logger.debug(
            "Set cluster state in Redis: model=%s cluster=%s is_enabled=%s auto_adapt_capacity=%s",
            model_id,
            cluster_id,
            is_enabled,
            auto_adapt_capacity,
        )
        return True
    except Exception as e:
        logger.warning(
            "Failed to set cluster state in Redis: model=%s cluster=%s error=%s",
            model_id,
            cluster_id,
            e,
        )
        return False


async def get_auto_adapt_capacity(model_id: str, cluster_id: str) -> Optional[bool]:
    """
    Get the auto_adapt_capacity state for a cluster from Redis.

    Args:
        model_id: The model identifier
        cluster_id: The cluster identifier

    Returns:
        True/False if state exists in Redis, None if not found or Redis unavailable
    """
    try:
        redis = get_redis_pool()
        key = _make_state_key(model_id, cluster_id)
        value = await redis.get(key)
        if value is None:
            return None
        state = json.loads(value)
        return state.get("auto_adapt_capacity")
    except Exception as e:
        logger.warning(
            "Failed to get auto_adapt_capacity from Redis: model=%s cluster=%s error=%s",
            model_id,
            cluster_id,
            e,
        )
        return None


async def delete_cluster_state(model_id: str, cluster_id: str) -> bool:
    """
    Delete the state for a cluster from Redis.

    Args:
        model_id: The model identifier
        cluster_id: The cluster identifier

    Returns:
        True if successful or key didn't exist, False if Redis error
    """
    try:
        redis = get_redis_pool()
        key = _make_state_key(model_id, cluster_id)
        await redis.delete(key)
        logger.debug(
            "Deleted cluster state from Redis: model=%s cluster=%s",
            model_id,
            cluster_id,
        )
        return True
    except Exception as e:
        logger.warning(
            "Failed to delete cluster state from Redis: model=%s cluster=%s error=%s",
            model_id,
            cluster_id,
            e,
        )
        return False


async def get_all_cluster_states(model_id: str) -> dict[str, dict[str, any]]:
    """
    Get all cluster states for a model from Redis.

    Args:
        model_id: The model identifier

    Returns:
        Dictionary mapping cluster_id to state dict with 'is_enabled' and
        'auto_adapt_capacity' keys
    """
    try:
        redis = get_redis_pool()
        pattern = f"{CLUSTER_STATE_PREFIX}{model_id}:*"
        keys = []
        async for key in redis.scan_iter(match=pattern):
            keys.append(key)

        if not keys:
            return {}

        states = {}
        values = await redis.mget(keys)
        for key, value in zip(keys, values):
            if value:
                # Extract cluster_id from key (format: cluster_state:model_id:cluster_id)
                cluster_id = key.split(":", 2)[2]
                state = json.loads(value)
                states[cluster_id] = {
                    "is_enabled": state.get("is_enabled", True),
                    "auto_adapt_capacity": state.get("auto_adapt_capacity", True),
                }

        return states
    except Exception as e:
        logger.warning(
            "Failed to get all cluster states from Redis: model=%s error=%s",
            model_id,
            e,
        )
        return {}


async def sync_cluster_states_to_redis(
    model_id: str, clusters: list[dict[str, any]]
) -> bool:
    """
    Sync all cluster states to Redis (used during initialization).

    Args:
        model_id: The model identifier
        clusters: List of cluster dicts with 'cluster_id', 'is_enabled', and
                 'auto_adapt_capacity' keys

    Returns:
        True if successful, False if Redis is unavailable
    """
    try:
        redis = get_redis_pool()
        pipe = redis.pipeline()
        for cluster in clusters:
            cluster_id = cluster["cluster_id"]
            is_enabled = cluster.get("is_enabled", True)
            auto_adapt_capacity = cluster.get("auto_adapt_capacity", True)
            key = _make_state_key(model_id, cluster_id)
            state = {"is_enabled": is_enabled, "auto_adapt_capacity": auto_adapt_capacity}
            pipe.set(key, json.dumps(state), ex=CLUSTER_STATE_TTL_SECONDS)
        await pipe.execute()
        logger.debug(
            "Synced %d cluster states to Redis for model=%s",
            len(clusters),
            model_id,
        )
        return True
    except Exception as e:
        logger.warning(
            "Failed to sync cluster states to Redis: model=%s error=%s",
            model_id,
            e,
        )
        return False


# ============================================================================
# Lua Script for Atomic Check-and-Reserve
# ============================================================================

# Lua script for atomic check-and-reserve operation
# This prevents race conditions where a cluster is disabled between the
# enablement check and token reservation.
# Returns: JSON-encoded result dict with 'success', 'inflight_tokens', and 'error'
# Note: METRICS_TTL_SECONDS is passed as ARGV[3] to make it configurable
CHECK_AND_RESERVE_SCRIPT_TEMPLATE = """
local cluster_state_key = KEYS[1]
local metrics_key = KEYS[2]
local tokens_to_reserve = tonumber(ARGV[1])
local max_capacity = tonumber(ARGV[2])
local metrics_ttl = tonumber(ARGV[3])

-- Get cluster state
local state_json = redis.call('GET', cluster_state_key)
local is_enabled = true  -- Default to enabled if no state

if state_json then
    local state = cjson.decode(state_json)
    if state.is_enabled ~= nil then
        is_enabled = state.is_enabled
    end
end

-- Check if cluster is enabled
if not is_enabled then
    return cjson.encode({
        success = false,
        inflight_tokens = 0,
        error = "cluster_disabled"
    })
end

-- Get current inflight tokens (default to 0)
local current_inflight = redis.call('GET', metrics_key)
if not current_inflight then
    current_inflight = 0
else
    current_inflight = tonumber(current_inflight)
end

-- Check if capacity is available
if current_inflight + tokens_to_reserve > max_capacity then
    return cjson.encode({
        success = false,
        inflight_tokens = current_inflight,
        error = "capacity_exceeded"
    })
end

-- Atomically reserve tokens
local new_inflight = redis.call('INCRBY', metrics_key, tokens_to_reserve)
redis.call('EXPIRE', metrics_key, metrics_ttl)

return cjson.encode({
    success = true,
    inflight_tokens = new_inflight,
    error = nil
})
"""


# ============================================================================
# Cluster Metrics Store
# ============================================================================

class ClusterMetricsStore:
    """
    Distributed storage for cluster runtime metrics using Redis.

    This class provides atomic operations for updating cluster metrics
    across multiple deployments, ensuring consistent state for:
    - Load balancing decisions (inflight_tokens, adaptive_capacity)
    - Performance tracking (ewma_ttft_ms, ewma_itl_ms)
    """

    def __init__(self, redis: Optional[aioredis.Redis] = None) -> None:
        """
        Initialize the metrics store.

        Args:
            redis: Redis client instance. If None, uses the global pool.
        """
        self._redis = redis
        self._check_and_reserve_script: Optional[aioredis.Script] = None

    def _get_redis(self) -> aioredis.Redis:
        """Get Redis client, using global pool if not provided."""
        if self._redis is None:
            return get_redis_pool()
        return self._redis

    async def _get_check_and_reserve_script(self) -> aioredis.Script:
        """Get or register the check-and-reserve Lua script."""
        if self._check_and_reserve_script is None:
            redis = self._get_redis()
            self._check_and_reserve_script = redis.register_script(
                CHECK_AND_RESERVE_SCRIPT_TEMPLATE
            )
        return self._check_and_reserve_script

    # ----------------------------------------------------------------------- #
    # Inflight tokens (atomic increment/decrement)
    # ----------------------------------------------------------------------- #

    async def reserve_tokens(self, model_id: str, cluster_id: str, tokens: int) -> int:
        """
        Atomically increment inflight tokens for a cluster.

        Args:
            model_id: The model identifier
            cluster_id: The cluster identifier
            tokens: Number of tokens to reserve

        Returns:
            int: New inflight token count after reservation
        """
        key = _make_metrics_key(model_id, cluster_id, INFLIGHT_TOKENS_SUFFIX)
        redis = self._get_redis()

        pipe = redis.pipeline()
        pipe.incrby(key, tokens)
        pipe.expire(key, METRICS_TTL_SECONDS)
        results = await pipe.execute()

        new_value = int(results[0])
        logger.debug(
            "Reserved %d tokens for %s/%s: new count = %d",
            tokens,
            model_id,
            cluster_id,
            new_value,
        )
        return new_value

    async def check_and_reserve_tokens(
        self,
        model_id: str,
        cluster_id: str,
        tokens: int,
        max_capacity: int,
    ) -> dict:
        """
        Atomically check cluster state and reserve tokens.

        This method uses a Lua script to perform the check-and-reserve
        operation atomically, preventing race conditions where a cluster
        could be disabled between the state check and token reservation.

        Args:
            model_id: The model identifier
            cluster_id: The cluster identifier
            tokens: Number of tokens to reserve
            max_capacity: Maximum capacity for the cluster

        Returns:
            dict: Result with keys:
                - success (bool): Whether reservation succeeded
                - inflight_tokens (int): Current inflight count (after reservation if success)
                - error (str): Error code if failed ('cluster_disabled' or 'capacity_exceeded')
        """
        state_key = _make_state_key(model_id, cluster_id)
        metrics_key = _make_metrics_key(model_id, cluster_id, INFLIGHT_TOKENS_SUFFIX)

        try:
            script = await self._get_check_and_reserve_script()
            result = await script(
                keys=[state_key, metrics_key],
                args=[str(tokens), str(max_capacity), str(METRICS_TTL_SECONDS)],
            )
            result_dict = json.loads(result)
            logger.debug(
                "Check-and-reserve for %s/%s: tokens=%d success=%s inflight=%d error=%s",
                model_id,
                cluster_id,
                tokens,
                result_dict.get("success"),
                result_dict.get("inflight_tokens", 0),
                result_dict.get("error"),
            )
            return result_dict
        except Exception as e:
            logger.error(
                "Check-and-reserve script failed for %s/%s: %s. "
                "Falling back to non-atomic reserve.",
                model_id,
                cluster_id,
                e,
            )
            # Fallback: try to reserve without atomic check
            # This is a safety measure if Redis script fails
            new_count = await self.reserve_tokens(model_id, cluster_id, tokens)
            return {
                "success": True,
                "inflight_tokens": new_count,
                "error": None,
            }

    async def release_tokens(self, model_id: str, cluster_id: str, tokens: int) -> int:
        """
        Atomically decrement inflight tokens for a cluster.

        The counter will not drop below 0.

        Args:
            model_id: The model identifier
            cluster_id: The cluster identifier
            tokens: Number of tokens to release

        Returns:
            int: New inflight token count after release (minimum 0)
        """
        key = _make_metrics_key(model_id, cluster_id, INFLIGHT_TOKENS_SUFFIX)
        redis = self._get_redis()

        # Use DECRBY and ensure we don't go below 0
        new_val = await redis.decrby(key, tokens)
        if new_val < 0:
            await redis.delete(key)
            return 0

        # Refresh TTL
        await redis.expire(key, METRICS_TTL_SECONDS)
        return int(new_val)

    async def get_inflight_tokens(self, model_id: str, cluster_id: str) -> int:
        """
        Get current inflight token count for a cluster.

        Args:
            model_id: The model identifier
            cluster_id: The cluster identifier

        Returns:
            int: Current inflight token count (0 if not set)
        """
        key = _make_metrics_key(model_id, cluster_id, INFLIGHT_TOKENS_SUFFIX)
        val = await self._get_redis().get(key)
        return int(val) if val is not None else 0

    # ----------------------------------------------------------------------- #
    # EWMA Latency metrics
    # ----------------------------------------------------------------------- #

    async def update_ewma_latency(
        self,
        model_id: str,
        cluster_id: str,
        ttft_ms: float,
        itl_ms: Optional[float],
        alpha_ttft: float = 0.3,
        alpha_itl: float = 0.3,
    ) -> tuple[float, float]:
        """
        Update EWMA latency metrics for a cluster.

        Uses Redis to atomically update the EWMA values across deployments.

        Args:
            model_id: The model identifier
            cluster_id: The cluster identifier
            ttft_ms: New TTFT measurement in milliseconds
            itl_ms: New ITL measurement in milliseconds (None if not available)
            alpha_ttft: Smoothing factor for TTFT (default 0.3)
            alpha_itl: Smoothing factor for ITL (default 0.3)

        Returns:
            tuple[float, float]: (new_ewma_ttft_ms, new_ewma_itl_ms)
        """
        redis = self._get_redis()
        ttft_key = _make_metrics_key(model_id, cluster_id, EWMA_TTFT_SUFFIX)
        itl_key = _make_metrics_key(model_id, cluster_id, EWMA_ITL_SUFFIX)
        has_samples_key = _make_metrics_key(model_id, cluster_id, HAS_SAMPLES_SUFFIX)

        # Get current values
        pipe = redis.pipeline()
        pipe.get(ttft_key)
        pipe.get(itl_key)
        pipe.get(has_samples_key)
        results = await pipe.execute()

        current_ttft = float(results[0]) if results[0] is not None else 0.0
        current_itl = float(results[1]) if results[1] is not None else 0.0
        has_samples = results[2] == "1"

        # Calculate new EWMA values
        if not has_samples:
            new_ttft = ttft_ms
            new_itl = itl_ms if itl_ms is not None else 0.0
            has_samples = True
        else:
            new_ttft = alpha_ttft * ttft_ms + (1 - alpha_ttft) * current_ttft
            if itl_ms is not None:
                new_itl = alpha_itl * itl_ms + (1 - alpha_itl) * current_itl
            else:
                new_itl = current_itl

        # Store updated values
        pipe = redis.pipeline()
        pipe.set(ttft_key, str(new_ttft), ex=METRICS_TTL_SECONDS)
        pipe.set(itl_key, str(new_itl), ex=METRICS_TTL_SECONDS)
        pipe.set(has_samples_key, "1", ex=METRICS_TTL_SECONDS)
        await pipe.execute()

        logger.debug(
            "Updated EWMA for %s/%s: TTFT=%.2f→%.2f ms, ITL=%.2f→%.2f ms",
            model_id,
            cluster_id,
            current_ttft,
            new_ttft,
            current_itl,
            new_itl,
        )
        return new_ttft, new_itl

    async def get_ewma_latency(
        self, model_id: str, cluster_id: str
    ) -> tuple[float, float, bool]:
        """
        Get current EWMA latency metrics for a cluster.

        Args:
            model_id: The model identifier
            cluster_id: The cluster identifier

        Returns:
            tuple[float, float, bool]: (ewma_ttft_ms, ewma_itl_ms, has_samples)
        """
        redis = self._get_redis()
        ttft_key = _make_metrics_key(model_id, cluster_id, EWMA_TTFT_SUFFIX)
        itl_key = _make_metrics_key(model_id, cluster_id, EWMA_ITL_SUFFIX)
        has_samples_key = _make_metrics_key(model_id, cluster_id, HAS_SAMPLES_SUFFIX)

        pipe = redis.pipeline()
        pipe.get(ttft_key)
        pipe.get(itl_key)
        pipe.get(has_samples_key)
        results = await pipe.execute()

        ttft = float(results[0]) if results[0] is not None else 0.0
        itl = float(results[1]) if results[1] is not None else 0.0
        has_samples = results[2] == "1"

        return ttft, itl, has_samples

    # ----------------------------------------------------------------------- #
    # Adaptive capacity
    # ----------------------------------------------------------------------- #

    async def set_adaptive_capacity(
        self, model_id: str, cluster_id: str, capacity: int
    ) -> None:
        """
        Set the adaptive capacity for a cluster.

        Args:
            model_id: The model identifier
            cluster_id: The cluster identifier
            capacity: New adaptive capacity value
        """
        key = _make_metrics_key(model_id, cluster_id, ADAPTIVE_CAPACITY_SUFFIX)
        await self._get_redis().set(key, str(capacity), ex=METRICS_TTL_SECONDS)
        logger.debug(
            "Set adaptive capacity for %s/%s: %d", model_id, cluster_id, capacity
        )

    async def get_adaptive_capacity(
        self, model_id: str, cluster_id: str, default_capacity: int
    ) -> int:
        """
        Get the adaptive capacity for a cluster.

        Args:
            model_id: The model identifier
            cluster_id: The cluster identifier
            default_capacity: Default value if not set

        Returns:
            int: Current adaptive capacity
        """
        key = _make_metrics_key(model_id, cluster_id, ADAPTIVE_CAPACITY_SUFFIX)
        val = await self._get_redis().get(key)
        if val is None:
            # Initialize with default capacity
            await self.set_adaptive_capacity(model_id, cluster_id, default_capacity)
            return default_capacity
        return int(val)

    # ----------------------------------------------------------------------- #
    # Bulk operations for admin/monitoring
    # ----------------------------------------------------------------------- #

    async def get_all_metrics(self, model_id: str, cluster_id: str) -> dict:
        """
        Get all runtime metrics for a cluster.

        Args:
            model_id: The model identifier
            cluster_id: The cluster identifier

        Returns:
            dict: All metrics for the cluster
        """
        redis = self._get_redis()

        keys = {
            "inflight_tokens": _make_metrics_key(model_id, cluster_id, INFLIGHT_TOKENS_SUFFIX),
            "ewma_ttft_ms": _make_metrics_key(model_id, cluster_id, EWMA_TTFT_SUFFIX),
            "ewma_itl_ms": _make_metrics_key(model_id, cluster_id, EWMA_ITL_SUFFIX),
            "adaptive_capacity": _make_metrics_key(
                model_id, cluster_id, ADAPTIVE_CAPACITY_SUFFIX
            ),
            "has_samples": _make_metrics_key(model_id, cluster_id, HAS_SAMPLES_SUFFIX),
        }

        pipe = redis.pipeline()
        for key in keys.values():
            pipe.get(key)
        results = await pipe.execute()

        return {
            "inflight_tokens": int(results[0]) if results[0] is not None else 0,
            "ewma_ttft_ms": float(results[1]) if results[1] is not None else 0.0,
            "ewma_itl_ms": float(results[2]) if results[2] is not None else 0.0,
            "adaptive_capacity": int(results[3]) if results[3] is not None else 0,
            "has_samples": results[4] == "1" if results[4] is not None else False,
        }

    async def reset_metrics(self, model_id: str, cluster_id: str) -> None:
        """
        Reset all metrics for a cluster (useful for testing or manual reset).

        Args:
            model_id: The model identifier
            cluster_id: The cluster identifier
        """
        redis = self._get_redis()

        keys = [
            _make_metrics_key(model_id, cluster_id, INFLIGHT_TOKENS_SUFFIX),
            _make_metrics_key(model_id, cluster_id, EWMA_TTFT_SUFFIX),
            _make_metrics_key(model_id, cluster_id, EWMA_ITL_SUFFIX),
            _make_metrics_key(model_id, cluster_id, ADAPTIVE_CAPACITY_SUFFIX),
            _make_metrics_key(model_id, cluster_id, HAS_SAMPLES_SUFFIX),
        ]

        await redis.delete(*keys)
        logger.info("Reset metrics for %s/%s", model_id, cluster_id)


# ============================================================================
# Convenience Exports
# ============================================================================

# Module-level singleton for cluster metrics store
cluster_metrics_store = ClusterMetricsStore()
