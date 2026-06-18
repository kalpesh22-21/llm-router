"""
INFRASTRUCTURE Configuration Management

This module stores STATIC infrastructure configuration for clusters:
- base_url, api_key, weight, number_of_replicas
- seed_capacity_tokens_per_pod, timeout

NOTE: Operational flags (is_enabled, auto_adapt_capacity) are managed
separately in cluster_operational_state.py

Key pattern: cluster_config:{model_id}:{cluster_id}

Features:
    - Full config storage: base_url, api_key, weight, number_of_replicas, etc.
    - Timestamp-based tracking with 60-second TTL
    - Simple set/get operations (no optimistic locking)
    - TTL-based expiration for automatic cleanup

The configuration is stored in Redis with a 60-second TTL. Pods refresh
configuration by re-fetching from Redis periodically. If Redis is unavailable,
the system falls back to local configuration.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from app.db.redis_client import get_redis_pool

logger = logging.getLogger(__name__)

# ============================================================================
# Constants
# ============================================================================

CLUSTER_CONFIG_PREFIX = "cluster_config:"

# TTL for cluster config keys (60 seconds)
CLUSTER_CONFIG_TTL_SECONDS = 60

# Default values
DEFAULT_TIMEOUT = 30.0


# ============================================================================
# Dataclasses
# ============================================================================


@dataclass
class ClusterConfig:
    """
    Infrastructure cluster configuration for distributed storage.

    This class contains STATIC infrastructure configuration only.
    Operational flags (is_enabled, auto_adapt_capacity) are managed
    separately in cluster_operational_state.py.

    Attributes:
        model_id: The model identifier
        cluster_id: The cluster identifier
        base_url: Base URL for the cluster endpoint
        api_key: Optional API key for authentication
        weight: Routing weight (higher = more traffic)
        number_of_replicas: Number of pod replicas in the cluster
        seed_capacity_tokens_per_pod: Base capacity per pod in tokens
        timeout: Request timeout in seconds
        updated_at: Timestamp of last update
    """

    model_id: str
    cluster_id: str
    base_url: str
    api_key: Optional[str]
    weight: int
    number_of_replicas: int
    seed_capacity_tokens_per_pod: int
    timeout: float
    updated_at: datetime

    def to_dict(self) -> dict[str, Any]:
        """Convert config to dictionary for JSON serialization."""
        return {
            "model_id": self.model_id,
            "cluster_id": self.cluster_id,
            "base_url": self.base_url,
            "api_key": self.api_key,
            "weight": self.weight,
            "number_of_replicas": self.number_of_replicas,
            "seed_capacity_tokens_per_pod": self.seed_capacity_tokens_per_pod,
            "timeout": self.timeout,
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ClusterConfig":
        """Create config from dictionary."""
        return cls(
            model_id=data["model_id"],
            cluster_id=data["cluster_id"],
            base_url=data["base_url"],
            api_key=data.get("api_key"),
            weight=data.get("weight", 1),
            number_of_replicas=data.get("number_of_replicas", 1),
            seed_capacity_tokens_per_pod=data.get(
                "seed_capacity_tokens_per_pod", 40000
            ),
            timeout=data.get("timeout", DEFAULT_TIMEOUT),
            updated_at=datetime.fromisoformat(data["updated_at"]),
        )

    @classmethod
    def create(
        cls,
        model_id: str,
        cluster_id: str,
        base_url: str,
        api_key: Optional[str] = None,
        weight: int = 1,
        number_of_replicas: int = 1,
        seed_capacity_tokens_per_pod: int = 40000,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> "ClusterConfig":
        """Create a new config with current timestamp."""
        return cls(
            model_id=model_id,
            cluster_id=cluster_id,
            base_url=base_url,
            api_key=api_key,
            weight=weight,
            number_of_replicas=number_of_replicas,
            seed_capacity_tokens_per_pod=seed_capacity_tokens_per_pod,
            timeout=timeout,
            updated_at=datetime.utcnow(),
        )


# ============================================================================
# Key Generation Helpers
# ============================================================================


def _make_config_key(model_id: str, cluster_id: str) -> str:
    """Generate the Redis key for cluster config."""
    return f"{CLUSTER_CONFIG_PREFIX}{model_id}:{cluster_id}"


# ============================================================================
# CRUD Operations
# ============================================================================


async def set_cluster_config(
    model_id: str,
    cluster_id: str,
    config_data: dict[str, Any],
    ttl: int = CLUSTER_CONFIG_TTL_SECONDS,
) -> ClusterConfig:
    """
    Set cluster configuration in Redis with timestamp.

    Args:
        model_id: The model identifier
        cluster_id: The cluster identifier
        config_data: Dictionary with config fields to set
        ttl: Time-to-live for the config key in seconds

    Returns:
        ClusterConfig: The new config with current timestamp
    """
    redis = get_redis_pool()
    config_key = _make_config_key(model_id, cluster_id)

    try:
        updated_at = datetime.utcnow()

        # Build complete config dict (infrastructure config only)
        config_dict = {
            "model_id": model_id,
            "cluster_id": cluster_id,
            "base_url": config_data.get("base_url", ""),
            "api_key": config_data.get("api_key"),
            "weight": config_data.get("weight", 1),
            "number_of_replicas": config_data.get("number_of_replicas", 1),
            "seed_capacity_tokens_per_pod": config_data.get(
                "seed_capacity_tokens_per_pod", 40000
            ),
            "timeout": config_data.get("timeout", DEFAULT_TIMEOUT),
            "updated_at": updated_at.isoformat(),
        }

        # Set config with TTL
        await redis.set(config_key, json.dumps(config_dict), ex=ttl)

        logger.debug(
            "Set cluster config: model=%s cluster=%s ttl=%ds",
            model_id,
            cluster_id,
            ttl,
        )

        return ClusterConfig.from_dict(config_dict)

    except Exception as e:
        logger.error(
            "Failed to set cluster config in Redis: model=%s cluster=%s error=%s",
            model_id,
            cluster_id,
            e,
        )
        raise


async def get_cluster_config(model_id: str, cluster_id: str) -> Optional[ClusterConfig]:
    """
    Get cluster configuration from Redis.

    Args:
        model_id: The model identifier
        cluster_id: The cluster identifier

    Returns:
        ClusterConfig if found, None otherwise
    """
    try:
        redis = get_redis_pool()
        config_key = _make_config_key(model_id, cluster_id)
        value = await redis.get(config_key)

        if value is None:
            return None

        config_dict = json.loads(value)
        return ClusterConfig.from_dict(config_dict)

    except Exception as e:
        logger.warning(
            "Failed to get cluster config from Redis: model=%s cluster=%s error=%s",
            model_id,
            cluster_id,
            e,
        )
        return None



async def get_all_cluster_configs_for_model(
    model_id: str,
) -> list[ClusterConfig]:
    """
    Get all cluster configurations for a model from Redis.

    Args:
        model_id: The model identifier

    Returns:
        List of ClusterConfig objects
    """
    try:
        redis = get_redis_pool()
        pattern = f"{CLUSTER_CONFIG_PREFIX}{model_id}:*"
        keys = []

        async for key in redis.scan_iter(match=pattern):
            keys.append(key)

        if not keys:
            return []

        configs = []
        values = await redis.mget(keys)

        for key, value in zip(keys, values):
            if value:
                try:
                    config_dict = json.loads(value)
                    configs.append(ClusterConfig.from_dict(config_dict))
                except (json.JSONDecodeError, KeyError) as e:
                    logger.warning(
                        "Failed to parse cluster config for key %s: %s", key, e
                    )

        return configs

    except Exception as e:
        logger.warning(
            "Failed to get all cluster configs from Redis: model=%s error=%s",
            model_id,
            e,
        )
        return []


async def update_cluster_config_field(
    model_id: str,
    cluster_id: str,
    field: str,
    value: Any,
) -> ClusterConfig:
    """
    Update a single field in cluster configuration.

    Args:
        model_id: The model identifier
        cluster_id: The cluster identifier
        field: Field name to update
        value: New value for the field

    Returns:
        Updated ClusterConfig

    Raises:
        ValueError: If field is not valid
    """
    valid_fields = {
        "base_url",
        "api_key",
        "weight",
        "number_of_replicas",
        "seed_capacity_tokens_per_pod",
        "timeout",
    }

    if field not in valid_fields:
        raise ValueError(f"Invalid field: {field}. Must be one of: {valid_fields}")

    # Get current config
    current = await get_cluster_config(model_id, cluster_id)

    if current is None:
        raise ValueError(
            f"Cluster config not found: model={model_id} cluster={cluster_id}"
        )

    # Build update dict
    config_data = current.to_dict()
    config_data[field] = value

    # Remove metadata fields that shouldn't be passed to set_cluster_config
    config_data.pop("updated_at", None)

    return await set_cluster_config(model_id, cluster_id, config_data)


async def sync_cluster_configs_to_redis(
    model_id: str, configs: list[dict[str, Any]]
) -> bool:
    """
    Sync multiple cluster configurations to Redis.

    Used during initialization to bulk-load configurations.

    Args:
        model_id: The model identifier
        configs: List of config dictionaries

    Returns:
        True if successful, False if Redis is unavailable
    """
    try:
        redis = get_redis_pool()
        pipe = redis.pipeline()

        for config_data in configs:
            cluster_id = config_data["cluster_id"]
            config_key = _make_config_key(model_id, cluster_id)

            # Build complete config (infrastructure config only)
            complete_config = {
                "model_id": model_id,
                "cluster_id": cluster_id,
                "base_url": config_data.get("base_url", ""),
                "api_key": config_data.get("api_key"),
                "weight": config_data.get("weight", 1),
                "number_of_replicas": config_data.get("number_of_replicas", 1),
                "seed_capacity_tokens_per_pod": config_data.get(
                    "seed_capacity_tokens_per_pod", 40000
                ),
                "timeout": config_data.get("timeout", DEFAULT_TIMEOUT),
                "updated_at": config_data.get(
                    "updated_at", datetime.utcnow().isoformat()
                ),
            }

            pipe.set(
                config_key, json.dumps(complete_config), ex=CLUSTER_CONFIG_TTL_SECONDS
            )

        await pipe.execute()

        logger.debug(
            "Synced %d cluster configs to Redis for model=%s",
            len(configs),
            model_id,
        )
        return True

    except Exception as e:
        logger.warning(
            "Failed to sync cluster configs to Redis: model=%s error=%s",
            model_id,
            e,
        )
        return False


# ============================================================================
# Convenience Exports
# ============================================================================


# Module-level helpers for common operations
async def get_config_or_default(
    model_id: str, cluster_id: str, base_url: str = "", api_key: Optional[str] = None
) -> ClusterConfig:
    """
    Get config from Redis or create a default if not found.

    Args:
        model_id: The model identifier
        cluster_id: The cluster identifier
        base_url: Default base URL if config not found
        api_key: Default API key if config not found

    Returns:
        ClusterConfig (from Redis or newly created)
    """
    config = await get_cluster_config(model_id, cluster_id)
    if config is not None:
        return config

    # Create default config
    default_config = ClusterConfig.create(
        model_id=model_id,
        cluster_id=cluster_id,
        base_url=base_url,
        api_key=api_key,
    )

    # Try to store it (but don't fail if Redis is unavailable)
    try:
        await set_cluster_config(model_id, cluster_id, default_config.to_dict())
    except Exception as e:
        logger.debug("Could not store default config (Redis unavailable): %s", e)

    return default_config
