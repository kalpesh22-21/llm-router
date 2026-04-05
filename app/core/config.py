"""
Core configuration module.

Loads environment variables via Pydantic Settings and parses models.yaml
to produce a list of model backend configurations.

Supported model entry shapes in models.yaml:

  # Static (Phase 1) — single URL
  - model_id: llama-3-70b
    base_url: http://llama-server:8000/v1
    api_key: ""

  # Multi-cluster (Phase 2) — N URLs with routing config
  - model_id: llama-3-70b
    type: multi_cluster
    clusters:
      - cluster_id: us-west
        base_url: http://llama-us-west.internal/v1
        api_key: ""
    routing:
      local_cluster_id: us-west   # or omit to use ROUTER_CLUSTER_ID env var
      spill_ratio: 0.8
      ...
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache
from typing import List, Optional, Union

import yaml
from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ModelConfig:
    """Configuration for a single LLM backend model (static/Phase 1)."""

    def __init__(self, model_id: str, base_url: str, api_key: str) -> None:
        self.model_id = model_id
        # Strip trailing slash so we can always append /endpoint cleanly
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key

    def __repr__(self) -> str:  # pragma: no cover
        return f"ModelConfig(model_id={self.model_id!r}, base_url={self.base_url!r})"


@dataclass
class ClusterConfig:
    """Configuration for one cluster endpoint within a multi-cluster model."""

    cluster_id: str
    base_url: str
    api_key: str


@dataclass
class RoutingConfig:
    """Routing tuning knobs for a MultiClusterBackend."""

    # Which cluster is "local" for this router instance.
    # Falls back to ROUTER_CLUSTER_ID env var if not set in yaml.
    local_cluster_id: str = ""

    # Spill to remotes when local load_ratio exceeds this
    spill_ratio: float = 0.8

    # Starting capacity estimate before enough data is observed
    seed_capacity_tokens: int = 200_000

    # Used when the request body omits max_tokens
    default_max_tokens: int = 2048

    # How long a cache entry is valid (seconds)
    cache_ttl_seconds: int = 3600

    # Latency budgets — the threshold above which the cluster is "stressed"
    ttft_budget_ms: float = 500.0
    itl_budget_ms: float = 80.0

    # EWMA smoothing factors
    ttft_alpha: float = 0.1   # slow — queue depth changes gradually
    itl_alpha: float = 0.2    # faster — decode saturation shows up quickly
    capacity_alpha: float = 0.1  # slow upward drift when healthy


@dataclass
class MultiClusterModelConfig:
    """Configuration for a multi-cluster LLM backend (Phase 2)."""

    model_id: str
    clusters: List[ClusterConfig]
    routing: RoutingConfig


class Settings(BaseSettings):
    """Application settings loaded from environment variables / .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Database
    DATABASE_URL: str = "mysql+aiomysql://root:password@localhost:3306/llm_router"

    # Redis
    REDIS_URL: str = "redis://localhost:6379/0"

    # JWT / Auth
    SECRET_KEY: str = "change-me-in-production-use-long-random-string"
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRE_MINUTES: int = 60

    # LDAP
    LDAP_SERVER: str = "ldap://ldap.example.com"
    LDAP_BASE_DN: str = "dc=example,dc=com"
    LDAP_BIND_DN: str = "cn=service,dc=example,dc=com"
    LDAP_BIND_PASSWORD: str = "service-password"

    # Super-user bootstrap
    SUPERUSER_USERNAME: str = "admin"

    # Model config file path (relative to CWD or absolute)
    MODELS_CONFIG_PATH: str = "config/models.yaml"

    # Prometheus
    PROMETHEUS_PORT: int = 9090

    # Fallback API key for backends whose api_key field is left empty
    API_KEY: str = ""

    # Identifies which cluster this router instance belongs to.
    # Used as the default local_cluster_id for multi-cluster models when
    # routing.local_cluster_id is not specified in models.yaml.
    ROUTER_CLUSTER_ID: str = ""

    # ------------------------------------------------------------------ #
    # Derived helpers
    # ------------------------------------------------------------------ #

    def load_model_configs(self) -> List[Union[ModelConfig, MultiClusterModelConfig]]:
        """
        Parse MODELS_CONFIG_PATH and return one config object per model.

        Static models (no `type` field or `type: static`) → ModelConfig.
        Multi-cluster models (`type: multi_cluster`)       → MultiClusterModelConfig.

        Empty api_key fields fall back to the global API_KEY env var.
        """
        path = self.MODELS_CONFIG_PATH
        if not os.path.isabs(path):
            path = os.path.join(os.getcwd(), path)

        with open(path, "r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)

        configs: List[Union[ModelConfig, MultiClusterModelConfig]] = []
        for entry in raw.get("models", []):
            backend_type = entry.get("type", "static")

            if backend_type == "multi_cluster":
                configs.append(self._parse_multi_cluster(entry))
            else:
                api_key = entry.get("api_key", "") or self.API_KEY
                configs.append(
                    ModelConfig(
                        model_id=entry["model_id"],
                        base_url=entry["base_url"],
                        api_key=api_key,
                    )
                )
        return configs

    def _parse_multi_cluster(self, entry: dict) -> MultiClusterModelConfig:
        """Parse a single multi_cluster model entry from models.yaml."""
        model_id: str = entry["model_id"]

        clusters: List[ClusterConfig] = []
        for c in entry.get("clusters", []):
            clusters.append(
                ClusterConfig(
                    cluster_id=c["cluster_id"],
                    base_url=c["base_url"].rstrip("/"),
                    api_key=c.get("api_key", "") or self.API_KEY,
                )
            )

        r = entry.get("routing", {})
        local_cluster_id = (
            r.get("local_cluster_id", "")
            or self.ROUTER_CLUSTER_ID
        )
        routing = RoutingConfig(
            local_cluster_id=local_cluster_id,
            spill_ratio=float(r.get("spill_ratio", 0.8)),
            seed_capacity_tokens=int(r.get("seed_capacity_tokens", 200_000)),
            default_max_tokens=int(r.get("default_max_tokens", 2048)),
            cache_ttl_seconds=int(r.get("cache_ttl_seconds", 3600)),
            ttft_budget_ms=float(r.get("ttft_budget_ms", 500.0)),
            itl_budget_ms=float(r.get("itl_budget_ms", 80.0)),
            ttft_alpha=float(r.get("ttft_alpha", 0.1)),
            itl_alpha=float(r.get("itl_alpha", 0.2)),
            capacity_alpha=float(r.get("capacity_alpha", 0.1)),
        )
        return MultiClusterModelConfig(
            model_id=model_id,
            clusters=clusters,
            routing=routing,
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the singleton Settings instance (cached after first call)."""
    return Settings()


# Convenience module-level alias
settings = get_settings()
