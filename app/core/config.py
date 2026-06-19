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

import logging
import os
from dataclasses import Field, dataclass, field
from functools import lru_cache
from typing import Annotated, List, Optional, Union

import yaml
from pydantic import (
    AfterValidator,
    Field,
    field_validator,
)
from pydantic_settings import BaseSettings

# paycompy is an internal package, only needed when VAULT_ENABLED=true. Guard the
# import so the app boots without it (e.g. local/dev, or any env using plain env
# vars instead of Vault). load_settings_from_vault() raises clearly if Vault is
# enabled but paycompy is missing.
try:
    from paycompy import vault
except ImportError:
    vault = None

from app.utils.api_key_validator import find_working_api_key

logger = logging.getLogger(__name__)


def _strip_config_value(value: str) -> str:
    """
    Trim surrounding whitespace/newlines and remove any literal ``\\n`` escape
    sequences from a config string.

    NB: the previous implementation used ``value.strip("\\n")``, which strips the
    *characters* ``\\`` and ``n`` from the ends — silently corrupting any value
    ending in ``n`` (e.g. ``"admin"`` -> ``"admi"``). This removes the two-char
    ``\\n`` sequence and trims real whitespace instead.
    """
    return value.replace("\\n", "").strip()


StrippedString = Annotated[str, AfterValidator(_strip_config_value)]


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
    number_of_replicas: int = 1  # Number of replicas for capacity calculation
    is_enabled: bool = True  # Whether cluster is enabled
    auto_adapt_capacity: bool = True  # Whether to auto-adapt capacity
    priority: int = 0  # Priority rank for routing (higher = preferred)


@dataclass
class RoutingConfig:
    """Routing tuning knobs for a MultiClusterBackend."""

    # Which cluster is "local" for this router instance.
    # Falls back to ROUTER_CLUSTER_ID env var if not set in yaml.
    local_cluster_id: str = ""

    # Spill to remotes when local load_ratio exceeds this
    spill_ratio: float = 0.8

    # Starting capacity estimate per pod before enough data is observed
    seed_capacity_tokens_per_pod: int = 40_000

    # Used when the request body omits max_tokens
    default_max_tokens: int = 2048

    # How long a cache entry is valid (seconds)
    cache_ttl_seconds: int = 3600

    # Latency budgets — the threshold above which the cluster is "stressed"
    ttft_budget_ms: float = 500.0
    itl_budget_ms: float = 80.0

    # EWMA smoothing factors
    ttft_alpha: float = 0.1  # slow — queue depth changes gradually
    itl_alpha: float = 0.2  # faster — decode saturation shows up quickly
    capacity_alpha: float = 0.1  # slow upward drift when healthy


@dataclass
class MultiClusterModelConfig:
    """Configuration for a multi-cluster LLM backend (Phase 2)."""

    model_id: str
    clusters: List[ClusterConfig]
    routing: RoutingConfig


# Known-insecure placeholder for JWT_SECRET_KEY. If Vault is enabled but the
# resolved secret is still this value (e.g. a transient Vault failure fell back
# to the default), the app must refuse to boot rather than serve with a public,
# forgeable signing key. See validate_vault_secrets() / load_settings_from_vault().
_INSECURE_JWT_SECRET_DEFAULT = "change-me-in-production-use-long-random-string"


class Settings(BaseSettings):
    class Config:
        env_file = os.getenv(
            "LOCAL_ENV_PATH",
            "/mnt/nfsdata/shreyan/gitlab/llm-router/develop/.env",
        )
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "ignore"
        # Allow environment variables to override defaults
        env_override_prefix = ""

    PROJECT_NAME: str = Field(
        default="LLM ROUTER",
        description="Name of the project",
        env="PROJECT_NAME",
    )
    # Database
    DATABASE_USER: StrippedString = Field(
        default="root",
        description="Database username",
        env="DATABASE_USER",
    )

    DATABASE_PASSWORD: StrippedString = Field(
        default="password",
        description="Database password",
        env="DATABASE_PASSWORD",
    )

    DATABASE_HOST: StrippedString = Field(
        default="localhost",
        description="Database host",
        env="DATABASE_HOST",
    )

    DATABASE_PORT: int = Field(
        default="3306",
        description="Database port",
        env="DATABASE_PORT",
    )

    DATABASE_NAME: StrippedString = Field(
        default="llm_router",
        description="Database name",
        env="DATABASE_NAME",
    )
    DATABASE_DRIVER: StrippedString = Field(
        default="postgresql+psycopg",
        description="Database name",
        env="DATABASE_DRIVER",
    )

    # Redis
    REDIS_HOST: StrippedString = Field(
        default="localhost",
        description="Redis connection URL",
        env="REDIS_HOST",
    )
    REDIS_PORT: int = Field(
        default=6379,
        description="Redis connection URL",
        env="REDIS_PORT",
    )

    # JWT / Auth
    JWT_SECRET_KEY: StrippedString = Field(
        default=_INSECURE_JWT_SECRET_DEFAULT,
        description="Secret key for JWT token generation",
        env="JWT_SECRET_KEY",
    )
    JWT_ALGORITHM: StrippedString = Field(
        default="HS256", description="JWT algorithm", env="JWT_ALGORITHM"
    )
    JWT_EXPIRE_MINUTES: int = Field(
        default=60,
        description="JWT token expiration time in minutes",
        env="JWT_EXPIRE_MINUTES",
    )

    # Super-user bootstrap
    SUPERUSER_USERNAME: StrippedString = Field(
        default="admin", description="Superuser username", env="SUPERUSER_USERNAME"
    )
    ADMIN_INITIAL_PASSWORD: StrippedString = Field(
        default="",
        description="Initial password for default admin user (created on first startup if no users exist)",
        env="ADMIN_INITIAL_PASSWORD",
    )
    # Model config file path (relative to CWD or absolute)
    MODELS_CONFIG_PATH: StrippedString = Field(
        default="/config/models.yaml",
        description="Path to models configuration file",
        env="MODELS_CONFIG_PATH",
    )
    # Prometheus
    PROMETHEUS_PORT: int = Field(
        default=9090, description="Prometheus metrics port", env="PROMETHEUS_PORT"
    )
    # Fallback API key for backends whose api_key field is left empty
    API_KEY: str = Field(
        default="EMPTY",
        description="Fallback API key for model backends",
        env="API_KEY",
    )

    VAULT_API_KEYs: dict = Field(
        default={}, description="Fallback API key for model backends"
    )

    # Pheonix
    PHEONIX_API_KEY: StrippedString = Field(
        default="",
        description="Fallback API key for model backends",
        env="PHEONIX_API_KEY",
    )
    PHOENIX_TRACING_ENABLED: bool = Field(
        default=False,
        description="Enable Pheonix Tracing",
        env="PHOENIX_TRACING_ENABLED",
    )
    PHOENIX_COLLECTOR_ENDPOINT: StrippedString = Field(
        default="https://arize-phoenix.api.devdatasci.dc2.paycomhq.com",
        description="Pheonix endpoint",
        env="PHOENIX_COLLECTOR_ENDPOINT",
    )

    ALLOWED_GROUPS: List[str] = Field(
        default=[],
        description="Allowed LDAP groups",
        env="ALLOWED_GROUPS",
    )
    ALLOWED_INDIVIUALS: List[str] = Field(
        default=[],
        description="Allowed LDAP Indivuals",
        env="ALLOWED_INDIVIUALS",
    )

    # LDAP Service Account for user verification
    LDAP_SERVICE_USERNAME: StrippedString = Field(
        default="",
        description="LDAP service account username for user verification",
        env="LDAP_SERVICE_USERNAME",
    )

    DOCS_ENABLED: bool = Field(
        default=False,
        description="Enable Swagger UI (/docs) and ReDoc (/redoc) endpoints",
        env="DOCS_ENABLED",
    )
    LDAP_SERVICE_PASSWORD: StrippedString = Field(
        default="",
        description="LDAP service account password for user verification",
        env="LDAP_SERVICE_PASSWORD",
    )

    @field_validator("ALLOWED_GROUPS", "ALLOWED_INDIVIUALS", mode="before")
    @classmethod
    def split_allowed_groups(cls, v):
        if v == "":
            return []
        if isinstance(v, str):
            return [x.strip() for x in v.split(",")]
        return v

    CONCURRENT_TTL_SECONDS: int = Field(
        default=300,
        description="TTL (seconds) applied every time the counter is touched.",
        env="CONCURRENT_TTL_SECONDS",
    )

    # Discovery / health-probe service (static backends only)
    DISCOVERY_ENABLED: bool = Field(
        default=True,
        description="Enable the background DiscoveryService that probes static backends.",
        env="DISCOVERY_ENABLED",
    )
    DISCOVERY_PROBE_INTERVAL_SECONDS: int = Field(
        default=15,
        description="Seconds between background availability probes per backend.",
        env="DISCOVERY_PROBE_INTERVAL_SECONDS",
    )
    DISCOVERY_FAILURE_THRESHOLD: int = Field(
        default=2,
        description="Consecutive failed probes before a backend is marked unavailable.",
        env="DISCOVERY_FAILURE_THRESHOLD",
    )

    # Discovery source mode: where backends come from.
    #   config     — static models.yaml only (default, backward compatible)
    #   kubernetes — scan K8s namespaces for labelled Services
    #   both       — static models.yaml AND discovered K8s services
    DISCOVERY_MODE: StrippedString = Field(
        default="config",
        description="Backend discovery source: config | kubernetes | both.",
        env="DISCOVERY_MODE",
    )

    # --- Kubernetes service discovery (DISCOVERY_MODE in {kubernetes, both}) --- #
    DISCOVERY_K8S_NAMESPACES: StrippedString = Field(
        default="default",
        description="Comma-separated list of namespaces to scan for model Services.",
        env="DISCOVERY_K8S_NAMESPACES",
    )
    DISCOVERY_K8S_LABEL_SELECTOR: StrippedString = Field(
        default="llm-router/enabled=true",
        description="Label selector identifying Services to register as backends.",
        env="DISCOVERY_K8S_LABEL_SELECTOR",
    )
    DISCOVERY_K8S_MODEL_ID_LABEL: StrippedString = Field(
        default="llm-router/model-id",
        description="Service label whose value is the model_id.",
        env="DISCOVERY_K8S_MODEL_ID_LABEL",
    )
    DISCOVERY_K8S_PORT_LABEL: StrippedString = Field(
        default="llm-router/port",
        description="Optional per-Service label overriding DISCOVERY_DEFAULT_PORT.",
        env="DISCOVERY_K8S_PORT_LABEL",
    )
    DISCOVERY_K8S_SCHEME_LABEL: StrippedString = Field(
        default="llm-router/scheme",
        description="Optional per-Service label overriding DISCOVERY_SCHEME.",
        env="DISCOVERY_K8S_SCHEME_LABEL",
    )
    DISCOVERY_K8S_PATH_LABEL: StrippedString = Field(
        default="llm-router/path",
        description="Optional per-Service label overriding DISCOVERY_PATH.",
        env="DISCOVERY_K8S_PATH_LABEL",
    )
    DISCOVERY_DEFAULT_PORT: int = Field(
        default=8000,
        description="Default port for a discovered Service's endpoint.",
        env="DISCOVERY_DEFAULT_PORT",
    )
    DISCOVERY_SCHEME: StrippedString = Field(
        default="http",
        description="Default scheme for a discovered Service's endpoint.",
        env="DISCOVERY_SCHEME",
    )
    DISCOVERY_PATH: StrippedString = Field(
        default="/v1",
        description="Default path suffix for a discovered Service's endpoint.",
        env="DISCOVERY_PATH",
    )
    DISCOVERY_K8S_CLUSTER_DOMAIN: StrippedString = Field(
        default="svc.cluster.local",
        description="Cluster DNS domain; URL = <scheme>://<svc>.<ns>.<domain>:<port><path>.",
        env="DISCOVERY_K8S_CLUSTER_DOMAIN",
    )
    DISCOVERY_K8S_RESYNC_SECONDS: int = Field(
        default=30,
        description="Seconds between Kubernetes Service re-list/reconcile passes.",
        env="DISCOVERY_K8S_RESYNC_SECONDS",
    )
    DISCOVERY_K8S_KUBECONFIG: StrippedString = Field(
        default="",
        description="Path to a kubeconfig (dev); empty uses in-cluster ServiceAccount.",
        env="DISCOVERY_K8S_KUBECONFIG",
    )
    DISCOVERY_K8S_FAKE_SERVICES_FILE: StrippedString = Field(
        default="",
        description="Test/E2E only: JSON file of fake Services to list instead of K8s.",
        env="DISCOVERY_K8S_FAKE_SERVICES_FILE",
    )

    CONTENT_FILTER_REFRESH: int = Field(
        default=60,
        description="Refresh frequency in seconds for filters",
        env="CONTENT_FILTER_REFRESH",
    )

    # TTL for cluster state keys (7 days)
    # This ensures state persists across pod restarts but doesn't accumulate forever
    CLUSTER_STATE_TTL_SECONDS: int = Field(
        default=60,
        description="TTL (seconds) for cluster state keys in Redis",
        env="CLUSTER_STATE_TTL_SECONDS",
    )

    # Default TTL for metrics (24 hours) - metrics should be refreshed regularly
    METRICS_TTL_SECONDS: int = Field(
        default=300,
        description="TTL (seconds) for metrics data in Redis",
        env="METRICS_TTL_SECONDS",
    )

    # Vault Configuration
    VAULT_ENABLED: bool = Field(
        default=False,
        description="Enable HashiCorp Vault integration",
        env="VAULT_ENABLED",
    )

    # Vault paths
    DATABASE_CREDS_PATH: str = Field(
        default="",
        description="Vault path to databse creds",
        env="DATABASE_CREDS_PATH",
    )
    JWT_CREDS_PATH: str = Field(
        default="",
        description="Vault path to JWT settings",
        env="JWT_CREDS_PATH",
    )
    API_KEY_PATH: str = Field(
        default="application/datascience/llm-engines",
        description="Vault path to API key",
        env="API_KEY_PATH",
    )
    PHOENIX_KEY_VAULT_PATH: str = Field(
        default="",
        description="Vault path to API key",
        env="PHOENIX_KEY_VAULT_PATH",
    )

    LDAP_SERVICE_USER_CRED_VAULT_PATH: str = Field(
        default="",
        description="Vault path to LDAP_SERVICE_USER",
        env="LDAP_SERVICE_USER_CRED_VAULT_PATH",
    )

    ROUTER_CLUSTER_ID: str = Field(
        default="",
        description="Incluster ID",
        env="ROUTER_CLUSTER_ID",
    )

    # Service account API key cache TTL
    SERVICE_API_KEY_CACHE_TTL_SECONDS: int = Field(
        default=60,
        description="TTL in seconds for the resolved service-account identity Redis cache",
        env="SERVICE_API_KEY_CACHE_TTL_SECONDS",
    )

    # ------------------------------------------------------------------ #
    # Derived helpers
    # ------------------------------------------------------------------ #
    async def load_model_configs(
        self,
        skip_unavailable: bool = True,
    ) -> List[Union[ModelConfig, MultiClusterModelConfig]]:
        """
        Parse MODELS_CONFIG_PATH and return one config object per model.

        Static models (no `type` field or `type: static`) → ModelConfig.
        Multi-cluster models (`type: multi_cluster`)       → MultiClusterModelConfig.

        Empty api_key fields fall back to the global API_KEY env var.

        skip_unavailable: when True (default, legacy behaviour) static models that
        are unreachable at startup or have no usable key are dropped. When False
        (used by build_registry_from_config for the DiscoveryService) they are
        kept with a best-effort key so a backend is still registered and the
        discovery loop can mark it available + re-resolve the key on recovery.
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
                cluster = await self._parse_multi_cluster(entry)
                configs.append(cluster)
            else:
                base_url = entry["base_url"]
                try:
                    api_key = await find_working_api_key(
                        base_url=base_url, API_Keys=self.VAULT_API_KEYs
                    )
                except Exception:
                    if skip_unavailable:
                        logger.warning(f"Skipping {entry['model_id']} failed to connect.")
                        continue
                    # Discovery mode: register anyway; the DiscoveryService will
                    # probe and re-resolve a working key once it recovers.
                    logger.warning(
                        "%s unreachable at startup; registering as unavailable.",
                        entry["model_id"],
                    )
                    api_key = ""
                if api_key == "":
                    if self.API_KEY == "EMPTY":
                        if skip_unavailable:
                            logger.warning(
                                f"Skipping {entry['model_id']} since no API key in vault is applicable and default key is not set."
                            )
                            continue
                        # No usable key yet — register keyless; discovery re-resolves.
                        api_key = ""
                    else:
                        api_key = self.API_KEY

                configs.append(
                    ModelConfig(
                        model_id=entry["model_id"],
                        base_url=base_url,
                        api_key=api_key,
                    )
                )
        return configs

    async def _parse_multi_cluster(self, entry: dict) -> MultiClusterModelConfig:
        """Parse a single multi_cluster model entry from models.yaml."""
        model_id: str = entry["model_id"]
        local_cluster_id = "in-cluster"
        clusters: List[ClusterConfig] = []
        for c in entry.get("clusters", []):
            base_url = c["base_url"]
            try:
                api_key = await find_working_api_key(
                    base_url=base_url, API_Keys=self.VAULT_API_KEYs
                )
            except:
                logger.warning(
                    f"Skipping {model_id}:{c['cluster_id']} failed to connect."
                )
                continue
            if api_key == "":
                if self.API_KEY == "EMPTY":
                    logger.warning(
                        f"Skipping {model_id}:{c['cluster_id']} since no API key in vault is applicable and default key is not set."
                    )
                    continue
                else:
                    api_key = self.API_KEY
            clusters.append(
                ClusterConfig(
                    cluster_id=c["cluster_id"],
                    base_url=c["base_url"].rstrip("/"),
                    api_key=api_key,
                    number_of_replicas=int(c.get("number_of_replicas", 1)),
                    is_enabled=bool(c.get("is_enabled", True)),
                    auto_adapt_capacity=bool(c.get("auto_adapt_capacity", True)),
                    priority=int(c.get("priority", 0)),
                )
            )

        r = entry.get("routing", {})

        routing = RoutingConfig(
            local_cluster_id=local_cluster_id,
            spill_ratio=float(r.get("spill_ratio", 0.8)),
            seed_capacity_tokens_per_pod=int(r["seed_capacity_tokens_per_pod"]),
            default_max_tokens=int(r.get("default_max_tokens", 2048)),
            cache_ttl_seconds=int(r.get("cache_ttl_seconds", 3600)),
            ttft_budget_ms=float(r.get("ttft_budget_ms", 800.0)),
            itl_budget_ms=float(r.get("itl_budget_ms", 200.0)),
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


def load_settings_from_vault() -> Settings:
    """
    Load settings with sensitive values from Vault.

    This function creates a Settings instance and then overrides
    sensitive values with those fetched from HashiCorp Vault.

    Priority for sensitive settings:
    1. Vault (if VAULT_ENABLED is true and configured)
    2. Environment variables
    3. Default values from Settings class

    Returns:
        Settings instance with values loaded from Vault where available
    """
    settings = get_settings()

    # If Vault is not enabled, return settings as-is
    if not settings.VAULT_ENABLED:
        return settings

    if vault is None:
        raise RuntimeError(
            "VAULT_ENABLED=true but the internal 'paycompy' package is not "
            "installed. Install paycompy or set VAULT_ENABLED=false."
        )

    # Initialize Vault settings loader
    vc = vault.get_client_using_os_environ()

    try:
        if not os.getenv("LOCAL_ENV_PATH", None):
            settings.DATABASE_USER = vc.read_kv_secret(
                "DATABASE_USER", settings.DATABASE_CREDS_PATH
            ).get_secret_value()
            settings.DATABASE_PASSWORD = vc.read_kv_secret(
                "DATABASE_PASSWORD", settings.DATABASE_CREDS_PATH
            ).get_secret_value()
            settings.DATABASE_HOST = vc.read_kv_secret(
                "DATABASE_HOST", settings.DATABASE_CREDS_PATH
            ).get_secret_value()
            settings.DATABASE_PORT = vc.read_kv_secret(
                "DATABASE_PORT", settings.DATABASE_CREDS_PATH
            ).get_secret_value()
        settings.JWT_SECRET_KEY = vc.read_kv_secret(
            "JWT_SECRET_KEY", settings.JWT_CREDS_PATH
        ).get_secret_value()
        settings.JWT_ALGORITHM = vc.read_kv_secret(
            "JWT_ALGORITHM", settings.JWT_CREDS_PATH
        ).get_secret_value()
        settings.PHEONIX_API_KEY = vc.read_kv_secret(
            "api_key", settings.PHOENIX_KEY_VAULT_PATH
        ).get_secret_value()
        settings.VAULT_API_KEYs = vc._read_full_kv_secret_path(settings.API_KEY_PATH)[
            "data"
        ]["data"]
        settings.LDAP_SERVICE_USERNAME = vc.read_kv_secret(
            "username",
            settings.LDAP_SERVICE_USER_CRED_VAULT_PATH,
        ).get_secret_value()
        settings.LDAP_SERVICE_PASSWORD = vc.read_kv_secret(
            "password",
            settings.LDAP_SERVICE_USER_CRED_VAULT_PATH,
        ).get_secret_value()

    except Exception as e:
        # Log the error but don't fail - fall back to environment variables
        import logging

        logger = logging.getLogger(__name__)
        logger.warning(
            f"Failed to load settings from Vault: {e}. Using environment variables."
        )

    # Fail closed: Vault is enabled (we only reach here when VAULT_ENABLED is
    # true), so the JWT signing key MUST have come from Vault. If it is still
    # the public placeholder or empty — e.g. a transient Vault failure fell
    # through to the default above — refuse to boot rather than silently serve
    # with a forgeable signing key. Crashing lets the orchestrator restart the
    # pod once Vault is healthy again.
    if not settings.JWT_SECRET_KEY or settings.JWT_SECRET_KEY == _INSECURE_JWT_SECRET_DEFAULT:
        raise RuntimeError(
            "JWT_SECRET_KEY was not provided by Vault (still the insecure default "
            "or empty). Refusing to start to avoid serving with a forgeable JWT "
            "signing key. Check Vault connectivity and JWT_CREDS_PATH."
        )

    return settings


# Convenience module-level alias
settings = load_settings_from_vault()
