"""
Backends package — backend construction and registration.

`build_registry_from_config()` is the single entry point called at startup
(app.main lifespan). It registers a backend for EVERY model in models.yaml:

  - Static models  → StaticBackend, registered even if unreachable at startup.
                     The DiscoveryService then probes them and flips availability
                     (and re-resolves the API key) without a restart.
  - Multi-cluster  → MultiClusterBackend (unchanged; has its own health machinery).
"""

from __future__ import annotations

import logging

from app.backends.base import registry
from app.backends.static import StaticBackend
from app.backends.multi_cluster import ClusterEndpoint, MultiClusterBackend
from app.core.config import MultiClusterModelConfig, get_settings

logger = logging.getLogger(__name__)


def _build_multi_cluster_backend(cfg: MultiClusterModelConfig) -> MultiClusterBackend:
    """Construct a MultiClusterBackend (and its ClusterEndpoints) from config."""
    endpoints = [
        ClusterEndpoint(
            cluster_id=c.cluster_id,
            base_url=c.base_url,
            api_key=c.api_key,
            model_id=cfg.model_id,
            number_of_replicas=c.number_of_replicas,
            seed_capacity_tokens_per_pod=cfg.routing.seed_capacity_tokens_per_pod,
            is_enabled=c.is_enabled,
            auto_adapt_capacity=c.auto_adapt_capacity,
            ttft_budget_ms=cfg.routing.ttft_budget_ms,
            itl_budget_ms=cfg.routing.itl_budget_ms,
            ttft_alpha=cfg.routing.ttft_alpha,
            itl_alpha=cfg.routing.itl_alpha,
            capacity_alpha=cfg.routing.capacity_alpha,
        )
        for c in cfg.clusters
    ]
    return MultiClusterBackend(
        model_id=cfg.model_id,
        clusters=endpoints,
        local_cluster_id=cfg.routing.local_cluster_id,
        spill_ratio=cfg.routing.spill_ratio,
        default_max_tokens=cfg.routing.default_max_tokens,
        cache_ttl_seconds=cfg.routing.cache_ttl_seconds,
    )


async def build_registry_from_config() -> None:
    """
    Populate the global backend registry from models.yaml.

    Unlike the legacy behaviour, static models that are unreachable at startup
    are still registered (skip_unavailable=False) so the DiscoveryService can
    mark them available when they come back up — no restart required.
    """
    settings = get_settings()
    configs = await settings.load_model_configs(skip_unavailable=False)

    for cfg in configs:
        if isinstance(cfg, MultiClusterModelConfig):
            backend = _build_multi_cluster_backend(cfg)
        else:
            backend = StaticBackend(
                model_id=cfg.model_id,
                base_url=cfg.base_url,
                api_key=cfg.api_key,
            )
        registry.register(backend)

    logger.info(
        "Backend registry built with %d model(s): %s",
        len(registry),
        registry.list_model_ids(),
    )
