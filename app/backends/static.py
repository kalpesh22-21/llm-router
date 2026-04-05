"""
StaticBackend — Phase 1 implementation of the Backend ABC.

Forwards requests to a fixed base_url loaded from models.yaml.
In Phase 2 this class will be replaced by PodBackend, which selects a
pod IP dynamically; the proxy layer is unaffected.
"""

from __future__ import annotations

import logging
from typing import AsyncIterator

import httpx
from fastapi import Request

from app.backends.base import Backend, BackendRegistry, registry
from app.core.config import ModelConfig, MultiClusterModelConfig, get_settings

logger = logging.getLogger(__name__)

# Shared httpx async client (connection pooling across all backends)
_HTTP_CLIENT: httpx.AsyncClient | None = None

# Timeout for forwarded requests (seconds)
BACKEND_TIMEOUT = httpx.Timeout(timeout=120.0, connect=10.0)


def get_http_client() -> httpx.AsyncClient:
    """Return the module-level httpx client, creating it on first call."""
    global _HTTP_CLIENT
    if _HTTP_CLIENT is None or _HTTP_CLIENT.is_closed:
        _HTTP_CLIENT = httpx.AsyncClient(
            timeout=BACKEND_TIMEOUT,
            follow_redirects=True,
            limits=httpx.Limits(max_connections=200, max_keepalive_connections=50),
        )
    return _HTTP_CLIENT


async def close_http_client() -> None:
    """Close the shared httpx client (called during app shutdown)."""
    global _HTTP_CLIENT
    if _HTTP_CLIENT is not None and not _HTTP_CLIENT.is_closed:
        await _HTTP_CLIENT.aclose()
        _HTTP_CLIENT = None


class StaticBackend(Backend):
    """
    Phase 1 backend: forwards to a static base_url.

    Phase 2 note: Replace with PodBackend that obtains a pod IP from the
    discovery service. The proxy layer (app/api/proxy.py) calls only
    backend.forward() / backend.forward_stream() and is unaffected.
    """

    def __init__(self, model_id: str, base_url: str, api_key: str) -> None:
        self._model_id = model_id
        # Ensure no trailing slash
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key

    # ------------------------------------------------------------------ #
    # Backend interface
    # ------------------------------------------------------------------ #

    @property
    def model_id(self) -> str:
        return self._model_id

    async def forward(self, request: Request, body: dict) -> httpx.Response:
        """
        Forward a non-streaming chat/completions request.

        The body is passed as-is (the proxy layer has already set stream=False).
        Authorization is injected from the backend's api_key.
        """
        url = f"{self._base_url}/chat/completions"
        headers = self._build_headers()

        client = get_http_client()
        response = await client.post(url, json=body, headers=headers)
        return response

    async def forward_stream(
        self, request: Request, body: dict
    ) -> AsyncIterator[bytes]:
        """
        Forward a streaming chat/completions request.

        Yields raw SSE byte chunks (e.g. b"data: {...}\\n\\n") as they arrive
        from the backend. The proxy layer consumes this iterator.
        """
        url = f"{self._base_url}/chat/completions"
        headers = self._build_headers()

        client = get_http_client()
        async with client.stream("POST", url, json=body, headers=headers) as response:
            response.raise_for_status()
            async for chunk in response.aiter_bytes():
                if chunk:
                    yield chunk

    async def forward_embeddings(self, request: Request, body: dict) -> httpx.Response:
        """Forward an embeddings request to /embeddings on the backend."""
        url = f"{self._base_url}/embeddings"
        headers = self._build_headers()
        client = get_http_client()
        response = await client.post(url, json=body, headers=headers)
        return response

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    def _build_headers(self) -> dict:
        """Build the HTTP headers to send to the backend."""
        headers: dict = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"
        return headers

    def __repr__(self) -> str:  # pragma: no cover
        return f"StaticBackend(model_id={self._model_id!r}, base_url={self._base_url!r})"


# ---------------------------------------------------------------------------
# Registry initialisation (called at startup)
# ---------------------------------------------------------------------------


def build_registry_from_config() -> BackendRegistry:
    """
    Load models.yaml via settings and populate the global BackendRegistry.

    Dispatches on config type:
      ModelConfig             → StaticBackend
      MultiClusterModelConfig → MultiClusterBackend

    Called once during application lifespan startup.
    """
    # Import here to avoid a circular import at module load time
    from app.backends.multi_cluster import ClusterEndpoint, MultiClusterBackend

    settings = get_settings()
    all_configs = settings.load_model_configs()

    for cfg in all_configs:
        if isinstance(cfg, MultiClusterModelConfig):
            r = cfg.routing
            if not r.local_cluster_id:
                raise ValueError(
                    f"Model {cfg.model_id!r}: local_cluster_id is required for "
                    "multi_cluster backends. Set routing.local_cluster_id in "
                    "models.yaml or the ROUTER_CLUSTER_ID environment variable."
                )
            endpoints = [
                ClusterEndpoint(
                    cluster_id=c.cluster_id,
                    base_url=c.base_url,
                    api_key=c.api_key,
                    seed_capacity_tokens=r.seed_capacity_tokens,
                    ttft_budget_ms=r.ttft_budget_ms,
                    itl_budget_ms=r.itl_budget_ms,
                    ttft_alpha=r.ttft_alpha,
                    itl_alpha=r.itl_alpha,
                    capacity_alpha=r.capacity_alpha,
                )
                for c in cfg.clusters
            ]
            backend: Backend = MultiClusterBackend(
                model_id=cfg.model_id,
                clusters=endpoints,
                local_cluster_id=r.local_cluster_id,
                spill_ratio=r.spill_ratio,
                default_max_tokens=r.default_max_tokens,
                cache_ttl_seconds=r.cache_ttl_seconds,
            )
            cluster_ids = [c.cluster_id for c in cfg.clusters]
            logger.info(
                "Registered multi-cluster backend: %r -> %s (local=%s)",
                cfg.model_id,
                cluster_ids,
                r.local_cluster_id,
            )
        else:
            backend = StaticBackend(
                model_id=cfg.model_id,
                base_url=cfg.base_url,
                api_key=cfg.api_key,
            )
            logger.info("Registered static backend: %r -> %s", cfg.model_id, cfg.base_url)

        registry.register(backend)

    logger.info("BackendRegistry initialised with %d model(s)", len(registry))
    return registry
