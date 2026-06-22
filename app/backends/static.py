"""
StaticBackend — Phase 1 implementation of the Backend ABC.

Forwards requests to a fixed base_url loaded from models.yaml.
In Phase 2 this class will be replaced by PodBackend, which selects a
pod IP dynamically; the proxy layer is unaffected.
"""

from __future__ import annotations

import logging
import time
from typing import AsyncIterator, Optional

import httpx
from fastapi import Request

from app.backends.base import Backend
from app.core.httpx import new_http_client

logger = logging.getLogger(__name__)


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

        # Availability state, maintained per-pod by the DiscoveryService.
        # Starts optimistically available; the initial probe pass at startup
        # corrects it before the app serves traffic. If discovery is disabled,
        # backends simply stay available (legacy behaviour).
        self._available: bool = True
        self._consecutive_failures: int = 0
        self._last_checked: Optional[float] = None

    # ------------------------------------------------------------------ #
    # Backend interface
    # ------------------------------------------------------------------ #

    @property
    def model_id(self) -> str:
        return self._model_id

    # ------------------------------------------------------------------ #
    # Availability (driven by DiscoveryService)
    # ------------------------------------------------------------------ #

    @property
    def is_available(self) -> bool:
        """Whether this backend's upstream is currently reachable."""
        return self._available

    @property
    def base_url(self) -> str:
        return self._base_url

    @property
    def api_key(self) -> str:
        return self._api_key

    def set_api_key(self, api_key: str) -> None:
        """Update the API key (used when discovery re-resolves a working key)."""
        self._api_key = api_key

    def record_probe_result(self, success: bool, failure_threshold: int = 2) -> None:
        """
        Apply a health-probe outcome with hysteresis.

        One success re-enables the backend immediately; `failure_threshold`
        consecutive failures are required to disable it — so an intermittent
        blip does not flap the backend in and out of rotation.
        """
        self._last_checked = time.monotonic()
        if success:
            self._consecutive_failures = 0
            if not self._available:
                logger.info("Model %s is AVAILABLE again", self._model_id)
            self._available = True
        else:
            self._consecutive_failures += 1
            if self._available and self._consecutive_failures >= failure_threshold:
                logger.warning(
                    "Model %s marked UNAVAILABLE after %d consecutive failed probes",
                    self._model_id,
                    self._consecutive_failures,
                )
                self._available = False
    async def forward(
        self, request: Request, body: dict, upstream_url: str = "/chat/completions"
    ) -> httpx.Response:
        """
        Forward a non-streaming chat/completions request.

        The body is passed as-is (the proxy layer has already set stream=False).
        Authorization is injected from the backend's api_key.
        """
        url = f"{self._base_url}{upstream_url}"
        headers = self._build_headers()

        async with new_http_client() as client:
            response = await client.post(url, json=body, headers=headers)
            return response

    async def forward_stream(
        self, request: Request, body: dict, upstream_url: str = "/chat/completions"
    ) -> AsyncIterator[bytes]:
        """
        Forward a streaming chat/completions request.

        Yields raw SSE byte chunks (e.g. b"data: {...}\\n\\n") as they arrive
        from the backend. The proxy layer consumes this iterator.
        """
        url = f"{self._base_url}{upstream_url}"
        headers = self._build_headers()

        async with new_http_client() as client:
            async with client.stream("POST", url, json=body, headers=headers) as response:
                response.raise_for_status()
                async for chunk in response.aiter_bytes():
                    if chunk:
                        yield chunk

    async def forward_embeddings(self, request: Request, body: dict) -> httpx.Response:
        """Forward an embeddings request to /embeddings on the backend."""
        url = f"{self._base_url}/embeddings"
        headers = self._build_headers()
        async with new_http_client() as client:
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
        return (
            f"StaticBackend(model_id={self._model_id!r}, base_url={self._base_url!r})"
        )
