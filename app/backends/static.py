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

from app.backends.base import Backend
from app.core.httpx import get_http_client

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

    # ------------------------------------------------------------------ #
    # Backend interface
    # ------------------------------------------------------------------ #

    @property
    def model_id(self) -> str:
        return self._model_id
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

        client = get_http_client()
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
        return (
            f"StaticBackend(model_id={self._model_id!r}, base_url={self._base_url!r})"
        )
