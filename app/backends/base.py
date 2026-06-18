"""
Backend abstraction layer — the critical Phase 1 → Phase 2 seam.

Phase 1 uses StaticBackend (static URL from models.yaml).
Phase 2 will add PodBackend (pod IP from Kubernetes discovery) without
any changes to the proxy layer — it only ever calls backend.forward() /
backend.forward_stream().

BackendRegistry is the single lookup point: model_id -> Backend instance.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
import logging
from typing import AsyncIterator, Dict, Optional

import httpx
from fastapi import Request

logger = logging.getLogger(__name__)


class Backend(ABC):
    """
    Abstract LLM backend.

    Implementors must provide:
      - model_id property   — the canonical model identifier
      - forward()           — non-streaming request/response proxy
      - forward_stream()    — streaming response proxy (yields raw SSE bytes)
    """

    @property
    @abstractmethod
    def model_id(self) -> str:
        """The canonical model identifier this backend serves."""
        ...

    @abstractmethod
    async def forward(self, request: Request, body: dict,upstream_url:str) -> httpx.Response:
        """
        Forward a non-streaming request to the backend.

        Args:
            request: The original FastAPI Request (for headers, etc.).
            body:    The parsed JSON body (may be mutated before forwarding).

        Returns:
            The raw httpx.Response from the backend.
        """
        ...

    @abstractmethod
    async def forward_stream(
        self, request: Request, body: dict,upstream_url:str
    ) -> AsyncIterator[bytes]:
        """
        Forward a streaming request to the backend.

        Yields raw SSE byte chunks exactly as received from the backend,
        allowing the proxy to pass them through to the client while also
        inspecting them for metrics.

        Args:
            request: The original FastAPI Request.
            body:    The parsed JSON body (may be mutated before forwarding).

        Yields:
            Raw bytes chunks from the backend's SSE stream.
        """
        ...


class BackendRegistry:
    """
    Registry mapping model_id strings to Backend instances.

    Populated at application startup from models.yaml.
    """

    def __init__(self) -> None:
        self._backends: Dict[str, Backend] = {}

    def register(self, backend: Backend) -> None:
        """Register a backend. Raises ValueError if model_id is already taken."""
        if backend.model_id in self._backends:
            logger.warning(
                f"Backend for model_id {backend.model_id!r} is already registered"
            )
            return
        self._backends[backend.model_id] = backend

    def get(self, model_id: str) -> Optional[Backend]:
        """
        Return the Backend for *model_id*, or None if not registered.
        """
        return self._backends.get(model_id)

    def list_model_ids(self) -> list[str]:
        """Return all registered model IDs."""
        return list(self._backends.keys())

    def __len__(self) -> int:
        return len(self._backends)

    def __repr__(self) -> str:  # pragma: no cover
        return f"BackendRegistry(models={self.list_model_ids()!r})"


# Module-level singleton — populated during app lifespan startup
registry = BackendRegistry()
