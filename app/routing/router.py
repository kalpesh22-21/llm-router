"""
Request routing logic.

Responsible for selecting which Backend instance should handle a given
model_id. In Phase 1 this is a simple registry lookup; Phase 2 will add
cluster-aware selection here without changing the proxy layer.
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import HTTPException, status

from app.backends.base import Backend, registry as _global_registry

logger = logging.getLogger(__name__)


class ModelRouter:
    """
    Selects a Backend for a given model_id.

    Phase 1: delegates to the global BackendRegistry (static mapping).
    Phase 2: may add cluster selection, load-based routing, etc. here.
    """

    def resolve(self, model_id: str) -> Backend:
        """
        Return the Backend responsible for *model_id*.

        Raises:
            HTTPException(404): if no backend is registered for *model_id*.
        """
        backend: Optional[Backend] = _global_registry.get(model_id)
        if backend is None:
            logger.warning("No backend found for model_id=%r", model_id)
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Model {model_id!r} is not available",
            )
        return backend

    def list_available_model_ids(self) -> list[str]:
        """Return all model IDs that have a registered backend."""
        return _global_registry.list_model_ids()


# Module-level singleton used by proxy routes
model_router = ModelRouter()
