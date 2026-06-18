"""
Middleware for graceful shutdown handling.

This middleware ensures:
- During shutdown, new requests receive 503 Service Unavailable
- Health checks continue to work during shutdown (for graceful shutdown detection)
- Active requests are tracked for completion monitoring
- Streaming responses are tracked until the stream completes
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.middleware.base import BaseHTTPMiddleware,_StreamingResponse

from app.core.lifecycle import (
    is_shutting_down,
    increment_active_requests,
    decrement_active_requests,
)

if TYPE_CHECKING:
    from starlette.types import ASGIApp

logger = logging.getLogger(__name__)


class GracefulShutdownMiddleware(BaseHTTPMiddleware):
    """
    Middleware that handles graceful shutdown by rejecting new requests.

    During normal operation:
      - All requests proceed normally
      - Active requests are tracked

    During shutdown (after SIGTERM/SIGINT):
      - New requests (except health checks) receive 503 Service Unavailable
      - Ongoing requests are allowed to complete
      - Health check endpoint returns 503 to signal unavailability
    """

    # Endpoints that should work even during shutdown (health checks)
    UNGRACEFUL_PATHS = {"/metrics"}

    def __init__(self, app: ASGIApp) -> None:
        super().__init__(app)

    async def dispatch(self, request: Request, call_next) -> Response:
        # Skip graceful shutdown for metrics
        if request.url.path in self.UNGRACEFUL_PATHS:
            return await call_next(request)

        # Check if we're shutting down
        if is_shutting_down():
            logger.warning(
                "Rejecting %s %s - application is shutting down",
                request.method, request.url.path
            )
            return JSONResponse(
                status_code=503,
                content={
                    "detail": "Service is unavailable - the application is shutting down"
                },
                headers={"Retry-After": "10"}
            )

        # Track the request and process it normally
        # NOTE: We don't use the track_request() context manager here because
        # for streaming responses, the context would exit when call_next() returns
        # the StreamingResponse object, but the actual streaming continues in the
        # background via the generator. We need to track until the stream completes.
        active_count = await increment_active_requests()
        logger.debug(
            "Request started: %s %s (active: %d)",
            request.method, request.url.path, active_count
        )

        response: Response | None = None
        # FIX 3: Set is_streaming BEFORE wrapping so the finally block
        # correctly skips the decrement even if _track_streaming_response raises.
        is_streaming = False

        try:
            response = await call_next(request)

            # For streaming responses, we need to track until the stream completes.
            # FIX 1: Use the public StreamingResponse instead of private _StreamingResponse.
            if isinstance(response, _StreamingResponse):
                is_streaming = True  # set BEFORE wrapping
                response = self._track_streaming_response(response)
                return response
            else:
                return response
        except Exception:
            logger.exception(
                "Error processing request: %s %s",
                request.method, request.url.path
            )
            raise
        finally:
            # For non-streaming responses, decrement immediately.
            # For streaming responses, _track_streaming_response handles decrement.
            if not is_streaming:
                active_count = await decrement_active_requests()
                logger.debug(
                    "Request completed (non-streaming): %s %s (active: %d)",
                    request.method, request.url.path, active_count
                )

    def _track_streaming_response(self, response: _StreamingResponse) -> _StreamingResponse:
        """
        Wrap a StreamingResponse to track it until the stream completes.

        This is necessary because StreamingResponse returns immediately but the
        actual data streaming happens asynchronously via the body_iterator.
        We wrap the iterator to decrement the active request counter only when
        the stream actually completes (or errors).
        """
        original_iterator = response.body_iterator

        async def tracked_iterator():
            try:
                async for chunk in original_iterator:
                    yield chunk
            except Exception as exc:
                logger.debug("Streaming response error: %s", exc)
                raise
            finally:
                # Decrement active request counter when stream completes
                active_count = await decrement_active_requests()
                logger.debug(
                    "Request completed (streaming): active=%d",
                    active_count
                )

        # Replace the body iterator with our tracked version
        response.body_iterator = tracked_iterator()
        return response