"""
Core proxy endpoints.

POST /v1/chat/completions  — main LLM proxy (streaming and non-streaming)
POST /v1/embeddings        — embeddings proxy
GET  /v1/models            — list models accessible to the current user

Every request:
  1. Validates JWT
  2. Checks is_active against DB (not just JWT claim)
  3. Validates the requested model is in the user's accessible models
  4. Checks + increments concurrent request counter in Redis
  5. Forwards to the appropriate backend
  6. Collects metrics
  7. Decrements concurrent counter (finally block)
"""

from __future__ import annotations

import json
import logging
import time
from typing import Annotated, AsyncIterator, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

import redis.asyncio as aioredis

from app.api.auth_routes import get_current_user_payload
from app.backends.base import registry as backend_registry
from app.backends.static import StaticBackend
from app.db.mysql import User, get_db
from app.db.redis_client import get_redis
from app.metrics.collector import metrics_collector
from app.routing.router import model_router
from app.services.permission import permission_service
from app.services.rate_limit import rate_limit_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1", tags=["proxy"])

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


async def _resolve_user(
    payload: dict,
    db: AsyncSession,
) -> tuple[int, int]:
    """
    Extract user_id from JWT payload and verify the user is still active in DB.

    Returns:
        (user_id, max_concurrent_requests)

    Raises:
        HTTPException(401) if the user is inactive or not found.
    """
    user_id = int(payload["sub"])

    # Always check DB — a deactivated user may still hold a valid JWT
    is_active = await permission_service.is_user_active(db, user_id)
    if not is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Account is deactivated",
        )

    user = await permission_service.get_user(db, user_id)
    return user_id, user.max_concurrent_requests


def _extract_usage(data: dict) -> tuple[int, int]:
    """Extract (prompt_tokens, completion_tokens) from a response body dict."""
    usage = data.get("usage") or {}
    return (
        int(usage.get("prompt_tokens", 0)),
        int(usage.get("completion_tokens", 0)),
    )


def _parse_sse_data(chunk: bytes) -> Optional[dict]:
    """
    Parse a single SSE chunk and return the JSON payload if present.

    SSE format: ``data: <json>\\n\\n``
    Returns None for ``data: [DONE]`` or non-data lines.
    """
    text = chunk.decode("utf-8", errors="replace").strip()
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("data:"):
            payload = line[len("data:"):].strip()
            if payload == "[DONE]":
                return None
            try:
                return json.loads(payload)
            except json.JSONDecodeError:
                pass
    return None


# ---------------------------------------------------------------------------
# Streaming proxy
# ---------------------------------------------------------------------------


async def _stream_response(
    backend: StaticBackend,
    request: Request,
    body: dict,
    model_id: str,
    user_id: int,
    start_time: float,
    redis: aioredis.Redis,
) -> AsyncIterator[bytes]:
    """
    Stream SSE chunks from the backend to the client.

    While streaming, captures:
      - Time-to-first-token (TTFT)
      - Inter-token latencies (ITL) between consecutive token-bearing chunks
      - Token usage from the final chunk (if include_usage: true was set)

    The concurrent-request counter is always decremented in the finally block,
    regardless of success or failure.
    """
    # Ensure the backend includes usage in the last chunk
    body.setdefault("stream_options", {})["include_usage"] = True

    first_token = True
    ttft: float = 0.0
    prev_token_time: float = start_time
    inter_token_latencies: list[float] = []
    last_data: Optional[dict] = None
    http_status = "200"

    try:
        async for chunk in backend.forward_stream(request, body):
            now = time.monotonic()

            parsed = _parse_sse_data(chunk)
            if parsed is not None:
                # Track timing
                if first_token:
                    ttft = now - start_time
                    first_token = False
                    prev_token_time = now
                else:
                    itl = now - prev_token_time
                    inter_token_latencies.append(itl)
                    prev_token_time = now

                # Keep the last non-DONE data chunk (contains usage)
                last_data = parsed

            yield chunk

    except httpx.HTTPStatusError as exc:
        http_status = str(exc.response.status_code)
        logger.error("Backend streaming error for model=%s: %s", model_id, exc)
        # Yield an error SSE event so the client sees something
        error_payload = json.dumps({"error": {"message": str(exc), "type": "backend_error"}})
        yield f"data: {error_payload}\n\n".encode()
    except Exception:
        http_status = "500"
        logger.exception("Unexpected streaming error for model=%s", model_id)
        error_payload = json.dumps({"error": {"message": "Internal proxy error", "type": "proxy_error"}})
        yield f"data: {error_payload}\n\n".encode()
    finally:
        duration = time.monotonic() - start_time

        # Extract token counts from the final usage chunk
        input_tokens, output_tokens = 0, 0
        if last_data:
            input_tokens, output_tokens = _extract_usage(last_data)

        metrics_collector.record_request_complete(
            model_id=model_id,
            user_id=str(user_id),
            status=http_status,
            duration=duration,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )
        if not first_token:
            metrics_collector.record_streaming_metrics(
                model_id=model_id,
                ttft=ttft,
                inter_token_latencies=inter_token_latencies,
            )

        # Always decrement the concurrent request counter
        try:
            await rate_limit_service.decrement(redis, user_id)
        except Exception:
            logger.exception(
                "Failed to decrement concurrent counter for user_id=%d", user_id
            )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.post("/chat/completions", response_model=None)
async def chat_completions(
    request: Request,
    payload: Annotated[dict, Depends(get_current_user_payload)],
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
) -> StreamingResponse | JSONResponse:
    """
    Main LLM proxy endpoint (OpenAI-compatible).

    Handles both streaming (stream=true) and non-streaming requests.
    """
    # Parse request body
    try:
        body: dict = await request.json()
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid JSON body",
        )

    model_id: str = body.get("model", "")
    if not model_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="'model' field is required",
        )

    # Resolve and verify user
    user_id, max_concurrent = await _resolve_user(payload, db)

    # Check model access
    accessible = await permission_service.get_user_accessible_models(db, user_id)
    if model_id not in accessible:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Model {model_id!r} is not accessible to this user",
        )

    # Rate limit: check-and-increment (raises 429 if exceeded)
    await rate_limit_service.assert_within_limit(redis, user_id, max_concurrent)

    start_time = time.monotonic()
    is_streaming: bool = bool(body.get("stream", False))

    try:
        backend = model_router.resolve(model_id)

        if is_streaming:
            stream_gen = _stream_response(
                backend=backend,  # type: ignore[arg-type]
                request=request,
                body=body,
                model_id=model_id,
                user_id=user_id,
                start_time=start_time,
                redis=redis,
            )
            return StreamingResponse(
                stream_gen,
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "X-Accel-Buffering": "no",
                },
            )

        # ---- Non-streaming path ----
        response = await backend.forward(request, body)
        duration = time.monotonic() - start_time

        http_status = str(response.status_code)
        input_tokens, output_tokens = 0, 0

        if response.status_code == 200:
            try:
                resp_body = response.json()
                input_tokens, output_tokens = _extract_usage(resp_body)
            except Exception:
                resp_body = {}
        else:
            resp_body = {}
            try:
                resp_body = response.json()
            except Exception:
                pass

        metrics_collector.record_request_complete(
            model_id=model_id,
            user_id=str(user_id),
            status=http_status,
            duration=duration,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )

        return JSONResponse(
            content=resp_body,
            status_code=response.status_code,
        )

    except HTTPException:
        raise
    except Exception:
        duration = time.monotonic() - start_time
        logger.exception("Unhandled proxy error for model=%s user=%d", model_id, user_id)
        metrics_collector.record_request_complete(
            model_id=model_id,
            user_id=str(user_id),
            status="500",
            duration=duration,
            input_tokens=0,
            output_tokens=0,
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Backend error",
        )
    finally:
        # Always decrement for non-streaming; streaming decrements inside generator
        if not is_streaming:
            await rate_limit_service.decrement(redis, user_id)


@router.post("/embeddings")
async def embeddings(
    request: Request,
    payload: Annotated[dict, Depends(get_current_user_payload)],
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
) -> JSONResponse:
    """
    Embeddings proxy endpoint (OpenAI-compatible).

    Same auth/rate-limit flow as chat/completions but always non-streaming.
    """
    try:
        body: dict = await request.json()
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid JSON body",
        )

    model_id: str = body.get("model", "")
    if not model_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="'model' field is required",
        )

    user_id, max_concurrent = await _resolve_user(payload, db)

    accessible = await permission_service.get_user_accessible_models(db, user_id)
    if model_id not in accessible:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Model {model_id!r} is not accessible to this user",
        )

    await rate_limit_service.assert_within_limit(redis, user_id, max_concurrent)

    start_time = time.monotonic()

    try:
        backend = model_router.resolve(model_id)

        # StaticBackend has a forward_embeddings helper; fall back to generic forward
        if hasattr(backend, "forward_embeddings"):
            response = await backend.forward_embeddings(request, body)  # type: ignore[attr-defined]
        else:
            response = await backend.forward(request, body)

        duration = time.monotonic() - start_time
        http_status = str(response.status_code)

        resp_body: dict = {}
        try:
            resp_body = response.json()
        except Exception:
            pass

        metrics_collector.record_request_complete(
            model_id=model_id,
            user_id=str(user_id),
            status=http_status,
            duration=duration,
            input_tokens=0,
            output_tokens=0,
        )

        return JSONResponse(content=resp_body, status_code=response.status_code)

    except HTTPException:
        raise
    except Exception:
        duration = time.monotonic() - start_time
        logger.exception("Embeddings proxy error for model=%s", model_id)
        metrics_collector.record_request_complete(
            model_id=model_id,
            user_id=str(user_id),
            status="500",
            duration=duration,
            input_tokens=0,
            output_tokens=0,
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Backend error",
        )
    finally:
        await rate_limit_service.decrement(redis, user_id)


@router.get("/models")
async def list_models(
    payload: Annotated[dict, Depends(get_current_user_payload)],
    db: AsyncSession = Depends(get_db),
) -> JSONResponse:
    """
    Return the list of models accessible to the current user.

    Response format matches the OpenAI /v1/models shape.
    """
    user_id = int(payload["sub"])

    is_active = await permission_service.is_user_active(db, user_id)
    if not is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Account is deactivated",
        )

    model_ids = await permission_service.get_user_accessible_models(db, user_id)

    # Return only models that have a registered backend
    available = [mid for mid in model_ids if backend_registry.get(mid) is not None]

    data = [
        {"id": mid, "object": "model", "owned_by": "llm-router"}
        for mid in sorted(available)
    ]
    return JSONResponse(content={"object": "list", "data": data})
