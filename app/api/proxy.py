"""
Core proxy endpoints.

POST /v1/chat/completions  — main LLM proxy (OpenAI-compatible, streaming and non-streaming)
POST /v1/embeddings        — embeddings proxy (OpenAI-compatible)
GET  /v1/models            — list models accessible to the current user
POST /v1/messages          — Anthropic-compatible messages endpoint (streaming and non-streaming)

Every request:
  1. Validates JWT
  2. Checks is_active against DB randomly (~1% of requests)
  3. Validates the requested model is in the user's accessible models (from JWT)
  4. Checks + increments concurrent request counter in Redis (limit from JWT)
  5. Forwards to the appropriate backend
  6. Collects metrics
  7. Decrements concurrent counter (finally block)
"""

from __future__ import annotations

import json
import logging
import random
import time
from typing import Annotated, AsyncIterator, Optional, Tuple

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
import redis.asyncio as aioredis

from app.api.auth_routes import get_current_user_payload_from_request
from app.backends.base import Backend, registry
from app.db.mysql import get_db
from app.db.redis_client import (
    get_redis,
    is_user_marked_deactivated,
    mark_user_deactivated,
    unmark_user_deactivated,
)
from app.metrics.collector import metrics_collector
from app.routing.router import model_router
from app.services.content_filter import content_filter_service
from app.services.permission import permission_service
from app.services.rate_limit import rate_limit_service
from app.logging.pheonix import SpanKind, TracingHelper

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1", tags=["proxy"])

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


async def _resolve_user(
    payload: dict,
    db: AsyncSession,
    redis: aioredis.Redis,
) -> tuple[str, int | str, int]:
    """
    Extract user info from JWT payload and return the rate-limiting principal.

    For service keys (payload["auth"] == "service"):
      - Skips the users-table is_active DB poll entirely. Service account
        liveness is enforced during resolve_key (cache miss) and via
        evict_service_cache on deactivation.
      - Returns principal = payload["rate_principal"] (e.g. "svckey:{key_id}"),
        which namespaces per-key concurrency counters away from user-id counters.

    For human JWT users:
      - Checks DB for is_active:
          always  if user is in the deactivated tracking set,
          ~1%     otherwise to catch newly deactivated users.
      - Returns principal = int user_id.

    Returns:
        (username, principal, max_concurrent_requests)

    Raises:
        HTTPException(401) if the user is inactive or not found.
    """
    username = payload["username"]
    max_concurrent = payload.get("max_concurrent_requests", 10)

    if payload.get("auth") == "service":
        # Service path: liveness already enforced at key resolution
        principal: int | str = payload["rate_principal"]
        return username, principal, max_concurrent

    # Human JWT path
    user_id = int(payload["sub"])

    # Check if user is in the deactivated tracking set
    is_tracked_deactivated = await is_user_marked_deactivated(redis, user_id)

    # Always check DB if user was previously deactivated, otherwise random 1% check
    should_check_db = is_tracked_deactivated or (random.random() < 0.01)

    if should_check_db:
        is_active = await permission_service.is_user_active(db, user_id)
        if not is_active:
            # Mark user as deactivated in tracking set
            await mark_user_deactivated(redis, user_id, username)
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Account is deactivated",
            )
        # User is active - if they were in tracking, remove them
        if is_tracked_deactivated:
            await unmark_user_deactivated(redis, user_id)
            logger.info(
                "User %s (id=%d) reactivated, removed from deactivated tracking",
                username,
                user_id,
            )

    return username, user_id, max_concurrent


def _assert_scope(payload: dict, required: str) -> None:
    """
    Enforce operation scope restrictions for service API keys.

    For human JWT users this is a no-op. For service keys, if the key was
    issued with a non-empty scopes list the requested operation must be present.

    Raises:
        HTTPException(403) if the key is not authorized for *required*.
    """
    if payload.get("auth") != "service":
        return
    scopes = payload.get("scopes") or []
    if scopes and required not in scopes:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"This API key is not authorized for '{required}'",
        )


def _extract_usage(data: dict) -> tuple[int, int]:
    """Extract (prompt_tokens, completion_tokens) from a response body dict."""
    return (
        int(data.get("prompt_tokens", 0)),
        int(data.get("completion_tokens", 0)),
    )


def _extract_error_body(response: httpx.Response) -> dict:
    """
    Extract error body from a backend error response.

    Tries to parse JSON first, falls back to text content if JSON parsing fails.
    Returns an empty dict if neither can be extracted.
    """
    try:
        return response.json()
    except Exception:
        pass

    try:
        text_content = response.text
        if text_content:
            return {"detail": text_content}
    except Exception:
        pass

    return {}


def _are_all_choices_finished(payload: dict) -> bool:
    choices = payload.get("choices", [])
    return all([choice.get("finish_reason") for choice in choices])


def _parse_sse_data(chunk: bytes) -> Tuple[bool, Optional[dict]]:
    """
    Parse a single SSE chunk and return the JSON payload if present.

    SSE format: ``data: <json>\\n\\n``
    """
    is_last = False
    text = chunk.decode("utf-8", errors="replace").strip()
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("data:"):
            data_str = line[len("data:") :].strip()
            # Check for [DONE] BEFORE trying to parse as JSON
            if data_str == "[DONE]":
                return True, None
            try:
                payload = json.loads(data_str)
                if _are_all_choices_finished(payload):
                    is_last = True
                return is_last, payload
            except json.JSONDecodeError:
                logging.warning("Failed SSE Decoding for: %s", data_str[:100])
                continue
    return is_last, None


# ---------------------------------------------------------------------------
# Streaming proxy
# ---------------------------------------------------------------------------


async def _stream_response(
    backend: Backend,
    request: Request,
    upstream_url: str,
    body: dict,
    model_id: str,
    user_id: int | str,
    username: str,
    start_time: float,
    redis: aioredis.Redis,
    request_landing_time: float,
) -> AsyncIterator[bytes]:
    """
    Stream SSE chunks from the backend to the client.

    While streaming, captures:
      - Router internal latency (from request landing to stream start)
      - Time-to-first-token (TTFT)
      - Inter-token latencies (ITL) between consecutive token-bearing chunks
      - Token usage from the final chunk (if include_usage: true was set)

    The concurrent-request counter is always decremented in the finally block,
    regardless of success or failure.
    """
    # Ensure the backend includes usage in the last chunk
    body.setdefault("stream_options", {})["continuous_usage_stats"] = True
    first_token = True
    ttft: float = 0.0
    token_counter: int = 0
    last_usage_data: Optional[dict] = None
    inter_token_latencies: list[float] = []
    http_status = "200"
    metrics_collector.record_router_latency_to_stream(
        model_id=model_id,
        latency=time.monotonic() - request_landing_time,
    )
    try:
        async for chunk in backend.forward_stream(
            request, body, upstream_url=upstream_url
        ):
            now = time.monotonic()
            is_last, parsed = _parse_sse_data(chunk)
            # Track timing for actual content chunks
            if parsed:
                if first_token:
                    ttft = now - start_time
                    first_token = False
                    prev_token_time = now
                else:
                    itl = now - prev_token_time
                    inter_token_latencies.append(itl)
                    prev_token_time = now

                if is_last:
                    last_usage_data = parsed.get("usage", None)
                else:
                    parsed.pop("usage", None)
                token_counter += 1
                yield f"data: {json.dumps(parsed)}\n\n".encode()
            else:
                # parsed is None: either the upstream [DONE] sentinel (is_last)
                # or an un-parseable keepalive/comment line. Forward one
                # well-formed [DONE] for the sentinel; skip the rest rather than
                # emitting spurious/malformed terminators mid-stream.
                if is_last:
                    yield b"data: [DONE]\n\n"

    except httpx.HTTPStatusError as exc:
        http_status = str(exc.response.status_code)
        logger.error("Backend streaming error for model=%s: %s", model_id, exc)
        # Yield an error SSE event so the client sees something
        error_payload = json.dumps(
            {"error": {"message": str(exc), "type": "backend_error"}}
        )
        yield f"data: {error_payload}\n\n".encode()
    except Exception:
        http_status = "500"
        logger.exception("Unexpected streaming error for model=%s", model_id)
        error_payload = json.dumps(
            {"error": {"message": "Internal proxy error", "type": "proxy_error"}}
        )
        yield f"data: {error_payload}\n\n".encode()
    finally:
        # Always decrement the concurrent request counter
        try:
            await rate_limit_service.decrement(redis, user_id)
            metrics_collector.record_concurrent_decrement(
                model_id=model_id, username=username
            )
        except Exception:
            logger.exception(
                "Failed to decrement concurrent counter for user=%s", username
            )
        duration = time.monotonic() - start_time

        # Extract token counts from the final usage chunk
        input_tokens, output_tokens = 0, 0
        if last_usage_data is not None:
            try:
                input_tokens, output_tokens = _extract_usage(last_usage_data)
            except Exception:
                logger.warning("Failed to extract usage from final chunk")
        else:
            logger.debug("Last chunk was empty.")
        # Fallback: if backend doesn't send usage chunk, use token counter for output
        if output_tokens == 0:
            output_tokens = token_counter

        metrics_collector.record_request_complete(
            model_id=model_id,
            username=username,
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


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.post("/chat/completions", response_model=None)
@TracingHelper.trace_async(
    span_kind=SpanKind.LLM, sensitive_keys=["payload", "db", "redis"]
)
async def chat_completions(
    request: Request,
    payload: Annotated[dict, Depends(get_current_user_payload_from_request)],
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
) -> StreamingResponse | JSONResponse:
    """
    Main LLM proxy endpoint (OpenAI-compatible).

    Handles both streaming (stream=true) and non-streaming requests.
    """
    # Capture request landing time at the very beginning
    # This measures from request received to when processing starts
    request_landing_time = time.monotonic()

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
    username, principal, max_concurrent = await _resolve_user(payload, db, redis)
    logger.debug(f"Took {time.monotonic() - request_landing_time} for resolving user")

    # Scope check for service keys (no-op for JWT users)
    _assert_scope(payload, "chat")

    # Check model access from JWT claims (no DB call) — before content filter
    # so unauthorized requests are rejected without wasting work scanning content.
    accessible_models = set(payload.get("accessible_models", []))
    if model_id not in accessible_models:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Model {model_id!r} is not accessible to this user",
        )

    # Content filter check
    filter_result = await content_filter_service.check_request_body(body)
    logger.debug(f"Took {time.monotonic() - request_landing_time} for filtering user")
    if filter_result:
        # Content violation detected
        logger.warning(
            "Content filter violation detected for user=%s: rule=%s, matched=%s",
            username,
            filter_result.rule_name,
            filter_result.matched_text,
        )
        if filter_result.is_blocking:
            error_message = filter_result.message or "Content violates filter rules"
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Content filter blocked: {error_message}",
            )

    # Resolve the backend before touching the rate-limit counter so a resolution
    # failure (HTTPException 503 etc.) never leaks an unreleased increment.
    backend = model_router.resolve(model_id)

    # Rate limit: check-and-increment (raises 429 if exceeded)
    await rate_limit_service.assert_within_limit(redis, principal, max_concurrent)
    metrics_collector.record_concurrent_increment(model_id=model_id, username=username)

    logger.debug(f"Took {time.monotonic() - request_landing_time} for redis")
    start_time = time.monotonic()
    is_streaming: bool = bool(body.get("stream", False))

    try:
        if is_streaming:
            return StreamingResponse(
                _stream_response(
                    backend=backend,  # type: ignore[arg-type]
                    request=request,
                    upstream_url="/chat/completions",
                    body=body,
                    model_id=model_id,
                    user_id=principal,
                    username=username,
                    start_time=start_time,
                    redis=redis,
                    request_landing_time=request_landing_time,
                ),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "X-Accel-Buffering": "no",
                },
            )

        # ---- Non-streaming path ----
        response = await backend.forward(
            request, body, upstream_url="/chat/completions"
        )
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
            # Propagate backend error response to client
            resp_body = _extract_error_body(response)

        metrics_collector.record_request_complete(
            model_id=model_id,
            username=str(username),
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
        logger.exception(
            "Unhandled proxy error for model=%s user=%s", model_id, username
        )
        metrics_collector.record_request_complete(
            model_id=model_id,
            username=str(username),
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
            await rate_limit_service.decrement(redis, principal)
            metrics_collector.record_concurrent_decrement(
                model_id=model_id, username=username
            )


@router.post("/embeddings")
@TracingHelper.trace_async(
    span_kind=SpanKind.EMBEDDING, sensitive_keys=["payload", "db", "redis"]
)
async def embeddings(
    request: Request,
    payload: Annotated[dict, Depends(get_current_user_payload_from_request)],
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

    username, principal, max_concurrent = await _resolve_user(payload, db, redis)

    # Scope check for service keys (no-op for JWT users)
    _assert_scope(payload, "embeddings")

    # Check model access from JWT claims (no DB call)
    accessible_models = set(payload.get("accessible_models", []))
    if model_id not in accessible_models:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Model {model_id!r} is not accessible to this user",
        )

    await rate_limit_service.assert_within_limit(redis, principal, max_concurrent)
    metrics_collector.record_concurrent_increment(model_id=model_id, username=username)

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

        resp_body = _extract_error_body(response)

        metrics_collector.record_request_complete(
            model_id=model_id,
            username=str(username),
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
            username=str(username),
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
        await rate_limit_service.decrement(redis, principal)
        metrics_collector.record_concurrent_decrement(
            model_id=model_id, username=username
        )


@router.get("/models")
async def list_models(
    payload: Annotated[dict, Depends(get_current_user_payload_from_request)],
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
) -> JSONResponse:
    """
    Return the list of models accessible to the current user.

    Response format matches the OpenAI /v1/models shape.
    Uses JWT claims for model list (no DB call for models).
    """
    await _resolve_user(payload, db, redis)

    # Scope check for service keys (no-op for JWT users)
    _assert_scope(payload, "models")

    # Get models from JWT claims (no DB call)
    model_ids = payload.get("accessible_models", [])

    # Hide models whose backend is currently unavailable (per DiscoveryService),
    # and any that are no longer registered.
    data = []
    for mid in sorted(model_ids):
        backend = registry.get(mid)
        if backend is not None and backend.is_available:
            data.append({"id": mid, "object": "model", "owned_by": "llm-router"})

    # Get the default model from the default bucket
    default_model = await permission_service.get_default_model(db)

    return JSONResponse(content={"object": "list", "data": data, "default_model": default_model})


# ---------------------------------------------------------------------------
# Anthropic /v1/messages endpoint
# ---------------------------------------------------------------------------


@router.post("/messages", response_model=None)
@TracingHelper.trace_async(
    span_kind=SpanKind.LLM, sensitive_keys=["payload", "db", "redis"]
)
async def messages(
    request: Request,
    payload: Annotated[dict, Depends(get_current_user_payload_from_request)],
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
) -> StreamingResponse | JSONResponse:
    """
    Anthropic-compatible /v1/messages endpoint.

    Forwards Anthropic-format requests to the backend /v1/messages endpoint.
    Supports both streaming (stream=true) and non-streaming requests.
    """
    # Capture request landing time at the very beginning
    request_landing_time = time.monotonic()

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
    username, principal, max_concurrent = await _resolve_user(payload, db, redis)
    logger.debug(f"Took {time.monotonic() - request_landing_time} for resolving user")

    # Scope check for service keys (no-op for JWT users)
    _assert_scope(payload, "messages")

    # Check model access from JWT claims (no DB call) — before content filter
    # so unauthorized requests are rejected without wasting work scanning content.
    accessible_models = set(payload.get("accessible_models", []))
    if model_id not in accessible_models:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Model {model_id!r} is not accessible to this user",
        )

    # Content filter check
    filter_result = await content_filter_service.check_request_body(body)
    logger.debug(f"Took {time.monotonic() - request_landing_time} for filtering user")
    if filter_result:
        logger.warning(
            "Content filter violation detected for user=%s: rule=%s, matched=%s",
            username,
            filter_result.rule_name,
            filter_result.matched_text,
        )
        if filter_result.is_blocking:
            error_message = filter_result.message or "Content violates filter rules"
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Content filter blocked: {error_message}",
            )

    # Resolve the backend before touching the rate-limit counter so a resolution
    # failure (HTTPException 503 etc.) never leaks an unreleased increment.
    backend = model_router.resolve(model_id)

    # Rate limit: check-and-increment (raises 429 if exceeded)
    await rate_limit_service.assert_within_limit(redis, principal, max_concurrent)
    metrics_collector.record_concurrent_increment(model_id=model_id, username=username)

    logger.debug(f"Took {time.monotonic() - request_landing_time} for redis")
    start_time = time.monotonic()
    is_streaming: bool = bool(body.get("stream", False))

    try:
        if is_streaming:
            return StreamingResponse(
                _stream_response(
                    backend=backend,  # type: ignore[arg-type]
                    request=request,
                    upstream_url="/messages",
                    body=body,
                    model_id=model_id,
                    user_id=principal,
                    username=username,
                    start_time=start_time,
                    redis=redis,
                    request_landing_time=request_landing_time,
                ),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "X-Accel-Buffering": "no",
                },
            )

        # ---- Non-streaming path ----
        response = await backend.forward(request, body, upstream_url="/messages")
        duration = time.monotonic() - start_time

        http_status = str(response.status_code)
        input_tokens, output_tokens = 0, 0

        if response.status_code == 200:
            try:
                resp_body = response.json()
                # Anthropic format uses input_tokens/output_tokens
                usage = resp_body.get("usage", {})
                input_tokens = usage.get("input_tokens", 0)
                output_tokens = usage.get("output_tokens", 0)
            except Exception:
                resp_body = _extract_error_body(response)
        else:
            # Propagate backend error response to client
            resp_body = _extract_error_body(response)

        metrics_collector.record_request_complete(
            model_id=model_id,
            username=str(username),
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
        logger.exception(
            "Unhandled proxy error for model=%s user=%s", model_id, username
        )
        metrics_collector.record_request_complete(
            model_id=model_id,
            username=str(username),
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
            await rate_limit_service.decrement(redis, principal)
            metrics_collector.record_concurrent_decrement(
                model_id=model_id, username=username
            )
