"""
Session/trace header utilities.

Provides helpers for propagating correlation headers between clients and
LLM backends on both the request side (forwarding to upstream) and the
response side (passing back to the client).

Headers in SESSION_HEADER_ALLOWLIST are forwarded verbatim to the backend
and echoed back to the client when they are present in the backend response.
Hop-by-hop and framing headers in _RESPONSE_STRIP_SET are always removed from
backend responses before passing them to the client — we set our own framing.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # starlette.datastructures.Headers is dict-like and case-insensitive.
    from fastapi import Request

# ---------------------------------------------------------------------------
# Allowlist — headers we propagate from the client to the backend and back.
# All names are lowercase; Starlette Headers are case-insensitive on lookup.
# ---------------------------------------------------------------------------

SESSION_HEADER_ALLOWLIST: frozenset[str] = frozenset(
    {
        "x-session-id",
        "x-request-id",
        "x-correlation-id",
        "x-trace-id",
        "traceparent",
        "tracestate",
        "baggage",
    }
)

# ---------------------------------------------------------------------------
# Strip set — headers that must NOT be copied from backend responses.
#
# content-type / content-length: we set our own (JSONResponse / StreamingResponse).
# Transfer-encoding / content-encoding: framing changes between backend and client.
# Hop-by-hop headers (RFC 7230 §6.1): must never be forwarded by proxies.
# ---------------------------------------------------------------------------

_RESPONSE_STRIP_SET: frozenset[str] = frozenset(
    {
        "content-length",
        "content-encoding",
        "transfer-encoding",
        "connection",
        "keep-alive",
        "te",
        "trailer",
        "upgrade",
        "proxy-authorization",
        "proxy-authenticate",
        "host",
        # Never relay backend auth challenges or cookies to API clients.
        "www-authenticate",
        "set-cookie",
        # We own framing/content negotiation for these two:
        "content-type",
        "date",
        "server",
    }
)


def extract_session_headers(request: "Request") -> dict[str, str]:
    """
    Return the subset of *request* headers whose lowercase name is in
    SESSION_HEADER_ALLOWLIST, preserving original casing of values.

    Starlette's Headers object is case-insensitive, so lookups are safe
    regardless of how the client capitalised the header names.

    Args:
        request: The incoming FastAPI / Starlette Request.

    Returns:
        A plain dict mapping lowercase header name → original header value.
        Empty dict if no session headers are present.
    """
    result: dict[str, str] = {}
    for name in SESSION_HEADER_ALLOWLIST:
        value = request.headers.get(name)
        if value is not None:
            result[name] = value
    return result


def filter_response_headers(headers: "httpx.Headers | dict") -> dict[str, str]:  # type: ignore[name-defined]  # noqa: F821
    """
    Return a copy of *headers* with hop-by-hop and framing headers removed.

    Safe to pass directly to ``JSONResponse(headers=...)`` or as extra
    headers on a ``StreamingResponse`` — we never include headers that would
    conflict with response framing that FastAPI / Starlette sets itself.

    Args:
        headers: An httpx.Headers instance or any dict-like mapping.

    Returns:
        A plain ``dict[str, str]`` containing only the headers that are safe
        to forward to the client.
    """
    result: dict[str, str] = {}
    for key, value in headers.items():
        if key.lower() not in _RESPONSE_STRIP_SET:
            result[key.lower()] = value
    return result
