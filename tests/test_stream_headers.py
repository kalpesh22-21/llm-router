"""
Integration tests for session/trace header propagation through _stream_response.

These tests drive _stream_response end-to-end using a fake backend whose
forward_stream populates response_headers_out in-place (as the real StaticBackend
and MultiClusterBackend do) and yields a small number of SSE chunks.

Bootstrap strategy:
  app.api.proxy cannot be imported directly in this env because auth_routes.py
  pulls in ldap3 (not installed).  We apply the same monkeypatch/stub-injection
  technique used by test_sse_reframing.py: replace exactly the modules that
  proxy.py imports at module level with minimal fakes, grab the symbols we need,
  then restore sys.modules so nothing leaks across the session.

If the import-with-stubs still fails for any reason, the whole module is skipped
via pytest.importorskip-style handling at the bottom of the bootstrap block.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import types
from typing import AsyncIterator, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

# ---------------------------------------------------------------------------
# Hermetic bootstrap — mirrors test_sse_reframing.py exactly.
# ---------------------------------------------------------------------------

_saved: dict = {}


def _override(name: str, is_package: bool = False, **attrs) -> None:
    _saved.setdefault(name, (name in sys.modules, sys.modules.get(name)))
    mod = types.ModuleType(name)
    if is_package:
        mod.__path__ = []  # type: ignore[attr-defined]
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod


_saved.setdefault("app.api.proxy", ("app.api.proxy" in sys.modules,
                                     sys.modules.get("app.api.proxy")))

_override("app.services", is_package=True)
_override("app.backends", is_package=True)

_override("app.api.auth_routes",
          get_current_user_payload_from_request=MagicMock())
_override("app.backends.base", Backend=object, registry=MagicMock())
_override("app.db.mysql", get_db=MagicMock())
_override("app.db.redis_client",
          get_redis=MagicMock(),
          is_user_marked_deactivated=MagicMock(),
          mark_user_deactivated=MagicMock(),
          unmark_user_deactivated=MagicMock())
_override("app.metrics.collector", metrics_collector=MagicMock())
_override("app.routing.router", model_router=MagicMock())
_override("app.services.content_filter", content_filter_service=MagicMock())
_override("app.services.permission", permission_service=MagicMock())
_override("app.services.rate_limit", rate_limit_service=MagicMock())

_tracing_helper = MagicMock()
_tracing_helper.trace_async = lambda *a, **k: (lambda fn: fn)
_override("app.logging.pheonix", SpanKind=MagicMock(), TracingHelper=_tracing_helper)

_import_error: Optional[Exception] = None
_stream_response = None
_iter_sse_events = None

try:
    from app.api.proxy import _stream_response, _iter_sse_events  # noqa: E402
except Exception as _exc:
    _import_error = _exc

# Restore sys.modules so other tests re-import with their own fixtures.
for _name, (_was_present, _orig) in _saved.items():
    if _was_present:
        sys.modules[_name] = _orig
    else:
        sys.modules.pop(_name, None)

# If the import failed, mark every test in this module as skipped.
if _import_error is not None:
    pytestmark = pytest.mark.skip(
        reason=f"app.api.proxy could not be imported with stubs: {_import_error}"
    )


# ---------------------------------------------------------------------------
# Helpers shared by all tests below
# ---------------------------------------------------------------------------

_CHUNK_HELLO = b'data: {"choices":[{"delta":{"content":"Hello"},"finish_reason":null}]}\n\n'
_CHUNK_WORLD = b'data: {"choices":[{"delta":{"content":" world"},"finish_reason":null}]}\n\n'
_CHUNK_DONE  = b"data: [DONE]\n\n"


def _make_fake_backend(
    upstream_chunks: List[bytes],
    fake_upstream_headers: dict,
):
    """
    Return a fake backend whose forward_stream:
      1. Populates response_headers_out in-place from fake_upstream_headers.
      2. Yields upstream_chunks one by one (raw SSE bytes).
    """
    class _FakeBackend:
        async def forward_stream(
            self,
            request,
            body,
            upstream_url: str = "/chat/completions",
            response_headers_out: Optional[dict] = None,
        ) -> AsyncIterator[bytes]:
            if response_headers_out is not None:
                response_headers_out.update(fake_upstream_headers)
            for chunk in upstream_chunks:
                yield chunk

    return _FakeBackend()


def _drive_stream_with_headers(
    upstream_chunks: List[bytes],
    fake_upstream_headers: dict,
    session_headers: Optional[dict] = None,
) -> tuple[bytes, dict]:
    """
    Drive _stream_response with the given backend chunks and headers.

    Returns (body_bytes, populated_response_headers_out).
    """
    # rate_limit_service.decrement is awaited in the finally block.
    _iter_sse_events.__globals__["rate_limit_service"].decrement = AsyncMock()

    backend = _make_fake_backend(upstream_chunks, fake_upstream_headers)
    response_headers_out: dict = {}

    async def run() -> bytes:
        out: List[bytes] = []
        async for b in _stream_response(
            backend=backend,
            request=MagicMock(),
            upstream_url="/chat/completions",
            body={},
            model_id="test-model",
            user_id=42,
            username="tester",
            start_time=0.0,
            redis=MagicMock(),
            request_landing_time=0.0,
            session_headers=session_headers,
            response_headers_out=response_headers_out,
        ):
            out.append(b)
        return b"".join(out)

    body = asyncio.run(run())
    return body, response_headers_out


# ===========================================================================
# Tests: response_headers_out is populated in-place before first byte
# ===========================================================================


class TestResponseHeadersOut:
    """Verify that forward_stream populates response_headers_out before the first yield."""

    def test_response_headers_out_populated_by_backend(self):
        """
        After consuming the stream the response_headers_out dict must contain
        whatever the backend set — the upstream call happens before the first
        byte is yielded.
        """
        upstream_hdrs = {
            "x-session-id": "abc",
            "content-length": "5",
            "x-upstream-trace": "ut-001",
        }
        _, out_headers = _drive_stream_with_headers(
            [_CHUNK_HELLO, _CHUNK_DONE],
            fake_upstream_headers=upstream_hdrs,
        )
        assert out_headers["x-session-id"] == "abc"
        assert out_headers["content-length"] == "5"
        assert out_headers["x-upstream-trace"] == "ut-001"

    def test_response_headers_out_populated_even_on_minimal_stream(self):
        """A single-chunk stream still causes response_headers_out to be populated."""
        upstream_hdrs = {"x-request-id": "req-xyz"}
        _, out_headers = _drive_stream_with_headers(
            [_CHUNK_HELLO],
            fake_upstream_headers=upstream_hdrs,
        )
        assert out_headers["x-request-id"] == "req-xyz"

    def test_response_headers_out_empty_when_backend_sets_nothing(self):
        """When the backend doesn't touch response_headers_out it stays empty."""
        _, out_headers = _drive_stream_with_headers(
            [_CHUNK_HELLO, _CHUNK_DONE],
            fake_upstream_headers={},
        )
        assert out_headers == {}

    def test_response_headers_out_is_none_safe(self):
        """
        When response_headers_out is None (not passed), _stream_response must not
        raise — the backend's forward_stream skips the update safely.
        """
        _iter_sse_events.__globals__["rate_limit_service"].decrement = AsyncMock()

        backend = _make_fake_backend([_CHUNK_HELLO, _CHUNK_DONE], {"x-hdr": "v"})

        async def run() -> bytes:
            out: List[bytes] = []
            async for b in _stream_response(
                backend=backend,
                request=MagicMock(),
                upstream_url="/chat/completions",
                body={},
                model_id="m",
                user_id=1,
                username="u",
                start_time=0.0,
                redis=MagicMock(),
                request_landing_time=0.0,
                session_headers=None,
                response_headers_out=None,
            ):
                out.append(b)
            return b"".join(out)

        body = asyncio.run(run())
        assert b"Hello" in body  # stream body came through


# ===========================================================================
# Tests: diagnostic-log comparison (session_headers vs echoed backend headers)
# ===========================================================================


class TestSessionHeaderDiagnosticLog:
    """
    The first event consumed from the backend triggers a diagnostic log comparing
    forwarded session_headers against the session-allowlisted keys in
    response_headers_out.  These tests verify the log paths fire without raising.
    """

    def test_no_exception_when_session_headers_match_echoed(self, caplog):
        """
        When the backend echoes back every forwarded session header the INFO
        path fires; no exception must be raised and the stream body is intact.
        """
        session_hdrs = {"x-session-id": "abc"}
        upstream_hdrs = {"x-session-id": "abc", "content-length": "5"}

        with caplog.at_level(logging.INFO, logger="app.api.proxy"):
            body, _ = _drive_stream_with_headers(
                [_CHUNK_HELLO, _CHUNK_DONE],
                fake_upstream_headers=upstream_hdrs,
                session_headers=session_hdrs,
            )

        assert b"Hello" in body, "Stream body must be intact"
        # No exception was raised — test passes by reaching this line.

    def test_info_log_emitted_when_headers_match(self, caplog):
        """An INFO-level log must be emitted when all forwarded headers are echoed."""
        session_hdrs = {"x-session-id": "abc", "x-request-id": "r1"}
        upstream_hdrs = {"x-session-id": "abc", "x-request-id": "r1", "server": "nginx"}

        with caplog.at_level(logging.INFO, logger="app.api.proxy"):
            _drive_stream_with_headers(
                [_CHUNK_HELLO, _CHUNK_DONE],
                fake_upstream_headers=upstream_hdrs,
                session_headers=session_hdrs,
            )

        info_records = [
            r for r in caplog.records
            if r.levelno == logging.INFO and "header check" in r.message.lower()
        ]
        assert info_records, (
            "Expected at least one INFO log containing 'header check' but got: "
            + str([r.message for r in caplog.records])
        )

    def test_no_exception_when_session_headers_mismatch_echoed(self, caplog):
        """
        When the backend does NOT echo a forwarded header the WARNING path fires;
        no exception must be raised.
        """
        session_hdrs = {"x-session-id": "abc", "x-request-id": "r1"}
        # Backend only echoes x-session-id, not x-request-id
        upstream_hdrs = {"x-session-id": "abc"}

        with caplog.at_level(logging.WARNING, logger="app.api.proxy"):
            body, _ = _drive_stream_with_headers(
                [_CHUNK_HELLO, _CHUNK_DONE],
                fake_upstream_headers=upstream_hdrs,
                session_headers=session_hdrs,
            )

        assert b"Hello" in body, "Stream body must be intact even on header mismatch"

    def test_warning_log_emitted_when_headers_mismatch(self, caplog):
        """A WARNING-level log must be emitted when forwarded headers are not echoed."""
        session_hdrs = {"x-session-id": "abc"}
        upstream_hdrs = {}  # backend echoes nothing

        with caplog.at_level(logging.WARNING, logger="app.api.proxy"):
            _drive_stream_with_headers(
                [_CHUNK_HELLO, _CHUNK_DONE],
                fake_upstream_headers=upstream_hdrs,
                session_headers=session_hdrs,
            )

        warn_records = [
            r for r in caplog.records
            if r.levelno == logging.WARNING and "mismatch" in r.message.lower()
        ]
        assert warn_records, (
            "Expected at least one WARNING log containing 'mismatch' but got: "
            + str([r.message for r in caplog.records])
        )

    def test_no_log_when_no_session_headers_forwarded(self, caplog):
        """
        When session_headers is empty or None (no session headers on the request)
        neither an INFO nor a WARNING log is emitted for the header comparison.
        """
        upstream_hdrs = {"x-session-id": "will-not-be-compared", "server": "nginx"}

        with caplog.at_level(logging.DEBUG, logger="app.api.proxy"):
            _drive_stream_with_headers(
                [_CHUNK_HELLO, _CHUNK_DONE],
                fake_upstream_headers=upstream_hdrs,
                session_headers={},   # nothing forwarded
            )

        comparison_records = [
            r for r in caplog.records
            if "header check" in r.message.lower() or "mismatch" in r.message.lower()
        ]
        assert not comparison_records, (
            "No header comparison log expected when no session headers forwarded; "
            "got: " + str([r.message for r in comparison_records])
        )

    def test_diagnostic_log_fires_only_once(self, caplog):
        """
        The _backend_headers_logged guard ensures the comparison log is emitted
        at most once per stream, not once per SSE event.
        """
        session_hdrs = {"x-session-id": "s"}
        upstream_hdrs = {"x-session-id": "s"}

        # Send 3 chunks to ensure the guard works across multiple events.
        chunks = [_CHUNK_HELLO, _CHUNK_WORLD, _CHUNK_DONE]

        with caplog.at_level(logging.INFO, logger="app.api.proxy"):
            _drive_stream_with_headers(
                chunks,
                fake_upstream_headers=upstream_hdrs,
                session_headers=session_hdrs,
            )

        comparison_records = [
            r for r in caplog.records
            if "header check" in r.message.lower() or "mismatch" in r.message.lower()
        ]
        assert len(comparison_records) == 1, (
            f"Diagnostic log must fire exactly once; got {len(comparison_records)}: "
            + str([r.message for r in comparison_records])
        )


# ===========================================================================
# Tests: end-to-end body correctness (stream content is unaffected by headers)
# ===========================================================================


class TestStreamBodyWithHeaders:
    """Verify that header propagation does not corrupt the SSE stream body."""

    def test_stream_body_contains_content_chunks(self):
        body, _ = _drive_stream_with_headers(
            [_CHUNK_HELLO, _CHUNK_WORLD, _CHUNK_DONE],
            fake_upstream_headers={"x-session-id": "s1"},
            session_headers={"x-session-id": "s1"},
        )
        assert b"Hello" in body
        assert b"world" in body

    def test_done_emitted_at_end_of_stream(self):
        body, _ = _drive_stream_with_headers(
            [_CHUNK_HELLO, _CHUNK_DONE],
            fake_upstream_headers={"x-session-id": "s1"},
            session_headers={"x-session-id": "s1"},
        )
        text = body.decode()
        assert text.rstrip().endswith("data: [DONE]")
        assert text.count("[DONE]") == 1

    def test_multiple_session_headers_forwarded_all_echoed(self):
        session_hdrs = {
            "x-session-id": "sess-abc",
            "x-request-id": "req-xyz",
            "traceparent": "00-trace-01",
        }
        upstream_hdrs = {
            **session_hdrs,
            "content-length": "0",   # will be stripped by caller, not here
        }
        body, out_headers = _drive_stream_with_headers(
            [_CHUNK_HELLO, _CHUNK_DONE],
            fake_upstream_headers=upstream_hdrs,
            session_headers=session_hdrs,
        )
        # All three session headers are in the out dict (the caller reads it to
        # build the StreamingResponse headers).
        assert out_headers["x-session-id"] == "sess-abc"
        assert out_headers["x-request-id"] == "req-xyz"
        assert out_headers["traceparent"] == "00-trace-01"
        assert b"Hello" in body
