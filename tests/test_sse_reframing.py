"""
Unit tests for app.api.proxy._iter_sse_events — the SSE re-framer.

Background:
  httpx's aiter_bytes() yields arbitrary socket reads that do NOT respect SSE
  event boundaries. The proxy previously called _parse_sse_data() once per
  network chunk, which broke when upstream batched several events into one read
  (only the first was forwarded) or split one event's JSON across two reads
  (each half failed json.loads and was dropped). The symptom was mangled framing
  reaching the client, e.g. "[DONE]data: [DONE]data: {…}".

  _iter_sse_events buffers across reads and yields exactly one complete event
  block per iteration, so _parse_sse_data always sees a whole event.

conftest.py sets the safe env defaults required to import app.* without Vault.
"""

from __future__ import annotations

import asyncio
import json
import sys
import types
from typing import AsyncIterator, List
from unittest.mock import AsyncMock, MagicMock

# ---------------------------------------------------------------------------
# Hermetic bootstrap — import app.api.proxy with its heavy dependencies faked,
# then restore sys.modules exactly as it was.
#
# proxy.py pulls a deep import chain (ldap3, the MySQL engine, k8s cluster
# manager, …) that isn't available in the minimal local test env. The functions
# under test (_iter_sse_events, _parse_sse_data) are pure SSE byte-framing and
# need none of it, so we fake exactly the modules proxy.py imports directly.
#
# We OVERRIDE (not setdefault) because another test module in the same pytest
# session may have left partial stubs of these names behind, and snapshot every
# name we touch — including app.api.proxy itself — restoring it afterwards so
# other tests re-import the real chain with their own fixtures untouched. The
# two functions we grab keep working because they reference only json/logging.
# Mirrors the stub-injection pattern in test_service_api_keys.py.
# ---------------------------------------------------------------------------

_saved: dict = {}  # name -> (was_present, original_module)


def _override(name: str, is_package: bool = False, **attrs) -> None:
    _saved.setdefault(name, (name in sys.modules, sys.modules.get(name)))
    mod = types.ModuleType(name)
    if is_package:
        mod.__path__ = []  # type: ignore[attr-defined]
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod


# Snapshot proxy itself so our import doesn't leave a fake-backed proxy cached
# for other tests that import it for real.
_saved.setdefault("app.api.proxy", ("app.api.proxy" in sys.modules,
                                     sys.modules.get("app.api.proxy")))

# Packages whose __init__ has heavy eager imports (app.services → k8s cluster
# manager; app.backends → static/multi_cluster/config). Empty-__init__ packages
# (app.db, app.metrics, …) are left real so app.api.proxy still resolves.
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

# TracingHelper.trace_async is applied as a decorator at module import time;
# it must return a pass-through decorator. SpanKind.LLM/.EMBEDDING are read at
# decoration time — MagicMock resolves any attribute.
_tracing_helper = MagicMock()
_tracing_helper.trace_async = lambda *a, **k: (lambda fn: fn)
_override("app.logging.pheonix", SpanKind=MagicMock(), TracingHelper=_tracing_helper)

from app.api.proxy import (  # noqa: E402
    _iter_sse_events,
    _parse_sse_event,
    _stream_response,
)

# Restore sys.modules to its pre-bootstrap state so nothing leaks across the
# session (including the now fake-backed app.api.proxy).
for _name, (_was_present, _orig) in _saved.items():
    if _was_present:
        sys.modules[_name] = _orig
    else:
        sys.modules.pop(_name, None)


async def _feed(chunks: List[bytes]) -> AsyncIterator[bytes]:
    for c in chunks:
        yield c


def _collect(chunks: List[bytes]) -> List[bytes]:
    async def run() -> List[bytes]:
        return [event async for event in _iter_sse_events(_feed(chunks))]

    return asyncio.run(run())


def test_single_event_per_chunk_passthrough():
    events = _collect([b'data: {"a": 1}\n\n', b"data: [DONE]\n\n"])
    assert events == [b'data: {"a": 1}', b"data: [DONE]"]


def test_multiple_events_batched_in_one_chunk_are_split():
    # The exact failure mode: final content chunk batched with [DONE] in one read.
    batched = (
        b'data: {"id": "x", "choices": [{"finish_reason": "tool_calls"}]}\n\n'
        b"data: [DONE]\n\n"
    )
    events = _collect([batched])
    assert len(events) == 2
    assert _parse_sse_event(events[0]) == (
        [{"id": "x", "choices": [{"finish_reason": "tool_calls"}]}],
        False,
    )
    assert _parse_sse_event(events[1]) == ([], True)


def test_event_split_across_chunks_is_reassembled():
    # One JSON payload arrives across two socket reads; must not be dropped.
    part1 = b'data: {"id": "chatcmpl-1", "choices": [{"delta": '
    part2 = b'{"content": "hi"}, "finish_reason": null}]}\n\n'
    events = _collect([part1, part2])
    assert len(events) == 1
    chunks, saw_done = _parse_sse_event(events[0])
    assert saw_done is False
    assert chunks[0]["choices"][0]["delta"]["content"] == "hi"


def test_crlf_framing_is_normalized():
    events = _collect([b'data: {"a": 1}\r\n\r\ndata: [DONE]\r\n\r\n'])
    assert events == [b'data: {"a": 1}', b"data: [DONE]"]


def test_trailing_event_without_blank_line_is_flushed():
    # Upstream closes after a final event with no terminating blank line.
    events = _collect([b'data: {"a": 1}\n\n', b"data: [DONE]"])
    assert events == [b'data: {"a": 1}', b"data: [DONE]"]


def test_empty_and_keepalive_chunks_are_ignored():
    events = _collect([b"", b"\n\n", b'data: {"a": 1}\n\n', b"   \n\n"])
    assert events == [b'data: {"a": 1}']


def test_byte_dribble_one_byte_at_a_time():
    payload = b'data: {"id": "z", "choices": []}\n\ndata: [DONE]\n\n'
    events = _collect([payload[i : i + 1] for i in range(len(payload))])
    assert events == [b'data: {"id": "z", "choices": []}', b"data: [DONE]"]


# ---------------------------------------------------------------------------
# Guard against the cache-aware-router corruption: several records glued into
# one event with the \n\n separators stripped + duplicated/misordered [DONE].
# ---------------------------------------------------------------------------


def test_clean_event_parses_as_single_chunk():
    assert _parse_sse_event(b'data: {"a": 1}') == ([{"a": 1}], False)
    assert _parse_sse_event(b"data: [DONE]") == ([], True)
    assert _parse_sse_event(b"data: ") == ([], False)


def test_glued_done_done_chunk_is_recovered():
    # The exact reported corruption: two [DONE] sentinels glued ahead of the
    # real tool-call chunk, on one line with no \n\n separators.
    event = (
        b'data: [DONE]data: [DONE]data: '
        b'{"id": "c1", "choices": [{"delta": {"content": ""}, '
        b'"finish_reason": "tool_calls"}]}'
    )
    assert _parse_sse_event(event) == (
        [{"id": "c1", "choices": [{"delta": {"content": ""},
                                   "finish_reason": "tool_calls"}]}],
        True,
    )


def test_payload_containing_literal_data_colon_is_not_missplit():
    # A legitimate JSON payload whose content contains "data:" must parse as
    # ONE chunk — the glued-split fallback only triggers on parse failure.
    payload = {"choices": [{"delta": {"content": "see data: foo"}}]}
    assert _parse_sse_event(b"data: " + json.dumps(payload).encode()) == ([payload], False)


def test_n_dones_glued_before_chunk_recovered():
    # N (=3) [DONE] sentinels glued ahead of the real chunk on one line collapse
    # to saw_done=True and the single recovered chunk.
    event = b'data: [DONE]data: [DONE]data: [DONE]data: {"choices": [{"finish_reason": "tool_calls"}]}'
    assert _parse_sse_event(event) == (
        [{"choices": [{"finish_reason": "tool_calls"}]}],
        True,
    )


# ---------------------------------------------------------------------------
# End-to-end through _stream_response: the swallow-every-[DONE] + emit-one-
# terminal-at-true-end logic. This is the behavior the client actually sees.
# ---------------------------------------------------------------------------


def _drive_stream(upstream_chunks: List[bytes]) -> bytes:
    """Run _stream_response over a fake backend and return the bytes the client gets."""
    # rate_limit_service.decrement is awaited in the finally block.
    _iter_sse_events.__globals__["rate_limit_service"].decrement = AsyncMock()

    class _Backend:
        async def forward_stream(
            self, request, body, upstream_url="/chat/completions",
            response_headers_out=None,
        ):
            if response_headers_out is not None:
                response_headers_out.update({"content-type": "text/event-stream"})
            for c in upstream_chunks:
                yield c

    async def run() -> bytes:
        out = []
        async for b in _stream_response(
            backend=_Backend(),
            request=MagicMock(),
            upstream_url="/chat/completions",
            body={},
            model_id="m",
            user_id=1,
            username="u",
            start_time=0.0,
            redis=MagicMock(),
            request_landing_time=0.0,
        ):
            out.append(b)
        return b"".join(out)

    return asyncio.run(run())


def test_stream_swallows_n_dones_before_chunk_and_emits_single_terminal():
    # The reported corruption: a content chunk, then N [DONE] glued AHEAD of the
    # final tool-call chunk. Client must get both chunks + exactly one trailing
    # [DONE], correctly ordered.
    corrupted = (
        b'data: {"choices": [{"delta": {"content": "hi"}}]}\n\n'
        b'data: [DONE]data: [DONE]data: [DONE]data: '
        b'{"choices": [{"delta": {}, "finish_reason": "tool_calls"}]}\n\n'
    )
    text = _drive_stream([corrupted]).decode()

    assert text.count("[DONE]") == 1                      # de-duplicated
    assert text.rstrip().endswith("data: [DONE]")         # and emitted last
    assert '"content": "hi"' in text                      # content survived
    assert '"finish_reason": "tool_calls"' in text        # tool-call chunk survived
    # the terminal comes AFTER the tool-call chunk, not before
    assert text.index('"tool_calls"') < text.rstrip().rindex("[DONE]")


def test_stream_properly_framed_dones_before_chunk_also_deduped():
    # Same defect but framed with real \n\n separators (N separate [DONE] events).
    chunks = [
        b'data: [DONE]\n\n',
        b'data: [DONE]\n\n',
        b'data: {"choices": [{"finish_reason": "tool_calls"}]}\n\n',
    ]
    text = _drive_stream(chunks).decode()
    assert text.count("[DONE]") == 1
    assert text.rstrip().endswith("data: [DONE]")
    assert '"finish_reason": "tool_calls"' in text
