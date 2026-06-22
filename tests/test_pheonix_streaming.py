"""
Unit tests for Phoenix/OpenInference streaming tracing in app.logging.pheonix.

Covers:
  Tracing._parse_chunk          — parse one SSE chunk into (text, has_error).
  Tracing._extract_stream_text  — pure extraction from SSE chunks (no OTel deps).
  Tracing._trace_stream         — async generator span lifecycle using a FakeSpan.

Bootstrap note:
  The OpenTelemetry / Phoenix libraries are optional and NOT installed in this
  test environment; pheonix.py falls back to Status = StatusCode = None at
  module level.  We monkeypatch both names to simple fakes before importing the
  module-under-test so _trace_stream can call Status(StatusCode.OK/ERROR).

  conftest.py sets VAULT_ENABLED=false / JWT_SECRET_KEY / API_KEY /
  PHOENIX_TRACING_ENABLED=false before this file is imported, which prevents
  settings from trying to reach Vault and keeps get_tracing_config() from
  attempting to register a real Phoenix endpoint.

  The same stub-injection pattern used by test_model_backend_crypto.py and
  test_database_backend_source.py is applied here.

Known limitations (intentionally NOT fixed in pheonix.py):
  BUG-3 (un-primed generator aclose): aclose() on a _trace_stream generator
         that has never been advanced (no __anext__() call) does NOT fire the
         finally block on Python 3.9.  See test_early_disconnect_before_first_chunk
         for the documented known limitation.
"""

from __future__ import annotations

import asyncio
import pathlib
import sys
import types
from enum import Enum

import pytest

# ---------------------------------------------------------------------------
# Bootstrap — must happen BEFORE any app.* import.
# ---------------------------------------------------------------------------

_ROOT = pathlib.Path(__file__).parents[1]


def _stub_if_absent(name: str, attrs: dict) -> None:
    if name not in sys.modules:
        m = types.ModuleType(name)
        for k, v in attrs.items():
            setattr(m, k, v)
        sys.modules[name] = m


def _bootstrap_stubs() -> None:
    # app.core.httpx uses `X | None` union syntax (Python 3.10+).
    _stub_if_absent("app.core.httpx", {
        "new_http_client": lambda: None,
        "close_http_client": lambda: None,
        "BACKEND_TIMEOUT": None,
    })

    # app.utils.api_key_validator makes outbound HTTP calls.
    _stub_if_absent("app.utils.api_key_validator", {
        "find_working_api_key": lambda: None,
    })

    # app.auth.ldap_auth may not have an LDAP server available.
    _stub_if_absent("app.auth.ldap_auth", {
        "authenticate_ldap": lambda u, p: False,
        "get_ldap_user_info": lambda u, p: {},
    })

    # app/services/__init__.py eagerly imports cluster_manager (Redis/DB chain).
    if "app.services" not in sys.modules:
        stub_svc_pkg = types.ModuleType("app.services")
        stub_svc_pkg.__path__ = [str(_ROOT / "app" / "services")]  # type: ignore[attr-defined]
        stub_svc_pkg.__package__ = "app.services"  # type: ignore[attr-defined]
        sys.modules["app.services"] = stub_svc_pkg


_bootstrap_stubs()

# ---------------------------------------------------------------------------
# Monkeypatch Status / StatusCode BEFORE importing pheonix so that _trace_stream
# can call Status(StatusCode.OK/ERROR) even without OpenTelemetry installed.
# ---------------------------------------------------------------------------

import app.logging.pheonix as _pheonix_mod  # noqa: E402


class _FakeStatusCode(Enum):
    OK = "OK"
    ERROR = "ERROR"


class _FakeStatus:
    """Minimal stand-in for opentelemetry.trace.Status."""

    def __init__(self, status_code: _FakeStatusCode) -> None:
        self.status_code = status_code


# Replace module-level None with our fakes so _trace_stream can use them.
_pheonix_mod.Status = _FakeStatus       # type: ignore[attr-defined]
_pheonix_mod.StatusCode = _FakeStatusCode  # type: ignore[attr-defined]

from app.logging.pheonix import Tracing  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_tracing() -> Tracing:
    """Return a Tracing instance with tracing disabled (no real tracer needed)."""
    return Tracing(tracing_enabled=False)


class FakeSpan:
    """
    Minimal span double recording every call made on it.

    Attributes match what _trace_stream and trace_async invoke:
      set_output, set_status, record_exception, end.
    """

    def __init__(self) -> None:
        self.output = None
        self.status: _FakeStatus | None = None
        self.exceptions: list = []
        self.end_count: int = 0

    def set_output(self, value: object) -> None:
        self.output = value

    def set_status(self, status: _FakeStatus) -> None:
        self.status = status

    def record_exception(self, exc: Exception) -> None:
        self.exceptions.append(exc)

    def end(self) -> None:
        self.end_count += 1


# A few chunk constants reused across tests.
_OAI_HELLO = b'data: {"choices":[{"delta":{"content":"Hello"}}]}\n\n'
_OAI_WORLD = b'data: {"choices":[{"delta":{"content":" world"}}]}\n\n'
_OAI_DONE  = b"data: [DONE]\n\n"

_ANTHROPIC_HI = b'data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"Hi"}}\n\n'


# ---------------------------------------------------------------------------
# Async iterator helpers
# ---------------------------------------------------------------------------


async def _async_iter(chunks: list):
    """Yield each chunk from a list as an async iterator."""
    for chunk in chunks:
        yield chunk


async def _raising_iter(chunks: list, exc: Exception):
    """Yield chunks from a list, then raise exc."""
    for chunk in chunks:
        yield chunk
    raise exc


# ===========================================================================
# Tests: _extract_stream_text
# ===========================================================================


class TestExtractStreamText:
    """Pure extraction tests — no tracing deps, no async."""

    def setup_method(self) -> None:
        self.t = _make_tracing()

    # --- OpenAI format ---

    def test_openai_content_returned(self):
        chunk = b'data: {"choices":[{"delta":{"content":"Hello"}}]}\n\n'
        assert self.t._extract_stream_text(chunk) == "Hello"

    def test_openai_role_only_delta_returns_empty(self):
        """A role-only delta (first chunk) has no content — must return ''."""
        chunk = b'data: {"choices":[{"delta":{"role":"assistant"}}]}\n\n'
        assert self.t._extract_stream_text(chunk) == ""

    def test_openai_finish_reason_chunk_returns_empty(self):
        """A choices[0].finish_reason chunk with empty content returns ''."""
        chunk = b'data: {"choices":[{"delta":{},"finish_reason":"stop"}]}\n\n'
        assert self.t._extract_stream_text(chunk) == ""

    def test_openai_null_content_returns_empty(self):
        """Explicit null content value must not be appended."""
        chunk = b'data: {"choices":[{"delta":{"content":null}}]}\n\n'
        assert self.t._extract_stream_text(chunk) == ""

    def test_openai_empty_string_content_returns_empty(self):
        """Empty-string content is falsy and must not be appended."""
        chunk = b'data: {"choices":[{"delta":{"content":""}}]}\n\n'
        assert self.t._extract_stream_text(chunk) == ""

    # --- Anthropic format ---

    def test_anthropic_text_delta_returned(self):
        chunk = b'data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"Hi"}}\n\n'
        assert self.t._extract_stream_text(chunk) == "Hi"

    def test_anthropic_input_json_delta_returns_empty(self):
        """
        input_json_delta has delta.type == "input_json_delta" which is NOT in
        ("text_delta", None), so the impl correctly returns "".

        The partial_json value itself contains a bare '{' which is invalid JSON
        at the top level; we verify the impl does not raise on this.
        """
        chunk = (
            b'data: {"type":"content_block_delta",'
            b'"delta":{"type":"input_json_delta","partial_json":"{"}}\n\n'
        )
        assert self.t._extract_stream_text(chunk) == ""

    def test_anthropic_delta_with_no_type_field_returns_empty(self):
        """
        When delta.type is ABSENT, the tightened implementation now requires
        delta["type"] == "text_delta" explicitly.  A content_block_delta whose
        delta dict has a "text" key but NO "type" key therefore returns "".
        """
        chunk = b'data: {"type":"content_block_delta","delta":{"text":"Hi"}}\n\n'
        assert self.t._extract_stream_text(chunk) == ""

    def test_anthropic_non_content_block_delta_type_skipped(self):
        """Anthropic event types other than content_block_delta produce no text."""
        chunk = b'data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}\n\n'
        assert self.t._extract_stream_text(chunk) == ""

    # --- SSE sentinel and noise ---

    def test_done_sentinel_returns_empty(self):
        assert self.t._extract_stream_text(b"data: [DONE]\n\n") == ""

    def test_empty_data_line_returns_empty(self):
        assert self.t._extract_stream_text(b"data: \n\n") == ""

    def test_comment_keepalive_line_returns_empty(self):
        assert self.t._extract_stream_text(b": ping\n\n") == ""

    def test_non_json_data_line_returns_empty_no_raise(self):
        assert self.t._extract_stream_text(b"data: this is not json\n\n") == ""

    def test_partial_json_returns_empty_no_raise(self):
        assert self.t._extract_stream_text(b'data: {"choices":[{"delta"\n\n') == ""

    # --- str vs bytes ---

    def test_str_input_works_same_as_bytes(self):
        chunk_str = 'data: {"choices":[{"delta":{"content":"Hello"}}]}\n\n'
        assert self.t._extract_stream_text(chunk_str) == "Hello"

    def test_str_anthropic_input(self):
        chunk_str = 'data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"Hi"}}\n\n'
        assert self.t._extract_stream_text(chunk_str) == "Hi"

    # --- Multi-line chunks ---

    def test_multi_line_chunk_accumulates_both_data_lines(self):
        """A single chunk may contain multiple data: lines."""
        chunk = (
            b'data: {"choices":[{"delta":{"content":"A"}}]}\n'
            b'data: {"choices":[{"delta":{"content":"B"}}]}\n\n'
        )
        assert self.t._extract_stream_text(chunk) == "AB"

    def test_multi_line_mixes_data_and_noise(self):
        """Lines that are not data: prefixed must be silently skipped."""
        chunk = (
            b': keep-alive\n'
            b'data: {"choices":[{"delta":{"content":"X"}}]}\n'
            b'event: message\n'
            b'data: [DONE]\n\n'
        )
        assert self.t._extract_stream_text(chunk) == "X"

    # --- Malformed / garbage bytes ---

    def test_garbage_bytes_return_empty_no_raise(self):
        """High-byte garbage must not raise; utf-8 errors='replace' is used."""
        assert self.t._extract_stream_text(b"\xff\xfe\x00garbage") == ""

    def test_bytes_with_replacement_char_in_non_data_line(self):
        """Replacement characters in a comment line are ignored cleanly."""
        chunk = b"\xff\xfe: comment\n" + _OAI_HELLO
        assert self.t._extract_stream_text(chunk) == "Hello"

    def test_entirely_empty_bytes(self):
        assert self.t._extract_stream_text(b"") == ""

    def test_entirely_empty_str(self):
        assert self.t._extract_stream_text("") == ""

    def test_whitespace_only_chunk(self):
        assert self.t._extract_stream_text(b"   \n\n") == ""

    # --- Edge: both OpenAI and Anthropic keys present in same payload (shouldn't
    #     normally happen but the impl handles it by extracting both) ---

    def test_openai_choices_and_anthropic_branches_both_fire(self):
        """
        If a payload has both 'choices' (OpenAI format) and
        'type':'content_block_delta' (Anthropic format), both branches fire
        and their text is concatenated — OpenAI first, then Anthropic.

        NOTE: In this constructed payload the Anthropic branch reads
        payload['delta'] which is the top-level 'delta' key (shared with the
        OpenAI choices[0].delta nested structure).  The expected result is
        "OAIANT": "OAI" from choices[0].delta.content, then "ANT" from
        the top-level delta.text.
        """
        chunk = (
            b'data: {"choices":[{"delta":{"content":"OAI"}}],'
            b'"type":"content_block_delta",'
            b'"delta":{"type":"text_delta","text":"ANT"}}\n\n'
        )
        result = self.t._extract_stream_text(chunk)
        # OpenAI branch fires first (OAI), Anthropic branch fires second (ANT).
        assert result == "OAIANT"


# ===========================================================================
# Tests: _trace_stream
# ===========================================================================


class TestTraceStream:
    """Async generator lifecycle tests using FakeSpan."""

    def _make_span(self) -> FakeSpan:
        return FakeSpan()

    # --- Happy path ---

    def test_happy_path_chunks_yielded_unchanged_and_in_order(self):
        """Every chunk is passed through to the consumer unchanged and in order."""

        async def _run():
            t = _make_tracing()
            span = self._make_span()
            input_chunks = [_OAI_HELLO, _OAI_WORLD, _OAI_DONE]
            collected = []
            async for chunk in t._trace_stream(span, _async_iter(input_chunks)):
                collected.append(chunk)
            assert collected == input_chunks

        asyncio.run(_run())

    def test_happy_path_set_output_receives_full_concatenated_text(self):
        """set_output must be called with the joined text of all chunks."""

        async def _run():
            t = _make_tracing()
            span = self._make_span()
            chunks = [_OAI_HELLO, _OAI_WORLD, _OAI_DONE]
            async for _ in t._trace_stream(span, _async_iter(chunks)):
                pass
            assert span.output == "Hello world"

        asyncio.run(_run())

    def test_happy_path_set_status_ok(self):
        """After normal completion, set_status must be called with StatusCode.OK."""

        async def _run():
            t = _make_tracing()
            span = self._make_span()
            async for _ in t._trace_stream(span, _async_iter([_OAI_HELLO])):
                pass
            assert span.status is not None
            assert span.status.status_code == _FakeStatusCode.OK

        asyncio.run(_run())

    def test_happy_path_end_called_exactly_once(self):
        """span.end() must be called exactly once on normal completion."""

        async def _run():
            t = _make_tracing()
            span = self._make_span()
            async for _ in t._trace_stream(span, _async_iter([_OAI_HELLO, _OAI_DONE])):
                pass
            assert span.end_count == 1

        asyncio.run(_run())

    def test_happy_path_empty_iterator_ends_cleanly(self):
        """An empty body_iterator still triggers OK status and end()."""

        async def _run():
            t = _make_tracing()
            span = self._make_span()
            async for _ in t._trace_stream(span, _async_iter([])):
                pass
            assert span.output == ""
            assert span.status.status_code == _FakeStatusCode.OK
            assert span.end_count == 1

        asyncio.run(_run())

    def test_happy_path_anthropic_chunks_accumulate(self):
        """Anthropic SSE chunks are extracted and joined correctly."""

        async def _run():
            t = _make_tracing()
            span = self._make_span()
            chunks = [
                b'data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"Hello"}}\n\n',
                b'data: {"type":"content_block_delta","delta":{"type":"text_delta","text":" there"}}\n\n',
                b'data: {"type":"message_stop"}\n\n',
            ]
            async for _ in t._trace_stream(span, _async_iter(chunks)):
                pass
            assert span.output == "Hello there"

        asyncio.run(_run())

    # --- Exception path ---

    def test_exception_propagates_to_caller(self):
        """
        When body_iterator raises, the ORIGINAL exception must propagate out of
        _trace_stream to the caller — not a secondary TypeError or any other
        wrapper (BUG-1 is fixed: span.record_exception(e) now uses the original
        exception directly, no traceback.format_exception call).
        """

        async def _run():
            t = _make_tracing()
            span = self._make_span()
            exc = ValueError("stream broke")
            raised = None
            try:
                async for _ in t._trace_stream(span, _raising_iter([_OAI_HELLO], exc)):
                    pass
            except Exception as e:
                raised = e
            assert raised is exc, (
                f"Expected the original ValueError to propagate; got {raised!r}"
            )

        asyncio.run(_run())

    def test_exception_end_called_exactly_once(self):
        """
        span.end() must be called exactly once even when body_iterator raises.

        The finally block in _trace_stream guarantees this regardless of any
        secondary error in the except block (see BUG-1).
        """

        async def _run():
            t = _make_tracing()
            span = self._make_span()
            try:
                async for _ in t._trace_stream(span, _raising_iter([_OAI_HELLO], ValueError("oops"))):
                    pass
            except Exception:
                pass
            assert span.end_count == 1

        asyncio.run(_run())

    def test_exception_accumulated_text_set_as_output(self):
        """
        set_output must receive the text accumulated BEFORE the exception.

        With BUG-1 fixed (span.record_exception(e) uses the original exception
        directly), all three calls in the except block — set_output, record_exception,
        set_status — execute cleanly on Python 3.9+.  set_output receives the
        text from chunks yielded before the raise.
        """

        async def _run():
            t = _make_tracing()
            span = self._make_span()
            try:
                async for _ in t._trace_stream(
                    span,
                    _raising_iter([_OAI_HELLO], ValueError("oops"))
                ):
                    pass
            except Exception:
                pass
            return span.output

        result = asyncio.run(_run())
        assert result == "Hello"

    def test_exception_record_exception_called(self):
        """
        record_exception must be called exactly once with the ORIGINAL exception
        when body_iterator raises.

        BUG-1 is fixed: the except block now calls span.record_exception(e)
        directly (no traceback.format_exception wrapper), so it works on Python 3.9+.
        """

        async def _run():
            t = _make_tracing()
            span = self._make_span()
            exc = ValueError("oops")
            try:
                async for _ in t._trace_stream(
                    span,
                    _raising_iter([_OAI_HELLO], exc)
                ):
                    pass
            except Exception:
                pass
            return span.exceptions, exc

        exceptions, original_exc = asyncio.run(_run())
        assert len(exceptions) == 1, (
            f"Expected record_exception called once; got {exceptions!r}"
        )
        assert exceptions[0] is original_exc, (
            "record_exception must receive the original exception object"
        )

    def test_exception_status_error(self):
        """
        set_status must be called with StatusCode.ERROR when body_iterator raises.

        BUG-1 is fixed: all three calls in the except block (set_output,
        record_exception, set_status) now complete successfully on Python 3.9+.
        """

        async def _run():
            t = _make_tracing()
            span = self._make_span()
            try:
                async for _ in t._trace_stream(
                    span,
                    _raising_iter([_OAI_HELLO], ValueError("oops"))
                ):
                    pass
            except Exception:
                pass
            return span.status

        status = asyncio.run(_run())
        assert status is not None, "set_status must be called when iterator raises"
        assert status.status_code == _FakeStatusCode.ERROR

    # --- Early disconnect / aclose ---

    def test_early_disconnect_end_called_exactly_once(self):
        """
        Consuming only the first chunk and then calling aclose() on the
        generator must still trigger span.end() exactly once via the finally
        block.  This is the critical span-leak-prevention test.
        """

        async def _run():
            t = _make_tracing()
            span = self._make_span()

            async def _infinite():
                i = 0
                while True:
                    yield f'data: {{"choices":[{{"delta":{{"content":"chunk{i}"}}}}]}}\n\n'.encode()
                    i += 1

            gen = t._trace_stream(span, _infinite())
            # Consume one chunk then abort.
            _first = await gen.__anext__()
            await gen.aclose()

            assert span.end_count == 1, (
                f"Expected span.end() called once after aclose(); got {span.end_count}"
            )

        asyncio.run(_run())

    def test_early_disconnect_end_not_called_twice_on_full_drain_after_aclose(self):
        """
        aclose() followed by a re-attempt to iterate must not call end() again
        (the generator is exhausted after aclose).
        """

        async def _run():
            t = _make_tracing()
            span = self._make_span()
            chunks = [_OAI_HELLO, _OAI_WORLD, _OAI_DONE]

            gen = t._trace_stream(span, _async_iter(chunks))
            await gen.__anext__()   # consume first chunk
            await gen.aclose()      # disconnect

            # Any further iteration should raise StopAsyncIteration immediately.
            try:
                await gen.__anext__()
            except StopAsyncIteration:
                pass

            assert span.end_count == 1

        asyncio.run(_run())

    def test_early_disconnect_before_first_chunk(self):
        """
        aclose() called on _trace_stream BEFORE __anext__() is ever called
        (i.e. the generator has never been primed / advanced) does NOT fire
        the finally block on Python 3.9.

        This is a Python language behavior: an async generator that has never
        been advanced to its first suspension point (yield) has no suspended
        coroutine frame.  Calling aclose() on an un-primed async generator is
        a no-op — GeneratorExit is not thrown, and the finally block never
        executes.  The same behavior applies to regular generators.

        Implication for the implementation: if a StreamingResponse body_iterator
        is immediately aclose()'d by the ASGI server before any bytes are read
        (e.g. the client disconnects before the first byte), span.end() will NOT
        be called.  This is a span-leak risk in the current implementation.

        KNOWN, ACCEPTED LIMITATION (BUG-3): this behavior was intentionally NOT
        fixed in pheonix.py.  The test documents the observed end_count == 0 and
        must continue to pass as a regression guard for this known gap.
        """

        async def _run():
            t = _make_tracing()
            span = self._make_span()
            gen = t._trace_stream(span, _async_iter([_OAI_HELLO, _OAI_DONE]))
            # Never call __anext__() — aclose an un-primed generator.
            await gen.aclose()
            return span.end_count

        end_count = asyncio.run(_run())
        # Known accepted limitation: finally does not fire for an un-primed
        # async generator on Python 3.9.  end_count must be 0 here.
        assert end_count == 0, (
            "Un-primed async generator aclose() does not fire finally — "
            f"span.end() was called {end_count} time(s); "
            "expected 0 (known accepted limitation, BUG-3)."
        )

    # --- Chunk pass-through integrity ---

    def test_chunks_yielded_are_identical_objects_not_copies(self):
        """
        The impl yields each chunk directly from body_iterator.  Confirm the
        bytes objects are identical (not copies) to avoid inadvertent allocation.
        """

        async def _run():
            t = _make_tracing()
            span = self._make_span()
            original = [_OAI_HELLO, _OAI_WORLD]
            collected = []
            async for chunk in t._trace_stream(span, _async_iter(original)):
                collected.append(chunk)
            for orig, got in zip(original, collected):
                assert orig is got, "chunk was copied rather than yielded by reference"

        asyncio.run(_run())

    def test_mixed_text_and_noise_chunks_only_text_in_output(self):
        """Keepalive / non-text chunks yield unchanged but contribute nothing to output."""

        async def _run():
            t = _make_tracing()
            span = self._make_span()
            chunks = [
                b": keep-alive\n\n",
                _OAI_HELLO,
                b"data: [DONE]\n\n",
            ]
            collected = []
            async for chunk in t._trace_stream(span, _async_iter(chunks)):
                collected.append(chunk)

            assert collected == chunks            # all three yielded
            assert span.output == "Hello"         # only the text part accumulated

        asyncio.run(_run())

    # --- Backend error chunk (NEW: _stream_response SSE error event) ---

    def test_backend_error_chunk_sets_status_error_no_exception(self):
        """
        _stream_response swallows backend exceptions and surfaces them as an SSE
        error event.  _trace_stream must detect the top-level 'error' key and
        set StatusCode.ERROR on normal completion (no exception raised).

        All chunks — including the error chunk — must be yielded UNCHANGED.
        set_output receives only the accumulated text from content chunks.
        span.end() is called exactly once.
        """
        _ERROR_CHUNK = b'data: {"error": {"message": "boom", "type": "backend_error"}}\n\n'

        async def _run():
            t = _make_tracing()
            span = self._make_span()
            chunks = [_OAI_HELLO, _OAI_WORLD, _ERROR_CHUNK]
            collected = []
            async for chunk in t._trace_stream(span, _async_iter(chunks)):
                collected.append(chunk)
            return collected, span

        collected, span = asyncio.run(_run())

        assert collected == [_OAI_HELLO, _OAI_WORLD, _ERROR_CHUNK], (
            "All chunks including the error chunk must be yielded unchanged"
        )
        assert span.status is not None
        assert span.status.status_code == _FakeStatusCode.ERROR, (
            "A backend error chunk must cause set_status(ERROR)"
        )
        assert span.output == "Hello world", (
            "set_output must contain only text from content chunks, not the error chunk"
        )
        assert span.end_count == 1

    def test_clean_stream_without_error_chunk_sets_status_ok(self):
        """
        A stream with only normal content chunks (no error payload) must
        set StatusCode.OK on completion — confirming the error-detection logic
        does not fire a false positive.
        """

        async def _run():
            t = _make_tracing()
            span = self._make_span()
            chunks = [_OAI_HELLO, _OAI_WORLD, _OAI_DONE]
            async for _ in t._trace_stream(span, _async_iter(chunks)):
                pass
            return span

        span = asyncio.run(_run())
        assert span.status is not None
        assert span.status.status_code == _FakeStatusCode.OK
        assert span.output == "Hello world"
        assert span.end_count == 1


# ===========================================================================
# Tests: _parse_chunk
# ===========================================================================


class TestParseChunk:
    """Direct unit tests for the _parse_chunk(chunk) -> (text, has_error) method."""

    def setup_method(self) -> None:
        self.t = _make_tracing()

    def test_normal_openai_chunk_returns_text_and_no_error(self):
        result = self.t._parse_chunk(_OAI_HELLO)
        assert result == ("Hello", False)

    def test_done_sentinel_returns_empty_and_no_error(self):
        result = self.t._parse_chunk(_OAI_DONE)
        assert result == ("", False)

    def test_non_json_line_returns_empty_and_no_error(self):
        result = self.t._parse_chunk(b"data: not-json\n\n")
        assert result == ("", False)

    def test_comment_keepalive_returns_empty_and_no_error(self):
        result = self.t._parse_chunk(b": keep-alive\n\n")
        assert result == ("", False)

    def test_error_chunk_returns_empty_text_and_has_error_true(self):
        error_chunk = b'data: {"error": {"message": "boom", "type": "backend_error"}}\n\n'
        text, has_error = self.t._parse_chunk(error_chunk)
        assert text == ""
        assert has_error is True

    def test_anthropic_text_delta_returns_text_and_no_error(self):
        result = self.t._parse_chunk(_ANTHROPIC_HI)
        assert result == ("Hi", False)

    def test_empty_bytes_returns_empty_and_no_error(self):
        assert self.t._parse_chunk(b"") == ("", False)

    def test_garbage_bytes_returns_empty_and_no_error(self):
        assert self.t._parse_chunk(b"\xff\xfe\x00garbage") == ("", False)
