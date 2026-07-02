"""
Unit tests for app.core.session_headers.

Covers:
  extract_session_headers  — filters incoming request headers to the allowlist.
  filter_response_headers  — strips hop-by-hop / framing headers from backend responses.

This module imports only app.core.session_headers, which has no heavy dependencies
(no ldap3, no MySQL, no Redis).  It can always run in the minimal local test env.
"""

from __future__ import annotations

import pytest
from httpx import Headers as HttpxHeaders
from starlette.datastructures import Headers as StarletteHeaders

from app.core.session_headers import (
    SESSION_HEADER_ALLOWLIST,
    _RESPONSE_STRIP_SET,
    extract_session_headers,
    filter_response_headers,
)


# ---------------------------------------------------------------------------
# Minimal Request stand-in — we only need .headers
# ---------------------------------------------------------------------------

class _FakeRequest:
    """Minimal stand-in for fastapi.Request with a Starlette Headers object."""

    def __init__(self, raw_headers: dict[str, str]) -> None:
        # Starlette Headers expects a list of (name_bytes, value_bytes) pairs.
        scope = {
            "type": "http",
            "headers": [
                (k.lower().encode(), v.encode()) for k, v in raw_headers.items()
            ],
        }
        self.headers = StarletteHeaders(scope=scope)


# ===========================================================================
# Tests: extract_session_headers
# ===========================================================================


class TestExtractSessionHeaders:
    """Tests for extract_session_headers(request) -> dict[str, str]."""

    # --- Happy path: mix of allowlisted and non-allowlisted headers ---

    def test_returns_only_allowlisted_headers(self):
        """Only headers whose lowercase name is in SESSION_HEADER_ALLOWLIST are returned."""
        req = _FakeRequest({
            "x-session-id": "sess-123",
            "traceparent": "00-abc-def-01",
            "x-request-id": "req-456",
            "authorization": "Bearer token",          # NOT allowlisted
            "x-random-custom": "some-value",           # NOT allowlisted
        })
        result = extract_session_headers(req)
        assert "x-session-id" in result
        assert "traceparent" in result
        assert "x-request-id" in result
        assert "authorization" not in result
        assert "x-random-custom" not in result

    def test_values_are_preserved_verbatim(self):
        """Header values must be returned exactly as sent, no transformation."""
        req = _FakeRequest({
            "x-session-id": "my-session-value-with-dashes_and_chars",
            "traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        })
        result = extract_session_headers(req)
        assert result["x-session-id"] == "my-session-value-with-dashes_and_chars"
        assert result["traceparent"] == "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

    def test_keys_are_lowercased_in_output(self):
        """All returned dict keys must be lowercase."""
        req = _FakeRequest({"x-session-id": "s1", "x-request-id": "r1"})
        result = extract_session_headers(req)
        for key in result:
            assert key == key.lower(), f"Key {key!r} is not lowercase"

    # --- Case-insensitivity: client sends headers in mixed case ---

    def test_mixed_case_header_name_is_captured(self):
        """Client sends 'X-Session-Id'; Starlette Headers is case-insensitive so it must be found."""
        req = _FakeRequest({"X-Session-Id": "sess-789"})
        result = extract_session_headers(req)
        assert "x-session-id" in result
        assert result["x-session-id"] == "sess-789"

    def test_uppercase_traceparent_is_captured(self):
        req = _FakeRequest({"TRACEPARENT": "00-abc-01"})
        result = extract_session_headers(req)
        assert "traceparent" in result
        assert result["traceparent"] == "00-abc-01"

    def test_mixed_case_all_allowlisted_headers(self):
        """Every header in the allowlist works regardless of casing."""
        raw = {
            "X-Session-Id": "s",
            "X-Request-Id": "r",
            "X-Correlation-Id": "c",
            "X-Trace-Id": "t",
            "Traceparent": "tp",
            "Tracestate": "ts",
            "Baggage": "b",
        }
        req = _FakeRequest(raw)
        result = extract_session_headers(req)
        for expected_key in SESSION_HEADER_ALLOWLIST:
            assert expected_key in result, f"{expected_key!r} was not extracted"

    # --- Empty cases ---

    def test_empty_when_no_session_headers_present(self):
        """Returns empty dict when no allowlisted header is in the request."""
        req = _FakeRequest({"content-type": "application/json", "authorization": "Bearer t"})
        result = extract_session_headers(req)
        assert result == {}

    def test_empty_request_headers(self):
        """Returns empty dict for a request with no headers at all."""
        req = _FakeRequest({})
        result = extract_session_headers(req)
        assert result == {}

    # --- Partial presence ---

    def test_only_present_allowlisted_headers_are_returned(self):
        """Only headers that are BOTH allowlisted AND present should appear."""
        req = _FakeRequest({"x-session-id": "s1"})
        result = extract_session_headers(req)
        # Only x-session-id was sent; other allowlisted names must not appear.
        assert set(result.keys()) == {"x-session-id"}

    # --- Allowlist completeness check ---

    def test_all_full_allowlist_headers_are_recognised(self):
        """Every member of SESSION_HEADER_ALLOWLIST is correctly recognised when present."""
        raw = {name: f"value-{i}" for i, name in enumerate(SESSION_HEADER_ALLOWLIST)}
        req = _FakeRequest(raw)
        result = extract_session_headers(req)
        assert set(result.keys()) == SESSION_HEADER_ALLOWLIST

    # --- Non-allowlisted look-alikes are excluded ---

    def test_x_session_token_is_not_captured(self):
        """x-session-token is similar to x-session-id but is NOT in the allowlist."""
        req = _FakeRequest({"x-session-token": "should-not-appear"})
        result = extract_session_headers(req)
        assert "x-session-token" not in result

    def test_authorization_is_never_leaked(self):
        """authorization is never in SESSION_HEADER_ALLOWLIST and must not appear."""
        req = _FakeRequest({"authorization": "Bearer secret", "x-session-id": "ok"})
        result = extract_session_headers(req)
        assert "authorization" not in result
        assert "x-session-id" in result

    def test_returns_plain_dict(self):
        """Return type must be a plain dict, not any custom mapping."""
        req = _FakeRequest({"x-session-id": "v"})
        result = extract_session_headers(req)
        assert type(result) is dict  # noqa: E721


# ===========================================================================
# Tests: filter_response_headers
# ===========================================================================


class TestFilterResponseHeaders:
    """Tests for filter_response_headers(headers) -> dict[str, str]."""

    # --- Core strip-set: must be removed ---

    def test_strips_content_length(self):
        result = filter_response_headers({"content-length": "100", "x-upstream-id": "up1"})
        assert "content-length" not in result
        assert result["x-upstream-id"] == "up1"

    def test_strips_content_type(self):
        result = filter_response_headers({"content-type": "application/json", "x-upstream-id": "up2"})
        assert "content-type" not in result

    def test_strips_transfer_encoding(self):
        result = filter_response_headers({"transfer-encoding": "chunked", "x-ok": "yes"})
        assert "transfer-encoding" not in result
        assert result["x-ok"] == "yes"

    def test_strips_connection(self):
        result = filter_response_headers({"connection": "keep-alive", "x-ok": "yes"})
        assert "connection" not in result

    def test_strips_www_authenticate(self):
        result = filter_response_headers({"www-authenticate": 'Bearer realm="llm"', "x-ok": "y"})
        assert "www-authenticate" not in result

    def test_strips_set_cookie(self):
        result = filter_response_headers({"set-cookie": "sid=abc", "x-ok": "y"})
        assert "set-cookie" not in result

    def test_strips_date(self):
        result = filter_response_headers({"date": "Thu, 01 Jan 2026 00:00:00 GMT", "x-ok": "y"})
        assert "date" not in result

    def test_strips_server(self):
        result = filter_response_headers({"server": "nginx/1.24", "x-ok": "y"})
        assert "server" not in result

    def test_strips_host(self):
        result = filter_response_headers({"host": "backend.internal:8080", "x-ok": "y"})
        assert "host" not in result

    def test_strips_all_strip_set_members_at_once(self):
        """Every member of _RESPONSE_STRIP_SET is removed in a single call."""
        headers_in = {name: "strip-me" for name in _RESPONSE_STRIP_SET}
        headers_in["x-keep-this"] = "kept"
        result = filter_response_headers(headers_in)
        for stripped in _RESPONSE_STRIP_SET:
            assert stripped not in result, f"{stripped!r} was not stripped"
        assert result["x-keep-this"] == "kept"

    # --- Preserved headers ---

    def test_keeps_custom_upstream_header(self):
        result = filter_response_headers({"x-upstream-id": "u42", "content-length": "0"})
        assert result["x-upstream-id"] == "u42"

    def test_keeps_echoed_session_header(self):
        """Backend echoes x-session-id back; it must survive the filter."""
        result = filter_response_headers({
            "x-session-id": "sess-echo",
            "content-type": "text/event-stream",
            "content-length": "0",
        })
        assert result["x-session-id"] == "sess-echo"

    def test_keeps_x_request_id_echoed(self):
        result = filter_response_headers({"x-request-id": "rid-1", "server": "gunicorn"})
        assert result["x-request-id"] == "rid-1"

    def test_empty_input_returns_empty_dict(self):
        assert filter_response_headers({}) == {}

    def test_all_headers_stripped_returns_empty_dict(self):
        headers = {name: "v" for name in _RESPONSE_STRIP_SET}
        assert filter_response_headers(headers) == {}

    # --- Key casing ---

    def test_keys_are_lowercased_in_output(self):
        result = filter_response_headers({"X-Upstream-Id": "abc", "X-Request-Id": "r1"})
        for key in result:
            assert key == key.lower(), f"Key {key!r} is not lowercased"
        assert "x-upstream-id" in result

    def test_mixed_case_strip_set_header_is_stripped(self):
        """Mixed-case header whose lowercase is in the strip set must still be removed."""
        result = filter_response_headers({
            "Content-Length": "999",
            "Transfer-Encoding": "chunked",
            "x-keep": "yes",
        })
        assert "content-length" not in result
        assert "transfer-encoding" not in result
        assert result["x-keep"] == "yes"

    # --- Input types ---

    def test_accepts_plain_dict(self):
        headers = {"x-custom": "value", "content-length": "5"}
        result = filter_response_headers(headers)
        assert type(result) is dict  # noqa: E721
        assert result["x-custom"] == "value"

    def test_accepts_httpx_headers_instance(self):
        """filter_response_headers must work when given an httpx.Headers object."""
        raw_headers = [
            (b"x-upstream-id", b"up-httpx"),
            (b"content-length", b"123"),
            (b"x-session-id", b"s-httpx"),
            (b"content-type", b"application/json"),
            (b"server", b"uvicorn"),
        ]
        httpx_hdrs = HttpxHeaders(raw_headers)
        result = filter_response_headers(httpx_hdrs)
        assert result["x-upstream-id"] == "up-httpx"
        assert result["x-session-id"] == "s-httpx"
        assert "content-length" not in result
        assert "content-type" not in result
        assert "server" not in result

    def test_returns_plain_dict_for_httpx_input(self):
        httpx_hdrs = HttpxHeaders([(b"x-custom", b"v")])
        result = filter_response_headers(httpx_hdrs)
        assert type(result) is dict  # noqa: E721

    # --- Value preservation ---

    def test_values_are_preserved_verbatim(self):
        result = filter_response_headers({"x-trace-id": "trace-0af7651916cd43dd"})
        assert result["x-trace-id"] == "trace-0af7651916cd43dd"

    def test_multiple_custom_headers_all_preserved(self):
        headers = {
            "x-upstream-id": "u1",
            "x-session-id": "s1",
            "x-request-id": "r1",
            "x-trace-id": "t1",
            "content-length": "0",   # stripped
            "server": "nginx",       # stripped
        }
        result = filter_response_headers(headers)
        assert result == {
            "x-upstream-id": "u1",
            "x-session-id": "s1",
            "x-request-id": "r1",
            "x-trace-id": "t1",
        }
