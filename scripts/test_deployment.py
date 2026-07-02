#!/usr/bin/env python3
"""
Smoke-test script for a deployed LLM Router instance.

Sends real requests to a live deployment and reports pass/fail with latency
for each test case. Supports JWT auth, service-key auth, and username/password
login to obtain a JWT.

USAGE EXAMPLES
--------------

# Test with a service key (auto-detected by llmr_sk_ prefix), all tests:
python scripts/test_deployment.py \\
    --url https://router.example.com \\
    --model my-model \\
    --token llmr_sk_abcdef1234567890

# Test with a JWT passed directly:
python scripts/test_deployment.py \\
    --url https://router.example.com \\
    --model my-model \\
    --token eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...

# Authenticate via username/password login (router exchanges for JWT):
python scripts/test_deployment.py \\
    --url https://router.example.com \\
    --model my-model \\
    --username admin \\
    --password secret

# Run only the Anthropic tests (non-streaming + streaming):
python scripts/test_deployment.py \\
    --url https://router.example.com \\
    --model my-model \\
    --token llmr_sk_abcdef1234567890 \\
    --tests anthropic

# Run multiple specific tests, use x-api-key header, skip TLS verification:
python scripts/test_deployment.py \\
    --url https://router.internal \\
    --model my-model \\
    --token llmr_sk_abcdef1234567890 \\
    --auth-header x-api-key \\
    --tests models,openai \\
    --insecure \\
    --verbose

# Use environment variables for credentials:
export LLM_ROUTER_TOKEN=llmr_sk_abcdef1234567890
python scripts/test_deployment.py --url https://router.example.com --model my-model

# Or with username/password from env:
export LLM_ROUTER_USERNAME=admin
export LLM_ROUTER_PASSWORD=secret
python scripts/test_deployment.py --url https://router.example.com --model my-model
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Optional

import httpx


# ---------------------------------------------------------------------------
# ANSI colour helpers
# ---------------------------------------------------------------------------

def _use_colour() -> bool:
    """Return True if we should emit ANSI escape codes."""
    if os.environ.get("NO_COLOR"):
        return False
    return sys.stdout.isatty()


_COLOUR = _use_colour()

_GREEN = "\033[32m"
_RED = "\033[31m"
_YELLOW = "\033[33m"
_BOLD = "\033[1m"
_RESET = "\033[0m"


def _c(code: str, text: str) -> str:
    return f"{code}{text}{_RESET}" if _COLOUR else text


def green(t: str) -> str:
    return _c(_GREEN, t)


def red(t: str) -> str:
    return _c(_RED, t)


def yellow(t: str) -> str:
    return _c(_YELLOW, t)


def bold(t: str) -> str:
    return _c(_BOLD, t)


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class TestResult:
    name: str
    passed: bool
    latency_ms: float
    detail: str
    skipped: bool = False
    warning: Optional[str] = None


# ---------------------------------------------------------------------------
# SSE streaming consumer
# ---------------------------------------------------------------------------

@dataclass
class StreamResult:
    text: str = ""
    chunk_count: int = 0
    usage: dict = field(default_factory=dict)
    error: Optional[str] = None
    done: bool = False


def _consume_sse_stream(response: httpx.Response) -> StreamResult:
    """
    Lenient SSE consumer that handles both OpenAI and Anthropic delta shapes.

    Reads line-by-line, accumulates text content, and stops on [DONE].
    Captures the last non-empty usage dict seen.

    Handles:
      - OpenAI delta:    {"choices":[{"delta":{"content":"..."}}], "usage":{...}}
      - Anthropic delta: {"type":"content_block_delta","delta":{"type":"text_delta","text":"..."}}
      - Anthropic start: {"type":"content_block_start","content_block":{"text":"..."}}
      - Anthropic usage: {"usage":{"input_tokens":N,"output_tokens":N}}
      - Error chunk:     {"error":{...}}
    """
    result = StreamResult()

    for raw_line in response.iter_lines():
        line = raw_line.strip()
        if not line:
            continue
        if not line.startswith("data:"):
            continue

        payload_str = line[len("data:"):].strip()

        if payload_str == "[DONE]":
            result.done = True
            break

        try:
            payload = json.loads(payload_str)
        except json.JSONDecodeError:
            continue

        result.chunk_count += 1

        # Error chunk — surface and stop
        if "error" in payload:
            err = payload["error"]
            if isinstance(err, dict):
                result.error = err.get("message", str(err))
            else:
                result.error = str(err)
            break

        # Extract text — try OpenAI shape first, then Anthropic shapes
        text: Optional[str] = None

        # OpenAI: choices[0].delta.content
        choices = payload.get("choices")
        if choices and isinstance(choices, list):
            delta = choices[0].get("delta", {})
            text = delta.get("content")

        if text is None:
            # Anthropic content_block_delta: delta.text (prose only; a
            # tool-use block's partial_json is intentionally NOT counted as text)
            delta = payload.get("delta", {})
            if isinstance(delta, dict):
                text = delta.get("text")

        if text is None:
            # Anthropic content_block_start: content_block.text (usually "")
            cb = payload.get("content_block", {})
            if isinstance(cb, dict):
                text = cb.get("text")

        if text:
            result.text += text

        # Capture usage and MERGE it. Anthropic splits usage across events
        # (input_tokens on message_start, output_tokens on message_delta);
        # OpenAI sends it all in one final chunk. message_start nests it
        # under message.usage rather than top-level.
        usage = payload.get("usage")
        if not usage:
            msg = payload.get("message")
            if isinstance(msg, dict):
                usage = msg.get("usage")
        if usage and isinstance(usage, dict):
            result.usage = {**result.usage, **usage}

    return result


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

def _build_auth_header(token: str, header_style: str) -> dict[str, str]:
    """Return the appropriate auth header dict."""
    if header_style == "x-api-key":
        return {"x-api-key": token}
    return {"Authorization": f"Bearer {token}"}


def _resolve_token(
    args: argparse.Namespace,
    client: httpx.Client,
    base_url: str,
) -> tuple[str, str]:
    """
    Resolve authentication to a token and a human-readable label.

    Returns (token, label) where label is one of: "jwt", "service-key".
    Exits with code 2 on failure.
    """
    # Priority: --token / env LLM_ROUTER_TOKEN
    token: Optional[str] = args.token or os.environ.get("LLM_ROUTER_TOKEN")
    if token:
        label = "service-key" if token.startswith("llmr_sk_") else "jwt"
        return token, label

    # Fallback: --username / --password (or env vars)
    username: Optional[str] = args.username or os.environ.get("LLM_ROUTER_USERNAME")
    password: Optional[str] = args.password or os.environ.get("LLM_ROUTER_PASSWORD")

    if username and password:
        login_url = f"{base_url}/auth/login"
        try:
            resp = client.post(
                login_url,
                json={"username": username, "password": password},
                timeout=args.timeout,
            )
        except (httpx.ConnectError, httpx.TimeoutException) as exc:
            print(red(f"Connection error reaching {login_url}: {exc}"), file=sys.stderr)
            sys.exit(2)

        if resp.status_code != 200:
            try:
                body = resp.json()
                detail = body.get("detail", resp.text[:200])
            except Exception:
                detail = resp.text[:200]
            print(
                red(f"Login failed ({resp.status_code}): {detail}"),
                file=sys.stderr,
            )
            sys.exit(2)

        try:
            token = resp.json()["access_token"]
        except (KeyError, json.JSONDecodeError):
            print(red("Login response did not contain access_token"), file=sys.stderr)
            sys.exit(2)

        return token, "jwt"

    print(
        red(
            "No credentials provided. Supply --token (or env LLM_ROUTER_TOKEN) "
            "or --username/--password (or env LLM_ROUTER_USERNAME/LLM_ROUTER_PASSWORD)."
        ),
        file=sys.stderr,
    )
    sys.exit(2)


# ---------------------------------------------------------------------------
# Individual test runners
# ---------------------------------------------------------------------------

def _truncate(text: str, max_len: int = 120) -> str:
    if len(text) <= max_len:
        return text
    return text[:max_len] + "..."


def run_models(
    client: httpx.Client,
    base_url: str,
    model: str,
    auth_headers: dict[str, str],
    timeout: float,
    verbose: bool,
) -> TestResult:
    name = "models"
    t0 = time.monotonic()
    try:
        resp = client.get(f"{base_url}/v1/models", headers=auth_headers, timeout=timeout)
        latency_ms = (time.monotonic() - t0) * 1000

        if resp.status_code != 200:
            body_snip = resp.text[:300] if verbose else resp.text[:80]
            return TestResult(
                name=name,
                passed=False,
                latency_ms=latency_ms,
                detail=f"HTTP {resp.status_code}: {body_snip}",
            )

        try:
            data = resp.json()
        except json.JSONDecodeError:
            return TestResult(
                name=name,
                passed=False,
                latency_ms=latency_ms,
                detail="Response is not valid JSON",
            )

        model_ids = [m["id"] for m in data.get("data", [])]
        count = len(model_ids)
        model_present = model in model_ids
        detail = f"count={count}"

        warning: Optional[str] = None
        if model_present:
            detail += f"  model={model!r} found"
        else:
            warning = (
                f"Model {model!r} is NOT in /v1/models response — "
                "subsequent calls will likely return 403"
            )
            detail += f"  model={model!r} NOT found (see warning)"

        if verbose:
            detail += f"  ids={model_ids}"

        return TestResult(
            name=name,
            passed=True,
            latency_ms=latency_ms,
            detail=detail,
            warning=warning,
        )

    except (httpx.ConnectError, httpx.TimeoutException) as exc:
        latency_ms = (time.monotonic() - t0) * 1000
        return TestResult(
            name=name,
            passed=False,
            latency_ms=latency_ms,
            detail=f"Connection error: {exc}",
        )


def run_openai_nonstream(
    client: httpx.Client,
    base_url: str,
    model: str,
    auth_headers: dict[str, str],
    prompt: str,
    max_tokens: int,
    timeout: float,
    verbose: bool,
) -> TestResult:
    name = "openai-nonstream"
    body = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False,
        "max_tokens": max_tokens,
    }
    t0 = time.monotonic()
    try:
        resp = client.post(
            f"{base_url}/v1/chat/completions",
            headers=auth_headers,
            json=body,
            timeout=timeout,
        )
        latency_ms = (time.monotonic() - t0) * 1000

        if resp.status_code != 200:
            body_snip = resp.text[:300] if verbose else resp.text[:80]
            return TestResult(
                name=name,
                passed=False,
                latency_ms=latency_ms,
                detail=f"HTTP {resp.status_code}: {body_snip}",
            )

        try:
            data = resp.json()
        except json.JSONDecodeError:
            return TestResult(
                name=name,
                passed=False,
                latency_ms=latency_ms,
                detail="Response is not valid JSON",
            )

        if verbose:
            print(f"\n  [verbose] openai-nonstream response:\n  {json.dumps(data, indent=2)}\n")

        try:
            content = data["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError):
            return TestResult(
                name=name,
                passed=False,
                latency_ms=latency_ms,
                detail=f"Missing choices[0].message.content in response: {str(data)[:200]}",
            )

        if not content:
            return TestResult(
                name=name,
                passed=False,
                latency_ms=latency_ms,
                detail="choices[0].message.content is empty",
            )

        usage = data.get("usage", {})
        pt = usage.get("prompt_tokens", "?")
        ct = usage.get("completion_tokens", "?")
        detail = f'tokens={pt}/{ct}  "{_truncate(content)}"'

        return TestResult(name=name, passed=True, latency_ms=latency_ms, detail=detail)

    except (httpx.ConnectError, httpx.TimeoutException) as exc:
        latency_ms = (time.monotonic() - t0) * 1000
        return TestResult(
            name=name, passed=False, latency_ms=latency_ms, detail=f"Connection error: {exc}"
        )


def run_openai_stream(
    client: httpx.Client,
    base_url: str,
    model: str,
    auth_headers: dict[str, str],
    prompt: str,
    max_tokens: int,
    timeout: float,
    verbose: bool,
) -> TestResult:
    name = "openai-stream"
    body = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "stream": True,
        "max_tokens": max_tokens,
    }
    t0 = time.monotonic()
    try:
        with client.stream(
            "POST",
            f"{base_url}/v1/chat/completions",
            headers=auth_headers,
            json=body,
            timeout=timeout,
        ) as resp:
            if resp.status_code != 200:
                body_text = resp.read().decode("utf-8", errors="replace")
                latency_ms = (time.monotonic() - t0) * 1000
                snip = body_text[:300] if verbose else body_text[:80]
                return TestResult(
                    name=name,
                    passed=False,
                    latency_ms=latency_ms,
                    detail=f"HTTP {resp.status_code}: {snip}",
                )

            stream_result = _consume_sse_stream(resp)

        latency_ms = (time.monotonic() - t0) * 1000

        if stream_result.error:
            return TestResult(
                name=name,
                passed=False,
                latency_ms=latency_ms,
                detail=f"Stream error chunk: {stream_result.error}",
            )

        if not stream_result.text:
            return TestResult(
                name=name,
                passed=False,
                latency_ms=latency_ms,
                detail=f"No text accumulated (chunks={stream_result.chunk_count}, done={stream_result.done})",
            )

        usage = stream_result.usage
        pt = usage.get("prompt_tokens", "?")
        ct = usage.get("completion_tokens", "?")
        tokens_str = f"tokens={pt}/{ct}" if usage else "tokens=?"
        detail = (
            f'{tokens_str}  chunks={stream_result.chunk_count}  '
            f'"{_truncate(stream_result.text)}"'
        )

        if verbose:
            print(f"\n  [verbose] openai-stream full text:\n  {stream_result.text}\n")

        warning = None
        if not stream_result.done:
            warning = "stream ended without a [DONE] sentinel — backend may have truncated"

        return TestResult(name=name, passed=True, latency_ms=latency_ms, detail=detail, warning=warning)

    except (httpx.ConnectError, httpx.TimeoutException) as exc:
        latency_ms = (time.monotonic() - t0) * 1000
        return TestResult(
            name=name, passed=False, latency_ms=latency_ms, detail=f"Connection error: {exc}"
        )


def run_anthropic_nonstream(
    client: httpx.Client,
    base_url: str,
    model: str,
    auth_headers: dict[str, str],
    prompt: str,
    max_tokens: int,
    timeout: float,
    verbose: bool,
) -> TestResult:
    name = "anthropic-nonstream"
    body = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False,
    }
    t0 = time.monotonic()
    try:
        resp = client.post(
            f"{base_url}/v1/messages",
            headers=auth_headers,
            json=body,
            timeout=timeout,
        )
        latency_ms = (time.monotonic() - t0) * 1000

        if resp.status_code != 200:
            body_snip = resp.text[:300] if verbose else resp.text[:80]
            return TestResult(
                name=name,
                passed=False,
                latency_ms=latency_ms,
                detail=f"HTTP {resp.status_code}: {body_snip}",
            )

        try:
            data = resp.json()
        except json.JSONDecodeError:
            return TestResult(
                name=name,
                passed=False,
                latency_ms=latency_ms,
                detail="Response is not valid JSON",
            )

        if verbose:
            print(f"\n  [verbose] anthropic-nonstream response:\n  {json.dumps(data, indent=2)}\n")

        content_blocks = data.get("content")
        if not isinstance(content_blocks, list) or not content_blocks:
            return TestResult(
                name=name,
                passed=False,
                latency_ms=latency_ms,
                detail=f"Response has no content blocks: {str(data)[:200]}",
            )

        # Anthropic returns a LIST of typed blocks. The visible answer lives in
        # 'text' blocks — but a model with extended thinking emits a 'thinking'
        # (or 'redacted_thinking') block first, so content[0] is not necessarily
        # text. Concatenate every text block and ignore thinking/tool_use blocks.
        block_types = [b.get("type") for b in content_blocks if isinstance(b, dict)]
        text = "".join(
            b.get("text", "")
            for b in content_blocks
            if isinstance(b, dict) and b.get("type") == "text"
        )

        if not text:
            return TestResult(
                name=name,
                passed=False,
                latency_ms=latency_ms,
                detail=f"No 'text' block in content (block types={block_types})",
            )

        usage = data.get("usage", {})
        it = usage.get("input_tokens", "?")
        ot = usage.get("output_tokens", "?")
        thinking_note = " +thinking" if "thinking" in block_types else ""
        detail = f'tokens={it}/{ot}{thinking_note}  "{_truncate(text)}"'

        return TestResult(name=name, passed=True, latency_ms=latency_ms, detail=detail)

    except (httpx.ConnectError, httpx.TimeoutException) as exc:
        latency_ms = (time.monotonic() - t0) * 1000
        return TestResult(
            name=name, passed=False, latency_ms=latency_ms, detail=f"Connection error: {exc}"
        )


def run_anthropic_stream(
    client: httpx.Client,
    base_url: str,
    model: str,
    auth_headers: dict[str, str],
    prompt: str,
    max_tokens: int,
    timeout: float,
    verbose: bool,
) -> TestResult:
    name = "anthropic-stream"
    body = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
        "stream": True,
    }
    t0 = time.monotonic()
    try:
        with client.stream(
            "POST",
            f"{base_url}/v1/messages",
            headers=auth_headers,
            json=body,
            timeout=timeout,
        ) as resp:
            if resp.status_code != 200:
                body_text = resp.read().decode("utf-8", errors="replace")
                latency_ms = (time.monotonic() - t0) * 1000
                snip = body_text[:300] if verbose else body_text[:80]
                return TestResult(
                    name=name,
                    passed=False,
                    latency_ms=latency_ms,
                    detail=f"HTTP {resp.status_code}: {snip}",
                )

            stream_result = _consume_sse_stream(resp)

        latency_ms = (time.monotonic() - t0) * 1000

        if stream_result.error:
            return TestResult(
                name=name,
                passed=False,
                latency_ms=latency_ms,
                detail=f"Stream error chunk: {stream_result.error}",
            )

        if not stream_result.text:
            return TestResult(
                name=name,
                passed=False,
                latency_ms=latency_ms,
                detail=f"No text accumulated (chunks={stream_result.chunk_count}, done={stream_result.done})",
            )

        usage = stream_result.usage
        # Anthropic uses input_tokens / output_tokens
        it = usage.get("input_tokens", usage.get("prompt_tokens", "?"))
        ot = usage.get("output_tokens", usage.get("completion_tokens", "?"))
        tokens_str = f"tokens={it}/{ot}" if usage else "tokens=?"
        detail = (
            f'{tokens_str}  chunks={stream_result.chunk_count}  '
            f'"{_truncate(stream_result.text)}"'
        )

        if verbose:
            print(f"\n  [verbose] anthropic-stream full text:\n  {stream_result.text}\n")

        warning = None
        if not stream_result.done:
            warning = "stream ended without a [DONE] sentinel — backend may have truncated"

        return TestResult(name=name, passed=True, latency_ms=latency_ms, detail=detail, warning=warning)

    except (httpx.ConnectError, httpx.TimeoutException) as exc:
        latency_ms = (time.monotonic() - t0) * 1000
        return TestResult(
            name=name, passed=False, latency_ms=latency_ms, detail=f"Connection error: {exc}"
        )


def run_embeddings(
    client: httpx.Client,
    base_url: str,
    model: str,
    auth_headers: dict[str, str],
    timeout: float,
    verbose: bool,
) -> TestResult:
    name = "embeddings"
    body = {"model": model, "input": "hello world"}
    t0 = time.monotonic()
    try:
        resp = client.post(
            f"{base_url}/v1/embeddings",
            headers=auth_headers,
            json=body,
            timeout=timeout,
        )
        latency_ms = (time.monotonic() - t0) * 1000

        if resp.status_code != 200:
            body_snip = resp.text[:300] if verbose else resp.text[:80]
            return TestResult(
                name=name,
                passed=False,
                latency_ms=latency_ms,
                detail=f"HTTP {resp.status_code}: {body_snip}",
            )

        try:
            data = resp.json()
        except json.JSONDecodeError:
            return TestResult(
                name=name,
                passed=False,
                latency_ms=latency_ms,
                detail="Response is not valid JSON",
            )

        if verbose:
            embedding_preview = str(data)[:300]
            print(f"\n  [verbose] embeddings response (truncated):\n  {embedding_preview}\n")

        try:
            embedding = data["data"][0]["embedding"]
        except (KeyError, IndexError, TypeError):
            return TestResult(
                name=name,
                passed=False,
                latency_ms=latency_ms,
                detail=f"Missing data[0].embedding in response: {str(data)[:200]}",
            )

        if not embedding or not isinstance(embedding, list):
            return TestResult(
                name=name,
                passed=False,
                latency_ms=latency_ms,
                detail="data[0].embedding is empty or not a list",
            )

        dim = len(embedding)
        return TestResult(
            name=name,
            passed=True,
            latency_ms=latency_ms,
            detail=f"dim={dim}",
        )

    except (httpx.ConnectError, httpx.TimeoutException) as exc:
        latency_ms = (time.monotonic() - t0) * 1000
        return TestResult(
            name=name, passed=False, latency_ms=latency_ms, detail=f"Connection error: {exc}"
        )


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

_VALID_TEST_NAMES = {"models", "openai", "anthropic", "embeddings", "all"}

_ALL_EXPANDS_TO = ["models", "openai", "anthropic"]


def _parse_tests(raw: str) -> list[str]:
    """Parse and validate --tests comma-separated value."""
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    for part in parts:
        if part not in _VALID_TEST_NAMES:
            raise argparse.ArgumentTypeError(
                f"Unknown test {part!r}. Valid values: {', '.join(sorted(_VALID_TEST_NAMES))}"
            )
    return parts


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="test_deployment.py",
        description="Smoke-test a deployed LLM Router instance.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--url",
        required=True,
        metavar="URL",
        help="Base URL of the deployed router, e.g. https://router.example.com (do not include /v1)",
    )
    parser.add_argument(
        "--model",
        required=True,
        metavar="MODEL",
        help="Model ID to test, e.g. my-llm or org/model-name",
    )
    parser.add_argument(
        "--token",
        metavar="TOKEN",
        default=None,
        help=(
            "JWT or service key (auto-detected by llmr_sk_ prefix). "
            "Also accepts env LLM_ROUTER_TOKEN."
        ),
    )
    parser.add_argument(
        "--username",
        metavar="USER",
        default=None,
        help="Username for POST /auth/login. Also accepts env LLM_ROUTER_USERNAME.",
    )
    parser.add_argument(
        "--password",
        metavar="PASS",
        default=None,
        help="Password for POST /auth/login. Also accepts env LLM_ROUTER_PASSWORD.",
    )
    parser.add_argument(
        "--auth-header",
        choices=["bearer", "x-api-key"],
        default="bearer",
        help="Which header to send the credential in (default: bearer).",
    )
    parser.add_argument(
        "--tests",
        default="all",
        metavar="{models,openai,anthropic,embeddings,all}",
        help=(
            "Comma-separated list of tests to run. "
            "'all' runs models + openai (stream+non-stream) + anthropic (stream+non-stream). "
            "'embeddings' is opt-in only. Default: all."
        ),
    )
    parser.add_argument(
        "--prompt",
        default="Reply with a short one-sentence greeting.",
        metavar="TEXT",
        help="Prompt text to use for chat/messages tests.",
    )
    parser.add_argument(
        "--max-tokens",
        type=int,
        default=64,
        metavar="N",
        dest="max_tokens",
        help="max_tokens to request (default: 64).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        metavar="SECONDS",
        help="Per-request timeout in seconds (default: 60).",
    )
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="Disable TLS certificate verification (for self-signed certs).",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print full response bodies and extra debug info.",
    )
    return parser


# ---------------------------------------------------------------------------
# Test plan resolution
# ---------------------------------------------------------------------------

def _resolve_test_plan(test_specs: list[str]) -> list[str]:
    """
    Expand the test spec list into a canonical ordered list of test names.

    Each entry in test_specs is one of: models, openai, anthropic, embeddings, all.
    'all' expands to models + openai + anthropic (embeddings is opt-in only).
    Deduplication preserves first-seen order.
    """
    plan: list[str] = []
    seen: set[str] = set()

    for spec in test_specs:
        if spec == "all":
            candidates = _ALL_EXPANDS_TO
        else:
            candidates = [spec]

        for c in candidates:
            if c not in seen:
                seen.add(c)
                plan.append(c)

    return plan


# ---------------------------------------------------------------------------
# Result printing
# ---------------------------------------------------------------------------

def _print_result(result: TestResult) -> None:
    if result.skipped:
        label = yellow("[SKIP]")
    elif result.passed:
        label = green("[PASS]")
    else:
        label = red("[FAIL]")

    latency_str = f"{result.latency_ms:.0f}ms"
    name_str = result.name.ljust(24)
    print(f"{label} {name_str} {latency_str:<8}  {result.detail}")

    if result.warning:
        print(f"       {yellow('WARN:')} {result.warning}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    # Validate --tests value before doing any network I/O
    try:
        test_specs = _parse_tests(args.tests)
    except argparse.ArgumentTypeError as exc:
        print(red(f"Error: {exc}"), file=sys.stderr)
        sys.exit(2)

    test_plan = _resolve_test_plan(test_specs)

    # Normalise base URL: strip trailing slash, must not end in /v1
    base_url = args.url.rstrip("/")
    if base_url.endswith("/v1"):
        base_url = base_url[:-3]

    # Build httpx client (sync, context-managed at call sites for streaming)
    client_kwargs: dict = {
        "verify": not args.insecure,
        "follow_redirects": True,
    }
    if args.insecure:
        # httpx already suppresses its own warning when verify=False; silence any
        # generic "Unverified HTTPS request" warning without importing urllib3
        # (which may not be installed in a pure-httpx environment).
        import warnings
        warnings.filterwarnings("ignore", message="Unverified HTTPS request")

    with httpx.Client(**client_kwargs) as client:
        # Resolve credentials
        token, auth_label = _resolve_token(args, client, base_url)
        auth_headers = _build_auth_header(token, args.auth_header)

        # Print header
        print()
        print(bold("LLM Router Deployment Smoke Test"))
        print(f"  URL:         {base_url}")
        print(f"  Model:       {args.model}")
        print(f"  Auth:        {auth_label} via {args.auth_header}")
        print(f"  Tests:       {', '.join(test_plan)}")
        print(f"  Max-tokens:  {args.max_tokens}")
        print(f"  Timeout:     {args.timeout}s")
        if args.insecure:
            print(f"  TLS verify:  {yellow('disabled')}")
        print()

        results: list[TestResult] = []

        for test_name in test_plan:
            try:
                if test_name == "models":
                    result = run_models(
                        client, base_url, args.model, auth_headers,
                        args.timeout, args.verbose,
                    )
                elif test_name == "openai":
                    # Run non-streaming then streaming; both contribute to results
                    r1 = run_openai_nonstream(
                        client, base_url, args.model, auth_headers,
                        args.prompt, args.max_tokens, args.timeout, args.verbose,
                    )
                    _print_result(r1)
                    results.append(r1)
                    r2 = run_openai_stream(
                        client, base_url, args.model, auth_headers,
                        args.prompt, args.max_tokens, args.timeout, args.verbose,
                    )
                    _print_result(r2)
                    results.append(r2)
                    continue
                elif test_name == "anthropic":
                    r1 = run_anthropic_nonstream(
                        client, base_url, args.model, auth_headers,
                        args.prompt, args.max_tokens, args.timeout, args.verbose,
                    )
                    _print_result(r1)
                    results.append(r1)
                    r2 = run_anthropic_stream(
                        client, base_url, args.model, auth_headers,
                        args.prompt, args.max_tokens, args.timeout, args.verbose,
                    )
                    _print_result(r2)
                    results.append(r2)
                    continue
                elif test_name == "embeddings":
                    result = run_embeddings(
                        client, base_url, args.model, auth_headers,
                        args.timeout, args.verbose,
                    )
                else:
                    # Should not happen; _parse_tests validates
                    continue

            except Exception as exc:
                result = TestResult(
                    name=test_name,
                    passed=False,
                    latency_ms=0.0,
                    detail=f"Unexpected exception: {type(exc).__name__}: {exc}",
                )

            _print_result(result)
            results.append(result)

    # Summary
    n_passed = sum(1 for r in results if r.passed and not r.skipped)
    n_failed = sum(1 for r in results if not r.passed and not r.skipped)
    n_skipped = sum(1 for r in results if r.skipped)

    print()
    summary_parts = [
        green(f"{n_passed} passed"),
        red(f"{n_failed} failed") if n_failed else f"{n_failed} failed",
    ]
    if n_skipped:
        summary_parts.append(yellow(f"{n_skipped} skipped"))
    print(bold("Summary:"), "  ".join(summary_parts))
    print()

    sys.exit(0 if n_failed == 0 else 1)


if __name__ == "__main__":
    main()
