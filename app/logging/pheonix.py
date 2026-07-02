from enum import Enum
from functools import wraps
import json
import logging
from typing import Any, Dict

from fastapi.responses import StreamingResponse

# Tracing libraries are optional — only needed when PHOENIX_TRACING_ENABLED=true.
# Guard the imports so the app boots without them; get_tracing_config() degrades
# to a no-op tracer if they're absent.
try:
    from opentelemetry.trace import Status, StatusCode, use_span
    from openinference.semconv.trace import SpanAttributes
    from phoenix.otel import register
    from openinference.instrumentation.openai import OpenAIInstrumentor

    _TRACING_DEPS_AVAILABLE = True
except ImportError:
    Status = StatusCode = SpanAttributes = register = OpenAIInstrumentor = None
    use_span = None
    _TRACING_DEPS_AVAILABLE = False

from pydantic import BaseModel

from app.core.config import settings

logger = logging.getLogger(__name__)


class SpanKind(Enum):
    LLM = "llm"
    EMBEDDING = "embedding"


class Tracing:
    """
    Tracing is a class that provides functionality to trace functions in a Python application.
    It uses the OpenTelemetry API to create and manage spans.

    Args:
        tracer (Tracer): The tracer instance to use for creating spans.

    Attributes:
        tracer (Tracer): The tracer instance used for creating spans.
        sensitive_keys (List[str]): A list of keys in input data that should be sanitized.

    """

    def __init__(self, tracer=None, tracing_enabled=True, default_sensitive_keys=None):
        """
        Initializes a Tracing object.

        Args:
            tracer (Tracer): The tracer instance to use for creating spans.
            default_sensitive_keys (List[str]): The default sensitive keys to sanitize.

        """
        self.tracer = tracer
        self.default_sensitive_keys = (
            default_sensitive_keys if default_sensitive_keys else []
        )
        self.tracing_enabled = tracing_enabled

    def sanitize(self, data, additional_sensitive_keys=None):
        """
        Sanitizes sensitive data in input data.

        Args:
            data (Any): The input data to be sanitized.

        Returns:
            Any: The sanitized data.

        """
        additional_sensitive_keys = additional_sensitive_keys or []

        def sanitize_recursively(data):
            """
            Recursively sanitizes sensitive data in input data.

            Args:
                data (Any): The input data to be sanitized.

            Returns:
                Any: The sanitized data.

            """
            if isinstance(data, dict):
                return {
                    k: (
                        sanitize_recursively(v)
                        if k
                        not in (
                            self.default_sensitive_keys
                            + (additional_sensitive_keys or [])
                        )
                        else "*****"
                    )
                    for k, v in data.items()
                }
            elif isinstance(data, list):
                return [sanitize_recursively(v) for v in data]
            elif isinstance(data, BaseModel):  # check if data is a pydantic object
                return sanitize_recursively(data.model_dump())
            else:
                return data

        return sanitize_recursively(data)

    async def convert_args_to_kwargs(self, span_kind: SpanKind, func, args, kwargs):
        """
        Convert the arguments and keyword arguments to a dictionary of keyword arguments.

        Args:
            func (function): The function to extract argument names from.
            args (tuple): The positional arguments.
            kwargs (dict): The keyword arguments.

        Returns:
            dict: A dictionary of keyword arguments.
        """
        arg_names = func.__code__.co_varnames[: func.__code__.co_argcount]
        arg_dict = {name: arg for name, arg in zip(arg_names, args)}
        arg_dict.update(kwargs)
        arg_dict["username"] = arg_dict.get("payload", {}).get("username", "EMPTY")
        if span_kind == SpanKind.LLM:
            request_body = await kwargs["request"].json()
            arg_dict["request_body"] = request_body.copy()
            if "tools" in arg_dict["request_body"]:
                arg_dict["request_body"].pop("tools")
        if span_kind == SpanKind.EMBEDDING:
            arg_dict["request_body"] = await kwargs["request"].json()
        return arg_dict

    def trace_async(self, span_kind: SpanKind, sensitive_keys=None):
        """
        Decorator for tracing async functions.

        This decorator creates a span and sets it as the current span.
        It also sets the session_id and sanitizes the input.

        Args:
            span_kind (str): The kind of span to be created.
            sensitive_keys (List[str]): The keys that should be sanitized.

        Returns:
            A decorator function.
        """
        if not self.tracing_enabled:
            return lambda func: func

        def decorator(func):
            """
            The decorator function.

            This function creates a new span and sets it as the current span.
            It also sets the session_id and sanitizes the input.

            Args:
                *args: The positional arguments.
                **kwargs: The keyword arguments.

            Returns:
                The output of the function.

            Raises:
                Exception: The exception raised by the function.
            """

            @wraps(func)
            async def wrapper(*args, **kwargs):
                """
                The wrapper function.

                This function creates a new span and sets it as the current span.
                It also sets the session_id and sanitizes the input.

                For non-streaming responses the span is ended immediately after
                the function returns.  For streaming responses the span is kept
                open and ended by _trace_stream once the iterator is exhausted
                (or raises).

                Args:
                    *args: The positional arguments.
                    **kwargs: The keyword arguments.

                Returns:
                    The output of the function.

                Raises:
                    Exception: The exception raised by the function.
                """
                tracer = self.tracer
                # Create the span manually so we can keep it open past this
                # function's return when the response is a StreamingResponse.
                span = tracer.start_span(
                    func.__name__, openinference_span_kind=span_kind.value
                )
                try:
                    # Make the span current for the synchronous setup + await
                    # portion so any child spans parent correctly.
                    with use_span(span, end_on_exit=False, record_exception=False, set_status_on_exception=False):
                        session_id = kwargs.get("session_id")
                        # Only set the attribute when present — writing a None
                        # span attribute is invalid (and version-dependently can
                        # raise). No route currently supplies session_id, so this
                        # simply stays unset rather than erroring on every request.
                        if session_id is not None:
                            span.set_attribute(SpanAttributes.SESSION_ID, session_id)
                        kwargs_dict = await self.convert_args_to_kwargs(
                            span_kind, func, args, kwargs
                        )
                        # Sanitize the input to remove sensitive information
                        sanitized_input = self.sanitize(
                            kwargs_dict, additional_sensitive_keys=sensitive_keys
                        )
                        # Set the input of the span.
                        span.set_input(sanitized_input)
                        # Execute the function.
                        output = await func(*args, **kwargs)
                except Exception as e:
                    # Record the exception raised during setup or func execution.
                    span.record_exception(e)
                    span.set_status(Status(StatusCode.ERROR))
                    span.end()
                    raise

                # --- post-await: decide how to close the span ---
                if isinstance(output, StreamingResponse):
                    # The span stays open; _trace_stream ends it when the
                    # iterator is exhausted or raises.
                    output.body_iterator = self._trace_stream(span, output.body_iterator)
                    return output

                # Non-streaming path: identical behavior to before.
                span.set_output(output)
                span.set_status(Status(StatusCode.OK))
                span.end()
                return output

            return wrapper

        return decorator

    async def _trace_stream(self, span, body_iterator):
        """
        Async generator that wraps a StreamingResponse body iterator.

        Passes every chunk through to the caller unchanged while accumulating
        the assistant text and any tool calls. _stream_response swallows
        backend/proxy exceptions and surfaces them as an SSE ``error`` event,
        so a failed stream still completes normally here — detect that and mark
        the span ERROR rather than relying on an exception reaching this
        generator. The span is always ended in the finally block.
        """
        parts = []
        # Keyed by tool-call index; each value: {"id", "name", "arguments"}.
        tool_calls_acc: Dict[int, Dict[str, Any]] = {}
        error_seen = False
        try:
            async for chunk in body_iterator:
                text, tc_deltas, has_error = self._parse_chunk(chunk)
                parts.append(text)
                self._accumulate_tool_calls(tool_calls_acc, tc_deltas)
                if has_error:
                    error_seen = True
                yield chunk
            # Normal completion.
            span.set_output(self._build_stream_output(parts, tool_calls_acc))
            span.set_status(
                Status(StatusCode.ERROR if error_seen else StatusCode.OK)
            )
        except Exception as e:
            span.set_output(self._build_stream_output(parts, tool_calls_acc))
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR))
            raise
        finally:
            span.end()

    @staticmethod
    def _accumulate_tool_calls(
        acc: Dict[int, Dict[str, Any]],
        deltas: list,
    ) -> None:
        """
        Merge per-chunk tool-call fragments into the running accumulator.

        ``acc`` is keyed by tool-call ``index``.  Each entry holds:
          ``id``        — call ID (set on first fragment, kept thereafter)
          ``name``      — function name (set on first fragment, kept thereafter)
          ``arguments`` — concatenated argument fragments streamed so far
        """
        for delta in deltas:
            idx = delta.get("index", 0)
            # bool is a subclass of int, so guard it explicitly — "index": true
            # would otherwise collide with a legitimate index 1 entry.
            if not isinstance(idx, int) or isinstance(idx, bool):
                idx = 0
            entry = acc.setdefault(idx, {"id": None, "name": None, "arguments": ""})
            if delta.get("id") is not None:
                entry["id"] = delta["id"]
            if delta.get("name") is not None:
                entry["name"] = delta["name"]
            if delta.get("arguments"):
                entry["arguments"] += delta["arguments"]

    @staticmethod
    def _build_stream_output(parts: list, tool_calls_acc: Dict[int, Dict[str, Any]]) -> str:
        """
        Build the final span output string from accumulated text and tool calls.

        - Pure-text streams (no tool calls): returns ``"".join(parts)`` unchanged
          — identical to the pre-tool-call behavior, fully backward compatible.
        - Streams with tool calls: returns a JSON string
          ``{"content": "<text>", "tool_calls": [...]}`` where each tool-call
          entry is ``{"id": ..., "name": ..., "arguments": "<concatenated>"}``.
          The ``"content"`` key is omitted when text is empty.  Tool calls are
          ordered by their index.
        """
        if not tool_calls_acc:
            return "".join(parts)

        tool_calls_list = [
            {
                "id": entry["id"],
                "name": entry["name"],
                "arguments": entry["arguments"],
            }
            for _, entry in sorted(tool_calls_acc.items())
        ]
        content = "".join(parts)
        payload: Dict[str, Any] = {"tool_calls": tool_calls_list}
        if content:
            payload["content"] = content
        return json.dumps(payload, ensure_ascii=False)

    def _parse_chunk(self, chunk):
        """
        Parse one SSE chunk into ``(assistant_text, tool_call_deltas, has_error)``.

        ``tool_call_deltas`` is a list of per-chunk tool-call fragments; each
        fragment is a dict ``{"index": int, "id": str|None, "name": str|None,
        "arguments": str}`` where ``arguments`` is the partial string in THIS
        chunk only (empty string when absent).

        ``has_error`` is True when the chunk carries a top-level ``error``
        object — that is how ``_stream_response`` surfaces backend/proxy
        failures mid-stream (it swallows the exception and yields an error
        event instead of raising). Handles both the OpenAI chat.completions
        and Anthropic /v1/messages streaming formats. Never raises — returns
        ``("", [], False)`` on any unexpected input.
        """
        try:
            if isinstance(chunk, bytes):
                text = chunk.decode("utf-8", errors="replace")
            else:
                text = str(chunk)

            collected = []
            tc_deltas = []
            has_error = False
            for raw_line in text.splitlines():
                line = raw_line.strip()
                if not line.startswith("data:"):
                    continue
                data_str = line[len("data:"):].strip()
                if not data_str or data_str == "[DONE]":
                    continue
                try:
                    payload = json.loads(data_str)
                except json.JSONDecodeError:
                    continue
                if not isinstance(payload, dict):
                    continue

                # Error events: _stream_response synthesises
                # {"error": {...}} on backend/proxy failure; native Anthropic
                # streaming uses {"type": "error", "error": {...}}. Both carry a
                # top-level "error" key, but also accept type=="error" defensively.
                if payload.get("error") is not None or payload.get("type") == "error":
                    has_error = True

                # OpenAI chat.completions streaming format.
                for choice in payload.get("choices", []) or []:
                    delta = choice.get("delta") or {}

                    # Text content.
                    content = delta.get("content")
                    if content:
                        collected.append(content)

                    # Tool calls — streamed as incremental fragments keyed by index.
                    for tc in delta.get("tool_calls", []) or []:
                        func = tc.get("function") or {}
                        tc_deltas.append({
                            "index": tc.get("index", 0),
                            "id": tc.get("id"),
                            "name": func.get("name"),
                            "arguments": func.get("arguments") or "",
                        })

                    # Legacy / deprecated function_call shape (single call, index 0).
                    # Only emit when tool_calls is absent — a delta carrying both
                    # would otherwise overwrite the real tool name at index 0.
                    fc = delta.get("function_call")
                    if fc and isinstance(fc, dict) and not delta.get("tool_calls"):
                        tc_deltas.append({
                            "index": 0,
                            "id": None,
                            "name": fc.get("name"),
                            "arguments": fc.get("arguments") or "",
                        })

                # Anthropic /v1/messages streaming format.
                event_type = payload.get("type")

                if event_type == "content_block_delta":
                    # Use a distinct name to avoid shadowing the OpenAI choices-loop `delta`.
                    cb_delta = payload.get("delta") or {}
                    if cb_delta.get("type") == "text_delta":
                        text_val = cb_delta.get("text")
                        if text_val:
                            collected.append(text_val)
                    elif cb_delta.get("type") == "input_json_delta":
                        # Tool-call argument fragment; index carried at payload level.
                        tc_deltas.append({
                            "index": payload.get("index", 0),
                            "id": None,
                            "name": None,
                            "arguments": cb_delta.get("partial_json") or "",
                        })

                elif event_type == "content_block_start":
                    # Carries tool-use id + name; no argument text yet.
                    cb = payload.get("content_block") or {}
                    if cb.get("type") == "tool_use":
                        tc_deltas.append({
                            "index": payload.get("index", 0),
                            "id": cb.get("id"),
                            "name": cb.get("name"),
                            "arguments": "",
                        })

            return "".join(collected), tc_deltas, has_error
        except Exception:
            logger.debug("_parse_chunk: unexpected error", exc_info=True)
            return "", [], False

    def _extract_stream_text(self, chunk) -> str:
        """
        Return only the assistant text from a single SSE chunk.

        Thin wrapper over :meth:`_parse_chunk`; see it for format details.
        Never raises — returns "" on any unexpected input.
        """
        return self._parse_chunk(chunk)[0]


def get_tracing_config():
    if not settings.PHOENIX_TRACING_ENABLED:
        return Tracing(tracing_enabled=False)

    if not _TRACING_DEPS_AVAILABLE:
        logger.warning(
            "PHOENIX_TRACING_ENABLED=true but tracing libraries "
            "(phoenix / openinference / opentelemetry) are not installed; "
            "tracing is disabled."
        )
        return Tracing(tracing_enabled=False)

    if settings.PHEONIX_API_KEY:
        headers = {"Authorization": f"Bearer {settings.PHEONIX_API_KEY}"}
    else:
        headers = {}

    tracer_provider = register(
        project_name=settings.PROJECT_NAME,
        headers=headers,
        batch=True,
        endpoint=f"{settings.PHOENIX_COLLECTOR_ENDPOINT}/v1/traces",
    )
    OpenAIInstrumentor().instrument(tracer_provider=tracer_provider)
    return Tracing(
        tracer=tracer_provider.get_tracer(__name__),
        default_sensitive_keys=["token", "openai_api_key"],
    )


TracingHelper = get_tracing_config()
