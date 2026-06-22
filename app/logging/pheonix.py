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

    def sanitize(self, data, additional_sensitive_keys=[]):
        """
        Sanitizes sensitive data in input data.

        Args:
            data (Any): The input data to be sanitized.

        Returns:
            Any: The sanitized data.

        """

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
        the assistant text. _stream_response swallows backend/proxy exceptions
        and surfaces them as an SSE ``error`` event, so a failed stream still
        completes normally here — detect that and mark the span ERROR rather
        than relying on an exception reaching this generator. The span is
        always ended in the finally block.
        """
        parts = []
        error_seen = False
        try:
            async for chunk in body_iterator:
                text, has_error = self._parse_chunk(chunk)
                parts.append(text)
                if has_error:
                    error_seen = True
                yield chunk
            # Normal completion.
            span.set_output("".join(parts))
            span.set_status(
                Status(StatusCode.ERROR if error_seen else StatusCode.OK)
            )
        except Exception as e:
            span.set_output("".join(parts))
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR))
            raise
        finally:
            span.end()

    def _parse_chunk(self, chunk):
        """
        Parse one SSE chunk into ``(assistant_text, has_error)``.

        ``has_error`` is True when the chunk carries a top-level ``error``
        object — that is how ``_stream_response`` surfaces backend/proxy
        failures mid-stream (it swallows the exception and yields an error
        event instead of raising). Handles both the OpenAI chat.completions
        and Anthropic /v1/messages streaming formats. Never raises — returns
        ``("", False)`` on any unexpected input.
        """
        try:
            if isinstance(chunk, bytes):
                text = chunk.decode("utf-8", errors="replace")
            else:
                text = str(chunk)

            collected = []
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
                    content = (choice.get("delta") or {}).get("content")
                    if content:
                        collected.append(content)

                # Anthropic /v1/messages streaming format.
                if payload.get("type") == "content_block_delta":
                    delta = payload.get("delta") or {}
                    if delta.get("type") == "text_delta":
                        text_val = delta.get("text")
                        if text_val:
                            collected.append(text_val)

            return "".join(collected), has_error
        except Exception:
            logger.debug("_parse_chunk: unexpected error", exc_info=True)
            return "", False

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
