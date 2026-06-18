from enum import Enum
from functools import wraps
import logging
import traceback
from typing import Any, Dict
from opentelemetry.trace import Status, StatusCode
from openinference.semconv.trace import SpanAttributes
from phoenix.otel import register
from openinference.instrumentation.openai import OpenAIInstrumentor
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

                Args:
                    *args: The positional arguments.
                    **kwargs: The keyword arguments.

                Returns:
                    The output of the function.

                Raises:
                    Exception: The exception raised by the function.
                """
                tracer = self.tracer
                with tracer.start_as_current_span(
                    func.__name__, openinference_span_kind=span_kind.value
                ) as span:
                    try:
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
                        # Set the output of the span.
                        span.set_output(output)
                        # Set the status of the span to OK.
                        span.set_status(Status(StatusCode.OK))
                    except Exception as e:
                        # Record the exception.
                        span.record_exception(Exception(traceback.format_exception(e)))
                        # Set the status of the span to ERROR.
                        span.set_status(Status(StatusCode.ERROR))
                        raise
                return output

            return wrapper

        return decorator


def get_tracing_config():
    if not settings.PHOENIX_TRACING_ENABLED:
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
