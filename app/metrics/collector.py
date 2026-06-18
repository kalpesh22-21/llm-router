"""
Prometheus metrics definitions and collection helpers.

All metric objects are module-level singletons registered with the default
prometheus_client registry. The FastAPI app exposes them at /metrics.

Metric taxonomy:
  Counters  — cumulative totals (requests, tokens)
  Histograms— latency distributions (request duration, TTFT, ITL)
  Gauges    — point-in-time values (concurrent requests)
"""

from __future__ import annotations

import logging
from typing import List
from uuid import uuid4
from prometheus_client import Counter, Histogram, Gauge, REGISTRY

logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------

requests_total = Counter(
    "llm_router_requests_total",
    "Total number of requests handled by the router",
    ["model_id", "username", "status"],
)

tokens_input_total = Counter(
    "llm_router_tokens_input_total",
    "Total number of prompt (input) tokens forwarded to the backend",
    ["model_id", "username"],
)

tokens_output_total = Counter(
    "llm_router_tokens_output_total",
    "Total number of completion (output) tokens returned from the backend",
    ["model_id", "username"],
)

# ---------------------------------------------------------------------------
# Histograms
# ---------------------------------------------------------------------------

request_duration_seconds = Histogram(
    "llm_router_request_duration_seconds",
    "End-to-end request duration from the router's perspective",
    ["model_id"],
    buckets=(0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0),
)

time_to_first_token_seconds = Histogram(
    "llm_router_ttft_seconds",
    "Time from sending the request to receiving the first token (streaming only)",
    ["model_id"],
    buckets=(0.05, 0.1, 0.2, 0.4, 0.8, 1.5, 3.0, 6.0, 12.0),
)

inter_token_latency_seconds = Histogram(
    "llm_router_itl_seconds",
    "Inter-token latency — gap between consecutive tokens (streaming only)",
    ["model_id"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)

router_latency_to_stream_seconds = Histogram(
    "llm_router_latency_to_stream_seconds",
    "Time from request received by router to streaming starts (internal processing latency)",
    ["model_id"],
    buckets=(
        0.001,
        0.005,
        0.01,
        0.025,
        0.05,
        0.1,
        0.25,
        0.5,
        0.6,
        0.7,
        0.8,
        0.9,
        1.0,
        2.0,
        3.0,
    ),
)


# -----------------------------------------------------------------------------
# Gauges
# -----------------------------------------------------------------------------

concurrent_requests = Gauge(
    "llm_router_concurrent_requests",
    "Current number of concurrent requests being processed",
    ["model_id", "username"],
)


# ---------------------------------------------------------------------------
# MetricsCollector facade
# ---------------------------------------------------------------------------


class MetricsCollector:
    """
    Thin facade over the raw prometheus_client objects.

    Callers should use the module-level singleton `metrics_collector` rather
    than instantiating this class directly.
    """

    def record_request_complete(
        self,
        model_id: str,
        username: str,
        status: str,
        duration: float,
        input_tokens: int,
        output_tokens: int,
    ) -> None:
        """
        Record metrics at the end of a request (streaming or non-streaming).

        Args:
            model_id:      The model that served the request.
            username:       String representation of the user's DB name.
            status:        HTTP status code string, e.g. "200" or "500".
            duration:      Total elapsed seconds for the request.
            input_tokens:  Number of prompt tokens (from usage field).
            output_tokens: Number of completion tokens (from usage field).
        """
        requests_total.labels(
            model_id=model_id,
            username=username,
            status=status,
        ).inc()

        request_duration_seconds.labels(model_id=model_id).observe(duration)

        if input_tokens > 0:
            tokens_input_total.labels(model_id=model_id, username=username).inc(
                input_tokens
            )

        if output_tokens > 0:
            tokens_output_total.labels(model_id=model_id, username=username).inc(
                output_tokens
            )

    def record_streaming_metrics(
        self,
        model_id: str,
        ttft: float,
        inter_token_latencies: List[float],
    ) -> None:
        """
        Record streaming-specific latency metrics.

        Args:
            model_id:               The model that served the request.
            ttft:                   Time-to-first-token in seconds.
            inter_token_latencies:  List of gaps between consecutive tokens (seconds).
        """
        time_to_first_token_seconds.labels(model_id=model_id).observe(ttft)

        for itl in inter_token_latencies:
            inter_token_latency_seconds.labels(model_id=model_id).observe(itl)

    def record_router_latency_to_stream(
        self,
        model_id: str,
        latency: float,
    ) -> None:
        """
        Record router internal processing latency from request received to stream start.

        This measures the time spent in the router's internal processing before
        the backend connection is established and streaming begins.

        Args:
            model_id:  The model that will serve the request.
            latency:   Time from request landing to stream start in seconds.
        """
        router_latency_to_stream_seconds.labels(model_id=model_id).observe(latency)

    def record_concurrent_increment(
        self,
        model_id: str,
        username: str,
    ) -> None:
        """
        Increment the concurrent requests gauge.

        Call this when a request has passed rate limiting and is about to be
        processed. The corresponding decrement must be called in a finally
        block to ensure the gauge is always updated on exit.

        Args:
            model_id:  The model that will serve the request.
            username:  String representation of the user's DB name.
        """
        concurrent_requests.labels(model_id=model_id, username=username).inc()

    def record_concurrent_decrement(
        self,
        model_id: str,
        username: str,
    ) -> None:
        """
        Decrement the concurrent requests gauge.

        Call this in a finally block to ensure the gauge is always updated
        when a request completes, regardless of success or failure.

        Args:
            model_id:  The model that served the request.
            username:  String representation of the user's DB name.
        """
        concurrent_requests.labels(model_id=model_id, username=username).dec()


# Module-level singleton
metrics_collector = MetricsCollector()
