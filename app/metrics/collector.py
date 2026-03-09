"""
Prometheus metrics definitions and collection helpers.

All metric objects are module-level singletons registered with the default
prometheus_client registry. The FastAPI app exposes them at /metrics.

Metric taxonomy:
  Counters  — cumulative totals (requests, tokens)
  Histograms— latency distributions (request duration, TTFT, ITL)
"""

from __future__ import annotations

import logging
from typing import List

from prometheus_client import Counter, Histogram, REGISTRY

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------

requests_total = Counter(
    "llm_router_requests_total",
    "Total number of requests handled by the router",
    ["model_id", "user_id", "status"],
)

tokens_input_total = Counter(
    "llm_router_tokens_input_total",
    "Total number of prompt (input) tokens forwarded to backends",
    ["model_id", "user_id"],
)

tokens_output_total = Counter(
    "llm_router_tokens_output_total",
    "Total number of completion (output) tokens returned from backends",
    ["model_id", "user_id"],
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
        user_id: str,
        status: str,
        duration: float,
        input_tokens: int,
        output_tokens: int,
    ) -> None:
        """
        Record metrics at the end of a request (streaming or non-streaming).

        Args:
            model_id:      The model that served the request.
            user_id:       String representation of the user's DB id.
            status:        HTTP status code string, e.g. "200" or "500".
            duration:      Total elapsed seconds for the request.
            input_tokens:  Number of prompt tokens (from usage field).
            output_tokens: Number of completion tokens (from usage field).
        """
        requests_total.labels(
            model_id=model_id, user_id=user_id, status=status
        ).inc()

        request_duration_seconds.labels(model_id=model_id).observe(duration)

        if input_tokens > 0:
            tokens_input_total.labels(
                model_id=model_id, user_id=user_id
            ).inc(input_tokens)

        if output_tokens > 0:
            tokens_output_total.labels(
                model_id=model_id, user_id=user_id
            ).inc(output_tokens)

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


# Module-level singleton
metrics_collector = MetricsCollector()
