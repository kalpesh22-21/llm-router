# Metrics ([app/metrics/collector.py](../../app/metrics/collector.py))

Prometheus metrics exposed at `GET /metrics`. Scraped by the bundled Prometheus instance every 15 seconds.

## Counters

| Metric | Labels | Description |
|--------|--------|-------------|
| `llm_router_requests_total` | `model_id`, `user_id`, `status` | Total requests. `status` is `success` or `error`. |
| `llm_router_tokens_input_total` | `model_id`, `user_id` | Prompt tokens consumed |
| `llm_router_tokens_output_total` | `model_id`, `user_id` | Completion tokens generated |

## Histograms

| Metric | Labels | Buckets | Description |
|--------|--------|---------|-------------|
| `llm_router_request_duration_seconds` | `model_id` | 0.1s → 120s | End-to-end request latency |
| `llm_router_ttft_seconds` | `model_id` | 0.05s → 12s | Time to first token (streaming only) |
| `llm_router_itl_seconds` | `model_id` | 0.005s → 1s | Inter-token latency (streaming only) |

## Recording

`MetricsCollector.record_request_complete(model_id, user_id, status, duration, input_tokens, output_tokens)` — called after every request.

`MetricsCollector.record_streaming_metrics(model_id, ttft, itl_observations)` — called after a streaming response completes. `itl_observations` is a list of per-token durations.

## Prometheus Configuration

See [prometheus.yml](../../prometheus.yml). The scrape target is `app:8000` (the Docker service name), job `llm-router`.
