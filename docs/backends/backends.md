# Backends

## Backend ABC ([app/backends/base.py](../../app/backends/base.py))

All backends implement this interface. The proxy layer only calls these methods — it never constructs URLs or touches cluster internals directly.

```python
class Backend(ABC):
    @property
    def model_id(self) -> str: ...

    async def forward(self, request: Request, body: dict) -> Response: ...
    async def forward_stream(self, request: Request, body: dict) -> AsyncIterator[bytes]: ...
```

The `BackendRegistry` is a dict (`model_id → Backend`) populated at startup. `ModelRouter.resolve(model_id)` wraps the registry lookup and raises HTTP 404 for unknown models.

---

## StaticBackend ([app/backends/static.py](../../app/backends/static.py))

Phase 1 implementation. Forwards requests to a fixed `base_url` from `config/models.yaml`.

- Shared `httpx.AsyncClient`: 120 s timeout, 200 max connections (pooled across all backends).
- `forward()` → `POST {base_url}/chat/completions`
- `forward_stream()` → streaming `POST {base_url}/chat/completions`
- `forward_embeddings()` → `POST {base_url}/embeddings`

The backend's `api_key` replaces the `Authorization` header on every outbound request.

**Config shape:**

```yaml
- model_id: gpt-4o
  base_url: https://api.openai.com/v1
  api_key: ""          # empty = use API_KEY env var
```

---

## MultiClusterBackend ([app/backends/multi_cluster.py](../../app/backends/multi_cluster.py))

Phase 2 implementation. Routes one `model_id` across N cluster endpoints using load-aware, cache-biased routing.

### Cluster selection

```
local load_ratio < spill_ratio
  → route local                         (latency preference)

local saturated AND cache hit for this conversation context
  → route to cached remote cluster      (KV cache reuse)

otherwise
  → least-loaded remote cluster
```

`load_ratio = inflight_tokens / inferred_capacity`

### Inflight token tracking

Load is tracked in tokens rather than requests — request count is blind to size (one 4K-token request puts the same KV pressure as 80 short ones).

```
reservation = estimated_prompt_tokens + max_tokens   (pessimistic upper bound)
```

- **Reserve** on request arrival; **release** in a `finally` block so a client disconnect never leaks the counter.
- Prompt tokens are estimated as `len(json(messages)) / 4` (±20%, sufficient for routing).
- When `max_tokens` is absent from the request body, `default_max_tokens` from the routing config is used.

### Inferred capacity

Rather than hardcoding a capacity ceiling, the backend infers it from observed latency stress:

```
ttft_stress = max(0, ewma_ttft_ms / ttft_budget_ms − 1)
itl_stress  = max(0, ewma_itl_ms  / itl_budget_ms  − 1)
combined    = max(ttft_stress, itl_stress)           # worst bottleneck wins
```

`max()` over the two signals prevents a mild TTFT spike from masking a severe ITL spike.

| State | Capacity update |
|---|---|
| `combined_stress > 0` | Chase inflight tokens downward; alpha scales with stress (faster when more degraded) |
| Healthy | Drift upward slowly at `capacity_alpha` rate |

### Latency signals

Both are EWMA-smoothed and updated after every completed request:

| Signal | Measures | Tuning |
|---|---|---|
| **TTFT** | Queue depth + prefill time | `ttft_alpha` (default 0.1 — slow, queue depth changes gradually) |
| **Mean ITL** | Decode throughput per token | `itl_alpha` (default 0.2 — faster, decode saturation shows up quickly) |

ITL samples are normalized per token: `itl_per_token = chunk_interval_ms / estimated_tokens_in_chunk`. This prevents over-penalizing clusters that batch their SSE output (large infrequent chunks vs. one-token-per-chunk).

For non-streaming requests only TTFT is available; `mean_itl_ms=None` is passed and the ITL EWMA is left unchanged.

### Approximate cluster cache

Routes multi-turn conversations to the cluster that already holds their KV state.

- **Key**: `SHA-256(model_id + messages[:-1])[:16]` — everything except the current user turn, so the whole session hashes to the same key.
- **TTL**: configurable (default 1 h). Stale entries cause a cache miss and fall back to load routing safely — they never cause incorrect routing.
- Cache is written in the `finally` block of both `forward()` and `forward_stream()`, including on client disconnect, since partial KV state may still exist in the cluster.

**Config shape:**

```yaml
- model_id: llama-3-70b
  type: multi_cluster
  clusters:
    - cluster_id: us-west
      base_url: http://llama-us-west.internal/v1
      api_key: ""
    - cluster_id: us-east
      base_url: http://llama-us-east.internal/v1
      api_key: ""
  routing:
    # local_cluster_id: us-west   # or set ROUTER_CLUSTER_ID env var
    spill_ratio: 0.8
    seed_capacity_tokens: 200000
    default_max_tokens: 2048
    cache_ttl_seconds: 3600
    ttft_budget_ms: 500
    itl_budget_ms: 80
    ttft_alpha: 0.1
    itl_alpha: 0.2
    capacity_alpha: 0.1
```

`local_cluster_id` can be omitted from `models.yaml` and provided via `ROUTER_CLUSTER_ID` so the same config file deploys to every cluster unchanged.

---

## Model Configuration ([config/models.yaml](../../config/models.yaml))

Both backend types coexist in the same file. The `type` field (default `static`) selects the backend class.

```yaml
models:
  # Static — single URL
  - model_id: gpt-4o
    base_url: https://api.openai.com/v1
    api_key: ""

  # Multi-cluster — N URLs with routing config
  - model_id: llama-3-70b
    type: multi_cluster
    clusters: [...]
    routing: {...}
```

`build_registry_from_config()` is called at application startup. Adding or removing a model requires a restart.

### Config parsing ([app/core/config.py](../../app/core/config.py))

| YAML shape | Python type | Backend created |
|---|---|---|
| No `type` or `type: static` | `ModelConfig` | `StaticBackend` |
| `type: multi_cluster` | `MultiClusterModelConfig` | `MultiClusterBackend` |

The `ROUTER_CLUSTER_ID` environment variable is the global default for `local_cluster_id` across all multi-cluster models. A per-model `routing.local_cluster_id` overrides it.
