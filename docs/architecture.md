# Architecture

## Request Flow

```
POST /v1/chat/completions
  │
  ├─ 1. JWT validation (Bearer token)
  ├─ 2. User active check (DB + probabilistic LDAP re-check)
  ├─ 3. Model access check (user's buckets → allowed model IDs)
  ├─ 4. Concurrent limit check (Redis atomic increment)
  ├─ 5. Forward to backend (httpx)
  ├─ 6. Record metrics (Prometheus)
  └─ 7. Decrement concurrent counter (finally block)
```

## Access Control Model

Access is never granted to models directly. Users get access to **buckets**, and buckets enumerate model IDs.

```
User → UserBucketAccess → Bucket → BucketModel → model_id
```

On user creation, the default bucket is automatically granted. Admins grant additional buckets via the admin API.

## Backend Abstraction

The proxy layer only ever calls `backend.forward()` / `backend.forward_stream()` — it never touches URLs or cluster internals directly.

```python
class Backend(ABC):
    @property
    def model_id(self) -> str: ...
    async def forward(self, request, body) -> Response: ...
    async def forward_stream(self, request, body) -> AsyncIterator[bytes]: ...
```

| Backend | Phase | Description |
|---|---|---|
| `StaticBackend` | 1 | Fixed `base_url` from `models.yaml` |
| `MultiClusterBackend` | 2 | N cluster URLs; load-aware + cache-biased routing |

Both coexist in the same registry — per-model config selects the backend type.

## Rate Limiting

Per-user concurrent request cap is stored in the `users.max_concurrent_requests` column and enforced via a Redis counter with a Lua atomic check-and-increment. A 300-second TTL on the Redis key prevents permanent locks if a worker crashes mid-request.

## Multi-Cluster Routing

`MultiClusterBackend` routes a single `model_id` across N clusters. See [docs/backends/backends.md](backends/backends.md) for full detail.

**Cluster selection (in priority order):**

1. **Local cluster** — if `inflight_tokens / inferred_capacity < spill_ratio` (latency preference)
2. **Cached remote** — if the conversation context was last served by a healthy remote cluster (KV cache reuse)
3. **Least-loaded remote** — fallback

**Inferred capacity** adapts from live TTFT and mean ITL signals. Both metrics are EWMA-smoothed; the cluster is considered stressed when either exceeds its budget. Under stress, inferred capacity chases current inflight tokens downward (faster when more stressed); when healthy it drifts upward slowly.

**Hierarchical deployment:** This router acts as the central cluster router. Each model cluster runs its own routing agent (a Phase 1 router instance) that handles pod-level selection internally. The central router only needs to reach each cluster's stable LoadBalancer endpoint.

## Key Files

| Concern | File |
|---|---|
| App entry point | [app/main.py](../app/main.py) |
| Settings + config parsing | [app/core/config.py](../app/core/config.py) |
| Backend abstraction | [app/backends/base.py](../app/backends/base.py) |
| Static backend (Phase 1) | [app/backends/static.py](../app/backends/static.py) |
| Multi-cluster backend (Phase 2) | [app/backends/multi_cluster.py](../app/backends/multi_cluster.py) |
| Proxy endpoints | [app/api/proxy.py](../app/api/proxy.py) |
| Access control | [app/services/permission.py](../app/services/permission.py) |
| Rate limiting | [app/services/rate_limit.py](../app/services/rate_limit.py) |
| Metrics | [app/metrics/collector.py](../app/metrics/collector.py) |
