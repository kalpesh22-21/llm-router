# MultiClusterBackend — Routing & Cache Fix

## Background

Two bugs in `app/backends/multi_cluster.py`:

1. **Cache-first routing broken** — local cluster preference ignores cache entirely; cache is only consulted as a fallback when local is saturated. The intended logic is cache hit = primary signal, spill_ratio = overflow guard only.
2. **Flat hash causes collisions** — `_make_key` hashes the entire `messages[:-1]` blob. Unrelated conversations with shared content (e.g. same system prompt) collide. No prefix matching — can't detect partial KV cache hits.

---

## Change 1 — Chained prefix keys in `ClusterCache`

Replace `_make_key` with `_make_chain_keys`.

**Algorithm:**
```
key[0] = sha256(model_id + serialize(msg[0]))
key[i] = sha256(key[i-1] + serialize(msg[i]))   for i in 1..n-1
```

Each `key[i]` encodes the full conversation path from index 0 to i. Two messages with identical content at the same position but in different conversation contexts (different history) produce different keys — no collision.

**Signature:**
```python
def _make_chain_keys(self, model_id: str, messages: list[dict]) -> list[str]:
    ...
```

Returns a list of hex digest strings, one per message.

---

## Change 2 — Rewrite `ClusterCache.get`

Walk chain keys from index 0. Stop at first miss or TTL expiry. Return the cluster with the longest unbroken run from position 0.

**Signature:**
```python
def get(self, model_id: str, messages: list[dict]) -> Optional[str]:
    # returns cluster_id of longest unbroken prefix hit, or None
```

**Algorithm:**
```
keys = _make_chain_keys(model_id, messages[:-1])  # exclude last (new turn)
best_cluster = None
for key in keys:
    entry = _store.get(key)
    if entry is None or expired:
        break
    best_cluster = entry.cluster_id
return best_cluster
```

Stops at first miss — guarantees the returned cluster has the full prefix in KV cache, not just scattered hits.

---

## Change 3 — Rewrite `ClusterCache.set`

On request completion, store all prefix keys for the conversation. O(n) writes per request.

**Signature:**
```python
def set(self, model_id: str, messages: list[dict], cluster_id: str) -> None:
```

**Algorithm:**
```
keys = _make_chain_keys(model_id, messages)
for key in keys:
    _store[key] = (cluster_id, now + ttl)
```

Overwrites existing entries — last-writer-wins per key. Acceptable because key[i] encodes full prefix context, so a collision means the conversation genuinely shares that prefix.

---

## Change 4 — Reorder `_select_cluster` priority

**New priority:**

| Priority | Condition | Reason |
|----------|-----------|--------|
| 1 | Cache hit on **any** cluster (incl. local) AND `load < spill_ratio` | Maximize KV cache reuse |
| 2 | Local cluster AND `load < spill_ratio` | Latency preference |
| 3 | Least-loaded across **all** clusters | Load balance |
| 4 | Local regardless of load | Single-cluster fallback |

**Key removals:**
- Remove `cached_id != self._local_cluster_id` exclusion — local is a valid cache hit target.
- Remove the current top-priority local shortcut that bypasses cache entirely.

**New pseudocode:**
```python
def _select_cluster(self, messages):
    # 1. Cache hit
    cached_id = self._cache.get(self._model_id, messages)
    if cached_id:
        cached = self._clusters.get(cached_id)
        if cached and cached.load_ratio() < self._spill_ratio:
            return cached

    # 2. Local under spill threshold
    local = self._local()
    if local and local.load_ratio() < self._spill_ratio:
        return local

    # 3. Least loaded (all clusters)
    least = self._least_loaded()
    if least:
        return least

    # 4. Local fallback (single-cluster setup)
    if local:
        return local

    raise RuntimeError(...)
```

Note: `_least_loaded` should no longer exclude local cluster — it currently passes `exclude=self._local_cluster_id` which should be removed.

---

## Scope

| Component | Change |
|-----------|--------|
| `ClusterCache._make_key` | Replace with `_make_chain_keys` |
| `ClusterCache.get` | Rewrite — walk chain, longest unbroken prefix |
| `ClusterCache.set` | Rewrite — store all prefix keys |
| `MultiClusterBackend._select_cluster` | Reorder priority, remove local exclusions |
| `MultiClusterBackend._least_loaded` | Remove `exclude` default for local |
| `ClusterEndpoint`, `forward`, `forward_stream`, `forward_embeddings` | No changes |
