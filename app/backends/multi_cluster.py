"""
MultiClusterBackend — routes one model across N cluster endpoints.

Routing strategy:
  1. Cache hit on any cluster (incl. local) while load_ratio < spill_ratio — maximize KV cache reuse.
  2. Prefer local cluster while load_ratio < spill_ratio — lower WAN latency.
  3. Fall back to least-loaded across all clusters.

Inflight load is tracked in tokens (not requests):
  reservation = estimated_prompt_tokens + max_tokens (pessimistic upper bound)

Inferred capacity adapts based on TTFT and mean ITL stress:
  - Either signal spiking above its budget → chase inflight tokens downward
    (faster when more stressed).
  - Both healthy → drift capacity upward slowly.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import AsyncIterator, Optional

import httpx
from fastapi import Request

from app.backends.base import Backend
from app.backends.static import get_http_client
from app.db.redis_client import get_redis_pool

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Cluster-level approximate cache
# ---------------------------------------------------------------------------


class ClusterCache:
    """
    Approximate prefix cache: conversation context hash → cluster_id.

    Keys are chained: key[i] = sha256(key[i-1] + serialize(msg[i])), so each
    key encodes the full conversation path from index 0 to i. Lookup walks the
    chain and returns the cluster with the longest unbroken prefix hit —
    maximizing KV cache reuse without collisions across unrelated conversations.

    TTL eviction is intentionally generous (default 1 h) — a stale entry
    just causes a cache miss, which falls back to load-based routing safely.
    """

    _KEY_PREFIX = "cluster_cache:"

    def __init__(self, ttl_seconds: int = 3600) -> None:
        self._ttl = ttl_seconds

    def _make_chain_keys(self, model_id: str, messages: list[dict]) -> list[str]:
        """
        Build a chained prefix key list.

        key[0] = sha256(model_id + serialize(msg[0]))
        key[i] = sha256(key[i-1] + serialize(msg[i]))

        Each key encodes the full conversation path from index 0 to i, so two
        messages with identical content at the same position but different
        histories produce different keys — no collision.
        """
        keys: list[str] = []
        prev = model_id
        for msg in messages:
            blob = prev + json.dumps(msg, sort_keys=True)
            digest = hashlib.sha256(blob.encode()).hexdigest()
            keys.append(digest)
            prev = digest
        return keys

    async def get(self, model_id: str, messages: list[dict]) -> Optional[str]:
        """
        Return cluster_id of longest unbroken prefix hit, or None.

        Walks chain keys for messages[:-1] (excluding new turn) from index 0.
        Stops at first miss — guarantees the returned cluster has the full
        prefix in KV cache, not just scattered hits. TTL is handled by Redis.
        """
        context = messages[:-1] if len(messages) > 1 else messages[:1]
        keys = self._make_chain_keys(model_id, context)
        redis = get_redis_pool()
        best_cluster: Optional[str] = None
        for key in keys:
            value = await redis.get(self._KEY_PREFIX + key)
            if value is None:
                break
            best_cluster = value
        return best_cluster

    async def set(self, model_id: str, messages: list[dict], cluster_id: str) -> None:
        """Store all prefix keys for the conversation. O(n) writes per request."""
        keys = self._make_chain_keys(model_id, messages)
        redis = get_redis_pool()
        async with redis.pipeline(transaction=False) as pipe:
            for key in keys:
                pipe.set(self._KEY_PREFIX + key, cluster_id, ex=self._ttl)
            await pipe.execute()


# ---------------------------------------------------------------------------
# Per-cluster runtime state
# ---------------------------------------------------------------------------


class ClusterEndpoint:
    """
    Connection info and live runtime stats for one cluster endpoint.

    All mutations happen on the asyncio event loop (single-threaded) — no
    locking is needed.
    """

    def __init__(
        self,
        cluster_id: str,
        base_url: str,
        api_key: str,
        seed_capacity_tokens: int,
        ttft_budget_ms: float,
        itl_budget_ms: float,
        ttft_alpha: float,
        itl_alpha: float,
        capacity_alpha: float,
    ) -> None:
        self.cluster_id = cluster_id
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key

        # Inflight token counter — incremented on reserve, decremented on release
        self.inflight_tokens: int = 0

        # Inferred token capacity — starts at seed, adapts via latency stress
        self.inferred_capacity: int = seed_capacity_tokens

        # EWMA latency (milliseconds)
        self.ewma_ttft_ms: float = 0.0
        self.ewma_itl_ms: float = 0.0
        self._has_samples: bool = False

        # Tuning knobs
        self._ttft_budget_ms = ttft_budget_ms
        self._itl_budget_ms = itl_budget_ms
        self._ttft_alpha = ttft_alpha
        self._itl_alpha = itl_alpha
        self._capacity_alpha = capacity_alpha

    # ------------------------------------------------------------------ #
    # Token reservation
    # ------------------------------------------------------------------ #

    def reserve(self, tokens: int) -> None:
        self.inflight_tokens += tokens

    def release(self, reservation: int) -> None:
        """
        Release a token reservation.

        Always uses the original reservation amount — not actual completion
        tokens — so the inflight counter stays consistent even on client
        disconnect (where we never receive the final usage chunk).
        """
        self.inflight_tokens = max(0, self.inflight_tokens - reservation)

    # ------------------------------------------------------------------ #
    # Latency + capacity adaptation
    # ------------------------------------------------------------------ #

    def update_latency(self, ttft_ms: float, mean_itl_ms: Optional[float]) -> None:
        """
        Update EWMA latencies and adapt inferred capacity based on stress.

        Called once per completed request (streaming or non-streaming).
        mean_itl_ms is None for non-streaming requests (no inter-token data).
        """
        if not self._has_samples:
            self.ewma_ttft_ms = ttft_ms
            if mean_itl_ms is not None:
                self.ewma_itl_ms = mean_itl_ms
            self._has_samples = True
        else:
            self.ewma_ttft_ms = (
                self._ttft_alpha * ttft_ms
                + (1 - self._ttft_alpha) * self.ewma_ttft_ms
            )
            if mean_itl_ms is not None:
                self.ewma_itl_ms = (
                    self._itl_alpha * mean_itl_ms
                    + (1 - self._itl_alpha) * self.ewma_itl_ms
                )

        self._adapt_capacity()

    def _adapt_capacity(self) -> None:
        """
        Stress-driven capacity adaptation.

        Stress is the fractional overshoot above budget (0 when healthy).
        Taking max(ttft_stress, itl_stress) means the cluster is only as
        healthy as its worst bottleneck — a weighted average would let a
        mild TTFT spike mask a severe ITL spike.

        Under stress: chase current inflight downward, faster when more stressed.
        Healthy: drift capacity upward slowly (conservative to avoid re-spiking).
        """
        ttft_stress = max(0.0, self.ewma_ttft_ms / self._ttft_budget_ms - 1.0)
        itl_stress = (
            max(0.0, self.ewma_itl_ms / self._itl_budget_ms - 1.0)
            if self.ewma_itl_ms > 0
            else 0.0
        )
        combined_stress = max(ttft_stress, itl_stress)

        if combined_stress > 0.0 and self.inflight_tokens > 0:
            # Stressed: pull inferred capacity toward current inflight
            # (faster alpha = faster reaction when deeply stressed)
            effective_alpha = min(
                self._capacity_alpha * (1.0 + combined_stress), 1.0
            )
            self.inferred_capacity = int(
                effective_alpha * self.inflight_tokens
                + (1.0 - effective_alpha) * self.inferred_capacity
            )
            logger.debug(
                "Cluster %s stressed (ttft=%.1f itl=%.1f) → capacity → %d",
                self.cluster_id,
                self.ewma_ttft_ms,
                self.ewma_itl_ms,
                self.inferred_capacity,
            )
        elif combined_stress == 0.0:
            # Healthy: drift capacity upward slowly
            self.inferred_capacity = int(
                self.inferred_capacity * (1.0 + self._capacity_alpha)
            )

    # ------------------------------------------------------------------ #
    # Routing helpers
    # ------------------------------------------------------------------ #

    def load_ratio(self) -> float:
        """inflight_tokens / inferred_capacity. inf if capacity is zero."""
        if self.inferred_capacity <= 0:
            return float("inf")
        return self.inflight_tokens / self.inferred_capacity

    def build_headers(self) -> dict:
        headers: dict = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"ClusterEndpoint(id={self.cluster_id!r}, "
            f"inflight={self.inflight_tokens}, "
            f"capacity={self.inferred_capacity}, "
            f"load={self.load_ratio():.2f}, "
            f"ttft={self.ewma_ttft_ms:.0f}ms, "
            f"itl={self.ewma_itl_ms:.0f}ms)"
        )


# ---------------------------------------------------------------------------
# Token estimation helpers
# ---------------------------------------------------------------------------


def _estimate_prompt_tokens(messages: list[dict]) -> int:
    """
    Rough token estimate for a messages list: 1 token ≈ 4 characters.

    Accuracy is ±20%, which is sufficient for load routing decisions.
    """
    return max(1, len(json.dumps(messages)) // 4)


def _itl_per_token(chunk_interval_ms: float, content: str) -> float:
    """
    Normalize a raw chunk interval to per-token ITL.

    Some backends emit large chunks infrequently rather than one token at a
    time. Using content length as a proxy token count prevents over-penalizing
    a cluster that batches its SSE output.
    """
    estimated_tokens = max(1, len(content) // 4)
    return chunk_interval_ms / estimated_tokens


def _extract_delta_content(chunk: bytes) -> str:
    """
    Parse one SSE chunk and return the content delta string (may be empty).

    Used only for ITL normalization — failures are silent.
    """
    try:
        text = chunk.decode("utf-8", errors="replace")
        for line in text.splitlines():
            line = line.strip()
            if line.startswith("data:"):
                payload = line[5:].strip()
                if payload and payload != "[DONE]":
                    data = json.loads(payload)
                    delta = data.get("choices", [{}])[0].get("delta", {})
                    return delta.get("content") or ""
    except Exception:
        pass
    return ""


# ---------------------------------------------------------------------------
# MultiClusterBackend
# ---------------------------------------------------------------------------


class MultiClusterBackend(Backend):
    """
    Phase 2 backend: routes one model_id across N cluster endpoints.

    The proxy layer (app/api/proxy.py) calls only forward() / forward_stream()
    and is unaffected by routing decisions made here.
    """

    def __init__(
        self,
        model_id: str,
        clusters: list[ClusterEndpoint],
        local_cluster_id: str,
        spill_ratio: float = 0.8,
        default_max_tokens: int = 2048,
        cache_ttl_seconds: int = 3600,
    ) -> None:
        self._model_id = model_id
        self._clusters: dict[str, ClusterEndpoint] = {
            c.cluster_id: c for c in clusters
        }
        self._local_cluster_id = local_cluster_id
        self._spill_ratio = spill_ratio
        self._default_max_tokens = default_max_tokens
        self._cache = ClusterCache(ttl_seconds=cache_ttl_seconds)

    @property
    def model_id(self) -> str:
        return self._model_id

    # ------------------------------------------------------------------ #
    # Cluster selection
    # ------------------------------------------------------------------ #

    def _local(self) -> Optional[ClusterEndpoint]:
        return self._clusters.get(self._local_cluster_id)

    def _least_loaded(self) -> Optional[ClusterEndpoint]:
        candidates = list(self._clusters.values())
        if not candidates:
            return None
        return min(candidates, key=lambda c: c.load_ratio())

    async def _select_cluster(self, messages: list[dict]) -> ClusterEndpoint:
        """
        Pick a cluster for this request.

        Priority:
          1. Cache hit on any cluster (incl. local) AND load < spill_ratio  (KV cache reuse)
          2. Local cluster AND load < spill_ratio                            (latency preference)
          3. Least-loaded across all clusters                                (load balance)
          4. Local regardless of load                                        (single-cluster fallback)
        """
        # 1. Cache hit — maximize KV cache reuse
        cached_id = await self._cache.get(self._model_id, messages)
        if cached_id is not None:
            cached = self._clusters.get(cached_id)
            if cached is not None and cached.load_ratio() < self._spill_ratio:
                logger.debug(
                    "Cache hit: routing model=%s to cluster=%s", self._model_id, cached_id
                )
                return cached

        # 2. Local under spill threshold
        local = self._local()
        if local is not None and local.load_ratio() < self._spill_ratio:
            return local

        # 3. Least loaded across all clusters
        least = self._least_loaded()
        if least is not None:
            return least

        # 4. Local fallback (single-cluster setup)
        if local is not None:
            return local

        raise RuntimeError(f"No cluster available for model {self._model_id!r}")

    # ------------------------------------------------------------------ #
    # Token reservation
    # ------------------------------------------------------------------ #

    def _compute_reservation(self, body: dict) -> int:
        """
        Pessimistic token reservation: prompt_tokens + max_tokens.

        Pessimistic is intentional — prevents over-committing a cluster to
        requests that might expand. The over-reservation is corrected in
        load_ratio via capacity adaptation, not by trying to refine mid-flight.
        """
        messages = body.get("messages", [])
        prompt_tokens = _estimate_prompt_tokens(messages)
        max_tokens = body.get("max_tokens") or self._default_max_tokens
        return prompt_tokens + int(max_tokens)

    # ------------------------------------------------------------------ #
    # Backend interface
    # ------------------------------------------------------------------ #

    async def forward(self, request: Request, body: dict) -> httpx.Response:
        """Non-streaming request. Uses total response time as TTFT proxy."""
        messages = body.get("messages", [])
        cluster = await self._select_cluster(messages)
        reservation = self._compute_reservation(body)
        cluster.reserve(reservation)

        t_send = time.monotonic()
        try:
            url = f"{cluster.base_url}/chat/completions"
            response = await get_http_client().post(
                url, json=body, headers=cluster.build_headers()
            )
            ttft_ms = (time.monotonic() - t_send) * 1000
            cluster.update_latency(ttft_ms, mean_itl_ms=None)
            await self._cache.set(self._model_id, messages, cluster.cluster_id)
            return response
        finally:
            cluster.release(reservation)

    async def forward_stream(
        self, request: Request, body: dict
    ) -> AsyncIterator[bytes]:
        """
        Streaming request.

        Measures TTFT and per-token ITL from the live byte stream.
        Token reservation is held for the full duration of the stream and
        released in the finally block — even on client disconnect.
        """
        messages = body.get("messages", [])
        cluster = await self._select_cluster(messages)
        reservation = self._compute_reservation(body)
        cluster.reserve(reservation)

        t_send = time.monotonic()
        first_chunk = True
        ttft_ms: float = 0.0
        prev_chunk_t: float = t_send
        itl_samples: list[float] = []

        try:
            url = f"{cluster.base_url}/chat/completions"
            client = get_http_client()
            async with client.stream(
                "POST", url, json=body, headers=cluster.build_headers()
            ) as response:
                response.raise_for_status()
                async for chunk in response.aiter_bytes():
                    if chunk:
                        now = time.monotonic()
                        if first_chunk:
                            ttft_ms = (now - t_send) * 1000
                            first_chunk = False
                        else:
                            raw_interval_ms = (now - prev_chunk_t) * 1000
                            content = _extract_delta_content(chunk)
                            if content:
                                itl_samples.append(
                                    _itl_per_token(raw_interval_ms, content)
                                )
                            else:
                                # Non-content chunk (metadata, usage) — skip ITL sample
                                pass
                        prev_chunk_t = now
                        yield chunk
        finally:
            mean_itl = (
                sum(itl_samples) / len(itl_samples) if itl_samples else None
            )
            cluster.update_latency(ttft_ms, mean_itl_ms=mean_itl)
            cluster.release(reservation)
            # Record cache even on abort — partial KV state may still exist
            await self._cache.set(self._model_id, messages, cluster.cluster_id)

    async def forward_embeddings(self, request: Request, body: dict) -> httpx.Response:
        """
        Embeddings don't have a messages list — route to local or least-loaded.
        Token reservation is based on input text length.
        """
        cluster = self._local() or self._least_loaded()
        if cluster is None:
            raise RuntimeError(f"No cluster available for model {self._model_id!r}")

        inp = body.get("input", "")
        reservation = max(1, len(json.dumps(inp)) // 4)
        cluster.reserve(reservation)
        try:
            url = f"{cluster.base_url}/embeddings"
            response = await get_http_client().post(
                url, json=body, headers=cluster.build_headers()
            )
            return response
        finally:
            cluster.release(reservation)

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"MultiClusterBackend(model_id={self._model_id!r}, "
            f"local={self._local_cluster_id!r}, "
            f"clusters={list(self._clusters.keys())!r})"
        )
