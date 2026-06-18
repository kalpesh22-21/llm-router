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

import asyncio
import hashlib
import json
import logging
import time
from datetime import datetime
from typing import AsyncIterator, Optional

import httpx
from fastapi import HTTPException, Request

from app.backends.base import Backend
from app.backends.static import get_http_client
from app.db.cluster_operational_state import (
    cluster_metrics_store,
    get_auto_adapt_capacity,
    get_cluster_enabled,
)
from app.db.cluster_drain import (
    get_drain_state,
    DrainState,
)
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

    Runtime metrics (inflight_tokens, ewma latencies, adaptive_capacity) are
    stored in Redis for distributed access across multiple deployments.

    The updated_at field tracks when the cluster configuration was last applied
    from Redis or updated via API calls. This timestamp is critical for the
    two-step staleness check in ConfigSyncService._apply_config_update():

    1. First check: Compare Redis config timestamp against endpoint.updated_at
    2. Second check: Only if Redis is older than backend, compare against DB

    This ordering prevents premature purging of Redis configs when the backend
    has been updated via API calls after YAML was loaded into the database.

    Attributes:
        cluster_id: Unique identifier for this cluster endpoint
        base_url: Base URL for the cluster's API endpoint
        api_key: Authentication key for the cluster
        model_id: Model identifier this cluster serves
        number_of_replicas: Number of pod replicas for capacity calculation
        seed_capacity_tokens_per_pod: Base capacity per replica (default 40000)
        is_enabled: Whether this cluster accepts traffic
        auto_adapt_capacity: Whether to dynamically adjust capacity based on load
        updated_at: Timestamp of last config update (UTC), used for staleness checks
    """

    def __init__(
        self,
        cluster_id: str,
        base_url: str,
        api_key: str,
        model_id: str = "",
        number_of_replicas: int = 1,
        seed_capacity_tokens_per_pod: int = 40000,
        is_enabled: bool = True,
        auto_adapt_capacity: bool = True,
        ttft_budget_ms: float = 500.0,
        itl_budget_ms: float = 50.0,
        ttft_alpha: float = 0.3,
        itl_alpha: float = 0.3,
        capacity_alpha: float = 0.1,
        updated_at: Optional[datetime] = None,
    ) -> None:
        self.cluster_id = cluster_id
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model_id = model_id

        # Replica configuration for dynamic capacity calculation
        self.number_of_replicas = number_of_replicas
        self.seed_capacity_tokens_per_pod = seed_capacity_tokens_per_pod

        # Feature flags
        # Note: is_enabled and auto_adapt_capacity are stored in Redis for
        # distributed consistency. These local values are fallbacks if Redis
        # is unavailable.
        self.update_lock = asyncio.Lock()
        self.is_enabled = is_enabled
        self.auto_adapt_capacity = auto_adapt_capacity
        self._auto_adapt_capacity_default = auto_adapt_capacity

        # Tuning knobs
        self._ttft_budget_ms = ttft_budget_ms
        self._itl_budget_ms = itl_budget_ms
        self._ttft_alpha = ttft_alpha
        self._itl_alpha = itl_alpha
        self._capacity_alpha = capacity_alpha

        # Track when config was last applied from Redis or updated via API.
        # This timestamp is used by ConfigSyncService._apply_config_update() to
        # determine if a Redis config is stale compared to the running backend.
        # When updated_at is set (via API calls or initial load), configs older
        # than this timestamp are rejected to prevent stale data from overriding
        # fresh runtime updates. See ConfigSyncService for the two-step check.
        self.updated_at = updated_at

    # ------------------------------------------------------------------ #
    # Dynamic capacity property
    # ------------------------------------------------------------------ #

    @property
    def inferred_capacity(self) -> int:
        """
        Computed capacity based on replica count and per-pod capacity.

        Returns: number_of_replicas × seed_capacity_tokens_per_pod
        """
        return self.number_of_replicas * self.seed_capacity_tokens_per_pod

    # ------------------------------------------------------------------ #
    # Token reservation (Redis-backed)
    # ------------------------------------------------------------------ #

    async def release(self, reservation: int) -> int:
        """
        Release a token reservation.

        Always uses the original reservation amount — not actual completion
        tokens — so the inflight counter stays consistent even on client
        disconnect (where we never receive the final usage chunk).

        Args:
            reservation: Number of tokens to release

        Returns:
            int: New inflight token count after release
        """
        return await cluster_metrics_store.release_tokens(
            self.model_id, self.cluster_id, reservation
        )

    async def get_inflight_tokens(self) -> int:
        """Get current inflight token count from Redis."""
        return await cluster_metrics_store.get_inflight_tokens(
            self.model_id, self.cluster_id
        )

    # ------------------------------------------------------------------ #
    # Latency + capacity adaptation (Redis-backed)
    # ------------------------------------------------------------------ #

    async def update_latency(
        self, ttft_ms: float, mean_itl_ms: Optional[float]
    ) -> None:
        """
        Update EWMA latencies and adapt inferred capacity based on stress.

        Called once per completed request (streaming or non-streaming).
        mean_itl_ms is None for non-streaming requests (no inter-token data).
        """
        # Update EWMA latencies in Redis
        new_ttft, new_itl = await cluster_metrics_store.update_ewma_latency(
            self.model_id,
            self.cluster_id,
            ttft_ms,
            mean_itl_ms,
            alpha_ttft=self._ttft_alpha,
            alpha_itl=self._itl_alpha,
        )

        # Adapt capacity based on stress
        await self._adapt_capacity()

    async def auto_adapt_capacity_enabled(self) -> bool:
        """
        Check if auto_adapt_capacity is enabled, using distributed state from Redis.

        First checks Redis for the cluster state. If Redis is unavailable
        or has no state, falls back to the local default value.

        Returns:
            True if auto_adapt_capacity is enabled, False otherwise.
        """
        distributed_state = await get_auto_adapt_capacity(
            self.model_id, self.cluster_id
        )
        if distributed_state is not None:
            return distributed_state
        # Fallback to local default if Redis is unavailable
        return self._auto_adapt_capacity_default

    async def _adapt_capacity(self) -> None:
        """
        Stress-driven capacity adaptation.

        Stress is the fractional overshoot above budget (0 when healthy).
        Taking max(ttft_stress, itl_stress) means the cluster is only as
        healthy as its worst bottleneck — a weighted average would let a
        mild TTFT spike mask a severe ITL spike.

        Under stress: chase current inflight downward, faster when more stressed.
        Healthy: drift capacity upward slowly (conservative to avoid re-spiking).

        Adaptation is skipped if auto_adapt_capacity is disabled.
        """
        if not await self.auto_adapt_capacity_enabled():
            # Use static inferred capacity without adaptation
            await cluster_metrics_store.set_adaptive_capacity(
                self.model_id, self.cluster_id, self.inferred_capacity
            )
            return

        # Get current metrics from Redis
        ttft, itl, _ = await cluster_metrics_store.get_ewma_latency(
            self.model_id, self.cluster_id
        )
        inflight = await self.get_inflight_tokens()
        current_capacity = await cluster_metrics_store.get_adaptive_capacity(
            self.model_id, self.cluster_id, self.inferred_capacity
        )

        ttft_stress = max(0.0, ttft / self._ttft_budget_ms - 1.0)
        itl_stress = max(0.0, itl / self._itl_budget_ms - 1.0) if itl > 0 else 0.0
        combined_stress = max(ttft_stress, itl_stress)

        if combined_stress > 0.0 and inflight > 0:
            # Stressed: pull adaptive capacity toward current inflight
            # (faster alpha = faster reaction when deeply stressed)
            effective_alpha = min(self._capacity_alpha * (1.0 + combined_stress), 1.0)
            new_capacity = int(
                effective_alpha * inflight + (1.0 - effective_alpha) * current_capacity
            )
            await cluster_metrics_store.set_adaptive_capacity(
                self.model_id, self.cluster_id, new_capacity
            )
            logger.debug(
                "Cluster %s stressed (ttft=%.1f itl=%.1f) → capacity → %d",
                self.cluster_id,
                ttft,
                itl,
                new_capacity,
            )
        elif combined_stress == 0.0:
            # Healthy: drift capacity upward slowly
            new_capacity = int(current_capacity * (1.0 + self._capacity_alpha))
            await cluster_metrics_store.set_adaptive_capacity(
                self.model_id, self.cluster_id, new_capacity
            )

    async def get_adaptive_capacity(self) -> int:
        """Get current adaptive capacity from Redis."""
        return await cluster_metrics_store.get_adaptive_capacity(
            self.model_id, self.cluster_id, self.inferred_capacity
        )

    async def get_ewma_latencies(self) -> tuple[float, float]:
        """Get current EWMA latencies from Redis."""
        ttft, itl, _ = await cluster_metrics_store.get_ewma_latency(
            self.model_id, self.cluster_id
        )
        return ttft, itl

    # ------------------------------------------------------------------ #
    # Distributed state helpers
    # ------------------------------------------------------------------ #

    async def is_enabled_distributed(self) -> bool:
        """
        Check if this cluster is enabled, using distributed state from Redis.

        First checks Redis for the cluster state. If Redis is unavailable
        or has no state, falls back to the local is_enabled value.

        Returns:
            True if the cluster is enabled for routing, False otherwise.
        """
        distributed_state = await get_cluster_enabled(self.model_id, self.cluster_id)
        if distributed_state is not None:
            return distributed_state
        # Fallback to local state if Redis is unavailable
        return self.is_enabled

    # ------------------------------------------------------------------ #
    # Routing helpers
    # ------------------------------------------------------------------ #

    async def load_ratio(self) -> float:
        """inflight_tokens / adaptive_capacity. inf if capacity is zero."""
        adaptive_capacity = await self.get_adaptive_capacity()
        if adaptive_capacity <= 0:
            return float("inf")
        inflight = await self.get_inflight_tokens()
        return inflight / adaptive_capacity

    def build_headers(self) -> dict:
        headers: dict = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def __repr__(self) -> str:  # pragma: no cover
        updated_at_str = self.updated_at.isoformat() if self.updated_at else "None"
        return (
            f"ClusterEndpoint(id={self.cluster_id!r}, "
            f"replicas={self.number_of_replicas}, "
            f"inferred_cap={self.inferred_capacity}, "
            f"enabled={self.is_enabled}, "
            f"updated_at={updated_at_str})"
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
        # Store all clusters, routing logic will filter by is_enabled
        self._clusters: dict[str, ClusterEndpoint] = {c.cluster_id: c for c in clusters}
        self._local_cluster_id = local_cluster_id
        self._spill_ratio = spill_ratio
        self._default_max_tokens = default_max_tokens
        self._cache = ClusterCache(ttl_seconds=cache_ttl_seconds)

    @property
    def model_id(self) -> str:
        return self._model_id

    # ------------------------------------------------------------------ #
    # Dynamic cluster management
    # ------------------------------------------------------------------ #

    def enable_cluster(self, cluster: ClusterEndpoint) -> bool:
        """
        Enable a cluster for routing.

        Called when a cluster is enabled at runtime.
        Adds the cluster to the internal registry and sets is_enabled=True.
        Returns True if the cluster was newly added, False if it already existed.
        """
        cluster_id = cluster.cluster_id
        existing = self._clusters.get(cluster_id)

        if existing is not None:
            # Cluster exists, just enable it
            existing.is_enabled = True
            # Update other properties from the new cluster object
            # Note: auto_adapt_capacity is stored in Redis, not updated locally
            existing.base_url = cluster.base_url
            existing.api_key = cluster.api_key
            existing.number_of_replicas = cluster.number_of_replicas
            existing.seed_capacity_tokens_per_pod = cluster.seed_capacity_tokens_per_pod
            logger.info(
                "Enabled existing cluster '%s' for model '%s'",
                cluster_id,
                self._model_id,
            )
            return False
        else:
            # New cluster, add it
            cluster.is_enabled = True
            self._clusters[cluster_id] = cluster
            logger.info(
                "Added and enabled new cluster '%s' for model '%s'",
                cluster_id,
                self._model_id,
            )
            return True

    def disable_cluster(self, cluster_id: str) -> bool:
        """
        Disable a cluster from routing.

        Called when a cluster is disabled at runtime.
        Sets is_enabled=False to allow in-flight requests to complete
        while preventing new requests from being scheduled.
        Returns True if the cluster was disabled, False if not found.
        """
        cluster = self._clusters.get(cluster_id)
        if cluster is not None:
            cluster.is_enabled = False
            logger.info(
                "Disabled cluster '%s' for model '%s' "
                "(in-flight requests will complete)",
                cluster_id,
                self._model_id,
            )
            return True
        return False

    def get_cluster(self, cluster_id: str) -> Optional[ClusterEndpoint]:
        """Get a cluster endpoint by ID if it exists."""
        return self._clusters.get(cluster_id)

    def list_cluster_ids(self) -> list[str]:
        """List all cluster IDs (both enabled and disabled)."""
        return list(self._clusters.keys())

    def list_enabled_cluster_ids(self) -> list[str]:
        """List only enabled cluster IDs."""
        return [cid for cid, c in self._clusters.items() if c.is_enabled]

    # ------------------------------------------------------------------ #
    # Cluster selection
    # ------------------------------------------------------------------ #

    def _local(self) -> Optional[ClusterEndpoint]:
        return self._clusters.get(self._local_cluster_id)

    async def _least_loaded(self) -> Optional[ClusterEndpoint]:
        candidates = list(self._clusters.values())
        if not candidates:
            return None
        # Load ratio is now async, so we need to compute them all
        load_ratios = []
        for c in candidates:
            ratio = await c.load_ratio()
            load_ratios.append((ratio, c))
        return min(load_ratios, key=lambda x: x[0])[1]

    async def _select_cluster(self, messages: list[dict]) -> ClusterEndpoint:
        """
        Pick a cluster for this request.

        Priority:
          1. Cache hit on enabled cluster AND load < spill_ratio   (KV cache reuse)
          2. Local enabled cluster AND load < spill_ratio          (latency preference)
          3. Least-loaded among enabled clusters                   (load balance)
          4. Local enabled cluster (last resort)                   (single-cluster fallback)

        Disabled clusters (is_enabled=False) are excluded from routing.
        Uses distributed state from Redis for is_enabled checks.
        """

        # Helper to check if cluster is routable (enabled, not draining/disabled, and under spill threshold)
        async def is_routable(cluster: ClusterEndpoint) -> bool:
            # Use distributed state from Redis for is_enabled check
            if not await cluster.is_enabled_distributed():
                return False

            # Check drain state - reject if draining or disabled
            drain_state = await get_drain_state(self._model_id, cluster.cluster_id)
            if drain_state in (DrainState.DRAINING, DrainState.DISABLED):
                return False

            load = await cluster.load_ratio()
            return load < self._spill_ratio

        # 1. Cache hit — maximize KV cache reuse (must be enabled + under spill)
        cached_id = await self._cache.get(self._model_id, messages)
        if cached_id is not None:
            cached = self._clusters.get(cached_id)
            if cached is not None and await is_routable(cached):
                logger.debug(
                    "Cache hit: routing model=%s to cluster=%s",
                    self._model_id,
                    cached_id,
                )
                return cached

        # 2. Local under spill threshold (must be enabled)
        local = self._local()
        if local is not None and await is_routable(local):
            return local

        # 3. Least loaded across enabled clusters (check distributed state)
        enabled_clusters = []
        for c in self._clusters.values():
            if await c.is_enabled_distributed():
                enabled_clusters.append(c)

        if enabled_clusters:
            # Compute load ratios asynchronously
            cluster_loads = []
            for c in enabled_clusters:
                load = await c.load_ratio()
                cluster_loads.append((load, c))
            least = min(cluster_loads, key=lambda x: x[0])[1]
            return least

        # 4. Local fallback (single-cluster setup) - last resort, even if over spill
        if local is not None and await local.is_enabled_distributed():
            logger.warning(
                "All clusters over spill ratio for model %s, using local as fallback",
                self._model_id,
            )
            return local

        raise HTTPException(
            status_code=503,
            detail=f"No enabled clusters available for model {self._model_id!r}",
        )

    # ------------------------------------------------------------------ #
    # Token reservation
    # ------------------------------------------------------------------ #

    async def _reserve_tokens_atomic(
        self, cluster: ClusterEndpoint, reservation: int
    ) -> bool:
        """
        Atomically check cluster state and reserve tokens.

        Uses the distributed cluster state to ensure the cluster is still
        enabled at the moment of reservation, preventing race conditions
        where the cluster is disabled between routing and reservation.

        Args:
            cluster: The cluster endpoint to reserve tokens on
            reservation: Number of tokens to reserve

        Returns:
            True if reservation succeeded, False otherwise

        Raises:
            HTTPException: If reservation fails due to disabled cluster or
                          capacity exceeded
        """
        max_capacity = await cluster.get_adaptive_capacity()
        result = await cluster_metrics_store.check_and_reserve_tokens(
            model_id=self._model_id,
            cluster_id=cluster.cluster_id,
            tokens=reservation,
            max_capacity=max_capacity,
        )

        if result.get("success"):
            return True

        error = result.get("error")
        if error == "cluster_disabled":
            raise HTTPException(
                status_code=503,
                detail=f"Cluster '{cluster.cluster_id}' was disabled during request routing. "
                f"Please retry.",
            )
        elif error == "capacity_exceeded":
            inflight = result.get("inflight_tokens", 0)
            raise HTTPException(
                status_code=503,
                detail=f"Cluster '{cluster.cluster_id}' is at capacity. "
                f"Inflight: {inflight}, Capacity: {max_capacity}, Requested: {reservation}. "
                f"Please retry.",
            )
        else:
            raise HTTPException(
                status_code=503,
                detail=f"Failed to reserve tokens on cluster '{cluster.cluster_id}'. "
                f"Please retry.",
            )

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

    async def forward(self, request: Request, body: dict,upstream_url: str = "/chat/completions") -> httpx.Response:
        """Non-streaming request. Uses total response time as TTFT proxy."""
        messages = body.get("messages", [])
        cluster = await self._select_cluster(messages)
        reservation = self._compute_reservation(body)

        # Atomic check-and-reserve to prevent race conditions with cluster disable
        await self._reserve_tokens_atomic(cluster, reservation)

        t_send = time.monotonic()
        try:
            url = url = f"{cluster.base_url}{upstream_url}"
            response = await get_http_client().post(
                url, json=body, headers=cluster.build_headers()
            )
            ttft_ms = (time.monotonic() - t_send) * 1000
            await cluster.update_latency(ttft_ms, mean_itl_ms=None)
            await self._cache.set(self._model_id, messages, cluster.cluster_id)
            return response
        finally:
            await cluster.release(reservation)

    async def forward_stream(
        self, request: Request, body: dict, upstream_url: str = "/chat/completions"
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

        # Atomic check-and-reserve to prevent race conditions with cluster disable
        await self._reserve_tokens_atomic(cluster, reservation)

        t_send = time.monotonic()
        first_chunk = True
        ttft_ms: Optional[float] = None
        prev_chunk_t: float = t_send
        itl_samples: list[float] = []

        try:
            url = f"{cluster.base_url}{upstream_url}"
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
            if ttft_ms:
                mean_itl = sum(itl_samples) / len(itl_samples) if itl_samples else None
                await cluster.update_latency(ttft_ms, mean_itl_ms=mean_itl)
            await cluster.release(reservation)
            # Record cache even on abort — partial KV state may still exist
            await self._cache.set(self._model_id, messages, cluster.cluster_id)

    async def forward_embeddings(self, request: Request, body: dict) -> httpx.Response:
        """
        Embeddings don't have a messages list — route to local or least-loaded.
        Token reservation is based on input text length.
        Only enabled clusters are considered.
        Uses distributed state from Redis for is_enabled checks.
        """
        # Filter to enabled clusters only - disabled clusters must never receive traffic
        # Use distributed state from Redis
        enabled_clusters = []
        for c in self._clusters.values():
            if await c.is_enabled_distributed():
                # Also check drain state - exclude DRAINING and DISABLED
                drain_state = await get_drain_state(self._model_id, c.cluster_id)
                if drain_state not in (DrainState.DRAINING, DrainState.DISABLED):
                    enabled_clusters.append(c)

        cluster = self._local()
        if cluster is None or not await cluster.is_enabled_distributed():
            # Local is disabled or unavailable - find least loaded among enabled
            if enabled_clusters:
                # Compute load ratios asynchronously
                cluster_loads = []
                for c in enabled_clusters:
                    load = await c.load_ratio()
                    cluster_loads.append((load, c))
                cluster = min(cluster_loads, key=lambda x: x[0])[1]
                logger.debug(
                    "Local cluster disabled/unavailable, routing to %s for model=%s",
                    cluster.cluster_id,
                    self._model_id,
                )
            else:
                # No enabled clusters available - ensure we don't use disabled local
                cluster = None
        # else: local cluster is enabled, use it (already assigned)

        # Safety check: cluster must be enabled before proceeding
        if cluster is None or not await cluster.is_enabled_distributed():
            raise HTTPException(
                status_code=503,
                detail=f"No enabled clusters available for model {self._model_id!r}",
            )

        inp = body.get("input", "")
        reservation = max(1, len(json.dumps(inp)) // 4)

        # Atomic check-and-reserve to prevent race conditions with cluster disable
        await self._reserve_tokens_atomic(cluster, reservation)

        try:
            url = f"{cluster.base_url}/embeddings"
            response = await get_http_client().post(
                url, json=body, headers=cluster.build_headers()
            )
            return response
        finally:
            await cluster.release(reservation)

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"MultiClusterBackend(model_id={self._model_id!r}, "
            f"local={self._local_cluster_id!r}, "
            f"clusters={list(self._clusters.keys())!r})"
        )
