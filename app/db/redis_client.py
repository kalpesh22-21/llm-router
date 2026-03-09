"""
Async Redis client and concurrent-request tracking helpers.

The concurrent request counter uses Redis INCR/DECR with a safety TTL
so that counts do not accumulate forever if a worker crashes mid-request.
"""

from __future__ import annotations

from typing import AsyncGenerator, Optional

import redis.asyncio as aioredis

from app.core.config import get_settings

settings = get_settings()

# TTL (seconds) applied every time the counter is touched.
# Acts as a safety net: if the router crashes without decrementing,
# the key expires within CONCURRENT_TTL_SECONDS seconds.
CONCURRENT_TTL_SECONDS: int = 300

_REDIS_POOL: Optional[aioredis.Redis] = None  # module-level singleton


def _make_key(user_id: int) -> str:
    return f"concurrent:{user_id}"


async def init_redis() -> None:
    """Create the module-level Redis connection pool (called at startup)."""
    global _REDIS_POOL
    _REDIS_POOL = aioredis.from_url(
        settings.REDIS_URL,
        encoding="utf-8",
        decode_responses=True,
    )


async def close_redis() -> None:
    """Close the Redis connection pool gracefully (called at shutdown)."""
    global _REDIS_POOL
    if _REDIS_POOL is not None:
        await _REDIS_POOL.aclose()
        _REDIS_POOL = None


def get_redis_pool() -> aioredis.Redis:
    """Return the module-level Redis pool, raising if not initialised."""
    if _REDIS_POOL is None:
        raise RuntimeError("Redis pool has not been initialised. Call init_redis() first.")
    return _REDIS_POOL


async def get_redis() -> AsyncGenerator[aioredis.Redis, None]:
    """FastAPI dependency that yields the shared Redis client."""
    yield get_redis_pool()


# ---------------------------------------------------------------------------
# Concurrent-request helpers
# ---------------------------------------------------------------------------


async def increment_concurrent(redis: aioredis.Redis, user_id: int) -> int:
    """
    Atomically increment the in-flight counter for *user_id*.

    Refreshes the safety TTL on every call so that a crash does not
    leave a stale counter indefinitely.

    Returns the counter value *after* incrementing.
    """
    key = _make_key(user_id)
    pipe = redis.pipeline()
    pipe.incr(key)
    pipe.expire(key, CONCURRENT_TTL_SECONDS)
    results = await pipe.execute()
    return int(results[0])


async def decrement_concurrent(redis: aioredis.Redis, user_id: int) -> int:
    """
    Atomically decrement the in-flight counter for *user_id*.

    The counter will not drop below 0 — if DECR would produce a negative
    value the key is deleted instead.

    Returns the counter value *after* decrementing (minimum 0).
    """
    key = _make_key(user_id)
    new_val = await redis.decr(key)
    if new_val < 0:
        await redis.delete(key)
        return 0
    # Refresh TTL
    await redis.expire(key, CONCURRENT_TTL_SECONDS)
    return int(new_val)


async def get_concurrent(redis: aioredis.Redis, user_id: int) -> int:
    """Return the current in-flight request count for *user_id* (0 if absent)."""
    key = _make_key(user_id)
    val = await redis.get(key)
    return int(val) if val is not None else 0
