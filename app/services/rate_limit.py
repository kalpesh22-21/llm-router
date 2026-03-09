"""
Rate limiting service — concurrent request tracking via Redis.

Uses a Lua script executed atomically on Redis to check-and-increment the
counter in a single round-trip, eliminating TOCTOU races.
"""

from __future__ import annotations

import logging

import redis.asyncio as aioredis
from fastapi import HTTPException, status

from app.db.redis_client import (
    CONCURRENT_TTL_SECONDS,
    decrement_concurrent,
    get_concurrent,
    increment_concurrent,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lua script for atomic check-and-increment
# ---------------------------------------------------------------------------
# Returns 1 (allowed) or 0 (limit exceeded).
# KEYS[1] = "concurrent:{user_id}"
# ARGV[1] = max_concurrent limit (integer)
# ARGV[2] = TTL in seconds
_CHECK_AND_INCREMENT_LUA = """
local key   = KEYS[1]
local limit = tonumber(ARGV[1])
local ttl   = tonumber(ARGV[2])

local current = tonumber(redis.call('GET', key) or '0')
if current >= limit then
    return 0
end

redis.call('INCR', key)
redis.call('EXPIRE', key, ttl)
return 1
"""


class RateLimitService:
    """
    Manages per-user concurrent request limits backed by Redis.

    The check-and-increment operation is executed atomically via a
    Lua script, so there is no race condition between reading the
    current count and writing the new value.
    """

    async def check_and_increment(
        self,
        redis: aioredis.Redis,
        user_id: int,
        max_concurrent: int,
    ) -> bool:
        """
        Atomically check if *user_id* is under their concurrent limit and,
        if so, increment the counter.

        Args:
            redis:          The async Redis client.
            user_id:        The user to check.
            max_concurrent: The limit fetched from the User DB record.

        Returns:
            True if the request is allowed (counter was incremented).
            False if the user is already at or above the limit.
        """
        key = f"concurrent:{user_id}"
        result = await redis.eval(  # type: ignore[misc]
            _CHECK_AND_INCREMENT_LUA,
            1,  # number of keys
            key,
            max_concurrent,
            CONCURRENT_TTL_SECONDS,
        )
        allowed = bool(result)
        if not allowed:
            logger.warning(
                "Rate limit exceeded for user_id=%d (limit=%d)", user_id, max_concurrent
            )
        return allowed

    async def decrement(self, redis: aioredis.Redis, user_id: int) -> None:
        """
        Decrement the in-flight counter for *user_id*.

        Should be called in a finally block to ensure cleanup on both
        successful and failed requests.
        """
        await decrement_concurrent(redis, user_id)

    async def get_current_count(self, redis: aioredis.Redis, user_id: int) -> int:
        """Return the current in-flight request count for *user_id*."""
        return await get_concurrent(redis, user_id)

    async def assert_within_limit(
        self,
        redis: aioredis.Redis,
        user_id: int,
        max_concurrent: int,
    ) -> None:
        """
        Convenience wrapper: check_and_increment, raise 429 if denied.

        Raises:
            HTTPException(429): if the user is at or above their limit.
        """
        allowed = await self.check_and_increment(redis, user_id, max_concurrent)
        if not allowed:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=(
                    f"Concurrent request limit of {max_concurrent} reached. "
                    "Please wait for an in-flight request to complete."
                ),
            )


# Module-level singleton
rate_limit_service = RateLimitService()
