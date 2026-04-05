# Rate Limiting ([app/services/rate_limit.py](../../app/services/rate_limit.py))

Enforces per-user **concurrent request limits** using Redis atomic counters.

## How It Works

The limit is stored in `users.max_concurrent_requests`. On each request:

1. `assert_within_limit(user_id, max_concurrent)` runs a Lua script that atomically:
   - Gets the current counter value for `concurrent:{user_id}`.
   - If `current >= max_concurrent`, returns 0 (denied) — HTTP 429 raised.
   - Otherwise increments and sets a 300-second TTL, returns 1 (allowed).
2. The proxy runs `decrement(user_id)` in a `finally` block after the request completes (success or error).

## Redis Key

```
concurrent:{user_id}    # integer, TTL = 300s
```

The 300-second TTL is a safety net: if a worker process crashes mid-request without reaching the `finally` block, the counter self-heals within 5 minutes rather than permanently locking the user.

## Atomicity

The Lua script eliminates the TOCTOU race condition that would exist with a read-then-write approach. Under concurrent load, two simultaneous requests for a user at their limit will not both slip through.

## Decrement Clamp

`decrement_concurrent()` in [app/db/redis_client.py](../../app/db/redis_client.py) clamps the counter to 0 — it will not go negative even if called spuriously.
