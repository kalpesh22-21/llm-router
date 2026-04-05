# API: Proxy (`/v1`)

OpenAI-compatible endpoints. All require a `Authorization: Bearer <jwt>` header.

## Endpoints

### `POST /v1/chat/completions`

Forwards to the backend's `/chat/completions`. Supports both streaming and non-streaming.

**Streaming** (`"stream": true`): returns `text/event-stream` SSE. The router injects `stream_options: {include_usage: true}` so token counts are reported in the final chunk.

**Non-streaming**: returns the backend's JSON response as-is.

Metrics recorded: request duration, input/output tokens, TTFT (streaming only), inter-token latency (streaming only).

### `POST /v1/embeddings`

Forwards to the backend's `/embeddings`. Always non-streaming.

### `GET /v1/models`

Returns an OpenAI-compatible model list containing only the models accessible to the authenticated user (based on their bucket grants).

```json
{
  "object": "list",
  "data": [
    { "id": "gpt-4o", "object": "model", "owned_by": "router" }
  ]
}
```

## Per-Request Guards (applied to all proxy endpoints)

1. JWT decoded → `user_id`, `username`
2. `PermissionService.is_user_active()` — checks DB; probabilistically (~10% of requests) re-validates against LDAP; auto-deactivates users removed from LDAP
3. `PermissionService.get_user_accessible_models()` — model must appear in at least one of the user's buckets
4. `RateLimitService.assert_within_limit()` — atomically checks and increments the Redis concurrent counter against `users.max_concurrent_requests`
5. Concurrent counter decremented in a `finally` block regardless of outcome
