# LLM Router — Security & Correctness Audit Follow-Up

**Audit date:** 2026-06-17 · **Scope of fixes so far:** static / Phase-1 path only.

A full-codebase review (8 subsystems, ~11.7k LOC) surfaced ~55 findings. Fixes
below were applied **one-by-one with explicit approval**. Multi-cluster / Phase-2
findings were **deliberately deferred** (see Group B). Nothing has been committed
yet — all applied changes are in the working tree.

Verify edits with `python3 -m py_compile <file>` (there is no `python` on PATH).

---

## ✅ Done (static / Phase-1)

- [x] **JWT fail-closed** — refuse to boot if `VAULT_ENABLED` but `JWT_SECRET_KEY` is the placeholder/empty — [app/core/config.py](app/core/config.py)
- [x] **Superuser admin env-only** — removed every-login username re-promotion + `count==0` bootstrap; admin now only via env path — [app/api/auth_routes.py](app/api/auth_routes.py)
- [x] **Content-filter updater** — fixed session use-after-close + added `try/except` (was silently disabling all filtering) — [app/services/content_filter.py](app/services/content_filter.py)
- [x] **Op-state sync lock** — `with` → `async with` on `asyncio.Lock` — [app/services/cross_pod_sync_service.py](app/services/cross_pod_sync_service.py)
- [x] **Token-reservation fallback** — now fail-closed (was admitting past capacity/enabled checks) — [app/db/cluster_operational_state.py](app/db/cluster_operational_state.py)
- [x] **Graceful-shutdown signal handlers** — moved to lifespan so they arm against a running loop — [app/main.py](app/main.py)
- [x] **Tracing** — guard `set_attribute(SESSION_ID, None)` — [app/logging/pheonix.py](app/logging/pheonix.py)
- [x] **permission.py docstrings** — match the partial-subset behavior
- [x] **LDAP batch** — search-filter paren + `escape_filter_chars`, empty/control-char password guard, `get_ldap_user_info` returns `{}`, `allowed_individuals` var fix — [app/auth/ldap_auth.py](app/auth/ldap_auth.py)
- [x] **admin.py crashes** — fixed `check_content` arg order; removed broken `get_ldap_user_info(username)` call — [app/api/admin.py](app/api/admin.py)
- [x] **`decrement_concurrent` atomic** — single Lua script (matches increment) — [app/db/redis_client.py](app/db/redis_client.py)
- [x] **Content-filter scope** — also scans `system` + `tool_calls` args; no `KeyError` on multimodal parts — [app/services/content_filter.py](app/services/content_filter.py)
- [x] **Streaming `[DONE]`** — forward one well-formed `b"data: [DONE]\n\n"`, skip keepalives — [app/api/proxy.py](app/api/proxy.py)
- [x] **Regex ReDoS cap** — `MAX_REGEX_SCAN_CHARS = 100_000` (partial mitigation) — [app/services/content_filter.py](app/services/content_filter.py)
- [x] **Deactivation rate** — kept 1%, aligned docs — [app/api/proxy.py](app/api/proxy.py), [app/auth/jwt_handler.py](app/auth/jwt_handler.py)
- [x] **Tier 1 revocation** — admin deactivate/reactivate writes Redis flag → caught next request — [app/api/admin.py](app/api/admin.py)

---

## ⏳ Pick up here

### 1. Tier 2 — token-version revocation  *(recommended next)*

**Goal:** instantly invalidate **all** of a user's outstanding JWTs on any status/
permission change (model access, `max_concurrent`, admin demotion, deactivation) —
not just deactivation. Today a permission change doesn't take effect until the JWT
expires (≤60 min), because the claims are baked into the token.

**Core idea:** give every user a `token_version` integer. Bake it into the JWT as a
`tv` claim at issuance. On every request, compare the token's `tv` to the user's
*current* version (read cheaply from Redis). **Bump the version → every existing
token for that user fails the comparison** and is rejected, forcing a re-login that
mints a fresh token with current claims. It's a generic "revoke all tokens for user
X" primitive.

**Moving parts:**

| Piece | Where | What |
|---|---|---|
| `token_version` column | [app/db/mysql.py](app/db/mysql.py) `User` | Source of truth (`default=0`); needs a migration |
| `tv` claim | [app/auth/jwt_handler.py](app/auth/jwt_handler.py) | `create_access_token(..., token_version)` adds `"tv"` |
| Issue with current `tv` | [app/api/auth_routes.py](app/api/auth_routes.py) login **and** `/refresh-token` | read `user.token_version`, pass it in, seed Redis |
| Redis cache | [app/db/redis_client.py](app/db/redis_client.py) | `user_tv:{id} → version`; `get/set_token_version()` |
| `bump_token_version()` | [app/services/permission.py](app/services/permission.py) | atomic DB `UPDATE ... SET token_version = token_version + 1 RETURNING ...` + write-through to Redis |
| Per-request check | [app/api/proxy.py](app/api/proxy.py) `_resolve_user` | compare token `tv` vs current; 401 on mismatch |

**Per-request flow (`_resolve_user`):**

```python
token_tv   = payload.get("tv")
current_tv = await get_token_version(redis, user_id)          # 1 Redis GET (cheap)
if current_tv is None:                                        # cache miss
    current_tv = await permission_service.get_token_version(db, user_id)
    await set_token_version(redis, user_id, current_tv)       # repopulate
if current_tv is not None and token_tv != current_tv:
    raise HTTPException(401, "Token revoked — please re-authenticate")
```

One Redis GET — same cost profile as the rate-limit call already made every request
(could be pipelined with it). No DB hit in the common case; token stays stateless;
lifetime unchanged.

**When to bump — the rule:**

> Bump on any change that **reduces** a user's access or status. Additive changes
> (granting a bucket, raising a limit) don't need a bump — they propagate on next
> login/expiry and carry no security urgency.

Bump events: deactivate, revoke bucket access, demote admin, lower `max_concurrent`,
delete user, (optionally) password change.

**Subtlety — bucket fan-out:** `accessible_models` is derived from *bucket* grants.
A per-user `tv` covers user-level changes cleanly, but removing a model from a
**bucket** affects *every* user with that bucket. So a bucket/model **removal** must
bump `tv` for **all affected users** (query holders, bump each); bucket **adds** can
skip it. Decide whether bucket-level reductions are frequent enough to warrant a
separate bucket-epoch instead of fan-out — fan-out-on-reduction is simplest/correct.

**Decisions to make before implementing:**
- [ ] **Redis-down behavior** — recommend **fail-open with DB fallback** (miss → DB read; full outage → skip check + warn, so revocation degrades to "by token expiry", today's worst case). Alternative fail-closed makes a Redis blip a total outage.
- [ ] **Rollout / grandfathering** — tokens already issued have no `tv`. Treat missing `tv` as **valid** during a transition (until they expire, ≤60 min) then enforce, or force everyone to re-login on deploy? Grandfather is smoother.
- [ ] **Fold in or keep Tier 1** — Tier 2 subsumes deactivation (deactivate = bump `tv` **and** keep `is_active=False` so login refuses re-auth). Decide whether to then retire the 1% random DB poll + deactivated-set, or keep them as belt-and-suspenders.

### 2. Group A — remaining (policy / infra decisions)
- [ ] **`/metrics` unauthenticated + `username` labels** ([app/main.py](app/main.py)) — PII / user enumeration. Decide: internal-only port, basic-auth, or drop `username` label. *(May already be covered by k8s NetworkPolicy — confirm.)*
- [ ] **Fail-open posture** — rate limit ([app/services/rate_limit.py](app/services/rate_limit.py)) and content filter degrade *open* on Redis/DB error. Decide fail-open vs fail-closed per control.
- [ ] **SSRF via admin `external_url`** ([app/api/admin.py](app/api/admin.py), [app/services/content_filter.py](app/services/content_filter.py)) — EXTERNAL_URL rules fetch admin-supplied URLs server-side; add private-IP/link-local block. Only relevant if EXTERNAL_URL filters are used.
- [ ] **Keyword filter substring match** ([app/services/content_filter.py](app/services/content_filter.py)) — `"kill"` ∈ `"skill"`. Semantic choice: switch to word-boundary regex, or document as substring.
- [ ] **`_extract_usage` token bug** ([app/api/proxy.py](app/api/proxy.py#L110)) — non-streaming OpenAI path reads top-level instead of `usage.*` → records 0 tokens. *(Declined this session; revisit if token metrics matter.)*
- [ ] **Regex ReDoS — full fix** — current cap is partial. Hard guarantee needs `re2` or `regex` lib with `timeout=` (a dependency decision).

### 3. Group B — multi-cluster / Phase-2  *(only when Phase 2 is live)*
Not reviewed for fixes this session.
- [ ] **Token reservation leak** — `_reserve_tokens_atomic` is outside `try/finally` in `forward` / `forward_stream` / `forward_embeddings`; a cancellation between reserve and `try` leaks the reservation until TTL — [app/backends/multi_cluster.py](app/backends/multi_cluster.py)
- [ ] **`_select_cluster` step 3 routes to DRAINING clusters** — only checks `is_enabled`, not drain state — [app/backends/multi_cluster.py](app/backends/multi_cluster.py)
- [ ] **Read-modify-write races** — EWMA latency, drain-state transitions, `update_cluster_config_field` are GET-modify-SET (lost updates under multi-pod) — [app/db/cluster_operational_state.py](app/db/cluster_operational_state.py), [app/db/cluster_drain.py](app/db/cluster_drain.py), [app/db/cluster_config.py](app/db/cluster_config.py)
- [ ] **Naive vs aware `datetime`** — staleness guard mixes `datetime.utcnow()` with possibly-aware timestamps → comparison throws → guard silently no-ops — [app/services/cross_pod_sync_service.py](app/services/cross_pod_sync_service.py)
- [ ] **Per-tick churn** — `_check_backend_configs` re-applies config + fires callbacks every 5s for every cluster (the `_timestamp_cache` is unused); deleted clusters are never removed from peer pods — [app/services/cross_pod_sync_service.py](app/services/cross_pod_sync_service.py)
- [ ] **`complete_drain` doesn't update in-memory `is_enabled`** — relies on Redis TTL not expiring; add `backend.disable_cluster()` — [app/services/drain_monitor.py](app/services/drain_monitor.py)

---

## Skipped by decision
- **CORS `allow_origins=["*"]` + `allow_credentials=True`** ([app/main.py](app/main.py)) — low real risk for header-bearer auth (no cookies); revisit only if cookie auth is added.
