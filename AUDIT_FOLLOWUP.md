# LLM Router — Security & Correctness Audit Follow-Up

**Audit date:** 2026-06-17 · **Scope of fixes so far:** static / Phase-1 path only.

A full-codebase review (8 subsystems, ~11.7k LOC) surfaced ~55 findings. Fixes
below were applied **one-by-one with explicit approval**. Multi-cluster / Phase-2
findings were **deliberately deferred** (see Group B). Nothing has been committed
yet — all applied changes are in the working tree.

Verify edits with `python3 -m py_compile <file>` (there is no `python` on PATH).

---

## ✨ Discovery service (PoC, added 2026-06)

Backends are now registered for **every** model at startup (even unreachable
ones); a background `DiscoveryService` probes each **static** backend and flips
its availability, so a down model returns **503** (not a permanent 404) and
**auto-recovers** without a restart. Per-pod / in-memory, polling-only.

Decisions locked: per-pod in-memory state · polling-only (no reactive breaker) ·
`/v1/models` hides down models · 503+`Retry-After` for known-but-down · register
with default key then re-resolve via `find_working_api_key` on recovery ·
hysteresis (down after N consecutive fails, up after 1 success).

Pieces:
- [app/backends/base.py](app/backends/base.py) — `Backend.is_available` default `True`
- [app/backends/static.py](app/backends/static.py) — availability state + `record_probe_result()` hysteresis
- [app/backends/__init__.py](app/backends/__init__.py) — **`build_registry_from_config()`** (was missing!) registers all models
- [app/services/discovery_service.py](app/services/discovery_service.py) — the probe loop
- [app/routing/router.py](app/routing/router.py) — 404 unknown / 503 down
- [app/api/proxy.py](app/api/proxy.py) — `/v1/models` filters unavailable
- [app/metrics/collector.py](app/metrics/collector.py) — `llm_router_model_available{model_id}` gauge
- [app/api/admin.py](app/api/admin.py) — `GET /admin/models/status`
- [app/core/config.py](app/core/config.py) — `DISCOVERY_ENABLED` / `DISCOVERY_PROBE_INTERVAL_SECONDS` / `DISCOVERY_FAILURE_THRESHOLD`
- [tests/test_discovery.py](tests/test_discovery.py) — hysteresis + probe + 503/404 gate units
- E2E harness: `docker-compose.e2e.yml`, `docker/mock_upstream.py`, `config/models.e2e.yaml`, `docker/e2e_test.sh`, `docker/stubs/` (paycompy/phoenix/openinference/opentelemetry shims — see below)

**Not yet built:** reactive circuit-breaking; config hot-reload for new models;
multi-cluster discovery (multi-cluster has its own health).

> ✅ **Repo is now container-runnable.** Previously `build_registry_from_config`
> was missing and `requirements.txt` omitted `sqlparse` + optional deps. Now:
> `build_registry_from_config` added, `sqlparse` in requirements, and `paycompy` /
> tracing imports are guarded so the app boots without them. No stubs needed —
> see the FIXED bugs section below.

### Real bugs surfaced while running the E2E — FIXED & re-verified in Docker
- [x] **`StrippedString` corrupted config values** — [config.py](app/core/config.py): was
  `strip("\\n")` (strips the chars `\`/`n` → `"admin"` became `"admi"`, superuser created as
  `admi`, `admin` login 401'd). Replaced with `_strip_config_value` =
  `value.replace("\\n", "").strip()`. Verified: `SUPERUSER_USERNAME="admin"` now stays
  `"admin"` and admin login works.
- [x] **`requirements.txt` incomplete** — added `sqlparse` (real). The internal `paycompy`
  and the tracing deps (`arize-phoenix`/`openinference-*`/`opentelemetry-*`) are now
  **optional**: their imports are guarded ([config.py](app/core/config.py),
  [ldap_auth.py](app/auth/ldap_auth.py), [pheonix.py](app/logging/pheonix.py)), so the app
  boots without them (Vault errors clearly if enabled-without-paycompy; LDAP degrades to the
  env-admin path; tracing disables). requirements.txt documents how to install them.
  **Result: the app now boots in a clean container with NO stubs** — verified
  (`paycompy/phoenix/openinference/opentelemetry importable=False`, app healthy, E2E passes).
- [ ] **`docker-compose.yml` (root) is stale** — sets `DATABASE_URL`/`REDIS_URL`/`SECRET_KEY`,
  but config reads `DATABASE_HOST/USER/PASSWORD/PORT/NAME` + `REDIS_HOST/PORT` +
  `JWT_SECRET_KEY`. Those env vars are silently ignored. `docker-compose.e2e.yml` has a
  working set; the root compose still needs fixing (left as-is for now).

### E2E result (passed, 2026-06)
`docker compose -f docker-compose.e2e.yml up -d --build` then `./docker/e2e_test.sh`:
UP → `available:true`, gauge `1.0`, listed, chat `200` · DOWN (mock stopped) →
`available:false`, gauge `0.0`, hidden from `/v1/models`, chat `503` · RECOVERED (mock
restarted) → `available:true`, gauge `1.0`, chat `200`. Unit tests: `10 passed`.
Teardown: `docker compose -f docker-compose.e2e.yml down -v`.

## ✨ Kubernetes service discovery (second discovery mode, added 2026-06)

A second discovery **source**: `DISCOVERY_MODE` = `config` | `kubernetes` | `both`.
In `kubernetes`/`both`, `KubernetesServiceDiscovery` periodically lists Services
in `DISCOVERY_K8S_NAMESPACES` matching `DISCOVERY_K8S_LABEL_SELECTOR` and
registers one `StaticBackend` per service; the existing health-probe loop then
probes them. Decisions: model_id from a label (`DISCOVERY_K8S_MODEL_ID_LABEL`) ·
poll+reconcile every `DISCOVERY_K8S_RESYNC_SECONDS` · URL =
`<scheme>://<svc>.<ns>.<cluster_domain>:<port><path>` from config defaults +
per-service label overrides · `both` mode never reaps/overwrites config models
(reconciler only touches model_ids it owns).

Pieces:
- [app/services/kubernetes_discovery.py](app/services/kubernetes_discovery.py) — `KubernetesServiceDiscovery` + pluggable `ServiceLister` (`K8sServiceLister` real, `FakeServiceLister` for tests/E2E). `kubernetes_asyncio` import is **guarded** (app boots in config mode without it).
- [app/backends/base.py](app/backends/base.py) — registry `upsert()` / `unregister()` / `contains()`.
- [app/core/config.py](app/core/config.py) — `DISCOVERY_MODE` + `DISCOVERY_K8S_*` settings.
- [app/main.py](app/main.py) — mode-aware lifespan wiring + shutdown.
- [tests/test_kubernetes_discovery.py](tests/test_kubernetes_discovery.py) — mapping + reconcile (add/remove/skip-config/update).
- E2E: `docker/e2e_k8s_test.sh` + `docker/k8s_fake_services.json` (fake lister; mock-upstream gets a `*.svc.cluster.local` network alias so the discovered DNS resolves).

**E2E result (passed, mode=both, no real cluster):** static `test-model` + discovered
`k8s-discovered-model` coexist → both chat `200`; remove the Service live → discovered
model unregisters (chat `404`) while `test-model` is untouched (`200`); re-add → back
(`200`). `kubernetes_asyncio importable=False` (guard verified). Unit: `17 passed`.
For a real cluster: `pip install kubernetes-asyncio`, set `DISCOVERY_MODE=kubernetes`,
unset `DISCOVERY_K8S_FAKE_SERVICES_FILE`, grant the pod RBAC to `list` Services.

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
