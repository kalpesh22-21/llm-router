# ADR-001: Dynamic Model Backend Management (DB-driven, Zero-Restart Propagation)

**Status:** Accepted  
**Date:** 2026-06-19  
**Authors:** Kalpesh Mulye  
**Scope:** v1 — static backends only (`type: static`)

---

## Context

LLM Router v1 loads backend definitions (model_id, base_url, api_key) exclusively from `config/models.yaml` at pod startup. Adding or changing a model requires editing that file and restarting every pod. In a multi-pod deployment this is operationally fragile: there is no UI, changes are not auditable, and there is a restart window during which pods may be inconsistent.

There is also no central store of what backends exist — `bucket_models.model_id` is a string reference that can silently become a dangling pointer.

### Forces

- Model definitions must be editable from the admin UI without pod restarts.
- The change must propagate to **all running pods** within a few seconds.
- After a pod restart it must recover the current state without human intervention.
- API keys are sensitive; they must be encrypted at rest and must never appear in logs or API responses.
- The existing `BackendRegistry` / `Backend` ABC must remain the single proxy-layer lookup point — the proxy code (`app/api/proxy.py`) must require zero changes.
- The design must not prevent Phase 2 additions (`type: multi_cluster` managed via DB).

---

## Decision

### Source of truth: MySQL `model_backends` table

MySQL is authoritative. The table stores one row per logical model backend, with an encrypted `api_key_enc` column and `is_enabled` for soft-disable. All create/edit/disable operations write here first.

### Propagation: polling reconcile loop + Redis pub/sub accelerator

Each pod runs a `DatabaseBackendSource` background service — a direct sibling of `KubernetesServiceDiscovery` — that:

1. **Polls** the DB every `DB_BACKEND_POLL_INTERVAL_SECONDS` (default 30 s), comparing a `models_version` Redis counter to its last-seen value. If the counter is unchanged, the poll is a single Redis GET (zero DB load). If it changed, it fetches the full `model_backends` table and reconciles the in-process registry.
2. **Subscribes** to the Redis channel `llmr:models_changed`. When any API write publishes to this channel, all subscribers trigger an immediate reconcile, making the typical propagation latency < 1 s.

Redis pub/sub is a latency optimization; the poll is the safety net. A pod that misses a pub/sub message (e.g. was briefly disconnected) converges on the next poll cycle.

`models_version` is an integer counter stored at Redis key `llmr:models_version`. Every successful write to `model_backends` via the API atomically `INCR`s this key. Pods store the last-seen value in memory and skip the DB query on poll when the value has not changed.

### Source precedence / ownership (the `_managed` contract)

Backends are owned by the source that first registered them. `DatabaseBackendSource` tracks its owned model_ids in `self._managed` (a `dict[model_id, row_version]`). On each reconcile:

- If a model_id is already in the registry and **not** in `self._managed`, it was registered by a higher-priority source (YAML config or k8s discovery) — skip it, log a warning.
- If a model_id is in `self._managed`, update or unregister freely.
- If is_enabled becomes False for a managed model_id, call `registry.unregister(model_id)` and remove from `self._managed`.

**Precedence order (highest → lowest):**

1. `config/models.yaml` (registered first, at startup, before any discovery source starts)
2. Kubernetes discovery (`KubernetesServiceDiscovery`, already uses `_managed`)
3. `model_backends` DB table (`DatabaseBackendSource`, this ADR)

This means: if a model_id appears in both YAML and the DB table, YAML wins. The DB source logs a warning and skips it. This is intentional — YAML is the escape hatch for static infrastructure that operators do not want managed dynamically.

**Implication for operators:** to hand a model over to DB management, remove it from models.yaml and write it to the DB table via the UI. No other action needed.

### Encryption: Fernet symmetric encryption

`api_key_enc` in `model_backends` stores the api_key encrypted with Fernet (AES-128-CBC + HMAC-SHA256). The Fernet key is a 32-byte URL-safe base64 string sourced from the `MODEL_BACKEND_ENCRYPTION_KEY` environment variable (or from Vault when `VAULT_ENABLED=true`, under the same name already used for other secrets). It is never written to the DB or logged.

Redaction: `api_key_enc` is never returned in API responses. All list/get endpoints return `"api_key": null`. The admin UI never receives the raw value.

### YAML api_key fields

YAML-defined backends continue to use the existing plaintext YAML/Vault api_key flow — no change. They are not migrated to the DB. Only backends created through the new admin UI use encrypted DB storage.

---

## Alternatives Considered

### ConfigMap reload (Kubernetes)
Requires a k8s-native deployment, inaccessible to local/dev environments, and does not provide a UI or audit trail. Rejected.

### Redis as source of truth
Redis is ephemeral and has no relational integrity (bucket_models FK checks). Rejected in favour of MySQL as the authoritative store.

### WebSocket push from API to pods
More complex subscriber management and not idiomatic for a Python/FastAPI service with multiple pods. The poll+pub/sub model delivers sub-1 s latency with minimal complexity. Rejected.

### Plaintext api_key in DB
Fails the at-rest encryption requirement. Rejected.

---

## Consequences

- Adds one new table (`model_backends`) and one ORM class to `app/db/mysql.py`.
- Adds one new service class (`DatabaseBackendSource`) in `app/services/database_backend_source.py`.
- Adds a new `DISCOVERY_MODE` value: `"db"` and `"db+config"` (config + DB), `"db+kubernetes"`, `"db+config+kubernetes"`.  Alternatively, add a separate boolean `DB_BACKENDS_ENABLED` to avoid multiplying DISCOVERY_MODE strings. **Chosen approach:** boolean `DB_BACKENDS_ENABLED=true` setting. Simpler to add alongside existing `DISCOVERY_MODE` without breaking existing deployments.
- `models_version` Redis key and `llmr:models_changed` channel are net-new — no collision with existing key namespaces.
- Admin API gets a new `/admin/model-backends` route group; existing `/admin/models` (read-only, registry snapshot) is unchanged.
- The read-after-write race (noted in project memory) is addressed at the API write site: after committing to DB, the writer pod calls `reconcile()` inline before returning 200, so the writing pod's registry is immediately consistent. Other pods receive the pub/sub signal and reconcile asynchronously.
