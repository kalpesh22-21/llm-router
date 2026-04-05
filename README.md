# LLM Router

A lightweight OpenAI-compatible proxy that sits between clients and LLM backends. It enforces user authentication (LDAP + JWT), bucket-based model access control, per-user concurrent request limits, and emits Prometheus metrics.

```
Client → LLM Router → LLM Backend (OpenAI, vLLM, etc.)
```

## Quick Start

```bash
cp .env.example .env          # fill in LDAP, DB, secret key
docker-compose up             # starts app, mysql, redis, ldap, prometheus
```

API available at `http://localhost:8000` — docs at `/docs`, UI at `/ui`.

## Docs

- [Architecture](docs/architecture.md) — request flow, access control model, phase 1/2 design
- [API: Proxy](docs/api/proxy.md) — `/v1/chat/completions`, `/v1/embeddings`, `/v1/models`
- [API: Auth](docs/api/auth.md) — login, JWT, current user
- [API: Admin](docs/api/admin.md) — user/bucket/access management
- [Backends](docs/backends/backends.md) — Backend abstraction, StaticBackend
- [Auth Internals](docs/auth/auth.md) — LDAP binding, JWT handling
- [Permissions](docs/services/permissions.md) — PermissionService, access control ops
- [Rate Limiting](docs/services/rate-limiting.md) — Redis concurrent counter, Lua atomics
- [Metrics](docs/services/metrics.md) — Prometheus counters and histograms
- [Database Schema](docs/db/schema.md) — MySQL tables, Redis key patterns
- [Deployment](docs/deployment/deployment.md) — Docker setup, environment variables
