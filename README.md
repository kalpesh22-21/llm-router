# LLM Router

A FastAPI gateway for shared LLM infrastructure, with OpenAI-compatible APIs, multi-cluster routing, access control, and a built-in administration UI. Connect clients to one endpoint and manage access to models served by vLLM or other compatible backends.

```text
Clients → LLM Router → Static or multi-cluster LLM backends
              │
              ├── MySQL: users, permissions, backend configuration
              └── Redis: concurrency limits, routing state, synchronization
```

## Features

- **Inference APIs:** chat completions with SSE streaming, embeddings, model listing, and an Anthropic-style messages proxy for compatible upstreams.
- **Authentication and access control:** LDAP-backed user login with JWTs, service accounts with scoped and revocable API keys, bucket-based model permissions, and per-user concurrency limits.
- **Multi-cluster routing:** local-cluster preference, spillover, session affinity, latency-based capacity adaptation, and cluster enable/disable and drain controls.
- **Backend management:** YAML configuration, health probing, optional Kubernetes service discovery, and UI-managed static backends with encrypted API keys and cross-pod synchronization.
- **Content filtering:** configurable keyword and regular-expression rules, with optional filter integrations.
- **Operations:** an admin UI, Prometheus metrics, health checks, and optional Phoenix tracing.

## Run the local demo

Prerequisites: Docker with Docker Compose. The self-contained demo runs the router, MySQL, Redis, and a mock upstream; no GPU or external model credentials are required.

```bash
docker compose -f docker-compose.e2e.yml up -d --build
curl -fsS http://localhost:8000/health
```

Once startup completes, open:

| Service | URL |
| --- | --- |
| Admin UI | http://localhost:8000/ui/ |
| Interactive API documentation | http://localhost:8000/docs |
| Prometheus metrics | http://localhost:8000/metrics |

Sign in with **admin / admin123**. These credentials and the secrets in the demo Compose file are for local testing only. The demo serves `test-model` using mock responses, not real inference, and uses a fake Kubernetes service list to exercise discovery.

The root `docker-compose.yml` and `.env.example` contain older configuration names. Use `docker-compose.e2e.yml` for the demo and [the current settings](app/core/config.py) when configuring a deployment.

Stop the demo with:

```bash
docker compose -f docker-compose.e2e.yml down
```

## Make an API request

Log in to obtain a JWT:

```bash
curl -fsS http://localhost:8000/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"admin123"}'
```

Copy the returned `access_token`, then list the models available to the user and request a completion:

```bash
export LLM_ROUTER_TOKEN='<access_token>'

curl -fsS http://localhost:8000/v1/models \
  -H "Authorization: Bearer $LLM_ROUTER_TOKEN"

curl -fsS http://localhost:8000/v1/chat/completions \
  -H "Authorization: Bearer $LLM_ROUTER_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"model":"test-model","messages":[{"role":"user","content":"Hello!"}]}'
```

For applications, create a service account in the UI, grant bucket access, and issue an API key with the required scopes. Proxy endpoints accept JWTs or service API keys through `Authorization: Bearer` or `x-api-key`. Configure OpenAI-compatible clients with the router's `/v1` base URL and the issued credential.

## Connect real backends

Configure static or multi-cluster models in the file selected by `MODELS_CONFIG_PATH`. A minimal static model configuration is:

```yaml
models:
  - model_id: my-model
    base_url: http://your-model-server:8000/v1
    api_key: ""
```

Use an upstream reachable from the router container. An empty `api_key` uses the configured fallback `API_KEY`. See [config/models.yaml](config/models.yaml) for the multi-cluster configuration shape; replace its environment-specific endpoints before use.

To manage static backends through the UI, set `DB_BACKENDS_ENABLED=true` and provide `MODEL_BACKEND_ENCRYPTION_KEY` as a Fernet key shared by all router instances. Database-managed backends propagate through Redis notifications and periodic reconciliation. Existing YAML or discovery-owned model IDs are not overwritten. See the [backend management design](docs/adr-001-dynamic-model-backends.md) for details.

Assign models to buckets and grant users or service accounts access through the admin UI.

## Configuration

The application reads environment variables; set `LOCAL_ENV_PATH` explicitly to load a local environment file. Key settings include:

| Setting | Purpose |
| --- | --- |
| `DATABASE_HOST`, `DATABASE_PORT`, `DATABASE_USER`, `DATABASE_PASSWORD`, `DATABASE_NAME` | Database connection |
| `DATABASE_DRIVER` | Use `mysql+aiomysql` for the included MySQL stack |
| `REDIS_HOST`, `REDIS_PORT` | Redis connection |
| `JWT_SECRET_KEY` | JWT signing secret |
| `SUPERUSER_USERNAME`, `ADMIN_INITIAL_PASSWORD` | Bootstrap administrator credentials |
| `MODELS_CONFIG_PATH` | YAML model configuration path |
| `VAULT_ENABLED` | Enable the optional internal Vault integration; `false` for standalone use |
| `DISCOVERY_ENABLED`, `DISCOVERY_MODE` | Health probing and static/Kubernetes discovery mode |
| `DB_BACKENDS_ENABLED`, `MODEL_BACKEND_ENCRYPTION_KEY` | Enable encrypted database-managed static backends |
| `PHOENIX_TRACING_ENABLED` | Enable optional Phoenix tracing |

See [app/core/config.py](app/core/config.py) for all settings and defaults. LDAP and Vault integrations require the internal `paycompy` package; standalone installations can use the administrator fallback and service accounts. Optional tracing and Kubernetes dependencies are listed in [requirements.txt](requirements.txt).

## Development and tests

The Docker image uses Python 3.12. For a local Python environment:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-test.txt
VAULT_ENABLED=false pytest tests/ -v
```

Run the browser suite against the running demo stack:

```bash
cd e2e
npm install
npx playwright install chromium
npm test
```

See [the E2E guide](e2e/README.md) for coverage and environment limitations, and [docker/e2e_test.sh](docker/e2e_test.sh) for the discovery integration checks.

## Documentation

Some guides describe earlier architecture phases; use the current API documentation at `/docs` and settings source for exact interfaces.

- [Architecture](docs/architecture.md)
- [Proxy API](docs/api/proxy.md), [authentication API](docs/api/auth.md), and [admin API](docs/api/admin.md)
- [Backend abstraction](docs/backends/backends.md) and [dynamic backend management](docs/adr-001-dynamic-model-backends.md)
- [Authentication internals](docs/auth/auth.md) and [permissions](docs/services/permissions.md)
- [Concurrency limiting](docs/services/rate-limiting.md) and [metrics](docs/services/metrics.md)
- [Database schema](docs/db/schema.md) and [deployment](docs/deployment/deployment.md)
