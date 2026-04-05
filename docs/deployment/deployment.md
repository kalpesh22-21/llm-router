# Deployment

## Docker Compose

```bash
cp .env.example .env   # configure environment
docker-compose up      # build + start all services
```

Five services start on the `llm-router-net` bridge network:

| Service | Image | Port | Notes |
|---------|-------|------|-------|
| `app` | Built from Dockerfile | 8000 | Depends on mysql, redis, ldap |
| `mysql` | mysql:8.0 | 3306 | Auto-runs `schema.sql` + `seed.sql` on first start |
| `redis` | redis:7-alpine | 6379 | |
| `ldap` | osixia/openldap:1.5.0 | 389 | Bootstrapped with `ldap/users.ldif` |
| `prometheus` | prom/prometheus:latest | 9090 | Depends on app |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | — | `mysql+aiomysql://user:pass@host:3306/llm_router` |
| `REDIS_URL` | — | `redis://host:6379/0` |
| `SECRET_KEY` | — | HS256 signing key for JWTs. Use a long random string in production. |
| `JWT_ALGORITHM` | `HS256` | |
| `JWT_EXPIRE_MINUTES` | `60` | |
| `LDAP_SERVER` | — | e.g. `ldap://ldap.example.com` |
| `LDAP_BASE_DN` | — | e.g. `dc=example,dc=com` |
| `LDAP_BIND_DN` | — | Service account DN |
| `LDAP_BIND_PASSWORD` | — | Service account password |
| `SUPERUSER_USERNAME` | `admin` | First login with this username is auto-promoted to admin |
| `MODELS_CONFIG_PATH` | `config/models.yaml` | Path to model backend config |
| `API_KEY` | — | Fallback API key for backends that reference `${API_KEY}` |
| `PROMETHEUS_PORT` | `9090` | |

## Dockerfile

Multi-stage build:
- **Stage 1 (`builder`)**: Python 3.12-slim, installs dependencies into `/opt/venv`.
- **Stage 2 (`runtime`)**: Copies venv and app source, runs as non-root `appuser`, exposes 8000, health-checks `GET /health`.

Entrypoint: `uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 1`

## Endpoints After Startup

| URL | Description |
|-----|-------------|
| `http://localhost:8000/docs` | OpenAPI / Swagger UI |
| `http://localhost:8000/redoc` | ReDoc |
| `http://localhost:8000/ui` | Admin SPA |
| `http://localhost:8000/health` | Health check (`{"status": "ok"}`) |
| `http://localhost:8000/metrics` | Prometheus metrics |
| `http://localhost:9090` | Prometheus UI |

## LDAP Bootstrap Users

The bundled `ldap/users.ldif` seeds five users for local development:

| Username | Password |
|----------|----------|
| `admin` | `admin123` |
| `alice` | `alice123` |
| `bob` | `bob123` |
| `charlie` | `charlie123` |
| `kalpesh` | `kalpesh` |
