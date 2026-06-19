# LLM Router — Playwright E2E UI suite

End-to-end browser tests that drive the admin SPA (served at `/ui/`) against the
`docker-compose.e2e.yml` stack, exercising every UI surface and proving an
issued service-account API key actually works against the proxy.

## Prerequisites

The e2e docker stack must be running and healthy:

```bash
# from the repo root
docker compose -f docker-compose.e2e.yml up -d --build
curl -fsS http://localhost:8000/health   # -> {"status":"ok"}
```

## Run

```bash
cd e2e
npm install                 # first time only
npx playwright install chromium   # first time only
npm test                    # runs the whole suite (serial)
npx playwright show-report  # open the HTML report
```

Tests run **serially** (`workers: 1`) because they mutate shared backend state
(one MySQL instance; the "default bucket" is global). Resource names are unique
per run (`uid()`), so repeated runs don't collide.

## Coverage

| Spec | What it verifies |
|------|------------------|
| `01-auth` | Bad-credential rejection, admin login + shell render, bearer-token display/regenerate, logout |
| `02-service-accounts` | **Headline flow:** create bucket → create service account → issue API key (UI) → **the `llmr_sk_…` key works on `POST /v1/chat/completions` (200, proxied to mock upstream)** → chat-only scope is denied on `/v1/embeddings` (403) → revoke → **revoked key is rejected (401)**. Plus deactivate toggles status. |
| `03-buckets` | Create/edit/delete a bucket, model assignment, set-default + default-model dropdown, default bucket is undeletable, search filter |
| `04-users` | Create-user gated by LDAP (graceful error), server-side search, edit+revert max-concurrent, manage-buckets modal |
| `05-content-filters` | Create KEYWORD filter, **live Test** (blocked vs allowed), toggle active, edit, delete; REGEX pattern validation |
| `06-multicluster` | View loads, model selector populated, model switching |
| `07-proxy-api` | `/v1/models` lists the model, chat proxies (200), inaccessible model 403, missing auth 401 |

## Environment limitations (not test gaps)

- **User creation** requires the username to exist in LDAP (`verify_ldap_user_exists`).
  The e2e stack runs no LDAP server, so creation can't succeed — `04-users` asserts
  the UI surfaces that error and exercises the existing `admin` user for the rest
  (never disabling admin, the only login).
- **Multicluster** provisions no `cluster_config` rows in the static e2e stack, so
  `/admin/models/<id>/clusters` returns `[]` and the view shows its empty-state;
  cluster card edit/enable/disable/drain can't be exercised here.
- The **mock upstream** only implements `/v1/chat/completions`, `/v1/models`,
  `/health` — not embeddings/messages happy-paths.

## Bugs found while building this suite

- **A — `POST /admin/service-accounts` returned HTTP 500 (FIXED).** `MissingGreenlet`:
  the service flushed but the server-default `created_at`/`updated_at` weren't loaded
  before `model_validate`, triggering a sync lazy-load outside the async greenlet.
  Fixed by `await db.refresh(...)` in `create_service_account`, `set_active`, and the
  issue-key path (`app/services/service_account.py`, `app/services/api_key.py`).
- **C — shared modal Save button could be invisible (FIXED).** The "Test filter" modal
  sets `#modal-submit` to `display:none`; `openModal` didn't restore it, so a later
  Edit/Create modal had an unclickable Save button. Fixed centrally in `openModal`
  (`app/static/app.js`).
- **B — read-after-write race (OPEN, worked around in tests).** `GracefulShutdownMiddleware`
  is a Starlette `BaseHTTPMiddleware`, which defers `get_db()`'s post-yield commit until
  after the POST response is delivered. A fast client (the SPA's immediate post-create
  refresh) can read pre-commit data and render a stale list (e.g. "Buckets 9" when 10
  exist). Sequential API calls are always consistent; only sub-millisecond
  create-then-read races it. The `expectRowVisible` / `expectRowGone` helpers re-fetch
  after the commit lands. **Recommended fix:** convert `GracefulShutdownMiddleware` to a
  pure-ASGI middleware (so dependency finalization isn't deferred), or commit-before-return
  in write paths. Not fixed here — it touches graceful-shutdown + streaming and deserves
  its own change.
