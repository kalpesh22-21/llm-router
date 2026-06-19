#!/usr/bin/env bash
# =============================================================================
# Discovery-service end-to-end test.
#
# Drives the stack in docker-compose.e2e.yml through three states and prints
# what the router reports at each:
#   1. upstream UP        → model available, /v1/models lists it, chat → 200
#   2. upstream DOWN       → model unavailable, hidden from /v1/models, chat → 503
#   3. upstream RECOVERED  → available again, chat → 200
#
# Usage:
#   docker compose -f docker-compose.e2e.yml up -d --build
#   ./docker/e2e_test.sh
# =============================================================================
set -uo pipefail

BASE="http://localhost:8000"
COMPOSE="docker compose -f docker-compose.e2e.yml"

say() { printf '\n========== %s ==========\n' "$1"; }

login() {
  curl -fsS -X POST "$BASE/auth/login" -H 'Content-Type: application/json' \
    -d '{"username":"admin","password":"admin123"}' | jq -r .access_token
}
status()  { curl -fsS "$BASE/admin/models/status" -H "Authorization: Bearer $TOKEN" | jq -c '.data'; }
gauge()   { curl -fsSL "$BASE/metrics/" | grep -E '^llm_router_model_available' || echo '(no sample yet)'; }
models()  { curl -fsS "$BASE/v1/models" -H "Authorization: Bearer $TOKEN" | jq -c '.data'; }
chat()    { curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/v1/chat/completions" \
              -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
              -d '{"model":"test-model","messages":[{"role":"user","content":"ping"}]}'; }

say "Waiting for app /health"
for i in $(seq 1 90); do
  curl -fsS "$BASE/health" >/dev/null 2>&1 && { echo "app is up"; break; }
  sleep 2
  [ "$i" -eq 90 ] && { echo "ERROR: app never became healthy"; $COMPOSE logs --tail=50 app; exit 1; }
done

say "Login (env-admin) + grant test-model via a default bucket"
TOKEN=$(login); [ -n "$TOKEN" ] && [ "$TOKEN" != "null" ] || { echo "login failed"; exit 1; }
# Idempotent: create the e2e bucket, or reuse it if a prior run already made it.
BID=$(curl -fsS -X POST "$BASE/admin/buckets" -H "Authorization: Bearer $TOKEN" \
      -H 'Content-Type: application/json' \
      -d '{"name":"e2e","description":"e2e","model_ids":["test-model"]}' 2>/dev/null | jq -r '.id // empty')
if [ -z "$BID" ]; then
  BID=$(curl -fsS "$BASE/admin/buckets" -H "Authorization: Bearer $TOKEN" \
        | jq -r '.[] | select(.name=="e2e") | .id' | head -1)
fi
echo "bucket id=$BID"
curl -fsS -X PATCH "$BASE/admin/buckets/$BID/default" -H "Authorization: Bearer $TOKEN" >/dev/null
TOKEN=$(login)  # re-login so the JWT carries test-model in accessible_models

say "STATE 1 — upstream UP"
echo "admin/models/status: $(status)"
echo "metrics gauge:       $(gauge)"
echo "/v1/models:          $(models)"
echo "chat HTTP status:    $(chat)   (expect 200)"

say "Stopping mock-upstream to simulate the model going DOWN"
$COMPOSE stop mock-upstream
echo "waiting ~20s for discovery to mark it down (interval 5s x threshold 2)…"
sleep 20

say "STATE 2 — upstream DOWN"
echo "admin/models/status: $(status)"
echo "metrics gauge:       $(gauge)"
echo "/v1/models:          $(models)   (expect [] — hidden)"
echo "chat HTTP status:    $(chat)   (expect 503)"

say "Restarting mock-upstream to simulate RECOVERY"
$COMPOSE start mock-upstream
echo "waiting ~14s for the next probe to re-enable it…"
sleep 14

say "STATE 3 — upstream RECOVERED"
echo "admin/models/status: $(status)"
echo "metrics gauge:       $(gauge)"
echo "/v1/models:          $(models)"
echo "chat HTTP status:    $(chat)   (expect 200)"

say "E2E COMPLETE"
