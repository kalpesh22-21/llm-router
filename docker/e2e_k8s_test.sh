#!/usr/bin/env bash
# =============================================================================
# Kubernetes service-discovery E2E (DISCOVERY_MODE=both, fake lister).
#
# The app reads docker/k8s_fake_services.json as its "Service list" (stands in
# for the K8s API). This script edits that file to add/remove a Service and
# shows the router register/unregister the backend live — alongside the static
# config model (test-model), which is never touched.
#
#   docker compose -f docker-compose.e2e.yml up -d --build
#   ./docker/e2e_k8s_test.sh
# =============================================================================
set -uo pipefail

BASE="http://localhost:8000"
FAKE_FILE="docker/k8s_fake_services.json"
SERVICE_JSON='[{"name":"mock-upstream","namespace":"default","labels":{"llm-router/enabled":"true","llm-router/model-id":"k8s-discovered-model","llm-router/port":"9000"}}]'

say()    { printf '\n========== %s ==========\n' "$1"; }
login()  { curl -fsS -X POST "$BASE/auth/login" -H 'Content-Type: application/json' \
             -d '{"username":"admin","password":"admin123"}' | jq -r .access_token; }
status() { curl -fsS "$BASE/admin/models/status" -H "Authorization: Bearer $TOKEN" | jq -c '.data'; }
models() { curl -fsS "$BASE/v1/models" -H "Authorization: Bearer $TOKEN" | jq -c '[.data[].id]'; }
chat()   { curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/v1/chat/completions" \
             -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
             -d "{\"model\":\"$1\",\"messages\":[{\"role\":\"user\",\"content\":\"ping\"}]}"; }

# Start with the discovered service present.
printf '%s\n' "$SERVICE_JSON" > "$FAKE_FILE"

say "Waiting for app /health"
for i in $(seq 1 90); do curl -fsS "$BASE/health" >/dev/null 2>&1 && { echo "app up"; break; }; sleep 2; done

say "Login + grant BOTH models via a default bucket"
TOKEN=$(login); [ -n "$TOKEN" ] && [ "$TOKEN" != "null" ] || { echo "login failed"; exit 1; }
BID=$(curl -fsS -X POST "$BASE/admin/buckets" -H "Authorization: Bearer $TOKEN" \
      -H 'Content-Type: application/json' \
      -d '{"name":"e2e-k8s","description":"e2e","model_ids":["test-model","k8s-discovered-model"]}' 2>/dev/null | jq -r '.id // empty')
if [ -z "$BID" ]; then
  BID=$(curl -fsS "$BASE/admin/buckets" -H "Authorization: Bearer $TOKEN" | jq -r '.[]|select(.name=="e2e-k8s")|.id' | head -1)
fi
echo "bucket id=$BID"
curl -fsS -X PATCH "$BASE/admin/buckets/$BID/default" -H "Authorization: Bearer $TOKEN" >/dev/null
TOKEN=$(login)  # JWT now carries both models

say "STATE 1 — discovered service PRESENT (static + discovered coexist)"
echo "admin/models/status: $(status)"
echo "/v1/models:          $(models)"
echo "chat test-model:           $(chat test-model)   (config model, expect 200)"
echo "chat k8s-discovered-model: $(chat k8s-discovered-model)   (discovered, expect 200)"

say "Removing the discovered Service (write [] to $FAKE_FILE)"
printf '[]\n' > "$FAKE_FILE"
echo "waiting ~8s for the reconcile loop (resync 5s)…"
sleep 8

say "STATE 2 — discovered service REMOVED"
echo "admin/models/status: $(status)   (k8s-discovered-model gone; test-model remains)"
echo "/v1/models:          $(models)"
echo "chat test-model:           $(chat test-model)   (config model untouched, expect 200)"
echo "chat k8s-discovered-model: $(chat k8s-discovered-model)   (unregistered, expect 404)"

say "Re-adding the discovered Service"
printf '%s\n' "$SERVICE_JSON" > "$FAKE_FILE"
echo "waiting ~8s for the reconcile loop…"
sleep 8

say "STATE 3 — discovered service BACK"
echo "admin/models/status: $(status)"
echo "/v1/models:          $(models)"
echo "chat k8s-discovered-model: $(chat k8s-discovered-model)   (expect 200)"

say "E2E COMPLETE"
