#!/usr/bin/env bash
# scripts/smoke.sh — PROTEA end-to-end smoke
#
# Assumes a stack is already running and reachable at $PROTEA_API_URL
# (default http://127.0.0.1:8000). Does NOT start or stop services
# (per project policy: the stack is never restarted without explicit
# user permission; see memory/feedback_no_restart.md).
#
# What it checks:
#   1. /health responds 200.
#   2. /readiness responds 200 (DB + AMQP both up).
#   3. POST /jobs creates a ping job and returns a job_id.
#   4. The ping job transitions to SUCCEEDED within $PROTEA_SMOKE_TIMEOUT_S
#      seconds (default 30).
#   5. GET /jobs/{id}/events lists at least one event with event=job.succeeded.
#
# Usage:
#   bash scripts/smoke.sh                  # local stack, default URL
#   PROTEA_API_URL=https://... bash scripts/smoke.sh
#   PROTEA_SMOKE_TIMEOUT_S=60 bash scripts/smoke.sh
#
# Exit code:
#   0 = stack healthy, ping job ran end-to-end
#   1 = any step failed (with diagnostics on stderr)
#
# Designed to fit comfortably in CI: no fixtures persisted, no DB writes
# beyond the ping JobEvent, completes in <90s when the stack is up.

set -euo pipefail

API_URL="${PROTEA_API_URL:-http://127.0.0.1:8000}"
TIMEOUT_S="${PROTEA_SMOKE_TIMEOUT_S:-30}"

GREEN="\033[32m"; RED="\033[31m"; CYAN="\033[36m"; BOLD="\033[1m"; RESET="\033[0m"

_log() {
    printf "${CYAN}[smoke]${RESET} %s\n" "$*"
}
_ok() {
    printf "  ${GREEN}✓${RESET} %s\n" "$*"
}
_fail() {
    printf "  ${RED}✗${RESET} %s\n" "$*" >&2
    exit 1
}

# Ensure curl + jq are present.
command -v curl >/dev/null || _fail "curl not found"
command -v jq >/dev/null || _fail "jq not found (sudo apt install jq, or brew install jq)"

# 1. Health
_log "1/5  GET ${API_URL}/health"
if ! curl -sSf -o /dev/null -w "%{http_code}" "${API_URL}/health" | grep -q "^200$"; then
    _fail "/health did not return 200"
fi
_ok "/health is 200"

# 2. Readiness (DB + AMQP)
_log "2/5  GET ${API_URL}/health/ready"
ready=$(curl -sS "${API_URL}/health/ready" || echo '{}')
if [[ "$(echo "$ready" | jq -r '.status // empty')" != "ready" ]]; then
    _fail "/health/ready did not report ready: $ready"
fi
_ok "/health/ready is ready (db + amqp up)"

# 3. Submit ping job
_log "3/5  POST ${API_URL}/jobs (operation=ping)"
job_response=$(curl -sSf -X POST "${API_URL}/jobs" \
    -H "Content-Type: application/json" \
    -d '{"operation":"ping","queue_name":"protea.ping","payload":{"smoke":true}}')
job_id=$(echo "$job_response" | jq -r '.id // empty')
if [[ -z "$job_id" ]]; then
    _fail "no job id returned: $job_response"
fi
_ok "ping job submitted (id=$job_id)"

# 4. Poll until succeeded or timeout. Status is lowercase in the API.
_log "4/5  poll ${API_URL}/jobs/${job_id} until succeeded (timeout ${TIMEOUT_S}s)"
deadline=$(( $(date +%s) + TIMEOUT_S ))
status=""
while [[ $(date +%s) -lt $deadline ]]; do
    job=$(curl -sSf "${API_URL}/jobs/${job_id}")
    status=$(echo "$job" | jq -r '.status // empty' | tr '[:upper:]' '[:lower:]')
    case "$status" in
        succeeded) break ;;
        failed|cancelled) _fail "ping job ended in $status: $(echo "$job" | jq -c '.error_code // empty,.error_message // empty')" ;;
    esac
    sleep 1
done
if [[ "$status" != "succeeded" ]]; then
    _fail "ping job did not reach succeeded within ${TIMEOUT_S}s (last status=$status)"
fi
_ok "ping job succeeded"

# 5. Verify events log
_log "5/5  GET ${API_URL}/jobs/${job_id}/events"
events=$(curl -sSf "${API_URL}/jobs/${job_id}/events")
n_events=$(echo "$events" | jq 'length')
has_succeeded=$(echo "$events" | jq '[.[] | select(.event == "job.succeeded")] | length')
if [[ "$n_events" -lt 1 || "$has_succeeded" -lt 1 ]]; then
    _fail "events log incomplete (n=$n_events, succeeded=$has_succeeded)"
fi
_ok "events log: $n_events events, includes job.succeeded"

printf "\n${GREEN}${BOLD}smoke ok${RESET} — stack healthy and end-to-end path works\n"
