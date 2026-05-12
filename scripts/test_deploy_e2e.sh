#!/usr/bin/env bash
# scripts/test_deploy_e2e.sh - end-to-end deploy smoke test for CI.
#
# Brings up a minimal slice of the dev docker-compose stack
# (postgres + rabbitmq + migrate + api + worker-jobs), waits for the
# API to report healthy, runs a small probe sequence against the
# canonical /v1 surface, then tears the stack down. Exits non-zero on
# any failure with the offending service's logs dumped to stderr so
# CI artifacts capture the diagnosis automatically.
#
# Designed to catch regressions of the class fixed by PR #324
# (workers failing to start because the protea root package is not
# installed in the image): if migrate or worker-jobs cannot import
# their entrypoint the readiness probe never goes green and the
# script aborts with the failing container's logs attached.
#
# Usage:
#   bash scripts/test_deploy_e2e.sh                # build + run + smoke + tear down
#   PROTEA_E2E_KEEP_STACK=1 bash scripts/test_deploy_e2e.sh   # leave stack up
#
# Env overrides:
#   PROTEA_E2E_PROJECT     compose project name (default: protea-e2e)
#   PROTEA_E2E_API_URL     API base for smoke (default: http://127.0.0.1:8000)
#   PROTEA_E2E_READY_TIMEOUT_S  readiness wait window (default: 180)
#   PROTEA_E2E_JOB_TIMEOUT_S    ping job wait window (default: 60)
#   PROTEA_E2E_KEEP_STACK  if set to 1, skip the tear-down step
#
# The script is intentionally idempotent: re-running it from a dirty
# previous attempt is safe. The first thing it does is tear any
# leftover project state down.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

PROJECT="${PROTEA_E2E_PROJECT:-protea-e2e}"
API_URL="${PROTEA_E2E_API_URL:-http://127.0.0.1:8000}"
READY_TIMEOUT_S="${PROTEA_E2E_READY_TIMEOUT_S:-180}"
JOB_TIMEOUT_S="${PROTEA_E2E_JOB_TIMEOUT_S:-60}"
KEEP_STACK="${PROTEA_E2E_KEEP_STACK:-0}"

# Minimal service slice. Keeping the worker set tiny (just worker-jobs)
# is enough to exercise the path PR #324 fixed: if the image cannot
# import protea, the worker exits and migrate / api / worker-jobs all
# stay un-healthy.
SERVICES=(postgres rabbitmq migrate api worker-jobs)

COMPOSE=(docker compose -p "$PROJECT" -f docker-compose.yml -f docker-compose.e2e.yml)

GREEN=$'\e[32m'; RED=$'\e[31m'; CYAN=$'\e[36m'; BOLD=$'\e[1m'; RESET=$'\e[0m'

log()  { printf "${CYAN}[e2e]${RESET} %s\n" "$*"; }
ok()   { printf "  ${GREEN}✓${RESET} %s\n" "$*"; }
warn() { printf "  ${RED}!${RESET} %s\n" "$*" >&2; }

dump_logs() {
    warn "dumping container logs (last 200 lines each):"
    for svc in "${SERVICES[@]}"; do
        printf "\n${BOLD}--- %s ---${RESET}\n" "$svc" >&2
        "${COMPOSE[@]}" logs --tail=200 --no-color "$svc" 2>&1 >&2 || true
    done
}

tear_down() {
    if [[ "$KEEP_STACK" == "1" ]]; then
        log "PROTEA_E2E_KEEP_STACK=1 (leaving stack up for inspection)"
        return
    fi
    log "tearing stack down (-v to drop volumes)"
    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}

fail() {
    warn "$1"
    dump_logs
    tear_down
    exit 1
}

# Pre-flight: drop any leftover state from a prior aborted run.
log "pre-flight: clean any leftover state for project=$PROJECT"
"${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true

# 1. Build the local image. This is where PR #324-class regressions
#    surface: a Dockerfile that does not install the protea root
#    package will still build, but step 4 will then never go green.
log "1/5  building protea image"
"${COMPOSE[@]}" build "${SERVICES[@]}" >/dev/null
ok "image built"

# 2. Start the slice in detached mode. Auth is flipped off so the
#    probe can POST /jobs without provisioning an API key.
log "2/5  bringing up: ${SERVICES[*]}"
"${COMPOSE[@]}" up -d "${SERVICES[@]}"
ok "containers started"

# 3. Wait for /health/ready (DB + AMQP both connected from the API).
#    The migrate one-shot must succeed before api comes up, so a slow
#    or broken migration also surfaces here as a readiness timeout.
log "3/5  waiting for ${API_URL}/health/ready (timeout ${READY_TIMEOUT_S}s)"
deadline=$(( $(date +%s) + READY_TIMEOUT_S ))
ready=0
while [[ $(date +%s) -lt $deadline ]]; do
    if curl -sf "${API_URL}/health/ready" 2>/dev/null | grep -q '"status":"ready"'; then
        ready=1
        break
    fi
    sleep 3
done
[[ "$ready" == "1" ]] || fail "/health/ready never reported ready within ${READY_TIMEOUT_S}s"
ok "/health/ready is ready"

# 4. Canonical /v1 surface probes. The task spec calls these out
#    explicitly: a working deploy must serve both endpoints.
log "4/5  probing /v1 surface"
code=$(curl -s -o /dev/null -w "%{http_code}" "${API_URL}/v1/health")
[[ "$code" == "200" ]] || fail "/v1/health returned ${code}, want 200"
ok "/v1/health is 200"

code=$(curl -s -o /dev/null -w "%{http_code}" "${API_URL}/v1/jobs?limit=1")
[[ "$code" == "200" ]] || fail "/v1/jobs returned ${code}, want 200"
ok "/v1/jobs is 200"

# 5. Ping job through worker-jobs. Submitting to protea.jobs (instead
#    of protea.ping, which has no worker in docker-compose.yml) keeps
#    the slice small while still validating the publish → consume →
#    DB-update round trip, exactly the path that breaks when the
#    worker image cannot import protea.
log "5/5  submitting ping job to protea.jobs"
job_response=$(curl -sSf -X POST "${API_URL}/v1/jobs" \
    -H "Content-Type: application/json" \
    -d '{"operation":"ping","queue_name":"protea.jobs","payload":{"e2e":true}}' \
    || fail "POST /v1/jobs failed")
job_id=$(printf '%s' "$job_response" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("id",""))')
[[ -n "$job_id" ]] || fail "no job id returned: $job_response"
ok "ping job submitted (id=$job_id)"

deadline=$(( $(date +%s) + JOB_TIMEOUT_S ))
status=""
while [[ $(date +%s) -lt $deadline ]]; do
    job=$(curl -sSf "${API_URL}/v1/jobs/${job_id}" || echo '{}')
    status=$(printf '%s' "$job" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("status","") or "")' | tr '[:upper:]' '[:lower:]')
    case "$status" in
        succeeded) break ;;
        failed|cancelled) fail "ping job ended in $status: $job" ;;
    esac
    sleep 2
done
[[ "$status" == "succeeded" ]] || fail "ping job did not reach succeeded in ${JOB_TIMEOUT_S}s (last status=$status)"
ok "ping job succeeded"

tear_down
printf "\n${GREEN}${BOLD}e2e ok${RESET} (deploy slice built, came up green, and ran a ping end to end)\n"
