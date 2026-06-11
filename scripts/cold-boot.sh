#!/usr/bin/env bash
# scripts/cold-boot.sh - one-shot bring-up of PROTEA from a powered-off box.
#
# deploy.sh is the canonical deployer (sync ref + poetry install + GPU torch
# flip + frontend/Sphinx build + manage.sh start + health wait), but it
# assumes the infrastructure containers are already running. After a clean
# shutdown the postgres / rabbitmq / minio containers exit and nobody
# restarts them, and the public tunnel is gone. cold-boot.sh wraps the three
# pieces that a cold box needs, in order, without duplicating any of
# deploy.sh's logic:
#
#   1. start the infra containers (docker start + wait healthy)
#   2. delegate the whole app bring-up to scripts/deploy.sh
#   3. launch the public ngrok tunnels (detached)
#
# Usage:
#   bash scripts/cold-boot.sh                   full bring-up from origin/develop
#   bash scripts/cold-boot.sh --fast            skip poetry install + frontend
#                                               build (nothing changed since
#                                               last boot; maps to deploy.sh
#                                               --no-deps --no-build)
#   bash scripts/cold-boot.sh --no-ngrok        skip the public tunnel
#   bash scripts/cold-boot.sh <ref> [flags]     bring up a specific git ref;
#                                               unknown flags pass through to
#                                               scripts/deploy.sh verbatim
#
# Env overrides (shared with deploy.sh):
#   PROTEA_DEPLOY_PATH   deploy worktree (default ~/Thesis2/worktrees/protea-deploy)
#   PROTEA_DEPLOY_GPU    auto|1|0 GPU torch flip (default auto via nvidia-smi)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INFRA=(protea-postgres-1 protea-rabbitmq-1 protea-minio-1)

C_RESET=$'\e[0m'; C_BOLD=$'\e[1m'; C_GREEN=$'\e[32m'; C_YELLOW=$'\e[33m'; C_RED=$'\e[31m'
step() { printf "\n${C_BOLD}==> %s${C_RESET}\n" "$*"; }
ok()   { printf "  ${C_GREEN}OK${C_RESET} %s\n" "$*"; }
warn() { printf "  ${C_YELLOW}WARN${C_RESET} %s\n" "$*"; }
die()  { printf "  ${C_RED}FAIL${C_RESET} %s\n" "$*" >&2; exit 1; }

# --- parse our own flags; everything else is forwarded to deploy.sh ---------
DO_NGROK=1
DEPLOY_ARGS=()
for arg in "$@"; do
  case "$arg" in
    --fast)      DEPLOY_ARGS+=(--no-deps --no-build);;
    --no-ngrok)  DO_NGROK=0;;
    *)           DEPLOY_ARGS+=("$arg");;
  esac
done

# --- 1. infra containers ----------------------------------------------------
step "Infra containers (postgres + rabbitmq + minio)"
docker info >/dev/null 2>&1 || die "docker daemon unreachable -- sudo systemctl start docker"
for c in "${INFRA[@]}"; do
  [[ -n "$(docker ps -aq -f name="^${c}$")" ]] \
    || die "$c missing. Recreate: docker compose --profile storage up -d postgres rabbitmq minio"
done
docker start "${INFRA[@]}" >/dev/null
for _ in $(seq 1 45); do
  all_ok=1
  for c in "${INFRA[@]}"; do
    [[ "$(docker inspect -f '{{.State.Health.Status}}' "$c" 2>/dev/null)" == "healthy" ]] || all_ok=0
  done
  [[ $all_ok -eq 1 ]] && break
  sleep 2
done
for c in "${INFRA[@]}"; do
  st=$(docker inspect -f '{{.State.Health.Status}}' "$c" 2>/dev/null || echo "?")
  if [[ "$st" == "healthy" ]]; then
    ok "$c healthy"
  else
    die "$c not healthy ($st) -- docker logs $c"
  fi
done

# --- 2. app bring-up (delegated to deploy.sh) -------------------------------
step "App bring-up (scripts/deploy.sh ${DEPLOY_ARGS[*]:-})"
bash "$HERE/scripts/deploy.sh" "${DEPLOY_ARGS[@]}"

# --- 3. public tunnel -------------------------------------------------------
if [[ "$DO_NGROK" == "1" ]]; then
  step "Public tunnel (ngrok)"
  if ! command -v ngrok >/dev/null 2>&1; then
    warn "ngrok not on PATH -- skipping tunnel"
  elif pgrep -x ngrok >/dev/null 2>&1; then
    ok "ngrok already running"
  else
    nohup ngrok start --all --log=stdout >/tmp/ngrok.log 2>&1 &
    disown
    for _ in $(seq 1 10); do
      curl -sf http://localhost:4040/api/tunnels >/dev/null 2>&1 && break
      sleep 1
    done
    if curl -sf http://localhost:4040/api/tunnels >/dev/null 2>&1; then
      ok "ngrok up -> https://protea.ngrok.app (log: /tmp/ngrok.log)"
    else
      warn "ngrok started but API :4040 not answering yet -- check /tmp/ngrok.log"
    fi
  fi
fi

printf '\n%b\n' "${C_BOLD}${C_GREEN}=== cold-boot complete ===${C_RESET}"
printf "  Frontend  http://localhost:3000\n"
printf "  API       http://localhost:8000  (public: https://protea.ngrok.app)\n"
