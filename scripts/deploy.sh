#!/usr/bin/env bash
# scripts/deploy.sh - deploy any git ref (or local snapshot) of PROTEA to a
# fixed worktree, decoupled from the dev tree.
#
# The dev tree (~/Thesis2/repositories/PROTEA) can be on any branch and a
# parallel agent can move it freely. This script targets a separate slot
# (default ~/Thesis2/worktrees/protea-deploy) and re-deploys it explicitly
# from a chosen ref or from a local folder snapshot.
#
# Usage:
#   bash scripts/deploy.sh                       # update slot to origin/develop
#   bash scripts/deploy.sh <ref>                 # any branch / tag / sha
#   bash scripts/deploy.sh --from <path>         # copy a local folder snapshot
#   bash scripts/deploy.sh --status              # show what is currently deployed
#   bash scripts/deploy.sh --stop                # stop the deploy stack
#   bash scripts/deploy.sh --no-build            # skip frontend rebuild (dev iteration)
#   bash scripts/deploy.sh --no-deps             # skip poetry install (deps unchanged)
#
# Env overrides:
#   PROTEA_DEPLOY_PATH   target worktree path (default: ~/Thesis2/worktrees/protea-deploy)
#   PROTEA_DEPLOY_REF    default ref when none given (default: origin/develop)
#   PROTEA_DEPLOY_GPU    auto|1|0 (default auto = nvidia-smi -L decides). When the
#                        host has GPU drivers loaded, torch is flipped to the
#                        cu121 wheel after poetry install via install_gpu_torch.sh.
#                        CI / slim Docker hosts have no nvidia-smi → stays CPU.

set -euo pipefail

DEPLOY_PATH="${PROTEA_DEPLOY_PATH:-$HOME/Thesis2/worktrees/protea-deploy}"
DEFAULT_REF="${PROTEA_DEPLOY_REF:-origin/develop}"

REF=""
FROM_PATH=""
ACTION="deploy"
DO_BUILD=1
DO_DEPS=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --from)       FROM_PATH="$2"; shift 2;;
    --status)     ACTION="status"; shift;;
    --stop)       ACTION="stop"; shift;;
    --no-build)   DO_BUILD=0; shift;;
    --no-deps)    DO_DEPS=0; shift;;
    --help|-h)    ACTION="help"; shift;;
    -*)           echo "unknown flag: $1" >&2; exit 2;;
    *)            REF="$1"; shift;;
  esac
done

[[ -z "$REF" ]] && REF="$DEFAULT_REF"

C_RESET=$'\e[0m'; C_BOLD=$'\e[1m'; C_GREEN=$'\e[32m'; C_YELLOW=$'\e[33m'; C_RED=$'\e[31m'

show_help() {
  sed -n 's/^# \{0,1\}//;3,21p' "$0"
}

[[ "$ACTION" == "help" ]] && { show_help; exit 0; }

if [[ ! -d "$DEPLOY_PATH" ]]; then
  echo "${C_RED}deploy slot $DEPLOY_PATH does not exist${C_RESET}" >&2
  echo "create it once with:" >&2
  echo "  git worktree add $DEPLOY_PATH -b feat/deploy-tooling origin/develop" >&2
  exit 1
fi

show_status() {
  cd "$DEPLOY_PATH"
  echo "${C_BOLD}deploy slot:${C_RESET} $DEPLOY_PATH"
  if [[ -f .deploy/from.txt ]]; then
    echo "${C_BOLD}source:${C_RESET}      local snapshot from $(cat .deploy/from.txt)"
  else
    local sha branch
    sha=$(git rev-parse --short HEAD 2>/dev/null || echo "?")
    branch=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "?")
    echo "${C_BOLD}source:${C_RESET}      git $branch @ $sha"
  fi
  echo "${C_BOLD}stack:${C_RESET}"
  if curl -sf -o /dev/null http://localhost:8000/health; then
    echo "  api      ${C_GREEN}up${C_RESET}      http://localhost:8000"
  else
    echo "  api      ${C_YELLOW}down${C_RESET}"
  fi
  if curl -sf -o /dev/null http://localhost:3000; then
    echo "  frontend ${C_GREEN}up${C_RESET}      http://localhost:3000"
  else
    echo "  frontend ${C_YELLOW}down${C_RESET}"
  fi
}

stop_stack() {
  cd "$DEPLOY_PATH"
  if [[ -x scripts/manage.sh ]]; then
    bash scripts/manage.sh stop || true
  else
    pkill -9 -f "uvicorn protea.api"  2>/dev/null || true
    pkill -9 -f "next-server"          2>/dev/null || true
  fi
}

case "$ACTION" in
  status) show_status; exit 0;;
  stop)   stop_stack;  exit 0;;
esac

cd "$DEPLOY_PATH"

stop_stack

if [[ -n "$FROM_PATH" ]]; then
  FROM_PATH=$(cd "$FROM_PATH" && pwd)
  echo "${C_BOLD}rsync from local snapshot:${C_RESET} $FROM_PATH"
  mkdir -p .deploy
  echo "$FROM_PATH @ $(date -Is)" >.deploy/from.txt
  rsync -a --delete \
    --exclude='.git/' \
    --exclude='.deploy/' \
    --exclude='node_modules/' \
    --exclude='.next/' \
    --exclude='__pycache__/' \
    --exclude='*.pyc' \
    --exclude='logs/' \
    --exclude='.pytest_cache/' \
    --exclude='.ruff_cache/' \
    --exclude='.mypy_cache/' \
    "$FROM_PATH"/ ./
else
  rm -f .deploy/from.txt
  echo "${C_BOLD}fetch + checkout:${C_RESET} $REF"
  git fetch --quiet origin
  git checkout --force "$REF" 2>&1 | tail -1
fi

if [[ "$DO_DEPS" == "1" ]]; then
  echo "${C_BOLD}poetry install...${C_RESET}"
  poetry install --quiet --no-root || poetry install --no-root

  # pyproject pins torch to the pytorch-cpu source so CI runners and the
  # slim Docker image do not pull the ~6 GB NVIDIA / triton stack. Hosts
  # that actually have GPU drivers loaded need the cu121 wheels post
  # install. Auto-detect via nvidia-smi; PROTEA_DEPLOY_GPU=0/1 forces.
  want_gpu="${PROTEA_DEPLOY_GPU:-auto}"
  if [[ "$want_gpu" == "auto" ]]; then
    if command -v nvidia-smi >/dev/null 2>&1 && nvidia-smi -L >/dev/null 2>&1; then
      want_gpu=1
    else
      want_gpu=0
    fi
  fi
  if [[ "$want_gpu" == "1" ]]; then
    echo "${C_BOLD}flipping torch to CUDA wheel${C_RESET}"
    bash scripts/install_gpu_torch.sh
  fi
fi

# Always rebuild Sphinx HTML for PROTEA + sibling repos; the cost is
# only a few seconds per repo and keeps /docs/<slug>/ in lockstep with
# the deployed source. --no-build skips the frontend build (npm) but
# still produces docs because the API mounts them dynamically and a
# stale doc set is visible to end users via the stack page.
#
# build_docs.py reads SIBLINGS_DIR. We default it to a frozen worktree
# directory so the docs are built against origin/develop of every
# sibling, not whatever feature branch happens to be checked out in
# ~/Thesis2/repositories/<slug>/ (where other agents may have WIP).
# The loop's protea_refresh_siblings.sh keeps that directory on tip.
default_siblings="$HOME/Thesis2/worktrees/_siblings"
export SIBLINGS_DIR="${PROTEA_SIBLINGS_DIR:-${SIBLINGS_DIR:-$default_siblings}}"
echo "${C_BOLD}building docs${C_RESET} (siblings_dir=$SIBLINGS_DIR)"
poetry run task docs 2>&1 | tail -10 || echo "${C_YELLOW}docs build failed (non-fatal)${C_RESET}"

if [[ "$DO_BUILD" == "1" ]]; then
  # apps/web/.env.local is gitignored. Seed a default if missing so the
  # build picks up NEXT_PUBLIC_API_URL. Default to a same-origin proxy
  # path that Next rewrites server-side; override via PROTEA_PUBLIC_API_URL.
  local_env="apps/web/.env.local"
  if [[ ! -f "$local_env" ]]; then
    api_url="${PROTEA_PUBLIC_API_URL:-/api-proxy}"
    echo "${C_BOLD}seeding $local_env${C_RESET} (NEXT_PUBLIC_API_URL=$api_url)"
    printf 'NEXT_PUBLIC_API_URL=%s\n' "$api_url" >"$local_env"
  fi
  echo "${C_BOLD}npm install + build...${C_RESET}"
  ( cd apps/web && npm ci --silent && npm run build ) | tail -5
fi

echo "${C_BOLD}starting stack...${C_RESET}"
bash scripts/manage.sh start

sleep 3
echo
show_status
