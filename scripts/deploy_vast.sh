#!/usr/bin/env bash
# scripts/deploy_vast.sh — Push code updates to a running vast.ai instance
#
# Usage:
#   bash scripts/deploy_vast.sh <IP> <SSH_PORT> [BATCH_WORKERS]
#
# Examples:
#   bash scripts/deploy_vast.sh 173.206.147.184 41624
#   bash scripts/deploy_vast.sh 173.206.147.184 41624 2
#
# What it does:
#   1. rsync code to /root/PROTEA (excludes venvs, node_modules, logs, local config)
#   2. poetry install --without dev (only if pyproject.toml changed)
#   3. npm install (only if package.json changed)
#   4. alembic upgrade head
#   5. restart the full PROTEA stack

set -euo pipefail

IP="${1:?Usage: deploy_vast.sh <IP> <SSH_PORT> [BATCH_WORKERS]}"
PORT="${2:?Usage: deploy_vast.sh <IP> <SSH_PORT> [BATCH_WORKERS]}"
BATCH_WORKERS="${3:-1}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SSH="ssh -p $PORT root@$IP"
GREEN="\033[32m"; YELLOW="\033[33m"; BOLD="\033[1m"; RESET="\033[0m"
step() { printf "\n${BOLD}==> %s${RESET}\n" "$*"; }
ok()   { printf "  ${GREEN}✓${RESET} %s\n" "$*"; }
warn() { printf "  ${YELLOW}⚠${RESET}  %s\n" "$*"; }

# ── 0. Verify SSH connectivity ─────────────────────────────────────────────────
step "Checking SSH connectivity"
if ! $SSH "echo ok" &>/dev/null; then
    printf "${BOLD}ERROR${RESET}: Cannot reach root@$IP on port $PORT\n"
    printf "  Is the instance running? Check: vastai show instances\n"
    exit 1
fi
ok "Connected to $IP:$PORT"

# ── 1. Sync code ───────────────────────────────────────────────────────────────
step "Syncing code → /root/PROTEA"

rsync -az --delete \
    --exclude='.git/' \
    --exclude='__pycache__/' \
    --exclude='*.pyc' \
    --exclude='*.egg-info/' \
    --exclude='.venv/' \
    --exclude='logs/' \
    --exclude='node_modules/' \
    --exclude='.next/' \
    --exclude='storage/' \
    --exclude='protea/config/system.yaml' \
    --exclude='apps/web/.env.local' \
    -e "ssh -p $PORT" \
    "$ROOT/" "root@$IP:/root/PROTEA/"

ok "Code synced"

# ── 2. Install Python deps (only if pyproject.toml changed) ───────────────────
step "Installing Python dependencies"
$SSH "cd /root/PROTEA && export PATH=\$HOME/.local/bin:\$PATH && poetry install --without dev"
ok "Python deps up to date"

# ── 3. Install frontend deps (only if package.json changed) ───────────────────
step "Installing frontend dependencies"
$SSH "cd /root/PROTEA/apps/web && npm install --silent"
ok "Frontend deps up to date"

# ── 4. Run Alembic migrations ──────────────────────────────────────────────────
step "Running database migrations"
$SSH "cd /root/PROTEA && export PATH=\$HOME/.local/bin:\$PATH && poetry run alembic upgrade head"
ok "Schema up to date"

# ── 5. Restart stack ───────────────────────────────────────────────────────────
step "Restarting PROTEA stack ($BATCH_WORKERS batch worker(s))"
$SSH "cd /root/PROTEA && export PATH=\$HOME/.local/bin:\$PATH && bash scripts/manage.sh start $BATCH_WORKERS"
ok "Stack restarted"

# ── Done ───────────────────────────────────────────────────────────────────────
FRONTEND_PORT=$($SSH "vastai show instance --raw 2>/dev/null | python3 -c \"import sys,json; p=json.load(sys.stdin).get('ports',{}); print(p.get('3000/tcp',[{'HostPort':'3000'}])[0]['HostPort'])\" 2>/dev/null || echo '3000'")

printf "\n${BOLD}╔══════════════════════════════════════════════════╗${RESET}\n"
printf "${BOLD}║           PROTEA deployed successfully           ║${RESET}\n"
printf "${BOLD}╚══════════════════════════════════════════════════╝${RESET}\n\n"
printf "  Logs:    $SSH 'bash /root/PROTEA/scripts/manage.sh logs'\n"
printf "  Status:  $SSH 'bash /root/PROTEA/scripts/manage.sh status'\n\n"
