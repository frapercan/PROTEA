#!/usr/bin/env bash
# scripts/deploy_vast.sh — Deploy PROTEA to a vast.ai instance via Docker
#
# Usage:
#   bash scripts/deploy_vast.sh <IP> <SSH_PORT> [GHCR_TOKEN]
#
# Examples:
#   bash scripts/deploy_vast.sh 173.206.147.184 41624
#   bash scripts/deploy_vast.sh 173.206.147.184 41624 ghp_xxxxx
#
# What it does:
#   1. Sync docker-compose files to the remote (no source code needed)
#   2. Login to ghcr.io on the remote
#   3. Pull latest images from ghcr.io
#   4. Run migrations and restart the stack (migrate service runs automatically)
#
# Requirements on the remote:
#   - Docker with NVIDIA Container Toolkit (standard vast.ai images)

set -euo pipefail

IP="${1:?Usage: deploy_vast.sh <IP> <SSH_PORT> [GHCR_TOKEN]}"
PORT="${2:?Usage: deploy_vast.sh <IP> <SSH_PORT> [GHCR_TOKEN]}"
GHCR_TOKEN="${3:-${GITHUB_TOKEN:-}}"

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

# ── 1. Sync compose files (no source code needed) ─────────────────────────────
step "Syncing compose files → /root/PROTEA"
$SSH "mkdir -p /root/PROTEA/docker"
rsync -az -e "ssh -p $PORT" \
    "$ROOT/docker-compose.yml" \
    "$ROOT/docker-compose.prod.yml" \
    "root@$IP:/root/PROTEA/"
rsync -az -e "ssh -p $PORT" \
    "$ROOT/docker/init.sql" \
    "root@$IP:/root/PROTEA/docker/"
ok "Compose files synced"

# ── 2. Login to ghcr.io ───────────────────────────────────────────────────────
step "Logging in to ghcr.io"
if [[ -n "$GHCR_TOKEN" ]]; then
    $SSH "echo '$GHCR_TOKEN' | docker login ghcr.io -u frapercan --password-stdin"
    ok "Logged in to ghcr.io"
else
    warn "No GHCR_TOKEN provided — assuming images are public or already logged in"
fi

# ── 3. Pull latest images ─────────────────────────────────────────────────────
step "Pulling latest images from ghcr.io"
$SSH "cd /root/PROTEA && docker compose -f docker-compose.yml -f docker-compose.prod.yml pull"
ok "Images up to date"

# ── 4. Restart stack (migrate runs automatically before API/workers) ───────────
step "Restarting PROTEA stack"
$SSH "cd /root/PROTEA && docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d"
ok "Stack restarted"

# ── Done ──────────────────────────────────────────────────────────────────────
printf "\n${BOLD}╔══════════════════════════════════════════════════╗${RESET}\n"
printf "${BOLD}║           PROTEA deployed successfully           ║${RESET}\n"
printf "${BOLD}╚══════════════════════════════════════════════════╝${RESET}\n\n"
printf "  Logs:    $SSH 'cd /root/PROTEA && docker compose -f docker-compose.yml -f docker-compose.prod.yml logs -f'\n"
printf "  Status:  $SSH 'cd /root/PROTEA && docker compose -f docker-compose.yml -f docker-compose.prod.yml ps'\n\n"
