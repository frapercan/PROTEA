#!/usr/bin/env bash
# scripts/sync_db_vast.sh — Dump local DB and restore it on a vast.ai instance
#
# Usage:
#   bash scripts/sync_db_vast.sh <IP> <SSH_PORT> [OPTIONS]
#
# Options:
#   --local-db      Local database name       (default: BioData)
#   --local-user    Local PostgreSQL user      (default: usuario)
#   --remote-db     Remote database name       (default: protea)
#   --remote-user   Remote PostgreSQL user     (default: protea)
#   --full-reset    Drop and recreate remote DB before restore (default: true)
#
# Examples:
#   bash scripts/sync_db_vast.sh 173.206.147.184 41624
#   bash scripts/sync_db_vast.sh 173.206.147.184 41624 --full-reset

set -euo pipefail

IP="${1:?Usage: sync_db_vast.sh <IP> <SSH_PORT>}"
PORT="${2:?Usage: sync_db_vast.sh <IP> <SSH_PORT>}"
shift 2

# Defaults
LOCAL_DB="BioData"
LOCAL_USER="usuario"
REMOTE_DB="protea"
REMOTE_USER="protea"
FULL_RESET=true

while [[ $# -gt 0 ]]; do
    case "$1" in
        --local-db)    LOCAL_DB="$2";    shift 2 ;;
        --local-user)  LOCAL_USER="$2";  shift 2 ;;
        --remote-db)   REMOTE_DB="$2";   shift 2 ;;
        --remote-user) REMOTE_USER="$2"; shift 2 ;;
        --no-full-reset) FULL_RESET=false; shift ;;
        --full-reset)    FULL_RESET=true;  shift ;;
        *) printf "Unknown option: %s\n" "$1"; exit 1 ;;
    esac
done

SSH="ssh -p $PORT root@$IP"
DUMP_FILE="/tmp/protea_dump_$(date +%Y%m%d_%H%M%S).pgdump"

GREEN="\033[32m"; YELLOW="\033[33m"; RED="\033[31m"; BOLD="\033[1m"; RESET="\033[0m"
step() { printf "\n${BOLD}==> %s${RESET}\n" "$*"; }
ok()   { printf "  ${GREEN}✓${RESET} %s\n" "$*"; }
warn() { printf "  ${YELLOW}⚠${RESET}  %s\n" "$*"; }

# ── 0. Verify SSH ──────────────────────────────────────────────────────────────
step "Checking SSH connectivity"
if ! $SSH "echo ok" &>/dev/null; then
    printf "${RED}ERROR${RESET}: Cannot reach root@$IP on port $PORT\n"
    exit 1
fi
ok "Connected to $IP:$PORT"

# ── 1. Dump local DB ───────────────────────────────────────────────────────────
step "Dumping local database '$LOCAL_DB' → $DUMP_FILE"
pg_dump \
    --username="$LOCAL_USER" \
    --host=localhost \
    --port=5432 \
    --format=custom \
    --compress=9 \
    --no-privileges \
    --no-owner \
    "$LOCAL_DB" \
    > "$DUMP_FILE"

DUMP_SIZE=$(du -sh "$DUMP_FILE" | cut -f1)
ok "Dump complete ($DUMP_SIZE)"

# ── 2. Transfer to instance ────────────────────────────────────────────────────
step "Transferring dump to instance"
REMOTE_DUMP="/tmp/$(basename "$DUMP_FILE")"
rsync -az --progress -e "ssh -p $PORT" "$DUMP_FILE" "root@$IP:$REMOTE_DUMP"
ok "Transferred to $REMOTE_DUMP"

# ── 3. Stop the PROTEA stack (to avoid writes during restore) ─────────────────
step "Stopping PROTEA stack on instance"
$SSH "cd /root/PROTEA && export PATH=\$HOME/.local/bin:\$PATH && bash scripts/manage.sh stop 2>/dev/null || true"
ok "Stack stopped"

# ── 4. Restore on remote ───────────────────────────────────────────────────────
step "Restoring database on instance"

if [[ "$FULL_RESET" == "true" ]]; then
    warn "Full reset: dropping and recreating '$REMOTE_DB'"
    $SSH "su -c \"psql -c 'DROP DATABASE IF EXISTS $REMOTE_DB;'\" postgres"
    $SSH "su -c \"psql -c \\\"CREATE DATABASE $REMOTE_DB OWNER $REMOTE_USER;\\\"\" postgres"
    $SSH "su -c \"psql -d $REMOTE_DB -c 'CREATE EXTENSION IF NOT EXISTS vector;'\" postgres"
    ok "Database recreated"
fi

$SSH "export PGPASSWORD=protea && pg_restore \
    --username=$REMOTE_USER \
    --host=localhost \
    --port=5432 \
    --dbname=$REMOTE_DB \
    --no-privileges \
    --no-owner \
    --exit-on-error \
    $REMOTE_DUMP"
ok "Restore complete"

# ── 5. Run pending migrations (in case code is newer than dump) ────────────────
step "Running Alembic migrations (to apply any new schema changes)"
$SSH "cd /root/PROTEA && export PATH=\$HOME/.local/bin:\$PATH && poetry run alembic upgrade head"
ok "Schema up to date"

# ── 6. Restart stack ───────────────────────────────────────────────────────────
step "Restarting PROTEA stack"
$SSH "cd /root/PROTEA && export PATH=\$HOME/.local/bin:\$PATH && bash scripts/manage.sh start 1"
ok "Stack restarted"

# ── 7. Cleanup ────────────────────────────────────────────────────────────────
rm -f "$DUMP_FILE"
$SSH "rm -f $REMOTE_DUMP"
ok "Temporary dump files removed"

printf "\n${BOLD}╔══════════════════════════════════════════════════╗${RESET}\n"
printf "${BOLD}║        Database synced successfully              ║${RESET}\n"
printf "${BOLD}╚══════════════════════════════════════════════════╝${RESET}\n\n"
printf "  Source:  ${LOCAL_USER}@localhost/${LOCAL_DB}\n"
printf "  Target:  ${REMOTE_USER}@${IP}/${REMOTE_DB}\n\n"
