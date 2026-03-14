#!/usr/bin/env bash
# scripts/setup_vast.sh — Bootstrap PROTEA on a fresh vast.ai instance
#
# Usage (on the vast.ai instance, from the repo root):
#   bash scripts/setup_vast.sh [DB_PASSWORD] [BATCH_WORKERS]
#
# After running:
#   Frontend  →  http://<PUBLIC_IP>:3000
#   API       →  http://<PUBLIC_IP>:8000
#   RabbitMQ  →  http://<PUBLIC_IP>:15672  (guest/guest)

set -euo pipefail

DB_PASSWORD="${1:-protea}"
BATCH_WORKERS="${2:-1}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
GREEN="\033[32m"; YELLOW="\033[33m"; BOLD="\033[1m"; RESET="\033[0m"
step() { printf "\n${BOLD}==> %s${RESET}\n" "$*"; }
ok()   { printf "  ${GREEN}✓${RESET} %s\n" "$*"; }

# ── 0. Detect public IP ───────────────────────────────────────────────────────
step "Detecting public IP"
PUBLIC_IP=$(curl -sf https://ifconfig.me || curl -sf https://api.ipify.org || echo "127.0.0.1")
ok "Public IP: $PUBLIC_IP"

# ── 1. System packages + Python 3.12 ─────────────────────────────────────────
step "Installing system packages + Python 3.12"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq \
    curl wget gnupg lsb-release ca-certificates \
    build-essential git software-properties-common libpq-dev

if ! python3.12 --version &>/dev/null; then
    add-apt-repository -y ppa:deadsnakes/ppa
    apt-get update -qq
    apt-get install -y -qq python3.12 python3.12-dev python3.12-venv
fi
curl -sS https://bootstrap.pypa.io/get-pip.py | python3.12
ok "Python $(python3.12 --version)"

# ── 2. PostgreSQL 16 + pgvector ───────────────────────────────────────────────
step "Installing PostgreSQL 16 + pgvector"
if ! command -v psql &>/dev/null; then
    curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
        | gpg --dearmor -o /usr/share/keyrings/postgresql.gpg
    echo "deb [signed-by=/usr/share/keyrings/postgresql.gpg] \
https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
        > /etc/apt/sources.list.d/pgdg.list
    apt-get update -qq
    apt-get install -y -qq postgresql-16 postgresql-16-pgvector
fi
ok "PostgreSQL installed"

service postgresql start || pg_ctlcluster 16 main start
sleep 2

su -c "psql -tc \"SELECT 1 FROM pg_roles WHERE rolname='protea'\" | grep -q 1 || \
    psql -c \"CREATE USER protea WITH PASSWORD '${DB_PASSWORD}';\"" postgres
su -c "psql -tc \"SELECT 1 FROM pg_database WHERE datname='protea'\" | grep -q 1 || \
    psql -c \"CREATE DATABASE protea OWNER protea;\"" postgres
su -c "psql -d protea -c \"CREATE EXTENSION IF NOT EXISTS vector;\"" postgres
ok "Database 'protea' ready"

# ── 3. Erlang 26 + RabbitMQ ───────────────────────────────────────────────────
step "Installing Erlang 26 + RabbitMQ"
if ! command -v rabbitmqctl &>/dev/null; then
    # Erlang 26 from RabbitMQ's Cloudsmith repo (manual setup, no helper script)
    rm -f /usr/share/keyrings/rabbitmq-erlang.gpg /usr/share/keyrings/rabbitmq-server.gpg
    curl -fsSL https://github.com/rabbitmq/signing-keys/releases/download/3.0/cloudsmith.rabbitmq-erlang.E495BB49CC4BBE5B.key \
        -o /tmp/rabbitmq-erlang.key
    gpg --batch --no-tty --dearmor < /tmp/rabbitmq-erlang.key > /usr/share/keyrings/rabbitmq-erlang.gpg
    curl -fsSL https://github.com/rabbitmq/signing-keys/releases/download/3.0/cloudsmith.rabbitmq-server.9F4587F226208342.key \
        -o /tmp/rabbitmq-server.key
    gpg --batch --no-tty --dearmor < /tmp/rabbitmq-server.key > /usr/share/keyrings/rabbitmq-server.gpg
    cat > /etc/apt/sources.list.d/rabbitmq.list <<'APTEOF'
deb [arch=amd64 signed-by=/usr/share/keyrings/rabbitmq-erlang.gpg] https://ppa1.rabbitmq.com/rabbitmq/rabbitmq-erlang/deb/ubuntu jammy main
deb [arch=amd64 signed-by=/usr/share/keyrings/rabbitmq-server.gpg] https://ppa1.rabbitmq.com/rabbitmq/rabbitmq-server/deb/ubuntu jammy main
APTEOF
    apt-get update -qq
    # Pin RabbitMQ's Erlang over Ubuntu's older version
    apt-get install -y -qq -o Dpkg::Options::="--force-overwrite" \
        erlang-base erlang-asn1 erlang-crypto erlang-eldap \
        erlang-ftp erlang-inets erlang-mnesia erlang-os-mon erlang-parsetools \
        erlang-public-key erlang-runtime-tools erlang-snmp erlang-ssl \
        erlang-syntax-tools erlang-tftp erlang-tools erlang-xmerl
    apt-get install -y -qq rabbitmq-server
fi

rabbitmq-plugins enable rabbitmq_management
service rabbitmq-server start || rabbitmq-server -detached
sleep 3
ok "RabbitMQ running (UI on :15672)"

# ── 4. Node.js 20 ─────────────────────────────────────────────────────────────
step "Installing Node.js 20"
if ! node --version 2>/dev/null | grep -q "^v2[0-9]"; then
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
    apt-get install -y -qq nodejs
fi
ok "Node $(node --version)"

# ── 5. Poetry ─────────────────────────────────────────────────────────────────
step "Installing Poetry"
export PATH="$HOME/.local/bin:$PATH"
if ! command -v poetry &>/dev/null; then
    curl -sSL https://install.python-poetry.org | python3.12 -
    echo 'export PATH="$HOME/.local/bin:$PATH"' >> "$HOME/.bashrc"
fi
ok "Poetry $(poetry --version)"

# ── 6. Python dependencies ────────────────────────────────────────────────────
step "Installing Python dependencies (torch + ESM, ~10 min)"
cd "$ROOT"
poetry env use python3.12
poetry install --without dev
ok "Python deps installed"

# ── 7. Configure system.yaml ──────────────────────────────────────────────────
step "Writing protea/config/system.yaml"
mkdir -p "$ROOT/protea/config"
cat > "$ROOT/protea/config/system.yaml" <<EOF
database:
  url: postgresql+psycopg://protea:${DB_PASSWORD}@localhost:5432/protea

queue:
  amqp_url: amqp://guest:guest@localhost:5672/
EOF
ok "system.yaml written"

# ── 8. Alembic migrations ─────────────────────────────────────────────────────
step "Running database migrations"
cd "$ROOT"
poetry run alembic upgrade head
ok "Schema up to date"

# ── 9. Configure frontend ─────────────────────────────────────────────────────
step "Configuring frontend"
cd "$ROOT/apps/web"
cat > .env.local <<EOF
NEXT_PUBLIC_API_URL=http://${PUBLIC_IP}:8000
EOF
npm install --silent
ok ".env.local → http://${PUBLIC_IP}:8000"

# ── 10. Start PROTEA stack ────────────────────────────────────────────────────
step "Starting PROTEA stack ($BATCH_WORKERS batch worker(s))"
cd "$ROOT"
bash "$ROOT/scripts/manage.sh" start "$BATCH_WORKERS"

printf "\n${BOLD}╔══════════════════════════════════════════════════╗${RESET}\n"
printf "${BOLD}║           PROTEA on vast.ai — ready             ║${RESET}\n"
printf "${BOLD}╚══════════════════════════════════════════════════╝${RESET}\n\n"
printf "  Frontend   →  ${GREEN}http://${PUBLIC_IP}:3000${RESET}\n"
printf "  API        →  ${GREEN}http://${PUBLIC_IP}:8000${RESET}\n"
printf "  RabbitMQ   →  ${GREEN}http://${PUBLIC_IP}:15672${RESET}  (guest/guest)\n\n"
printf "  ${YELLOW}Ports needed open in vast.ai: 3000, 8000, 15672${RESET}\n\n"
printf "  Logs:  bash scripts/manage.sh logs\n"
printf "  Stop:  bash scripts/manage.sh stop\n\n"
