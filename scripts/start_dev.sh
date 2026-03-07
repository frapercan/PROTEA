#!/usr/bin/env bash
# scripts/start_dev.sh
# Start the full PROTEA dev stack: API + workers + frontend.
# Run from the repository root: bash scripts/start_dev.sh
set -e

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$ROOT/logs"
mkdir -p "$LOG_DIR"

echo "=== PROTEA dev stack ==="

# ── Kill previous processes ──────────────────────────────────────────────────
echo "[1/5] Stopping previous processes..."
pkill -f "uvicorn protea" 2>/dev/null && echo "  API stopped" || true
pkill -f "scripts/worker.py" 2>/dev/null && echo "  Workers stopped" || true
pkill -f "next dev" 2>/dev/null && echo "  Frontend stopped" || true
sleep 2

# ── API ──────────────────────────────────────────────────────────────────────
echo "[2/5] Starting API (port 8000)..."
cd "$ROOT"
poetry run uvicorn protea.api.app:create_app \
    --factory --host 0.0.0.0 --port 8000 --reload \
    > "$LOG_DIR/api.log" 2>&1 &
sleep 3
curl -sf http://localhost:8000/jobs > /dev/null && echo "  API OK" || echo "  API FAILED — check logs/api.log"

# ── Worker: protea.ping ───────────────────────────────────────────────────────
echo "[3/5] Starting worker: protea.ping..."
poetry run python scripts/worker.py --queue protea.ping \
    > "$LOG_DIR/worker-ping.log" 2>&1 &
sleep 1
echo "  Worker ping started (PID $!)"

# ── Worker: protea.jobs ───────────────────────────────────────────────────────
echo "[4/5] Starting worker: protea.jobs..."
poetry run python scripts/worker.py --queue protea.jobs \
    > "$LOG_DIR/worker-jobs.log" 2>&1 &
sleep 1
echo "  Worker jobs started (PID $!)"

# ── Frontend ──────────────────────────────────────────────────────────────────
echo "[5/5] Starting frontend (port 3000)..."
cd "$ROOT/apps/web"
npm run dev > "$LOG_DIR/frontend.log" 2>&1 &
sleep 6
curl -sf http://localhost:3000 -o /dev/null && echo "  Frontend OK" || echo "  Frontend FAILED — check logs/frontend.log"

echo ""
echo "=== Stack running ==="
echo "  Frontend  → http://localhost:3000"
echo "  API       → http://localhost:8000"
echo "  RabbitMQ  → http://localhost:15672  (guest/guest)"
echo "  Logs      → $LOG_DIR/"
echo ""
echo "Stop all:  pkill -f 'uvicorn protea|scripts/worker.py|next dev'"
