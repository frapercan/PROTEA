#!/usr/bin/env bash
# scripts/start_dev.sh
# Start the full PROTEA dev stack: API + workers + frontend.
# Run from the repository root: bash scripts/start_dev.sh
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$ROOT/logs"
PID_DIR="$ROOT/logs/pids"
mkdir -p "$LOG_DIR" "$PID_DIR"

_stop_pid() {
    local name="$1" pidfile="$PID_DIR/$1.pid"
    if [[ -f "$pidfile" ]]; then
        local pid
        pid=$(cat "$pidfile")
        if kill -0 "$pid" 2>/dev/null; then
            kill -9 "$pid" 2>/dev/null && echo "  $name stopped (PID $pid)" || true
        fi
        rm -f "$pidfile"
    fi
}

_start_bg() {
    local name="$1"; shift
    "$@" &
    local pid=$!
    echo "$pid" > "$PID_DIR/$name.pid"
    echo "  $name started (PID $pid)"
}

echo "=== PROTEA dev stack ==="

# ── Stop previous processes (PID files first, then sweep) ────────────────────
echo "[1/5] Stopping previous processes..."
for f in "$PID_DIR"/*.pid; do
    [[ -e "$f" ]] && _stop_pid "$(basename "$f" .pid)"
done
# Safety net: kill any survivors not tracked by PID files
kill -9 $(pgrep -f "uvicorn protea.api" 2>/dev/null) 2>/dev/null || true
kill -9 $(pgrep -f "scripts/worker.py" 2>/dev/null) 2>/dev/null || true
kill -9 $(pgrep -f "next-server" 2>/dev/null) 2>/dev/null || true
sleep 1

# ── API ──────────────────────────────────────────────────────────────────────
echo "[2/5] Starting API (port 8000)..."
cd "$ROOT"
_start_bg api poetry run uvicorn protea.api.app:create_app \
    --factory --host 0.0.0.0 --port 8000 \
    >> "$LOG_DIR/api.log" 2>&1
sleep 3
curl -sf http://localhost:8000/jobs > /dev/null \
    && echo "  API OK" \
    || { echo "  API FAILED — check logs/api.log"; exit 1; }

# ── Worker: protea.ping ───────────────────────────────────────────────────────
echo "[3/5] Starting worker: protea.ping..."
_start_bg worker-ping poetry run python scripts/worker.py --queue protea.ping \
    >> "$LOG_DIR/worker-ping.log" 2>&1

# ── Worker: protea.jobs ───────────────────────────────────────────────────────
echo "[4/6] Starting worker: protea.jobs..."
_start_bg worker-jobs poetry run python scripts/worker.py --queue protea.jobs \
    >> "$LOG_DIR/worker-jobs.log" 2>&1

# ── Worker: protea.embeddings (coordinator) ───────────────────────────────────
echo "[5/7] Starting worker: protea.embeddings..."
_start_bg worker-embeddings-coord poetry run python scripts/worker.py --queue protea.embeddings \
    >> "$LOG_DIR/worker-embeddings-coord.log" 2>&1

# ── Worker: protea.embeddings.batch ──────────────────────────────────────────
echo "[6/7] Starting worker: protea.embeddings.batch..."
_start_bg worker-embeddings poetry run python scripts/worker.py --queue protea.embeddings.batch \
    >> "$LOG_DIR/worker-embeddings.log" 2>&1

# ── Frontend ──────────────────────────────────────────────────────────────────
echo "[7/7] Starting frontend (port 3000)..."
cd "$ROOT/apps/web"
_start_bg frontend npm run dev >> "$LOG_DIR/frontend.log" 2>&1
sleep 6
curl -sf http://localhost:3000 -o /dev/null \
    && echo "  Frontend OK" \
    || echo "  Frontend FAILED — check logs/frontend.log"

echo ""
echo "=== Stack running ==="
echo "  Frontend  → http://localhost:3000"
echo "  API       → http://localhost:8000"
echo "  RabbitMQ  → http://localhost:15672  (guest/guest)"
echo "  Logs      → $LOG_DIR/"
echo "  PIDs      → $PID_DIR/"
echo ""
echo "Stop all:  bash scripts/stop_dev.sh"
