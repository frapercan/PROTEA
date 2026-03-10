#!/usr/bin/env bash
# scripts/stop_dev.sh
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PID_DIR="$ROOT/logs/pids"

echo "=== Stopping PROTEA dev stack ==="
for f in "$PID_DIR"/*.pid; do
    [[ -e "$f" ]] || continue
    name=$(basename "$f" .pid)
    pid=$(cat "$f")
    if kill -0 "$pid" 2>/dev/null; then
        kill -9 "$pid" && echo "  $name stopped (PID $pid)"
    else
        echo "  $name already stopped"
    fi
    rm -f "$f"
done
echo "Done."
