#!/usr/bin/env bash
# scripts/manage.sh — PROTEA dev stack manager
#
# Usage:
#   bash scripts/manage.sh start [N]   Start stack (N = embed+predict batch workers, default 1)
#   bash scripts/manage.sh stop        Stop all processes
#   bash scripts/manage.sh status      Show worker status table
#   bash scripts/manage.sh logs [name] Tail logs (no name = pick from menu)
#   bash scripts/manage.sh scale <queue> [N]  Add N extra workers to a queue (default 1)

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$ROOT/logs"
PID_DIR="$ROOT/logs/pids"

# ── colours ──────────────────────────────────────────────────────────────────
GREEN="\033[32m"; RED="\033[31m"; YELLOW="\033[33m"
CYAN="\033[36m"; BOLD="\033[1m"; RESET="\033[0m"

# ── env sourcing ──────────────────────────────────────────────────────────────
# A naive `manage.sh start` (no caller-side env sourcing) used to crash the API
# with `RuntimeError: PROTEA_JWT_SECRET not set` and leave the stack down
# (incident project_stack_env_not_sourced_outage). manage.sh now sources its own
# env so it is correct regardless of caller. `.env` is the canonical bundle (a
# symlink to ~/.secrets/protea.env in the deploy slot); `.env.local` holds any
# non-tracked per-host overrides and is sourced last so it wins. Both are
# optional: on hosts without them (CI, fresh clones) sourcing is a silent no-op.
_source_env() {
    local f
    for f in "$ROOT/.env" "$ROOT/.env.local"; do
        if [[ -f "$f" ]]; then
            printf "  ${CYAN}sourcing %s${RESET}\n" "$(basename "$f")"
            set -a
            # shellcheck disable=SC1090
            source "$f"
            set +a
        fi
    done
}

# ── helpers ───────────────────────────────────────────────────────────────────
_start_bg() {
    local name="$1"; shift
    mkdir -p "$LOG_DIR" "$PID_DIR"
    setsid "$@" >> "$LOG_DIR/${name}.log" 2>&1 &
    local pid=$!
    echo "$pid" > "$PID_DIR/${name}.pid"
    # Record the launch command so the watchdog can restart a dead worker
    # with the identical invocation (one arg per line, NUL-safe enough for
    # our fixed worker.py/poetry argv). Env vars (PORT/HOSTNAME) set inline by
    # the caller are NOT captured; only the api/frontend carry those and the
    # watchdog restarts them via their own dedicated paths, not from .cmd.
    printf '%s\n' "$@" > "$PID_DIR/${name}.cmd"
    printf "  ${GREEN}✓${RESET} %-35s PID %s\n" "$name" "$pid"
}

_stop_pid() {
    local name="$1" pidfile="$PID_DIR/$1.pid"
    if [[ -f "$pidfile" ]]; then
        local pid; pid=$(cat "$pidfile")
        if kill -0 "$pid" 2>/dev/null; then
            # Kill the whole process group (setsid guarantees PID == PGID)
            kill -15 -- -"$pid" 2>/dev/null || kill -15 "$pid" 2>/dev/null
            printf "  ${RED}✗${RESET} %-35s stopping (PID %s) — SIGTERM sent\n" "$name" "$pid"
        fi
        rm -f "$pidfile" "$PID_DIR/$name.cmd"
    fi
}

_worker_name() {
    # Generate a unique name for scaled workers: worker-<queue-slug>-<n>
    local queue="$1" n="${2:-1}"
    local slug="${queue//protea./}"; slug="${slug//./-}"
    echo "worker-${slug}-${n}"
}

_pid_rss_mb() {
    local pid="$1"
    awk '/VmRSS/{printf "%d", $2/1024}' "/proc/$pid/status" 2>/dev/null || echo "?"
}

# ── start ─────────────────────────────────────────────────────────────────────
cmd_start() {
    local BATCH_WORKERS="${1:-1}"

    printf "\n${BOLD}=== PROTEA dev stack (${BATCH_WORKERS} batch worker(s)) ===${RESET}\n\n"

    # Source env BEFORE touching the API so AUTHN_REQUIRED + JWT secret are set;
    # a naive caller that forgot to source .env must not crash the API.
    printf "${BOLD}[0] Environment${RESET}\n"
    _source_env

    # Stop survivors
    printf "${BOLD}[1] Stopping previous processes...${RESET}\n"
    for f in "$PID_DIR"/*.pid; do
        [[ -e "$f" ]] && _stop_pid "$(basename "$f" .pid)"
    done
    # Kill API and frontend (no long-running jobs, safe to force-kill)
    kill -9 $(pgrep -f "uvicorn protea.api" 2>/dev/null) 2>/dev/null || true
    kill -9 $(pgrep -f "next-server" 2>/dev/null) 2>/dev/null || true
    # Kill ALL worker.py processes, tracked or not. Leaving orphaned workers
    # from a previous start alive accumulates duplicate queue consumers (4x
    # observed), duplicated job processing, stale-code execution after a
    # redeploy, and unbounded RAM growth (each predictions.batch worker pins a
    # ~1 GB+ reference pool; three orphans reached ~45 GB). The fresh set takes
    # over the queues; any in-flight job is requeued at-least-once and the
    # reaper reclaims its expired lease. SIGTERM for a graceful drain, then
    # SIGKILL the stragglers. The "scripts/worker[.]py" char-class keeps this
    # command from matching its own argv.
    pkill -TERM -f "scripts/worker[.]py" 2>/dev/null || true
    for _ in 1 2 3; do
        pgrep -f "scripts/worker[.]py" >/dev/null 2>&1 || break
        sleep 1
    done
    pkill -KILL -f "scripts/worker[.]py" 2>/dev/null || true
    sleep 1

    # Database migrations
    printf "\n${BOLD}[2] Database schema${RESET}\n"
    if [ "${PROTEA_SKIP_ALEMBIC:-}" != "1" ]; then
        printf "  Waiting for postgres to accept connections...\n"
        for i in $(seq 1 30); do
            if pg_isready -h localhost -p 5432 -U protea -d protea -q 2>/dev/null; then
                break
            fi
            sleep 1
        done

        printf "  Running alembic upgrade head...\n"
        if ! poetry run alembic upgrade head; then
            printf "  ${RED}✗ alembic upgrade head FAILED${RESET} — stack will not start cleanly.\n"
            exit 1
        fi
        printf "  ${GREEN}✓ Schema at head${RESET}\n"
    else
        printf "  ${YELLOW}(skipped: PROTEA_SKIP_ALEMBIC=1)${RESET}\n"
    fi

    # API
    printf "\n${BOLD}[3] API${RESET}\n"
    cd "$ROOT"
    _start_bg api poetry run uvicorn protea.api.app:create_app \
        --factory --host 0.0.0.0 --port 8000 --root-path /api-proxy
    api_ready=0
    for _ in $(seq 1 120); do
        if curl -sf http://localhost:8000/jobs > /dev/null 2>&1; then api_ready=1; break; fi
        sleep 1
    done
    if [[ $api_ready -eq 1 ]]; then
        printf "  ${GREEN}API OK${RESET} → http://localhost:8000\n"
    else
        printf "  ${RED}API FAILED${RESET} — check logs/api.log\n"; exit 1
    fi

    # Core workers
    printf "\n${BOLD}[4] Core workers${RESET}\n"
    _start_bg worker-ping        poetry run python scripts/worker.py --queue protea.ping
    _start_bg worker-jobs        poetry run python scripts/worker.py --queue protea.jobs
    _start_bg worker-training    poetry run python scripts/worker.py --queue protea.training

    # Embeddings pipeline
    printf "\n${BOLD}[5] Embeddings pipeline${RESET}\n"
    _start_bg worker-embeddings-coord  poetry run python scripts/worker.py --queue protea.embeddings
    for i in $(seq 1 "$BATCH_WORKERS"); do
        _start_bg "worker-embeddings-batch-${i}" \
            poetry run python scripts/worker.py --queue protea.embeddings.batch
    done
    _start_bg worker-embeddings-write  poetry run python scripts/worker.py --queue protea.embeddings.write

    # Predictions pipeline
    printf "\n${BOLD}[6] Predictions pipeline${RESET}\n"
    _start_bg worker-predictions-coord poetry run python scripts/worker.py --queue protea.predictions
    for i in $(seq 1 "$BATCH_WORKERS"); do
        _start_bg "worker-predictions-batch-${i}" \
            poetry run python scripts/worker.py --queue protea.predictions.batch
    done
    _start_bg worker-predictions-write poetry run python scripts/worker.py --queue protea.predictions.write

    # Evaluations pipeline
    printf "\n${BOLD}[7] Evaluations pipeline${RESET}\n"
    _start_bg worker-evaluations poetry run python scripts/worker.py --queue protea.evaluations

    # Export minijob pipeline (only when PROTEA_EXPORT_MINIJOBS=1)
    if [[ "${PROTEA_EXPORT_MINIJOBS:-0}" == "1" ]]; then
        printf "\n${BOLD}[8a] Export minijob workers (PROTEA_EXPORT_MINIJOBS=1)${RESET}\n"
        _start_bg worker-export-knn-batch \
            poetry run python scripts/worker.py --queue protea.training.knn-batch
        _start_bg worker-export-features \
            poetry run python scripts/worker.py --queue protea.training.features
        _start_bg worker-export-write \
            poetry run python scripts/worker.py --queue protea.training.write
    fi

    # Stale job reaper
    printf "\n${BOLD}[8] Stale job reaper${RESET}\n"
    _start_bg worker-reaper poetry run python scripts/worker.py --queue reaper

    # Frontend
    # Production mode by default: the dev server serves unminified Turbopack
    # chunks + HMR websocket, which destroys any bandwidth-capped tunnel
    # (ngrok, Cloudflare free tier, etc). Override with FRONTEND_MODE=dev for
    # local hacking where HMR is actually useful.
    local FRONTEND_MODE="${FRONTEND_MODE:-prod}"
    printf "\n${BOLD}[9] Frontend (%s)${RESET}\n" "$FRONTEND_MODE"
    # Next.js 16 needs Node >=20.9. The watchdog cron's PATH may only carry the
    # system Node 18 (nvm is not sourced), which fails BOTH `npm run build` and
    # `node server.js`. Prefer the newest nvm-installed Node 20+ toolchain so a
    # frontend (re)start after a reboot does not silently 502 the public tunnel.
    if ! { command -v node >/dev/null 2>&1 \
           && [[ "$(node -e 'process.stdout.write(process.versions.node.split(".")[0])' 2>/dev/null)" -ge 20 ]]; }; then
        _nvm_node=$(ls -d "$HOME"/.nvm/versions/node/v2[0-9]* 2>/dev/null | sort -Vr | head -1)
        if [[ -n "$_nvm_node" && -x "$_nvm_node/bin/node" ]]; then
            export PATH="$_nvm_node/bin:$PATH"
            printf "  ${GREEN}✓${RESET} using Node %s from nvm (%s)\n" "$("$_nvm_node/bin/node" --version)" "$_nvm_node/bin"
        else
            printf "  ${YELLOW}WARNING: no Node >=20 found via nvm; frontend may fail on $(node --version 2>/dev/null)${RESET}\n"
        fi
    fi
    cd "$ROOT/apps/web"
    if [[ "$FRONTEND_MODE" == "prod" ]]; then
        printf "  Building production bundle (this may take ~30-60s)...\n"
        if npm run build >> "$LOG_DIR/frontend-build.log" 2>&1; then
            printf "  ${GREEN}✓${RESET} build OK → logs/frontend-build.log\n"
            # Copy static assets and public dir into the standalone tree so
            # node server.js can serve them.  Next 16 emits server.js at
            # .next/standalone/server.js; static and public files must sit
            # alongside it for correct asset resolution.
            local STANDALONE_DIR=".next/standalone"
            if [[ -d "$STANDALONE_DIR" ]]; then
                mkdir -p "$STANDALONE_DIR/.next" "$STANDALONE_DIR/public"
                if ! cp -r .next/static "$STANDALONE_DIR/.next/static"; then
                    printf "  ${RED}✗ FAILED to copy .next/static into standalone${RESET}\n"; exit 1
                fi
                if ! cp -r public/. "$STANDALONE_DIR/public/"; then
                    printf "  ${RED}✗ FAILED to copy public/ into standalone${RESET}\n"; exit 1
                fi
                if ! ls "$STANDALONE_DIR/.next/static/chunks/"*.css > /dev/null 2>&1; then
                    printf "  ${RED}✗ FAILED to populate standalone static chunks (no CSS found)${RESET}\n"; exit 1
                fi
                if [[ ! -f "$STANDALONE_DIR/public/protea-mark.png" ]]; then
                    printf "  ${RED}✗ FAILED to populate standalone public (protea-mark.png missing)${RESET}\n"; exit 1
                fi
                printf "  ${GREEN}✓${RESET} standalone assets copied\n"
            fi
        else
            printf "  ${RED}✗ build FAILED${RESET} — see logs/frontend-build.log\n"
            printf "  ${YELLOW}Falling back to dev mode.${RESET}\n"
            FRONTEND_MODE="dev"
        fi
    fi
    if [[ "$FRONTEND_MODE" == "prod" ]]; then
        local STANDALONE_SERVER=".next/standalone/server.js"
        if [[ -f "$STANDALONE_SERVER" ]]; then
            # output:standalone build — serve via node, not next start
            PORT=3000 HOSTNAME=0.0.0.0 _start_bg frontend node "$STANDALONE_SERVER"
        else
            printf "  ${YELLOW}WARNING: standalone server.js not found at %s; falling back to next start${RESET}\n" \
                "$STANDALONE_SERVER"
            _start_bg frontend npm run start
        fi
    else
        _start_bg frontend npm run dev
    fi
    sleep 6
    curl -sf http://localhost:3000 -o /dev/null \
        && printf "  ${GREEN}Frontend OK${RESET} → http://localhost:3000\n" \
        || printf "  ${YELLOW}Frontend not ready yet${RESET} — check logs/frontend.log\n"

    printf "\n${BOLD}=== Stack running ===${RESET}\n"
    printf "  Frontend  → http://localhost:3000\n"
    printf "  API       → http://localhost:8000\n"
    printf "  RabbitMQ  → http://localhost:15672  (guest/guest)\n"
    printf "\n  ${CYAN}bash scripts/manage.sh status${RESET}   — show worker status\n"
    printf "  ${CYAN}bash scripts/manage.sh logs${RESET}      — browse logs\n"
    printf "  ${CYAN}bash scripts/manage.sh stop${RESET}      — stop everything\n\n"
}

# ── stop ──────────────────────────────────────────────────────────────────────
cmd_stop() {
    printf "\n${BOLD}=== Stopping PROTEA dev stack ===${RESET}\n\n"

    # Collect all worker PIDs before removing pid files
    local worker_pids=()
    local stopped=0
    for f in "$PID_DIR"/*.pid; do
        [[ -e "$f" ]] || continue
        local pid; pid=$(cat "$f")
        _stop_pid "$(basename "$f" .pid)"
        kill -0 "$pid" 2>/dev/null && worker_pids+=("$pid")
        (( stopped++ )) || true
    done

    # Also catch any untracked worker.py processes (manual launches etc.)
    while IFS= read -r pid; do
        kill -15 -- -"$pid" 2>/dev/null || kill -15 "$pid" 2>/dev/null
        worker_pids+=("$pid")
    done < <(pgrep -f "scripts/worker.py" 2>/dev/null || true)

    # Force-kill API and frontend immediately (no long-running state)
    kill -9 $(pgrep -f "uvicorn protea.api" 2>/dev/null) 2>/dev/null || true
    kill -9 $(pgrep -f "next-server" 2>/dev/null) 2>/dev/null || true

    # Wait up to 60 s for workers to finish current job, then force-kill
    if [[ ${#worker_pids[@]} -gt 0 ]]; then
        printf "  Waiting up to 5s for workers to finish current jobs...\n"
        local deadline=$(( $(date +%s) + 5 ))
        for pid in "${worker_pids[@]}"; do
            while kill -0 "$pid" 2>/dev/null && (( $(date +%s) < deadline )); do
                sleep 2
            done
            if kill -0 "$pid" 2>/dev/null; then
                kill -9 -- -"$pid" 2>/dev/null || kill -9 "$pid" 2>/dev/null
                printf "  ${YELLOW}⚠${RESET}  PID %s force-killed (job still running)\n" "$pid"
            fi
        done
    fi

    [[ $stopped -eq 0 ]] && printf "  (nothing was running)\n"
    printf "\n${GREEN}Done.${RESET}\n\n"
}

# ── status ────────────────────────────────────────────────────────────────────
cmd_status() {
    printf "\n${BOLD}=== PROTEA worker status ===${RESET}\n\n"
    printf "  ${BOLD}%-35s %-8s %-8s %s${RESET}\n" "NAME" "PID" "RAM" "STATUS"
    printf "  %s\n" "$(printf '─%.0s' {1..60})"

    for f in "$PID_DIR"/*.pid; do
        [[ -e "$f" ]] || continue
        local name; name="$(basename "$f" .pid)"
        local pid; pid="$(cat "$f")"
        if kill -0 "$pid" 2>/dev/null; then
            local rss; rss="$(_pid_rss_mb "$pid") MB"
            printf "  ${GREEN}●${RESET} %-35s %-8s %-8s ${GREEN}running${RESET}\n" "$name" "$pid" "$rss"
        else
            printf "  ${RED}●${RESET} %-35s %-8s %-8s ${RED}dead${RESET}\n" "$name" "$pid" "-"
        fi
    done

    # Check for untracked workers
    local untracked
    untracked=$(pgrep -f "scripts/worker.py" 2>/dev/null || true)
    if [[ -n "$untracked" ]]; then
        local tracked_pids
        tracked_pids=$(cat "$PID_DIR"/*.pid 2>/dev/null | tr '\n' '|' | sed 's/|$//')
        while IFS= read -r pid; do
            if [[ -n "$tracked_pids" ]] && echo "$pid" | grep -qE "^(${tracked_pids})$"; then
                continue
            fi
            local queue; queue=$(ps -p "$pid" -o args= 2>/dev/null | grep -o '\-\-queue [^ ]*' | awk '{print $2}')
            local rss; rss="$(_pid_rss_mb "$pid") MB"
            printf "  ${YELLOW}●${RESET} %-35s %-8s %-8s ${YELLOW}untracked${RESET}\n" \
                "worker (${queue})" "$pid" "$rss"
        done <<< "$untracked"
    fi

    printf "\n"

    # API
    if curl -sf http://localhost:8000/jobs > /dev/null 2>&1; then
        printf "  ${GREEN}●${RESET} API        → http://localhost:8000  ${GREEN}up${RESET}\n"
    else
        printf "  ${RED}●${RESET} API        → http://localhost:8000  ${RED}down${RESET}\n"
    fi

    # Frontend
    if curl -sf http://localhost:3000 -o /dev/null 2>&1; then
        printf "  ${GREEN}●${RESET} Frontend   → http://localhost:3000  ${GREEN}up${RESET}\n"
    else
        printf "  ${RED}●${RESET} Frontend   → http://localhost:3000  ${RED}down${RESET}\n"
    fi

    printf "\n"
}

# ── logs ──────────────────────────────────────────────────────────────────────
cmd_logs() {
    local target="${1:-}"

    if [[ -n "$target" ]]; then
        # Direct: find log file matching the given name fragment
        local match
        match=$(find "$LOG_DIR" -maxdepth 1 -name "*.log" | grep -i "$target" | head -1)
        if [[ -z "$match" ]]; then
            printf "${RED}No log found matching '%s'${RESET}\n" "$target"
            printf "Available logs:\n"
            find "$LOG_DIR" -maxdepth 1 -name "*.log" -exec basename {} \; | sort | sed 's/^/  /'
            exit 1
        fi
        printf "${CYAN}=== %s ===${RESET}\n" "$(basename "$match")"
        tail -f "$match"
        return
    fi

    # Interactive picker
    local logs
    mapfile -t logs < <(find "$LOG_DIR" -maxdepth 1 -name "*.log" | sort | xargs -I{} basename {})

    if [[ ${#logs[@]} -eq 0 ]]; then
        printf "No log files found in %s\n" "$LOG_DIR"
        exit 1
    fi

    printf "\n${BOLD}Available logs:${RESET}\n\n"
    for i in "${!logs[@]}"; do
        printf "  ${CYAN}%2d${RESET}  %s\n" "$((i+1))" "${logs[$i]}"
    done
    printf "\n  ${CYAN} a${RESET}  all (tail -f all logs)\n"
    printf "\nSelect [1-%d / a]: " "${#logs[@]}"
    read -r choice

    if [[ "$choice" == "a" ]]; then
        tail -f "$LOG_DIR"/*.log
    elif [[ "$choice" =~ ^[0-9]+$ ]] && (( choice >= 1 && choice <= ${#logs[@]} )); then
        local selected="$LOG_DIR/${logs[$((choice-1))]}"
        printf "\n${CYAN}=== %s ===${RESET}\n" "${logs[$((choice-1))]}"
        tail -f "$selected"
    else
        printf "${RED}Invalid choice.${RESET}\n"
        exit 1
    fi
}

# ── scale ─────────────────────────────────────────────────────────────────────
cmd_scale() {
    local queue="${1:-}"
    local n="${2:-1}"

    if [[ -z "$queue" ]]; then
        printf "Usage: manage.sh scale <queue> [N]\n"
        printf "Example: manage.sh scale protea.predictions.batch 2\n"
        exit 1
    fi

    printf "\n${BOLD}Adding %s worker(s) to %s${RESET}\n\n" "$n" "$queue"
    cd "$ROOT"
    for i in $(seq 1 "$n"); do
        # Find a free index
        local idx=1
        while [[ -f "$PID_DIR/$(_worker_name "$queue" "$idx").pid" ]]; do
            (( idx++ ))
        done
        _start_bg "$(_worker_name "$queue" "$idx")" \
            poetry run python scripts/worker.py --queue "$queue"
    done
    printf "\n"
}

# ── self-healing watchdog ─────────────────────────────────────────────────────
# Health probes return 0 (healthy) / 1 (down). Kept tiny so a watch loop can
# poll them cheaply every WATCH_INTERVAL seconds without forking the world.

_api_healthy() {
    curl -sf http://localhost:8000/health -o /dev/null 2>&1
}

_frontend_healthy() {
    curl -sf http://localhost:3000 -o /dev/null 2>&1
}

# Restart the API in-place (no full stack bounce). Env is re-sourced so the
# restarted process gets PROTEA_JWT_SECRET etc. even if the watchdog was
# launched from a bare shell.
_restart_api() {
    _source_env
    _stop_pid api
    kill -9 "$(pgrep -f 'uvicorn protea.api' 2>/dev/null)" 2>/dev/null || true
    cd "$ROOT"
    _start_bg api poetry run uvicorn protea.api.app:create_app \
        --factory --host 0.0.0.0 --port 8000 --root-path /api-proxy
}

# Restart any tracked worker whose process is dead, replaying its recorded
# launch command. Idempotent: a worker whose PID is still alive is left alone,
# so this never duplicates workers. Untracked (manually launched) workers and
# the api/frontend are skipped here — the api has its own probe-driven restart
# and long-running manual jobs must not be disturbed.
_heal_dead_workers() {
    local healed=0 f name pid
    for f in "$PID_DIR"/*.pid; do
        [[ -e "$f" ]] || continue
        name="$(basename "$f" .pid)"
        # api/frontend are not "$@"-replayable workers (they carry inline env /
        # node argv); the API is handled by _restart_api, frontend by its probe.
        [[ "$name" == "api" || "$name" == "frontend" ]] && continue
        [[ -f "$PID_DIR/$name.cmd" ]] || continue
        pid="$(cat "$f" 2>/dev/null || echo)"
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            continue  # still alive — leave it
        fi
        # Dead: replay the recorded command (one arg per line).
        local -a argv=()
        mapfile -t argv < "$PID_DIR/$name.cmd"
        [[ ${#argv[@]} -gt 0 ]] || continue
        printf "  ${YELLOW}↻${RESET} restarting dead worker %-28s\n" "$name"
        ( cd "$ROOT" && _start_bg "$name" "${argv[@]}" )
        healed=$((healed + 1))
    done
    return "$healed"
}

# One-shot health check + heal. Returns 0 if everything was already healthy,
# non-zero (count of components healed) otherwise. Safe to run from cron.
cmd_health() {
    _source_env
    local healed=0
    if ! _api_healthy; then
        printf "  ${RED}●${RESET} API down — restarting\n"
        _restart_api
        # Give it a moment to bind before reporting.
        for _ in $(seq 1 30); do _api_healthy && break; sleep 1; done
        healed=$((healed + 1))
    else
        printf "  ${GREEN}●${RESET} API healthy\n"
    fi

    _heal_dead_workers || healed=$((healed + $?))

    if [[ $healed -eq 0 ]]; then
        printf "  ${GREEN}stack healthy${RESET}\n"
    else
        printf "  ${YELLOW}healed %s component(s)${RESET}\n" "$healed"
    fi
    return 0
}

# Supervised loop: probe + heal every WATCH_INTERVAL seconds (default 30) until
# killed. Run under `nohup`/systemd/tmux for a persistent watchdog. Idempotent:
# a second `watch` invocation refuses to start if a watchdog pidfile is already
# live, so it never double-supervises (which would race two restarts).
cmd_watch() {
    local interval="${WATCH_INTERVAL:-30}"
    mkdir -p "$PID_DIR"
    local self_pidfile="$PID_DIR/watchdog.pid"
    if [[ -f "$self_pidfile" ]]; then
        local prev; prev="$(cat "$self_pidfile" 2>/dev/null || echo)"
        if [[ -n "$prev" ]] && kill -0 "$prev" 2>/dev/null; then
            printf "${YELLOW}watchdog already running (PID %s)${RESET}\n" "$prev"
            exit 0
        fi
    fi
    echo "$$" > "$self_pidfile"
    trap 'rm -f "$self_pidfile"' EXIT
    printf "${BOLD}watchdog up${RESET} (interval ${interval}s, PID $$). Probing API + workers.\n"
    while true; do
        cmd_health >> "$LOG_DIR/watchdog.log" 2>&1 || true
        sleep "$interval"
    done
}

# ── dispatch ──────────────────────────────────────────────────────────────────
CMD="${1:-help}"
shift || true

case "$CMD" in
    start)  cmd_start "${1:-1}" ;;
    stop)   cmd_stop ;;
    status) cmd_status ;;
    logs)   cmd_logs "${1:-}" ;;
    scale)  cmd_scale "${1:-}" "${2:-1}" ;;
    health) cmd_health ;;
    watch)  cmd_watch ;;
    help|--help|-h)
        printf "\n${BOLD}PROTEA dev stack manager${RESET}\n\n"
        printf "  ${CYAN}bash scripts/manage.sh start [N]${RESET}           Start stack (N batch workers per pipeline)\n"
        printf "  ${CYAN}bash scripts/manage.sh stop${RESET}                Stop all processes\n"
        printf "  ${CYAN}bash scripts/manage.sh status${RESET}              Show worker status + RAM\n"
        printf "  ${CYAN}bash scripts/manage.sh logs [name]${RESET}         Tail logs (interactive if no name)\n"
        printf "  ${CYAN}bash scripts/manage.sh scale <queue> [N]${RESET}   Add N extra workers to a queue\n"
        printf "  ${CYAN}bash scripts/manage.sh health${RESET}              One-shot probe + heal dead components\n"
        printf "  ${CYAN}bash scripts/manage.sh watch${RESET}               Supervised self-heal loop (WATCH_INTERVAL=30s)\n\n"
        printf "Examples:\n"
        printf "  bash scripts/manage.sh start          # 1 batch worker per pipeline\n"
        printf "  bash scripts/manage.sh start 2        # 2 batch workers per pipeline\n"
        printf "  bash scripts/manage.sh scale protea.predictions.batch 2\n"
        printf "  bash scripts/manage.sh logs predictions\n"
        printf "  nohup bash scripts/manage.sh watch &  # background watchdog\n\n"
        ;;
    *)
        printf "${RED}Unknown command: %s${RESET}\n" "$CMD"
        printf "Run ${CYAN}bash scripts/manage.sh help${RESET} for usage.\n"
        exit 1
        ;;
esac
