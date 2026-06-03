#!/usr/bin/env bash
# scripts/disaster-recovery.sh - dump the live PROTEA postgres and restore
# the dump into an ephemeral container to exercise the disaster-recovery path
# without touching production state.
#
# The script supports three execution modes:
#
#   1. End-to-end drill (default): pg_dump live, restore into a temp container,
#      compare row counts table-by-table, tear down. Exits non-zero on any
#      integrity mismatch (row count differs, missing pgvector extension, no
#      alembic_version row).
#
#   2. --dump-only: pg_dump live to a path. No temp container is spun up. The
#      resulting file is the same one used as input to --restore-only and is
#      printed on stdout so callers can capture the path.
#
#   3. --restore-only PATH: skip pg_dump; use the dump at PATH as the restore
#      source. Useful when validating an older dump captured by an operator
#      from a different host, or when re-running the verification step after a
#      partial drill.
#
# Usage:
#   bash scripts/disaster-recovery.sh
#   bash scripts/disaster-recovery.sh --dump-only
#   bash scripts/disaster-recovery.sh --restore-only /path/to/foo.dump
#   bash scripts/disaster-recovery.sh --keep-container   # leave temp pg running
#
# Env overrides:
#   PROTEA_DR_LIVE_CONTAINER  live postgres container name (default: protea-postgres-1)
#   PROTEA_DR_DRILL_CONTAINER temp container name           (default: protea-pg-drill)
#   PROTEA_DR_DRILL_PORT      host port for temp container  (default: 5433)
#   PROTEA_DR_BACKUP_DIR      directory for dump files      (default: ~/Thesis2/backups)
#   PROTEA_DR_IMAGE           postgres image                (default: pgvector/pgvector:pg16)
#   PROTEA_DR_DB              database name                 (default: protea)
#   PROTEA_DR_USER            postgres role                 (default: protea)
#   PROTEA_DR_PASSWORD        postgres password             (default: protea)
#
# Hard constraints:
#   - The live container is touched only by pg_dump, a read-only operation.
#   - All restore operations target the ephemeral container; nothing mutative
#     ever runs against the live database.
#
# Exit codes:
#   0  drill succeeded, all integrity checks passed
#   1  generic failure (dump/restore/setup)
#   2  invalid arguments
#   3  integrity mismatch (row counts, missing extension, alembic_version)

set -euo pipefail

LIVE_CONTAINER="${PROTEA_DR_LIVE_CONTAINER:-protea-postgres-1}"
DRILL_CONTAINER="${PROTEA_DR_DRILL_CONTAINER:-protea-pg-drill}"
DRILL_PORT="${PROTEA_DR_DRILL_PORT:-5433}"
BACKUP_DIR="${PROTEA_DR_BACKUP_DIR:-$HOME/Thesis2/backups}"
PG_IMAGE="${PROTEA_DR_IMAGE:-pgvector/pgvector:pg16}"
DB_NAME="${PROTEA_DR_DB:-protea}"
DB_USER="${PROTEA_DR_USER:-protea}"
DB_PASS="${PROTEA_DR_PASSWORD:-protea}"

MODE="full"
DUMP_PATH=""
KEEP_CONTAINER=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dump-only)        MODE="dump-only"; shift;;
    --restore-only)     MODE="restore-only"; DUMP_PATH="${2:-}"; shift 2;;
    --keep-container)   KEEP_CONTAINER=1; shift;;
    --help|-h)          sed -n 's/^# \{0,1\}//;3,40p' "$0"; exit 0;;
    *)                  echo "unknown argument: $1" >&2; exit 2;;
  esac
done

C_RESET=$'\e[0m'; C_BOLD=$'\e[1m'; C_GREEN=$'\e[32m'; C_YELLOW=$'\e[33m'; C_RED=$'\e[31m'
log()  { printf "%s[dr]%s %s\n" "$C_BOLD" "$C_RESET" "$*" >&2; }
ok()   { printf "%s[dr]%s %s%s%s\n" "$C_BOLD" "$C_RESET" "$C_GREEN" "$*" "$C_RESET" >&2; }
warn() { printf "%s[dr]%s %s%s%s\n" "$C_BOLD" "$C_RESET" "$C_YELLOW" "$*" "$C_RESET" >&2; }
fail() { printf "%s[dr]%s %s%s%s\n" "$C_BOLD" "$C_RESET" "$C_RED" "$*" "$C_RESET" >&2; }

# Tables that must match between live and drill. Order is alphabetical for
# stable diffs; the verification loop iterates this list.
TABLES=(
  annotation_set
  dataset
  embedding_config
  evaluation_result
  evaluation_set
  go_prediction
  go_term
  interpro_annotation
  job
  job_event
  ontology_snapshot
  prediction_set
  protein
  protein_go_annotation
  query_set
  reranker_model
  sequence
  sequence_embedding
)

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

require_docker() {
  if ! command -v docker >/dev/null 2>&1; then
    fail "docker not on PATH"; exit 1
  fi
}

require_live_container() {
  if ! docker ps --format '{{.Names}}' | grep -qx "$LIVE_CONTAINER"; then
    fail "live container '$LIVE_CONTAINER' is not running"; exit 1
  fi
}

# Run a SQL query in a container; print the scalar result. On error print
# the literal token MISSING so the comparison loop can still proceed.
psql_scalar() {
  local container="$1"; shift
  local sql="$1"; shift
  docker exec "$container" psql -U "$DB_USER" -d "$DB_NAME" -tAc "$sql" 2>/dev/null \
    || echo "MISSING"
}

dump_live() {
  mkdir -p "$BACKUP_DIR"
  local out
  out="$BACKUP_DIR/drill-$(date +%Y%m%d-%H%M%S).dump"
  log "dumping $LIVE_CONTAINER -> $out"
  local t0=$SECONDS
  docker exec "$LIVE_CONTAINER" pg_dump -U "$DB_USER" -F c -d "$DB_NAME" > "$out"
  local elapsed=$((SECONDS - t0))
  ok "dump finished in ${elapsed}s ($(du -h "$out" | cut -f1))"
  printf "%s\n" "$out"
}

drill_container_exists() {
  docker ps -a --format '{{.Names}}' | grep -qx "$DRILL_CONTAINER"
}

start_drill_container() {
  if drill_container_exists; then
    warn "drill container '$DRILL_CONTAINER' already exists; removing first"
    docker rm -f "$DRILL_CONTAINER" >/dev/null
  fi
  if ss -ltn 2>/dev/null | awk '{print $4}' | grep -qE ":${DRILL_PORT}\$"; then
    fail "host port $DRILL_PORT already in use; set PROTEA_DR_DRILL_PORT to a free port"
    exit 1
  fi
  log "starting drill container '$DRILL_CONTAINER' on port $DRILL_PORT (image $PG_IMAGE)"
  docker run -d \
    --name "$DRILL_CONTAINER" \
    -e POSTGRES_USER="$DB_USER" \
    -e POSTGRES_PASSWORD="$DB_PASS" \
    -e POSTGRES_DB="$DB_NAME" \
    -p "${DRILL_PORT}:5432" \
    "$PG_IMAGE" >/dev/null
  local tries=0
  until docker exec "$DRILL_CONTAINER" pg_isready -U "$DB_USER" >/dev/null 2>&1; do
    tries=$((tries + 1))
    if (( tries > 60 )); then
      fail "drill container did not become ready within 60s"
      docker logs --tail 50 "$DRILL_CONTAINER" >&2 || true
      exit 1
    fi
    sleep 1
  done
  # The pgvector image ships the extension preinstalled into the template1
  # database, so newly created databases already have it. We restore on top
  # of an existing protea DB, which means CREATE EXTENSION will be a no-op.
  ok "drill container is ready"
}

restore_into_drill() {
  local dump="$1"
  [[ -f "$dump" ]] || { fail "dump file not found: $dump"; exit 1; }
  log "restoring $dump into $DRILL_CONTAINER"
  local t0=$SECONDS
  # pg_restore exits non-zero on benign errors (extension already exists,
  # role 'protea' already owns objects). Capture exit but do not abort; the
  # subsequent verification step is the real gate.
  set +e
  docker exec -i "$DRILL_CONTAINER" pg_restore -U "$DB_USER" -d "$DB_NAME" \
      < "$dump" 2>/tmp/protea-dr-restore.err
  local rc=$?
  set -e
  local elapsed=$((SECONDS - t0))
  if (( rc != 0 )); then
    local errs
    errs=$(wc -l < /tmp/protea-dr-restore.err)
    warn "pg_restore exited $rc with $errs warning lines (extension reload is benign)"
  fi
  ok "restore finished in ${elapsed}s"
}

# Compare row counts between live and drill for every table in TABLES.
# Prints a table-by-table report and returns 0 on full parity, 3 otherwise.
verify_integrity() {
  local mismatches=0
  printf "\n%-30s %15s %15s %s\n" "table" "live" "drill" "status"
  printf "%-30s %15s %15s %s\n" "------------------------------" "---------------" "---------------" "------"
  for tbl in "${TABLES[@]}"; do
    local live drill status
    live=$(psql_scalar "$LIVE_CONTAINER" "SELECT count(*) FROM $tbl")
    drill=$(psql_scalar "$DRILL_CONTAINER" "SELECT count(*) FROM $tbl")
    if [[ "$live" == "$drill" ]]; then
      status="OK"
    else
      status="MISMATCH"
      mismatches=$((mismatches + 1))
    fi
    printf "%-30s %15s %15s %s\n" "$tbl" "$live" "$drill" "$status"
  done

  local vector_ext alembic_rev
  vector_ext=$(psql_scalar "$DRILL_CONTAINER" \
    "SELECT extname FROM pg_extension WHERE extname='vector'")
  alembic_rev=$(psql_scalar "$DRILL_CONTAINER" \
    "SELECT version_num FROM alembic_version")

  printf "\npgvector extension : %s\n" "${vector_ext:-MISSING}"
  printf "alembic revision    : %s\n" "${alembic_rev:-MISSING}"

  if [[ "$vector_ext" != "vector" ]]; then
    fail "pgvector extension missing in drill database"
    mismatches=$((mismatches + 1))
  fi
  if [[ -z "$alembic_rev" || "$alembic_rev" == "MISSING" ]]; then
    fail "alembic_version row missing in drill database"
    mismatches=$((mismatches + 1))
  fi

  if (( mismatches > 0 )); then
    fail "integrity verification: $mismatches issue(s) detected"
    return 3
  fi
  ok "integrity verification: all $((${#TABLES[@]})) tables match, pgvector + alembic present"
  return 0
}

teardown_drill_container() {
  if (( KEEP_CONTAINER == 1 )); then
    warn "leaving drill container '$DRILL_CONTAINER' running (--keep-container)"
    return
  fi
  if drill_container_exists; then
    log "tearing down drill container '$DRILL_CONTAINER'"
    docker rm -f "$DRILL_CONTAINER" >/dev/null
  fi
}

# Ensure teardown runs even on failure inside the full-drill path.
cleanup_trap() {
  local rc=$?
  if [[ "$MODE" != "dump-only" ]]; then
    teardown_drill_container || true
  fi
  exit $rc
}

# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

require_docker

case "$MODE" in
  dump-only)
    require_live_container
    out=$(dump_live)
    # stdout: dump path (so callers can capture it). All logs went to stderr.
    printf "%s\n" "$out"
    ;;
  restore-only)
    [[ -n "$DUMP_PATH" ]] || { fail "--restore-only requires a path argument"; exit 2; }
    [[ -f "$DUMP_PATH" ]] || { fail "dump path not found: $DUMP_PATH"; exit 1; }
    require_live_container   # needed for the live-vs-drill row-count diff
    trap cleanup_trap EXIT
    start_drill_container
    restore_into_drill "$DUMP_PATH"
    verify_integrity
    ;;
  full)
    require_live_container
    trap cleanup_trap EXIT
    DUMP_PATH=$(dump_live)
    start_drill_container
    restore_into_drill "$DUMP_PATH"
    verify_integrity
    ;;
  *)
    fail "unknown mode: $MODE"; exit 2;;
esac
