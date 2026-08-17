#!/usr/bin/env bash
# Take a restorable checkpoint of the PROTEA database.
#
# Run this after every expensive stage of a campaign (corpus load, each
# embedding cell). Recomputing an embedding grid costs days of GPU; a dump
# costs minutes of disk, and on 2026-07-30 the absence of one turned a single
# bad test invocation into the loss of the entire run.
#
# Usage:
#   scripts/checkpoint_db.sh <label>          take a checkpoint
#   scripts/checkpoint_db.sh --list           list existing checkpoints
#   scripts/checkpoint_db.sh --restore <file> restore one (asks for confirmation)
#
# Custom format (-Fc) so a single file can be restored selectively with
# pg_restore, and so it compresses without a separate step.

set -euo pipefail

BACKUP_DIR="${PROTEA_BACKUP_DIR:-$HOME/Thesis-laptop/backups}"
DB_HOST="${PGHOST:-127.0.0.1}"
DB_PORT="${PGPORT:-5432}"
DB_USER="${PGUSER:-protea}"
DB_NAME="${PGDATABASE:-protea}"
export PGPASSWORD="${PGPASSWORD:-protea}"

mkdir -p "$BACKUP_DIR"

_size_of_db() {
  psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -tAc \
    "select pg_size_pretty(pg_database_size('$DB_NAME'));" 2>/dev/null || echo "?"
}

case "${1:-}" in
  --list)
    printf '%-46s %10s  %s\n' "CHECKPOINT" "SIZE" "TAKEN"
    for f in "$BACKUP_DIR"/protea-*.dump; do
      [ -e "$f" ] || { echo "  (none yet in $BACKUP_DIR)"; exit 0; }
      printf '%-46s %10s  %s\n' "$(basename "$f")" \
        "$(du -h "$f" | cut -f1)" "$(date -r "$f" '+%Y-%m-%d %H:%M')"
    done
    exit 0
    ;;
  --restore)
    SRC="${2:?usage: checkpoint_db.sh --restore <file>}"
    [ -f "$SRC" ] || { echo "no such checkpoint: $SRC" >&2; exit 1; }
    echo "About to DROP and rebuild '$DB_NAME' from $(basename "$SRC")."
    echo "Current database size: $(_size_of_db)"
    read -r -p "Type the database name to confirm: " confirm
    [ "$confirm" = "$DB_NAME" ] || { echo "aborted"; exit 1; }
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres -q \
      -c "select pg_terminate_backend(pid) from pg_stat_activity where datname='$DB_NAME' and pid <> pg_backend_pid();" \
      -c "drop database if exists \"$DB_NAME\";" \
      -c "create database \"$DB_NAME\";"
    pg_restore -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" --no-owner --jobs=4 "$SRC"
    echo "restored. size now: $(_size_of_db)"
    exit 0
    ;;
esac

LABEL="${1:?usage: checkpoint_db.sh <label>}"
SAFE_LABEL="$(printf '%s' "$LABEL" | tr -c 'A-Za-z0-9._-' '-')"
STAMP="$(date '+%Y%m%dT%H%M%S')"
OUT="$BACKUP_DIR/protea-${STAMP}-${SAFE_LABEL}.dump"

echo "[checkpoint] database is $(_size_of_db); dumping to $OUT"
pg_dump -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
  --format=custom --compress=6 --file="$OUT"

# A dump that cannot be read back is not a backup. Verify the archive is
# structurally intact and non-trivial before reporting success.
ENTRIES="$(pg_restore --list "$OUT" 2>/dev/null | grep -cv '^;' || true)"
if [ "${ENTRIES:-0}" -lt 1 ]; then
  echo "[checkpoint] FAILED: $OUT has no readable table of contents" >&2
  exit 1
fi

sha256sum "$OUT" > "$OUT.sha256"
echo "[checkpoint] ok: $(du -h "$OUT" | cut -f1), ${ENTRIES} archive entries"
echo "[checkpoint] restore with: scripts/checkpoint_db.sh --restore $OUT"
