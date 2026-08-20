#!/usr/bin/env bash
# db-query.sh -- read-only, time-bounded SQL against the live database.
#
# WHY THIS EXISTS. On 2026-08-20 three hand-written diagnostics were found
# running against the live database: 56 hours, 4 hours and 2.5 hours, all
# three the same pathology (a correlated subquery against go_prediction, the
# big table, evaluated once per row). None of them could ever have finished.
# Two were orphans whose client had long since timed out; the server-side
# query never learned about it.
#
# The guards are set server-side for the session, so they hold regardless of
# what the SQL says:
#
#   default_transaction_read_only=on   a diagnostic cannot write, ever
#   statement_timeout                  a bad plan dies instead of accruing
#   idle_in_transaction_session_timeout an abandoned client cannot pin a
#                                      transaction open
#
# The timeout is the important one. A diagnostic that needs more than two
# minutes is not a diagnostic, it is a job, and a job belongs in the queue
# with a producer and a registered output.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TIMEOUT="${DB_QUERY_TIMEOUT:-120s}"

usage() {
  cat >&2 <<'USAGE'
usage: db-query.sh [-t TIMEOUT] <sql>
       db-query.sh [-t TIMEOUT] -f <file.sql>
       cat q.sql | db-query.sh [-t TIMEOUT]

Read-only. Writes are refused by the server, not by this script.
TIMEOUT is a postgres interval string (default 120s, or $DB_QUERY_TIMEOUT).
USAGE
  exit 2
}

FILE=""
while getopts ":t:f:h" opt; do
  case "$opt" in
    t) TIMEOUT="$OPTARG" ;;
    f) FILE="$OPTARG" ;;
    h) usage ;;
    *) usage ;;
  esac
done
shift $((OPTIND - 1))

URL="$(grep -oP '^PROTEA_DB_URL=\K.*' "$ROOT/.env" 2>/dev/null | head -1 | sed 's#postgresql+psycopg#postgresql#')"
if [[ -z "$URL" ]]; then
  echo "db-query.sh: PROTEA_DB_URL not found in $ROOT/.env" >&2
  exit 1
fi

# Server-side, for the whole session. A SET inside the SQL cannot lift these
# because the connection carries them from the start.
export PGOPTIONS="-c default_transaction_read_only=on -c statement_timeout=$TIMEOUT -c idle_in_transaction_session_timeout=60s"
export PGAPPNAME="protea-db-query"

if [[ -n "$FILE" ]]; then
  exec psql "$URL" -v ON_ERROR_STOP=1 -f "$FILE"
elif [[ $# -gt 0 ]]; then
  exec psql "$URL" -v ON_ERROR_STOP=1 -c "$*"
else
  exec psql "$URL" -v ON_ERROR_STOP=1 -f -
fi
