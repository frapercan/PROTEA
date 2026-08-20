#!/usr/bin/env bash
# db-cancel.sh -- cancel a running query. Cancel only; never terminate.
#
# pg_cancel_backend sends the equivalent of an interrupt to the running
# statement and leaves the connection alive. It is not pg_terminate_backend
# and this script deliberately cannot reach that function: killing a
# connection can roll back work the client still believes in, and nothing
# a diagnostic does is worth that risk.
#
# WHY IT IS A SCRIPT AND NOT A psql CALL. The capability wanted is "may
# cancel a query", not "may execute arbitrary SQL". A script that takes pids
# and calls exactly one function is auditable in a way that a psql
# permission is not.
#
# Listing is the default because the pid is the thing a caller usually does
# not have, and because seeing the durations before cancelling is how you
# avoid cancelling something that was about to finish.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

URL="$(grep -oP '^PROTEA_DB_URL=\K.*' "$ROOT/.env" 2>/dev/null | head -1 | sed 's#postgresql+psycopg#postgresql#')"
if [[ -z "$URL" ]]; then
  echo "db-cancel.sh: PROTEA_DB_URL not found in $ROOT/.env" >&2
  exit 1
fi

export PGOPTIONS="-c statement_timeout=30s"
export PGAPPNAME="protea-db-cancel"

if [[ $# -eq 0 || "${1:-}" == "list" ]]; then
  psql "$URL" -P pager=off -c "
    SELECT pid,
           coalesce(host(client_addr), 'local')            AS client,
           coalesce(nullif(application_name, ''), '-')     AS app,
           backend_type,
           round(extract(epoch FROM now() - query_start))  AS seconds,
           left(regexp_replace(query, '\s+', ' ', 'g'), 60) AS query
    FROM pg_stat_activity
    WHERE state <> 'idle' AND pid <> pg_backend_pid()
    ORDER BY query_start;"
  exit 0
fi

for pid in "$@"; do
  if [[ ! "$pid" =~ ^[0-9]+$ ]]; then
    echo "db-cancel.sh: '$pid' is not a pid" >&2
    exit 2
  fi
done

# One statement, one function, pids bound as parameters rather than
# interpolated into SQL text.
psql "$URL" -P pager=off -c "
  SELECT pid,
         round(extract(epoch FROM now() - query_start)) AS was_running_seconds,
         pg_cancel_backend(pid)                         AS cancelled
  FROM pg_stat_activity
  WHERE pid = ANY(ARRAY[$(IFS=,; echo "$*")]::int[]);"
