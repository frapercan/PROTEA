#!/usr/bin/env bash
# deploy-check.sh -- is what is SERVED the same as what is on disk?
#
# WHY THIS EXISTS. On 2026-08-20 the benchmark page stopped loading, and the
# cause was in neither the API nor the database. The frontend process had
# been running since 03:23 with its working directory at
# .next/standalone, and a rebuild at 07:18 deleted and recreated that
# directory underneath it. It served HTML from one build while the static
# chunks resolved against another. Five chunk hashes happened to match
# between the two builds and one did not, and that single 500 stopped
# hydration: the shell rendered, nothing else did.
#
# The same hour, the API was found serving a router that had been merged two
# hours earlier and never restarted, so an endpoint that existed in the
# repository returned 404 in production.
#
# Both are the same failure: a live process older than the tree it came
# from. Nothing detected either one. Not CI, which tests the tree and never
# looks at what is running; not a health check, because both processes were
# up and answering; and not the deploy, which rebuilt without restarting.
#
# So this checks the one thing neither of those can: agreement between the
# running processes and the files on disk. Read-only. It starts nothing,
# stops nothing and signals nothing.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WEB="$ROOT/apps/web"
FRONT="${PROTEA_FRONTEND_URL:-http://localhost:3000}"
API="${PROTEA_API_URL:-http://localhost:8000}"
PROBE_PATH="${PROTEA_PROBE_PATH:-/es/instrument/benchmark}"

fail=0
ok()   { printf '  ok    %s\n' "$*"; }
bad()  { printf '  FAIL  %s\n' "$*"; fail=$((fail + 1)); }
warn() { printf '  warn  %s\n' "$*"; }

# ── is any managed process older than the code it loaded? ────────────────────
#
# THE CHECK THAT WOULD HAVE CAUGHT ALL OF TODAY'S. Five services were found
# running code older than the tree they run from: the frontend from a deleted
# build, the API without a router merged two hours earlier, the API again
# fifteen minutes behind its own fix, and sixteen workers ten hours behind a
# payload field the coordinator needed.
#
# Neither obvious diagnostic finds them alone. Reading /proc/<pid>/cwd and the
# commit there reports clean when the tree was pulled forward underneath a
# process that had already imported the old modules. Comparing against the
# checkout you happen to be standing in reports everything stale when that is a
# different checkout, which is how the first version of this block failed: run
# from a worktree, it condemned twelve healthy processes.
#
# So it does both. Each process is resolved to the tree it actually runs from,
# and compared against the newest source in THAT tree. It says nothing about
# which commit matters, which is why it catches cases nobody thought to check.
newest_mtime() {
  find "$@" -type f -name '*.py' -printf '%T@\n' 2>/dev/null |
    sort -rn | head -1 | cut -d. -f1
}

check_age() {
  local label="$1" pid="$2" tree_mtime="$3" what="$4"
  [[ -z "$pid" || -z "$tree_mtime" ]] && return 0
  local started_ago now started
  started_ago="$(ps -o etimes= -p "$pid" 2>/dev/null | tr -d ' ')"
  [[ -z "$started_ago" ]] && return 0
  now="$(date +%s)"
  started=$(( now - started_ago ))
  if (( started < tree_mtime )); then
    bad "$label (pid $pid) started $(( (tree_mtime - started) / 60 )) min before $what changed"
    echo "        it is serving code that is no longer on disk; restart it"
  else
    ok "$label is newer than $what"
  fi
}

# The tree a process runs from, or empty when it cannot be trusted. A cwd
# marked (deleted) is reported by the caller as its own, worse, condition.
process_tree() {
  local cwd
  cwd="$(readlink "/proc/$1/cwd" 2>/dev/null || true)"
  [[ -z "$cwd" || "$cwd" == *"(deleted)"* ]] && return 0
  # Walk up to whatever holds a protea package, so a process started from a
  # subdirectory still resolves to its own checkout rather than to none.
  while [[ "$cwd" != "/" ]]; do
    [[ -d "$cwd/protea" ]] && { printf '%s' "$cwd"; return 0; }
    cwd="$(dirname "$cwd")"
  done
}

echo "== process age against the tree it runs from =="
declare -A TREE_MTIME=()
age_of_tree() {
  local t="$1"
  [[ -z "$t" ]] && return 0
  if [[ -z "${TREE_MTIME[$t]:-}" ]]; then
    TREE_MTIME[$t]="$(newest_mtime "$t/protea")"
  fi
  printf '%s' "${TREE_MTIME[$t]}"
}

checked=0
# Matched on the full command line rather than on a substring, and only for
# processes that are actually python. A `pgrep -f` on the bare name also finds
# any shell whose own command line mentions it, which here meant this script
# inspecting a process that was itself a `pgrep` for workers. That is the same
# mistake that has killed the wrong process on this host before, so the
# structure is: find candidates loosely, then confirm each one strictly.
is_python_service() {
  local cmd="$1"
  [[ "$cmd" =~ python[^[:space:]]*[[:space:]] ]] || return 1
  [[ "$cmd" =~ scripts/worker\.py[[:space:]]+--queue[[:space:]]+[A-Za-z0-9._-]+ ]] && return 0
  [[ "$cmd" =~ uvicorn[[:space:]]+protea\.api ]] && return 0
  return 1
}

for pid in $(pgrep -f 'uvicorn protea.api' 2>/dev/null) \
           $(pgrep -f 'scripts/worker\.py' 2>/dev/null); do
  [[ "$pid" == "$$" ]] && continue
  cmd="$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null || true)"
  is_python_service "$cmd" || continue
  if [[ "$cmd" == *"worker.py"* ]]; then
    queue="$(grep -oP -- '--queue \K[A-Za-z0-9._-]+' <<<"$cmd" | head -1)"
    # If the queue cannot be named, this is not a worker whatever else it
    # looks like. Checking something you cannot name produces a verdict
    # nobody can act on, which is worse than skipping it.
    [[ -z "$queue" ]] && continue
    label="worker $queue"
  else
    label="API"
  fi
  tree="$(process_tree "$pid")"
  if [[ -z "$tree" ]]; then
    bad "$label (pid $pid) runs from a directory that no longer exists"
    continue
  fi
  check_age "$label" "$pid" "$(age_of_tree "$tree")" "$tree/protea"
  checked=$((checked + 1))
done
[[ "$checked" == 0 ]] && warn "no API or worker process found"

echo "== frontend =="

# 1. A process whose working directory has been deleted is serving a build
#    that no longer exists. This is the failure that took the page down.
found_proc=0
for pid in $(pgrep -f 'next-server|standalone/server.js' 2>/dev/null); do
  [[ "$pid" == "$$" ]] && continue
  cwd="$(readlink "/proc/$pid/cwd" 2>/dev/null || true)"
  [[ -z "$cwd" ]] && continue
  found_proc=1
  if [[ "$cwd" == *"(deleted)"* ]]; then
    bad "pid $pid runs from a DELETED directory: $cwd"
    echo "        a rebuild replaced the tree underneath it; restart the frontend"
  else
    ok "pid $pid working directory exists"
  fi
done
[[ "$found_proc" == 0 ]] && warn "no frontend process found"

# 2. The build the server reports against the build on disk. These differ
#    whenever a rebuild happened without a restart, which is the condition
#    that produces the mismatched chunks above.
disk_id="$(cat "$WEB/.next/BUILD_ID" 2>/dev/null || true)"
served_id="$(curl -sL -m 30 "$FRONT$PROBE_PATH" 2>/dev/null \
             | grep -o '"buildId":"[^"]*"' | head -1 | cut -d'"' -f4)"
if [[ -z "$disk_id" ]]; then
  warn "no BUILD_ID on disk (not a production build?)"
elif [[ -z "$served_id" ]]; then
  warn "served page exposes no buildId; falling back to the asset check"
elif [[ "$disk_id" == "$served_id" ]]; then
  ok "build id matches disk ($disk_id)"
else
  bad "build id MISMATCH: served $served_id, on disk $disk_id"
fi

# 3. The check that actually reproduces the symptom: fetch every asset the
#    page asks for. A single 404 or 500 here stops hydration, and the page
#    still returns 200 while showing nothing.
html="$(curl -sL -m 45 "$FRONT$PROBE_PATH" 2>/dev/null || true)"
assets="$(grep -o '/_next/static/[^"]*\.\(js\|css\)' <<<"$html" | sort -u)"
if [[ -z "$assets" ]]; then
  warn "no static assets referenced by $PROBE_PATH"
else
  n=0; broken=0
  while read -r a; do
    [[ -z "$a" ]] && continue
    n=$((n + 1))
    code="$(curl -s -o /dev/null -w '%{http_code}' -m 20 "$FRONT$a" 2>/dev/null)"
    [[ "$code" == "200" ]] || { bad "asset $code $a"; broken=$((broken + 1)); }
  done <<<"$assets"
  [[ "$broken" == 0 ]] && ok "$n assets on $PROBE_PATH all 200"
fi

echo "== api =="

spec="$(curl -s -m 45 "$API/openapi.json" 2>/dev/null || true)"
if [[ -z "$spec" ]]; then
  bad "API is not answering at $API"
else
  ok "API answering"
  # Every router in the tree should have its prefix present in the served
  # spec. A router merged but not restarted shows up here as a missing
  # prefix, which is exactly how the stratum members endpoint returned 404
  # while its code sat in the working tree.
  missing=0
  while read -r file; do
    prefix="$(grep -oP 'APIRouter\(\s*prefix\s*=\s*"\K[^"]+' "$file" 2>/dev/null | head -1)"
    [[ -z "$prefix" ]] && continue
    # The prefix may be followed by a sub-path or end the route outright:
    # rungs.py registers at exactly /v1/rungs, so requiring a trailing slash
    # reports four routers missing that are all present.
    if ! grep -qE "\"[^\"]*${prefix}(/|\")" <<<"$spec"; then
      bad "router $(basename "$file") declares prefix $prefix, absent from the served spec"
      missing=$((missing + 1))
    fi
  done < <(find "$ROOT/protea/api/routers" -name '*.py' ! -name '__init__.py' 2>/dev/null)
  [[ "$missing" == 0 ]] && ok "every router prefix in the tree is present in the served spec"
fi

  # A route that exists is not a route that works. This check once reported
  # "served and on-disk agree" while three endpoints returned 500, because
  # openapi.json is built from the code and never touches the database. The
  # cause was a model merged ahead of its migration: the ORM asked for a
  # column that did not exist, and every query loading EmbeddingConfig failed.
  # Nothing noticed, because the process that was running predated the merge.
  #
  # So: actually call a few. They are chosen to span the joins rather than to
  # be exhaustive, one per table cluster a broken migration would take out.
  echo "== api smoke =="
  for ep in \
    /v1/benchmark/embeddings \
    /v1/benchmark/matrix \
    /v1/embeddings/configs \
    /v1/annotations/evaluation-sets \
    /v1/scoring/configs \
    /v1/rungs \
    /v1/jobs
  do
    code="$(curl -s -o /dev/null -w '%{http_code}' -m 60 "$API$ep?limit=3" 2>/dev/null)"
    if [[ "$code" == "200" ]]; then
      ok "$code $ep"
    else
      bad "$code $ep"
      [[ "$code" == "500" ]] && echo "        journalctl --user -u protea-api -n 40"
    fi
  done

echo
if [[ "$fail" -gt 0 ]]; then
  echo "$fail check(s) failed: what is running is not what is on disk."
  exit 1
fi
echo "served and on-disk agree."
