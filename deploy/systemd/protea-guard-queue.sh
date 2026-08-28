#!/usr/bin/env bash
# protea-guard-queue.sh QUEUE
#
# Exit 0 if NO worker for exactly QUEUE is running; exit 1 if one already is.
# Used as ExecStartPre so a systemd worker instance refuses to start alongside
# a human-launched worker that is still draining a job.
#
# Lives in a script rather than inline in the unit because the exact-match
# anchor is a literal '$', and '$' in a systemd ExecStart= line is consumed by
# systemd's own variable expansion (it must be written '$$'). Keeping it here
# removes that escaping trap entirely.
#
# READ-ONLY: inspects /proc via pgrep, kills nothing, starts nothing.
set -euo pipefail

queue=${1:?usage: protea-guard-queue.sh QUEUE}

# Exact-queue match. Unanchored, "--queue protea.predictions" also matches the
# running protea.predictions.batch and protea.predictions.write workers, which
# would wrongly block an idle queue. Verified on this host: 3 pids vs 1.
if pids=$(pgrep -f "worker[.]py --queue ${queue}\$"); then
  echo "protea-guard-queue: worker(s) already running for ${queue}: ${pids//$'\n'/ }" >&2
  echo "protea-guard-queue: refusing to start a duplicate; leaving them alone." >&2
  exit 1
fi
exit 0
