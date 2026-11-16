#!/usr/bin/env bash
# Ship the photo builder to a cluster as code only (ADR-D59: only code is shipped; the
# public sources are downloaded on the cluster). Runs where the repository is.
#
# The files are the ones builder_files.txt lists AS THE COMMIT REVISION HAS IT, taken
# from that commit with git archive (so the working tree never leaks into a send, neither
# its files nor its list: a send of an older revision ships what that revision listed),
# and land in DEST_DIR/<short revision> on HOST: that directory is the CODE_DIR the jobs
# put on PYTHONPATH, and its deploy/cluster/ holds the job scripts of the same revision.
# Every name must be a Python file under protea/ or a job script under deploy/cluster/;
# anything else refuses the send.
#   send_builder_code.sh REVISION HOST DEST_DIR
set -euo pipefail
REV=${1:?revision}; HOST=${2:?ssh host}; DEST=${3:?destination directory}
HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(git -C "$HERE" rev-parse --show-toplevel)
FULL=$(git -C "$REPO" rev-parse --verify "$REV^{commit}")
LIST=$(git -C "$REPO" show "$FULL:deploy/cluster/builder_files.txt")
mapfile -t FILES < <(printf '%s\n' "$LIST" | grep -vE '^\s*(#|$)')
for f in "${FILES[@]}"; do
  case "$f" in
    protea/*.py | deploy/cluster/*.sh) ;;
    *) echo "REFUSED: $f is neither a Python file under protea/ nor a job script"; exit 2 ;;
  esac
done
DIR=$DEST/${FULL:0:7}
git -C "$REPO" archive --format=tar "$FULL" "${FILES[@]}" \
  | ssh -o BatchMode=yes "$HOST" "mkdir -p '$DIR' && tar -x -C '$DIR' && find '$DIR' -type f | wc -l"
echo "sent ${#FILES[@]} files of $FULL to $HOST:$DIR"
