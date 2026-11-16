#!/usr/bin/env bash
# Ship the photo builder to a cluster as code only (ADR-D59: only code is shipped; the
# public sources are downloaded on the cluster). Runs where the repository is.
#
# The files are builder_files.txt, taken from the commit REVISION with git archive (so
# the working tree never leaks into a send), and land in DEST_DIR/<short revision> on
# HOST, which is the CODE_DIR the jobs put on PYTHONPATH. Every name must be a Python
# file under protea/; anything else refuses the send.
#   send_builder_code.sh REVISION HOST DEST_DIR
set -euo pipefail
REV=${1:?revision}; HOST=${2:?ssh host}; DEST=${3:?destination directory}
HERE=$(cd "$(dirname "$0")" && pwd)
mapfile -t FILES < <(grep -vE '^\s*(#|$)' "$HERE/builder_files.txt")
for f in "${FILES[@]}"; do
  case "$f" in protea/*.py) ;; *) echo "REFUSED: $f is not a Python file under protea/"; exit 2 ;; esac
done
REPO=$(git -C "$HERE" rev-parse --show-toplevel)
FULL=$(git -C "$REPO" rev-parse --verify "$REV^{commit}")
DIR=$DEST/${FULL:0:7}
git -C "$REPO" archive --format=tar "$FULL" "${FILES[@]}" \
  | ssh -o BatchMode=yes "$HOST" "mkdir -p '$DIR' && tar -x -C '$DIR' && find '$DIR' -type f | wc -l"
echo "sent ${#FILES[@]} files of $FULL to $HOST:$DIR"
