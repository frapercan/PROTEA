#!/bin/bash
# Prepare the embedding bank of the photos (ADR-D62) in ONE CPU job, with the tools shipped
# as code only by send_builder_code.sh: every member's sequence of every photo named, once,
# keyed by md5 and sorted by length, cut into shards of about SHARD_TOKENS tokens. Nothing
# is downloaded and no data is sent: the photos are read where their builds left them, each
# input checked against its photo's manifest.
#
# Positional arguments (a cluster that cancels jobs submitted with --export takes them
# this way):
#   PYTHON CODE_DIR BANK_DIR SHARD_TOKENS PHOTO_DIR [PHOTO_DIR ...]
#SBATCH -J embedding-bank-prepare
#SBATCH -c 4
#SBATCH --mem=48G
#SBATCH -t 01:00:00
#SBATCH -o embedding-bank-prepare-%j.log
set -euo pipefail
PY=${1:?python}; CODE=${2:?code dir}; BANK=${3:?bank dir}; SHARD_TOKENS=${4:?shard tokens}
shift 4
[ $# -gt 0 ] || { echo "MISSING: no photo named"; exit 1; }
export PYTHONPATH=$CODE
log() { echo "$(date -Is) $*"; }
[ ! -e "$BANK/prepare.json" ] || { log "REFUSED: $BANK already holds a prepared bank"; exit 2; }
photos=()
for photo in "$@"; do
  [ -s "$photo/photo.json" ] || { log "MISSING $photo/photo.json"; exit 1; }
  photos+=(--photo "$photo")
done
log "start embedding bank prepare, $# photos, on $(hostname)"
t=$(date +%s)
"$PY" -m protea.core.embedding_bank.build prepare "${photos[@]}" --out "$BANK" \
  --shard-tokens "$SHARD_TOKENS"
log "prepare pass in $(( $(date +%s) - t )) s; sha256 $(sha256sum "$BANK/prepare.json" | cut -d' ' -f1)"
log "peak memory $(sstat -j "${SLURM_JOB_ID:-0}.batch" --format=MaxRSS -n 2>/dev/null | tr -d ' ' || echo unknown)"
log "BANK_PREPARED $BANK"
