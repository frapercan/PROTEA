#!/bin/bash
# The self-alignment bit scores of the queries of one or more cuts' homology tables, in ONE
# job, with the tools shipped as code only by send_builder_code.sh: the prepare pass over the
# photos already on the cluster (each table's queries, the superset's unique sequences, taken
# as the table's own prepare pass takes them, and their union), the forced alignment of each
# sequence with itself with MMseqs2 at the tables' scoring and no e-value cut, and one
# manifest per cut. Nothing is downloaded and no data is sent.
#
# MMSEQS is the static MMseqs2 binary the tables ran (fetched on the login node from the
# release the record pins), and it must hash to MMSEQS_SHA256 before it runs. SAMPLE is
# "full", or a number of queries of the first photo to align and check against an
# easy-search of the same queries in that photo's bank, self hits kept: the two bit scores
# must be equal. A sampled manifest says "measurement" and the platform does not load it.
# Each cut's manifest says complete, measurement or refused (exit 3 when refused, 2 when a
# pass refuses its inputs, 4 when the sampled check finds a bit score that differs).
#
# Positional arguments (a cluster that cancels jobs submitted with --export takes them
# this way):
#   PYTHON CODE_DIR OUT REVISION MMSEQS MMSEQS_SHA256 SAMPLE PHOTO_DIR [PHOTO_DIR ...]
#SBATCH -J homology-self-scores
#SBATCH -c 32
#SBATCH --mem=32G
#SBATCH -t 00:45:00
#SBATCH -o homology-self-scores-%j.log
set -euo pipefail
PY=${1:?python}; CODE=${2:?code dir}; OUT=${3:?out}; REV=${4:?revision}
MMSEQS=${5:?mmseqs binary}; MMSEQS_SHA=${6:?mmseqs sha256}; SAMPLE=${7:?full or a sample size}
shift 7
[ "$#" -ge 1 ] || { echo "MISSING: at least one photo directory"; exit 1; }
export PYTHONPATH=$CODE
log() { echo "$(date -Is) $*"; }
photos=()
for photo in "$@"; do
  [ -s "$photo/photo.json" ] || { log "MISSING $photo/photo.json"; exit 1; }
  photos+=(--photo "$photo")
done
[ -x "$MMSEQS" ] || { log "MISSING $MMSEQS"; exit 1; }
[ ! -e "$OUT/prepare.json" ] || { log "REFUSED: $OUT already holds a run"; exit 2; }
sample=()
if [ "$SAMPLE" != "full" ]; then
  [[ "$SAMPLE" =~ ^[1-9][0-9]*$ ]] || { log "REFUSED: SAMPLE is full or a positive count"; exit 2; }
  sample=(--sample "$SAMPLE")
fi
mkdir -p "$OUT"
TMP=${TMPDIR:-/tmp}/homology-self-scores-${SLURM_JOB_ID:-$$}
mkdir -p "$TMP"
trap 'rm -rf "$TMP"' EXIT
threads=${SLURM_CPUS_PER_TASK:-8}
stage() { "$PY" -m protea.core.homology.self_scores "$@"; }
log "start self scores, $# photos, sample $SAMPLE, code $REV on $(hostname), $threads threads"
t=$(date +%s)
stage prepare "${photos[@]}" --out "$OUT" "${sample[@]}"
log "prepare pass in $(( $(date +%s) - t )) s"; t=$(date +%s)
stage align --out "$OUT" --mmseqs "$MMSEQS" --mmseqs-sha256 "$MMSEQS_SHA" --threads "$threads"
log "align pass in $(( $(date +%s) - t )) s"
checked=0
if [ "$SAMPLE" != "full" ]; then
  t=$(date +%s)
  set +e
  stage check --out "$OUT" --mmseqs "$MMSEQS" --threads "$threads" --tmp "$TMP"; checked=$?
  set -e
  log "check pass in $(( $(date +%s) - t )) s, exit $checked (0 every bit score equal, 4 some differ)"
fi
set +e
stage manifest --out "$OUT" --revision "$REV"; rc=$?
set -e
for manifest in "$OUT"/*/self-scores.json; do
  log "manifest $manifest sha256 $(sha256sum "$manifest" | cut -d' ' -f1)"
done
log "manifest exit $rc (0 complete or measurement, 3 refused)"
log "peak memory $(sstat -j "${SLURM_JOB_ID:-0}.batch" --format=MaxRSS -n 2>/dev/null | tr -d ' ' || echo unknown)"
[ "$checked" -eq 0 ] || exit "$checked"
log "HOMOLOGY_SELF_SCORES_BUILT $OUT"
exit "$rc"
