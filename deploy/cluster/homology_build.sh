#!/bin/bash
# Build the homology table of a cut (ADR-D61) in ONE job, with the tools shipped as code
# only by send_builder_code.sh: the prepare pass over the photo already on the cluster
# (its superset as queries, every member as the bank, keyed by sequence md5), the
# MMseqs2 search with the tool's defaults, and the manifest. Nothing is downloaded and
# no data is sent: the photo's outputs are read where the photo build left them, each
# checked against the photo's manifest.
#
# MMSEQS is the static MMseqs2 binary, fetched on the login node from the release the
# record pins, and it must hash to MMSEQS_SHA256 before it runs. SAMPLE (optional) keeps
# that many queries for the measured first job; its manifest says "measurement" and the
# platform does not load it. The manifest says complete, measurement or refused (the
# build exits 3 when refused, 2 when a pass refuses its inputs).
#
# Positional arguments (a cluster that cancels jobs submitted with --export takes them
# this way):
#   PYTHON CODE_DIR PHOTO_DIR OUT REVISION MMSEQS MMSEQS_SHA256 [SAMPLE]
#SBATCH -J homology-build
#SBATCH -c 32
#SBATCH --mem=96G
#SBATCH -t 02:00:00
#SBATCH -o homology-build-%j.log
set -euo pipefail
PY=${1:?python}; CODE=${2:?code dir}; PHOTO=${3:?photo dir}; OUT=${4:?out}
REV=${5:?revision}; MMSEQS=${6:?mmseqs binary}; MMSEQS_SHA=${7:?mmseqs sha256}
SAMPLE=${8:-}
export PYTHONPATH=$CODE
log() { echo "$(date -Is) $*"; }
[ -s "$PHOTO/photo.json" ] || { log "MISSING $PHOTO/photo.json"; exit 1; }
[ -x "$MMSEQS" ] || { log "MISSING $MMSEQS"; exit 1; }
[ ! -e "$OUT/homology.json" ] || { log "REFUSED: $OUT already holds a table"; exit 2; }
mkdir -p "$OUT"
TMP=${TMPDIR:-/tmp}/homology-${SLURM_JOB_ID:-$$}
mkdir -p "$TMP"
trap 'rm -rf "$TMP"' EXIT
threads=${SLURM_CPUS_PER_TASK:-8}
# MMseqs2 reads the node's memory, not the job's: split its index to 70 % of the job.
split=""
[ -n "${SLURM_MEM_PER_NODE:-}" ] && split="$(( SLURM_MEM_PER_NODE * 7 / 10 ))M"
build() { "$PY" -m protea.core.homology.build "$@"; }
log "start homology build, photo $PHOTO, code $REV on $(hostname), $threads threads"
t=$(date +%s)
build prepare --photo "$PHOTO" --out "$OUT" ${SAMPLE:+--sample "$SAMPLE"}
log "prepare pass in $(( $(date +%s) - t )) s"; t=$(date +%s)
build search --out "$OUT" --mmseqs "$MMSEQS" --mmseqs-sha256 "$MMSEQS_SHA" \
  --threads "$threads" ${split:+--split-memory-limit "$split"} --tmp "$TMP"
log "search pass in $(( $(date +%s) - t )) s"
set +e
build manifest --out "$OUT" --revision "$REV"; rc=$?
set -e
log "manifest exit $rc (0 complete or measurement, 3 refused); sha256 $(sha256sum "$OUT/homology.json" | cut -d' ' -f1)"
log "peak memory $(sstat -j "${SLURM_JOB_ID:-0}.batch" --format=MaxRSS -n 2>/dev/null | tr -d ' ' || echo unknown)"
log "HOMOLOGY_BUILT $OUT"
