#!/bin/bash
# The segment families of one cut and one lexicon (ADR-D65, decision 2) on CPU, with the
# tools shipped as code only by send_builder_code.sh: prepare over the photo and the
# complete lexicon bank already on the cluster (donors = the photo's truth members,
# targets = its other superset members, both by sequence md5; no label read), the three
# MMseqs2 searches, the families at both scopes and the manifest. Nothing is downloaded
# and no data is sent.
#
# MMSEQS is the static MMseqs2 binary the homology table pins (18-8cc5c); it must hash to
# MMSEQS_SHA256 before it runs. MODE runs one pass (prepare, search, strict, permissive,
# manifest) or all of them in order (all); a pass already done is not repeated by "all".
# The manifest exits 3 when refused; a pass that refuses its inputs exits 2.
#
# Positional arguments (a cluster that cancels jobs submitted with --export takes them
# this way):
#   PYTHON CODE_DIR PHOTO_DIR BANK_DIR LEXICON_NAME OUT REVISION MMSEQS MMSEQS_SHA256 [MODE]
#SBATCH -J segment-families
#SBATCH -c 32
#SBATCH --mem=96G
#SBATCH -t 02:00:00
#SBATCH -o segment-families-%j.log
set -euo pipefail
PY=${1:?python}; CODE=${2:?code dir}; PHOTO=${3:?photo dir}; BANK=${4:?bank dir}
NAME=${5:?lexicon name}; OUT=${6:?out}; REV=${7:?revision}; MMSEQS=${8:?mmseqs binary}
MMSEQS_SHA=${9:?mmseqs sha256}; MODE=${10:-all}
export PYTHONPATH=$CODE
log() { echo "$(date -Is) $*"; }
[ -s "$PHOTO/photo.json" ] || { log "MISSING $PHOTO/photo.json"; exit 1; }
[ -s "$BANK/lexicon/$NAME/lexicon-bank.json" ] || { log "MISSING the lexicon bank of $NAME"; exit 1; }
[ -x "$MMSEQS" ] || { log "MISSING $MMSEQS"; exit 1; }
mkdir -p "$OUT"
TMP=${TMPDIR:-/tmp}/segment-families-${SLURM_JOB_ID:-$$}
mkdir -p "$TMP"
trap 'rm -rf "$TMP"' EXIT
threads=${SLURM_CPUS_PER_TASK:-8}
build() { "$PY" -m protea.core.segment_families.build "$@"; }
tool=(--mmseqs "$MMSEQS" --mmseqs-sha256 "$MMSEQS_SHA" --threads "$threads" --tmp "$TMP")
log "start segment families ($MODE), photo $PHOTO, lexicon $NAME, code $REV on $(hostname), $threads threads"
step() {  # NAME DONE_FILE COMMAND...
  local name=$1 done=$2; shift 2
  if [ "$MODE" = all ] && [ -e "$OUT/$done" ]; then log "$name already done"; return; fi
  local t; t=$(date +%s); "$@"; log "$name in $(( $(date +%s) - t )) s"
}
case "$MODE" in all|prepare) step prepare prepare.json build prepare --photo "$PHOTO" --bank "$BANK" --name "$NAME" --out "$OUT" ;; esac
case "$MODE" in all|search) step search search.json build search --out "$OUT" "${tool[@]}" ;; esac
case "$MODE" in all|strict) step strict families-strict.json build families --out "$OUT" --scope strict "${tool[@]}" ;; esac
case "$MODE" in all|permissive) step permissive families-permissive.json build families --out "$OUT" --scope permissive "${tool[@]}" ;; esac
case "$MODE" in all|manifest)
  set +e; build manifest --out "$OUT" --revision "$REV"; rc=$?; set -e
  log "manifest exit $rc (0 complete, 3 refused); sha256 $(sha256sum "$OUT/segment-families.json" | cut -d' ' -f1)" ;;
esac
log "peak memory $(sstat -j "${SLURM_JOB_ID:-0}.batch" --format=MaxRSS -n 2>/dev/null | tr -d ' ' || echo unknown)"
log "SEGMENT_FAMILIES_DONE $OUT"
