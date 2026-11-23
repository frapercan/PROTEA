#!/bin/bash
# Build the TRUTH-ONLY photo of a cut that only ends a window (ADR-D59; 205, the end of
# 177->205) in ONE job, with the tools shipped as code only by send_builder_code.sh: the
# superset measure pass on the paired Swiss-Prot archive, then the members (truth-only:
# no UniRef neighbours), rows, sequences and manifest passes. No UniRef and no TrEMBL is
# read: no method predicts from such a cut; a window reads from it what each target
# gained and whether it is still in Swiss-Prot.
#
# Reads the GOA file and the paired Swiss-Prot archive fetch_source.sh downloaded on the
# login node (SRC_DIR). The GOA file must hash to GOA_SHA256 before a line of it is read.
# The superset table and its record go to MEASURED_DIR; the photo to OUT. The manifest
# says complete or refused (the build exits 3 when refused).
#
# Positional arguments (a cluster that cancels jobs submitted with --export takes them
# this way):
#   PYTHON CODE_DIR CUT CUT_DATE UNIPROT_RELEASE UNIPROT_PUBLISHED SRC_DIR MEASURED_DIR
#   OUT REVISION GOA_URL GOA_SHA256 UNIPROT_URL
#SBATCH -J cut-photo-truth
#SBATCH -c 6
#SBATCH --mem=16G
#SBATCH -t 01:30:00
#SBATCH -o cut-photo-truth-%j.log
set -euo pipefail
PY=${1:?python}; CODE=${2:?code dir}; CUT=${3:?cut}; CUT_DATE=${4:?cut date}
UREL=${5:?uniprot release}; UPUB=${6:?uniprot published}; SRC=${7:?source dir}
MEAS=${8:?measured dir}; OUT=${9:?out}; REV=${10:?revision}
GOA_URL=${11:?goa url}; GOA_SHA=${12:?goa sha256}; UNIPROT_URL=${13:?uniprot url}
export PYTHONPATH=$CODE
log() { echo "$(date -Is) $*"; }
[[ "$CUT" =~ ^[0-9]+$ ]] && [ "$CUT" -le 227 ] || { log "REFUSED: cut $CUT is after the mark 227"; exit 2; }
GOA=$SRC/goa_uniprot_all.gaf.$CUT.gz
ARCHIVE=$SRC/uniprot_sprot-only$UREL.tar.gz
for f in "$GOA" "$ARCHIVE"; do [ -s "$f" ] || { log "MISSING $f"; exit 1; }; done
[ ! -e "$OUT/photo.json" ] || { log "REFUSED: $OUT already holds a photo"; exit 2; }
got=$(sha256sum "$GOA" | cut -d' ' -f1)
[ "$got" = "$GOA_SHA" ] || { log "REFUSED: $GOA hashes to $got, not $GOA_SHA"; exit 2; }
log "GOA sha256 ok"
mkdir -p "$MEAS" "$OUT"
build() { "$PY" -m protea.core.cut_photo.build "$@"; }
at_cut=(--cut-release "$CUT" --cut-date "$CUT_DATE" --out "$OUT")
goa_file=(--source-url "$GOA_URL" --source-sha256 "$GOA_SHA")
log "start truth-only photo, cut $CUT, code $REV on $(hostname)"
t=$(date +%s)
"$PY" -m protea.core.cut_photo.measure superset --archive "$ARCHIVE" --release "$UREL" \
  --published "$UPUB" --cut-release "$CUT" --cut-date "$CUT_DATE" --out "$MEAS" --revision "$REV"
log "superset pass in $(( $(date +%s) - t )) s"; t=$(date +%s)
pigz -dc -p 4 "$GOA" | build members --superset-table "$MEAS/superset.tsv" --truth-only \
  --release "$CUT" --published "$CUT_DATE" "${at_cut[@]}" "${goa_file[@]}"
log "members pass in $(( $(date +%s) - t )) s"; t=$(date +%s)
pigz -dc -p 4 "$GOA" | build rows --members "$OUT/members.parquet" \
  --release "$CUT" --published "$CUT_DATE" "${at_cut[@]}" "${goa_file[@]}"
log "rows pass in $(( $(date +%s) - t )) s"; t=$(date +%s)
build sequences --archive "$ARCHIVE" --release "$UREL" --published "$UPUB" "${at_cut[@]}" \
  --source-url "$UNIPROT_URL"
log "sequences pass in $(( $(date +%s) - t )) s"
set +e
build manifest --out "$OUT" --revision "$REV"; rc=$?
set -e
log "manifest exit $rc (0 complete, 3 refused); sha256 $(sha256sum "$OUT/photo.json" | cut -d' ' -f1)"
log "peak memory $(sstat -j "${SLURM_JOB_ID:-0}.batch" --format=MaxRSS -n 2>/dev/null | tr -d ' ' || echo unknown)"
log "CUT_PHOTO_BUILT $OUT"
