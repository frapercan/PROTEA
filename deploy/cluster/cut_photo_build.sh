#!/bin/bash
# Build the photo of a cut on a compute node (ADR-D59, step 2) in ONE job, with
# python -m protea.core.cut_photo.build shipped as code only by send_builder_code.sh:
# members, rows, sequences and manifest.
#
# Reads the GOA file and the paired Swiss-Prot archive fetch_source.sh downloaded on
# the login node (SRC_DIR), and the superset and neighbour tables cut_photo_measure.sh
# wrote, each with the record beside it (MEASURED_DIR). The GOA file is hashed here and
# must match GOA_SHA256, the digest recorded at download, before a line of it is read;
# its URL and digest, and the Swiss-Prot archive's URL, go into the manifest.
#
# The members outside Swiss-Prot take their sequence from the paired release's TrEMBL,
# read out of the release's full knowledgebase archive (KB_ARCHIVE: the path
# fetch_source.sh wrote the parts beside, FILE.part<N>, never joined). The parts are
# streamed in order, the md5 of the stream is written beside the photo
# (knowledgebase-stream.md5) and the manifest refuses the photo unless it is KB_MD5, the
# md5 UniProt publishes for the archive in the release's RELEASE.metalink.
#
# The manifest says complete or refused (the build exits 3 when refused). A refused
# photo still writes every output, and unknown_evidence.tsv lists each evidence key the
# channel table could not place, with its row count and up to three raw references, so
# the decision to classify it can be taken without reading GOA again.
#
# Positional arguments (a cluster that cancels jobs submitted with --export takes them
# this way):
#   PYTHON CODE_DIR CUT CUT_DATE UNIPROT_RELEASE UNIPROT_PUBLISHED SRC_DIR MEASURED_DIR
#   OUT REVISION GOA_URL GOA_SHA256 UNIPROT_URL KB_ARCHIVE KB_MD5 KB_URL
#SBATCH -J cut-photo-build
#SBATCH -c 8
#SBATCH --mem=32G
#SBATCH -t 02:00:00
#SBATCH -o cut-photo-build-%j.log
set -euo pipefail
PY=${1:?python}; CODE=${2:?code dir}; CUT=${3:?cut}; CUT_DATE=${4:?cut date}
UREL=${5:?uniprot release}; UPUB=${6:?uniprot published}; SRC=${7:?source dir}
MEAS=${8:?measured dir}; OUT=${9:?out}; REV=${10:?revision}
GOA_URL=${11:?goa url}; GOA_SHA=${12:?goa sha256}; UNIPROT_URL=${13:?uniprot url}
KB=${14:?knowledgebase archive}; KB_MD5=${15:?knowledgebase md5}; KB_URL=${16:?knowledgebase url}
export PYTHONPATH=$CODE
log() { echo "$(date -Is) $*"; }
[[ "$CUT" =~ ^[0-9]+$ ]] && [ "$CUT" -le 227 ] || { log "REFUSED: cut $CUT is after the mark 227"; exit 2; }
GOA=$SRC/goa_uniprot_all.gaf.$CUT.gz
ARCHIVE=$SRC/uniprot_sprot-only$UREL.tar.gz
for f in "$GOA" "$ARCHIVE" "$MEAS/superset.tsv" "$MEAS/superset.json" "$MEAS/neighbours.parquet" \
         "$MEAS/neighbours.json"; do
  [ -s "$f" ] || { log "MISSING $f"; exit 1; }
done
mapfile -t KB_PARTS < <(ls "$KB".part* 2>/dev/null | sort -V)
[ "${#KB_PARTS[@]}" -gt 0 ] || { log "MISSING the parts of $KB"; exit 1; }
[ ! -e "$OUT/photo.json" ] || { log "REFUSED: $OUT already holds a photo"; exit 2; }
got=$(sha256sum "$GOA" | cut -d' ' -f1)
[ "$got" = "$GOA_SHA" ] || { log "REFUSED: $GOA hashes to $got, not $GOA_SHA"; exit 2; }
log "GOA sha256 ok"
mkdir -p "$OUT"
build() { "$PY" -m protea.core.cut_photo.build "$@"; }
at_cut=(--cut-release "$CUT" --cut-date "$CUT_DATE" --out "$OUT")
goa_file=(--source-url "$GOA_URL" --source-sha256 "$GOA_SHA")
log "start build, cut $CUT, code $REV on $(hostname)"
t=$(date +%s)
pigz -dc -p 4 "$GOA" | build members --superset-table "$MEAS/superset.tsv" --release "$CUT" \
  --published "$CUT_DATE" "${at_cut[@]}" "${goa_file[@]}"
log "members pass in $(( $(date +%s) - t )) s"; t=$(date +%s)
pigz -dc -p 4 "$GOA" | build rows --members "$OUT/members.parquet" --neighbours "$MEAS/neighbours.parquet" \
  --release "$CUT" --published "$CUT_DATE" "${at_cut[@]}" "${goa_file[@]}"
log "rows pass in $(( $(date +%s) - t )) s"; t=$(date +%s)
build sequences --archive "$ARCHIVE" --release "$UREL" --published "$UPUB" "${at_cut[@]}" \
  --source-url "$UNIPROT_URL"
log "sequences pass in $(( $(date +%s) - t )) s"; t=$(date +%s)
rm -f "$OUT/knowledgebase-stream.md5" "$OUT/knowledgebase-stream.md5.tmp"
cat "${KB_PARTS[@]}" \
  | tee >(md5sum | cut -d' ' -f1 > "$OUT/knowledgebase-stream.md5.tmp") \
  | tar -xzOf - --wildcards '*uniprot_trembl.fasta.gz' | pigz -dc -p 4 \
  | build unreviewed --members "$OUT/members.parquet" --release "$UREL" --published "$UPUB" \
      "${at_cut[@]}" --source-url "$KB_URL" --source-md5 "$KB_MD5"
for _ in $(seq 1 120); do [ -s "$OUT/knowledgebase-stream.md5.tmp" ] && break; sleep 5; done
mv "$OUT/knowledgebase-stream.md5.tmp" "$OUT/knowledgebase-stream.md5"
log "TrEMBL pass in $(( $(date +%s) - t )) s, stream md5 $(cat "$OUT/knowledgebase-stream.md5")"
set +e
build manifest --out "$OUT" --revision "$REV"; rc=$?
set -e
log "manifest exit $rc (0 complete, 3 refused); sha256 $(sha256sum "$OUT/photo.json" | cut -d' ' -f1)"
log "peak memory $(sstat -j "${SLURM_JOB_ID:-0}.batch" --format=MaxRSS -n 2>/dev/null | tr -d ' ' || echo unknown)"
log "CUT_PHOTO_BUILT $OUT"
