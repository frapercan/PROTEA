#!/bin/bash
# Measure one cut on a compute node (ADR-D59, step 1), with PROTEA's own tool
# (protea.core.cut_photo.measure) shipped as code only by send_builder_code.sh.
#
# Two passes, from sources fetch_source.sh downloaded on the login node:
#   superset    the paired release's Swiss-Prot archive (its md5 checked at download);
#   neighbours  the paired release's UniRef archive, streamed from its parts in order
#               and never joined. Only the uniref50 member is read, through
#               UNIREF_LINE_FILTER, so no GO property of a UniRef entry is ever
#               written; the md5 of the stream is checked against the release's
#               metalink before the job reports success.
# Both write a record beside their table (superset.json, neighbours.json), which the
# build passes read to name and check what they were measured from.
#
# Positional arguments (a cluster that cancels jobs submitted with --export takes them
# this way):
#   PYTHON CODE_DIR CUT CUT_DATE UNIPROT_RELEASE UNIPROT_PUBLISHED SPROT_ARCHIVE
#   UNIREF_ARCHIVE UNIREF_MD5 OUT REVISION [UNIREF_INNER] [UNIREF_XML]
# UNIREF_ARCHIVE is the path fetch_source.sh wrote the parts beside (FILE.part<N>).
# UNIREF_INNER and UNIREF_XML name the uniref50 members inside it (uniref50.tar and
# uniref50.xml.gz in the 2022_03 archive); list an older archive's members first.
#SBATCH -J cut-photo-measure
#SBATCH -c 8
#SBATCH --mem=32G
#SBATCH -t 02:00:00
#SBATCH -o cut-photo-measure-%j.log
set -euo pipefail
PY=${1:?python}; CODE=${2:?code dir}; CUT=${3:?cut}; CUT_DATE=${4:?cut date}
REL=${5:?uniprot release}; REL_DATE=${6:?uniprot published}; SPROT=${7:?swiss-prot archive}
UNIREF=${8:?uniref archive}; UNIREF_MD5=${9:?uniref md5}; OUT=${10:?out}; REV=${11:?revision}
INNER=${12:-uniref50.tar}; XML=${13:-uniref50.xml.gz}
export PYTHONPATH=$CODE
log() { echo "$(date -Is) $*"; }
# The mark, checked before anything is read (the tool checks it again).
[[ "$CUT" =~ ^[0-9]+$ ]] && [ "$CUT" -le 227 ] || { log "REFUSED: cut $CUT is after the mark 227"; exit 2; }
mapfile -t PARTS < <(ls "$UNIREF".part* 2>/dev/null | sort -V)
[ "${#PARTS[@]}" -gt 0 ] || { log "MISSING the parts of $UNIREF"; exit 1; }
[ -s "$SPROT" ] || { log "MISSING $SPROT"; exit 1; }
mkdir -p "$OUT"
COMMON=(--cut-release "$CUT" --cut-date "$CUT_DATE" --out "$OUT" --revision "$REV")
log "start measure, cut $CUT ($CUT_DATE), UniProt $REL ($REL_DATE), code $REV on $(hostname)"

t=$(date +%s)
"$PY" -m protea.core.cut_photo.measure superset --archive "$SPROT" --release "$REL" \
  --published "$REL_DATE" "${COMMON[@]}"
log "superset pass in $(( $(date +%s) - t )) s"

t=$(date +%s)
FILTER=$("$PY" -c 'from protea.core.cut_photo.formats import UNIREF_LINE_FILTER as f; print(f)')
rm -f "$OUT/uniref-stream.md5"
cat "${PARTS[@]}" \
  | tee >(md5sum | cut -d' ' -f1 > "$OUT/uniref-stream.md5") \
  | tar -xzOf - "$INNER" | tar -xOf - "$XML" | pigz -dc -p 4 \
  | grep -E "$FILTER" \
  | "$PY" -m protea.core.cut_photo.measure neighbours --superset "$OUT/superset.tsv" \
      --release "$REL" --published "$REL_DATE" "${COMMON[@]}"
for _ in $(seq 1 120); do [ -s "$OUT/uniref-stream.md5" ] && break; sleep 5; done
got=$(cat "$OUT/uniref-stream.md5" 2>/dev/null || echo none)
[ "$got" = "$UNIREF_MD5" ] || { log "UNIREF STREAM MD5 MISMATCH $got != $UNIREF_MD5"; exit 1; }
log "neighbours pass in $(( $(date +%s) - t )) s, stream md5 ok"
log "CUT_PHOTO_MEASURED $OUT"
