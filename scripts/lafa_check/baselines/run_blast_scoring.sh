#!/usr/bin/env bash
# The scoring oracle of rung 2a: democafa's own BLAST scorer (blast_chunks.py, which takes
# each subject's best HSP and scores a term with the largest pident / 100 among the subjects
# that carry it) on the hits and donor terms export_blast_inputs.py wrote from a top-hit
# homology transfer set: the set's hits expanded to its admitted donor accessions, twins at
# pident 100, as BLAST tabular (pident = 100 x fident, nident = round(fident x alnlen)), and
# the donors' states at t0 as the set read them. It checks the scoring, not the search: the
# search's parity with BLAST+ settings is a cluster diagnostic. The scorer runs from a
# checkout of democafa_package at 1102e34. It reads files only. Run it in a unit:
#
#   systemd-run --user --unit=ladder-check-blast-scoring -p MemoryMax=8G -p MemorySwapMax=0 \
#     --collect bash scripts/lafa_check/baselines/run_blast_scoring.sh <out_dir> \
#     <python of the oracle environment> <democafa_package checkout>
#
# then compare the set with blast_scores.tsv, identical scores on the same terms (the donor
# terms hold no roots; democafa's own propagated files would add them, which the evaluator
# drops):
#
#   python scripts/lafa_check/baselines/compare_with_set.py <set id> \
#     <out_dir>/blast/blast_scores.tsv --tolerance 1e-6 --drop-roots
set -euo pipefail
OUT=$(cd "${1:?out dir}" && pwd)
PY=${2:?python of the oracle environment}
SOURCE_ARG=${3:-}
SOURCE=$(cd "${SOURCE_ARG:?a checkout of democafa_package}" && pwd)
[ "$(git -C "$SOURCE" rev-parse HEAD)" = "1102e34ec2cdc9aa3737ef8b48992b4fc7775e60" ] || {
  echo "REFUSED: $SOURCE is not democafa_package at 1102e34"; exit 2; }
BASELINES="$SOURCE/democafa/baselines"
W="$OUT/blast"
[ -s "$W/blast.tsv" ] && [ -s "$W/donor_terms.tsv" ] && [ -s "$W/targets.txt" ] || {
  echo "MISSING: run export_blast_inputs.py first"; exit 1; }
cd "$W"
NUM_THREADS=${NUM_THREADS:-4} "$PY" "$BASELINES/blast_container/blast_chunks.py" \
  --annot_file "$W/donor_terms.tsv" --query_file "$W/targets.txt" \
  --blast_results "$W/blast.tsv" --output_baseline "$W/blast_scores.tsv"
sha256sum "$W/blast.tsv" "$W/donor_terms.tsv" "$W/blast_scores.tsv"
