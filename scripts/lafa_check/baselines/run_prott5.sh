#!/usr/bin/env bash
# The oracle of the LAFA literal of the ProtT5 neighbour (rung 6b): democafa's own search and
# scoring, as CAFA_forever's PREDICT_PROTT5 runs them with --top_k 5 on the unpropagated
# train_terms.tsv. The inputs are written under <out_dir>/prott5 by the stage that compares
# the literal pool with it: query.h5 and db.h5, one dataset per accession holding the
# vector the pool searched, and train_terms.tsv (EntryID, term, aspect; the donors' direct
# truth-channel terms) with targets.txt. It reads files only. democafa's search is its GPU
# path (cupy and cuml besides baselines-environment.txt); the scoring runs on the CPU. Both
# run from a checkout of democafa_package at 1102e34. Run it in a unit:
#
#   systemd-run --user --unit=ladder-check-prott5 -p MemoryMax=16G -p MemorySwapMax=0 \
#     --collect bash scripts/lafa_check/baselines/run_prott5.sh <out_dir> \
#     <python of the oracle environment> <democafa_package checkout>
#
# Pass: the same neighbour accessions and term sets as the literal set, and scores equal
# after democafa's rescale of the retained distances (prott5_results_norm.tsv).
set -euo pipefail
OUT=$(cd "${1:?out dir}" && pwd)
PY=${2:?python of the oracle environment}
SOURCE_ARG=${3:-}
SOURCE=$(cd "${SOURCE_ARG:?a checkout of democafa_package}" && pwd)
[ "$(git -C "$SOURCE" rev-parse HEAD)" = "1102e34ec2cdc9aa3737ef8b48992b4fc7775e60" ] || {
  echo "REFUSED: $SOURCE is not democafa_package at 1102e34"; exit 2; }
BASELINES="$SOURCE/democafa/baselines"
W="$OUT/prott5"
for name in query.h5 db.h5 train_terms.tsv targets.txt; do
  [ -s "$W/$name" ] || { echo "MISSING: $W/$name"; exit 1; }
done
cd "$W"
# The search keeps the five nearest donors of each query and rescales the retained
# distances to percentages, writing prott5_results.tsv and prott5_results_norm.tsv.
"$PY" "$BASELINES/prott5_container/process_embeddings_gpu_optimized.py" \
  "$W/query.h5" "$W/db.h5" "$W/prott5_results.tsv" --top_k 5 --normalize
"$PY" "$BASELINES/prott5_container/prott5_chunks.py" \
  --annot_file "$W/train_terms.tsv" --query_file "$W/targets.txt" \
  --prott5_results "$W/prott5_results_norm.tsv" --output_baseline "$W/prott5.tsv"
sha256sum "$W/prott5_results_norm.tsv" "$W/prott5.tsv"
