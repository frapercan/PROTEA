#!/usr/bin/env bash
# The oracle of rung 0's literal: democafa's own Naive, as CAFA_forever's workflow runs it
# (modules/local/timepoint_build.nf, then modules/local/predictions.nf, PREDICT_NAIVE), on
# the t0 inputs scripts/lafa_check/export_inputs.py wrote. The t0 GAF is kept to the
# Swiss-Prot release of the cut (filter_gaf), retrieve_terms builds train_terms.tsv from it
# with the workflow's -sgc 'Experimental,IC,TAS' and no propagation, and naive.py scores the
# targets with --n_terms 1500. The baselines' scripts are not in democafa's installed package
# (their helpers and config.yaml are left out of it), so they run from a checkout of
# democafa_package at 1102e34, which is checked. At that commit retrieve_terms writes a
# fourth column (the row's date) that naive.py refuses ("Expected 3 columns"), so the date
# is cut before naive.py reads the file; the Naive never reads it. It reads files only: no
# store, no secrets. Run it in a unit:
#
#   systemd-run --user --unit=ladder-check-naive -p MemoryMax=8G -p MemorySwapMax=0 --collect \
#     bash scripts/lafa_check/baselines/run_naive.sh <out_dir> <python of the oracle environment> \
#     <targets.txt> <democafa_package checkout>
#
# then compare the literal set with naive.tsv: every score within 1e-7 (democafa writes
# float32) and the same term set, one vector for every target:
#
#   python scripts/lafa_check/baselines/compare_with_set.py <set id> <out_dir>/naive/naive.tsv \
#     --vector --tolerance 1e-7
set -euo pipefail
OUT=$(cd "${1:?out dir}" && pwd)
PY=${2:?python of the oracle environment}
TARGETS=$(cd "$(dirname "${3:?targets.txt}")" && pwd)/$(basename "$3")
SOURCE_ARG=${4:-}
SOURCE=$(cd "${SOURCE_ARG:?a checkout of democafa_package}" && pwd)
[ "$(git -C "$SOURCE" rev-parse HEAD)" = "1102e34ec2cdc9aa3737ef8b48992b4fc7775e60" ] || {
  echo "REFUSED: $SOURCE is not democafa_package at 1102e34"; exit 2; }
BASELINES="$SOURCE/democafa/baselines"
T0="$OUT/inputs/t0"
W="$OUT/naive"
mkdir -p "$W"
cd "$W"
"$PY" -m democafa.datacollection.filter_gaf -a "$T0/goa_uniprot_all.gaf.gz" \
  -q "$T0/uniprot_sprot.fasta.gz" -o "$W/goa_uniprot_sprot.gaf.gz"
"$PY" -m democafa.datacollection.retrieve_terms --annot "$W/goa_uniprot_sprot.gaf.gz" \
  -sgc 'Experimental,IC,TAS' -g "$T0/go-basic.obo" --tsv "$W/train_terms.tsv"
cut -f1-3 "$W/train_terms.tsv" > "$W/train_terms_three_columns.tsv"
# naive.py reads its config.yaml from where it runs, and its queries from a .txt file.
cp "$BASELINES/naive_container/config.yaml" "$W/config.yaml"
cp "$TARGETS" "$W/targets.txt"
"$PY" "$BASELINES/naive_container/naive.py" --annot_file "$W/train_terms_three_columns.tsv" \
  --query_file "$W/targets.txt" --graph "$T0/go-basic.obo" --output_baseline "$W/naive.tsv" \
  --n_terms 1500
sha256sum "$W/train_terms.tsv" "$W/naive.tsv"
