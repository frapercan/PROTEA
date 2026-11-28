#!/usr/bin/env bash
# The oracle of rung 0's literal: democafa's own Naive, as CAFA_forever's workflow runs it
# (modules/local/timepoint_build.nf, then modules/local/predictions.nf, PREDICT_NAIVE), on
# the t0 inputs scripts/lafa_check/export_inputs.py wrote. The t0 GAF is kept to the
# Swiss-Prot release of the cut (filter_gaf), retrieve_terms builds train_terms.tsv from it
# with the workflow's -sgc 'Experimental,IC,TAS' and no propagation, and naive.py scores the
# targets with --n_terms 1500. It reads files only: no store, no secrets. Run it in a unit:
#
#   systemd-run --user --unit=ladder-check-naive -p MemoryMax=8G -p MemorySwapMax=0 --collect \
#     bash scripts/lafa_check/baselines/run_naive.sh <out_dir> <python of the oracle environment> \
#     <targets.txt>
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
DEMOCAFA=$("$PY" -c 'import democafa, os; print(os.path.dirname(democafa.__file__))')
T0="$OUT/inputs/t0"
W="$OUT/naive"
mkdir -p "$W"
cd "$W"
"$PY" -m democafa.datacollection.filter_gaf -a "$T0/goa_uniprot_all.gaf.gz" \
  -q "$T0/uniprot_sprot.fasta.gz" -o "$W/goa_uniprot_sprot.gaf.gz"
"$PY" -m democafa.datacollection.retrieve_terms --annot "$W/goa_uniprot_sprot.gaf.gz" \
  -sgc 'Experimental,IC,TAS' -g "$T0/go-basic.obo" --tsv "$W/train_terms.tsv"
# naive.py reads its config.yaml from where it runs, and its queries from a .txt file.
cp "$DEMOCAFA/baselines/naive_container/config.yaml" "$W/config.yaml"
cp "$TARGETS" "$W/targets.txt"
"$PY" "$DEMOCAFA/baselines/naive_container/naive.py" --annot_file "$W/train_terms.tsv" \
  --query_file "$W/targets.txt" --graph "$T0/go-basic.obo" --output_baseline "$W/naive.tsv" \
  --n_terms 1500
sha256sum "$W/train_terms.tsv" "$W/naive.tsv"
