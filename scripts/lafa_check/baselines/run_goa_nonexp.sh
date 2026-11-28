#!/usr/bin/env bash
# The oracle of rung 1's literal: democafa's own goa_nonexp baseline on the targets' rows at
# t0 that export_targets.py wrote (every code, NOT rows included, as GAF 2.2), with the
# non-experimental groups CAFA5's baseline reads. It reads files only: no store, no secrets.
# Run it in a unit:
#
#   systemd-run --user --unit=ladder-check-goa-nonexp -p MemoryMax=8G -p MemorySwapMax=0 \
#     --collect bash scripts/lafa_check/baselines/run_goa_nonexp.sh <out_dir> \
#     <python of the oracle environment>
#
# democafa writes each target's direct non-experimental terms at score 1; the platform
# writes their closure under t0's ontology, which the evaluator's propagation makes the same
# prediction. So the comparison closes the oracle's terms first, under the window's t0:
#
#   python scripts/lafa_check/baselines/compare_with_set.py <set id> \
#     <out_dir>/goa_nonexp/goa_nonexp.tsv --close-under <evaluation set id> --drop-roots
#
# The recorded departures: the platform takes a target's experimental NOT terms and their
# descendants out of its terms (the author's frame rule) where democafa drops NOT rows
# without negating anything, and ND rows reach only roots, which the evaluator drops.
set -euo pipefail
OUT=$(cd "${1:?out dir}" && pwd)
PY=${2:?python of the oracle environment}
DEMOCAFA=$("$PY" -c 'import democafa, os; print(os.path.dirname(democafa.__file__))')
T0="$OUT/inputs/t0"
W="$OUT/goa_nonexp"
mkdir -p "$W"
cd "$W"
# goa_nonexp.py reads its config.yaml from where it runs and writes a scratch file beside its
# output, so both paths are absolute.
cp "$DEMOCAFA/baselines/goa_nonexp_container/config.yaml" "$W/config.yaml"
cp "$OUT/targets.txt" "$W/targets.txt"
"$PY" "$DEMOCAFA/baselines/goa_nonexp_container/goa_nonexp.py" \
  --annot_file "$T0/goa_targets.gaf.gz" \
  --selected_go 'Computational,Phylogenetical,Electronic,ND,NAS' \
  --graph "$T0/go-basic.obo" --query_file "$W/targets.txt" \
  --output_baseline "$W/goa_nonexp.tsv"
sha256sum "$W/goa_nonexp.tsv"
