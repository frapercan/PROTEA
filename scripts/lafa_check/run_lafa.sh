#!/usr/bin/env bash
# LAFA's own code on the inputs export_inputs.py wrote, step by step as
# CAFA_forever's workflow calls it (modules/local/timepoint_build.nf, then
# modules/local/evaluation.nf). It reads files only: no store, no secrets.
#
#   bash scripts/lafa_check/run_lafa.sh <out_dir> <python of the LAFA environment>
set -euo pipefail
OUT="$1"
PY="$2"
LAFA="$OUT/lafa"
mkdir -p "$LAFA/t0" "$LAFA/t1"
cd "$LAFA"
for end in t0 t1; do
  "$PY" -m democafa.datacollection.filter_gaf -a "$OUT/inputs/$end/goa_uniprot_all.gaf.gz" \
    -q "$OUT/inputs/$end/uniprot_sprot.fasta.gz" -o "$LAFA/$end/goa_uniprot_sprot.gaf.gz"
  "$PY" -m democafa.datacollection.retrieve_terms --annot "$LAFA/$end/goa_uniprot_sprot.gaf.gz" \
    -sgc 'Experimental,IC,TAS' -g "$OUT/inputs/$end/go-basic.obo" --tsv "$LAFA/$end/train_terms.tsv"
  "$PY" -m democafa.datacollection.propagate_and_ia --terms "$LAFA/$end/train_terms.tsv" \
    --graph "$OUT/inputs/$end/go-basic.obo" --tsv_propagated "$LAFA/$end/train_terms_propagated.tsv" \
    --output_tsv "$LAFA/$end/IA.tsv"
  gunzip -c "$OUT/inputs/$end/uniprot_sprot.fasta.gz" > "$LAFA/$end/test_sequences.fasta"
done
"$PY" -m democafa.datacollection.compare_fasta "$LAFA/t0/test_sequences.fasta" \
  "$LAFA/t1/test_sequences.fasta" "$LAFA/diff_test_sequences.fasta"
"$PY" -m democafa.groundtruth.classify_ground_truth \
  --annot_known "$LAFA/t0/train_terms_propagated.tsv" --annot2 "$LAFA/t1/train_terms_propagated.tsv" \
  --query_file "$LAFA/diff_test_sequences_common.fasta" \
  --graph "$OUT/inputs/t0/go-basic.obo" --graph2 "$OUT/inputs/t1/go-basic.obo" \
  --out_prefix "$LAFA/groundtruth.tsv"
