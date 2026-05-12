#!/usr/bin/env sh
# protea-v18 LAFA submission entrypoint.
#
# Wraps ``protea-predict`` (from the base ``protea-method-runtime``
# image, ADR-D15) with the full PROTEA v18 pipeline configuration:
#
#   * --aspect_separated   one KNN per GO aspect (P/F/C)
#   * v6 features ON       NW/SW alignment + taxonomy voters + Anc2Vec
#                          + embedding-PCA projection (no --no_v6)
#   * reranker ON          per-aspect LightGBM boosters from the bundle
#                          (no --no_reranker)
#
# Lineage features (``lineage_is_ancestor_of_known``,
# ``lineage_is_descendant_of_known``, ``lineage_ancestor_of_count``,
# ``lineage_descendant_of_count``) are part of the canonical
# ALL_FEATURES list from protea-contracts v0.3.0+; if the per-aspect
# booster was trained with them the LightGBM missing-value branch
# routes the predictions without per-query known-set lookup. The v18
# bundle does not ship an explicit ``go_parents`` map; lineage compute
# can be re-added once a LAFA cutoff exposes per-query known
# annotations (out of scope for the v2 submission).
#
# Bind-mount layout (per LAFA container guide,
# anphan0828/LAFA_container_guide):
#
#   /bundle               frozen reference bundle (read-only)
#   /input/queries.fasta  FASTA of query proteins (read-only)
#   /output/              writable dir for predictions.tsv
#   /hf-cache             HuggingFace cache for ProtT5 weights
#
# Any positional args appended to ``docker run`` are forwarded to
# ``protea-predict`` verbatim, so the evaluator can override the
# defaults (e.g. switch to faiss backend, sweep K) without rebuilding.
# The three pinned pipeline flags (``--aspect_separated`` plus the
# implicit ``v6`` and ``reranker`` paths) cannot be undone from the
# command line; if a non-default run is needed, use the
# ``method-runtime`` base image directly.

set -eu

BUNDLE="${PROTEA_V18_BUNDLE:-/bundle}"
QUERY="${PROTEA_V18_QUERY:-/input/queries.fasta}"
OUTPUT="${PROTEA_V18_OUTPUT:-/output/predictions.tsv}"

if [ ! -d "${BUNDLE}" ]; then
    echo "[protea-v18] frozen bundle not bind-mounted at ${BUNDLE}" >&2
    exit 64
fi
if [ ! -f "${QUERY}" ]; then
    echo "[protea-v18] queries FASTA not bind-mounted at ${QUERY}" >&2
    exit 65
fi
OUT_DIR="$(dirname "${OUTPUT}")"
if [ ! -d "${OUT_DIR}" ]; then
    echo "[protea-v18] output dir not bind-mounted at ${OUT_DIR}" >&2
    exit 66
fi

echo "[protea-v18] bundle=${BUNDLE} query=${QUERY} output=${OUTPUT}"
exec python /app/protea_predict.py \
    --query_file "${QUERY}" \
    --frozen_data_dir "${BUNDLE}" \
    --output "${OUTPUT}" \
    --aspect_separated \
    "$@"
