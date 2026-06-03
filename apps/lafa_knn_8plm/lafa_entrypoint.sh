#!/usr/bin/env sh
# protea-knn-8plm LAFA submission entrypoint.
#
# Drives the 8-PLM ensemble (see ensemble_driver.py) with the v2
# configuration:
#
#   * aggregation = mean   score-level mean across the 8 PLMs
#   * aspect-separated KNN (hard-coded inside ensemble_driver)
#   * no v6 features, no learned reranker (those ship in protea-v18)
#
# Bind-mount layout (per LAFA container guide,
# anphan0828/LAFA_container_guide):
#
#   /bundle               frozen reference bundle with per-PLM matrices
#                         (plms/<key>/reference_embeddings.parquet)
#   /input/queries.fasta  FASTA of query proteins (read-only)
#   /output/              writable dir for predictions.tsv
#   /hf-cache             HuggingFace cache for PLM weights
#
# Any positional args appended to ``docker run`` are forwarded to the
# ensemble driver verbatim, so the evaluator can override K, the
# aggregation strategy, or the PLM subset without rebuilding.

set -eu

BUNDLE="${PROTEA_KNN_8PLM_BUNDLE:-/bundle}"
QUERY="${PROTEA_KNN_8PLM_QUERY:-/input/queries.fasta}"
OUTPUT="${PROTEA_KNN_8PLM_OUTPUT:-/output/predictions.tsv}"
AGGREGATION="${PROTEA_KNN_8PLM_AGGREGATION:-mean}"

if [ ! -d "${BUNDLE}" ]; then
    echo "[protea-knn-8plm] frozen bundle not bind-mounted at ${BUNDLE}" >&2
    exit 64
fi
if [ ! -f "${QUERY}" ]; then
    echo "[protea-knn-8plm] queries FASTA not bind-mounted at ${QUERY}" >&2
    exit 65
fi
OUT_DIR="$(dirname "${OUTPUT}")"
if [ ! -d "${OUT_DIR}" ]; then
    echo "[protea-knn-8plm] output dir not bind-mounted at ${OUT_DIR}" >&2
    exit 66
fi

echo "[protea-knn-8plm] bundle=${BUNDLE} query=${QUERY} output=${OUTPUT} aggregation=${AGGREGATION}"
exec python -m apps.lafa_knn_8plm.ensemble_driver \
    --query_file "${QUERY}" \
    --frozen_data_dir "${BUNDLE}" \
    --output "${OUTPUT}" \
    --aggregation "${AGGREGATION}" \
    "$@"
