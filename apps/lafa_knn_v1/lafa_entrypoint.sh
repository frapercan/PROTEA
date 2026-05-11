#!/usr/bin/env sh
# protea-knn-v1 LAFA submission entrypoint.
#
# Wraps ``protea-predict`` (from the base ``protea-method-runtime``
# image, ADR-D15) with the v1 KNN configuration:
#
#   * --aspect_separated   one KNN per GO aspect (P/F/C)
#   * --no_v6              skip v6 feature enrichment
#   * --no_reranker        skip the LightGBM rerank stage
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
# defaults (e.g. switch to faiss backend) without rebuilding.

set -eu

BUNDLE="${PROTEA_KNN_V1_BUNDLE:-/bundle}"
QUERY="${PROTEA_KNN_V1_QUERY:-/input/queries.fasta}"
OUTPUT="${PROTEA_KNN_V1_OUTPUT:-/output/predictions.tsv}"

if [ ! -d "${BUNDLE}" ]; then
    echo "[protea-knn-v1] frozen bundle not bind-mounted at ${BUNDLE}" >&2
    exit 64
fi
if [ ! -f "${QUERY}" ]; then
    echo "[protea-knn-v1] queries FASTA not bind-mounted at ${QUERY}" >&2
    exit 65
fi
OUT_DIR="$(dirname "${OUTPUT}")"
if [ ! -d "${OUT_DIR}" ]; then
    echo "[protea-knn-v1] output dir not bind-mounted at ${OUT_DIR}" >&2
    exit 66
fi

echo "[protea-knn-v1] bundle=${BUNDLE} query=${QUERY} output=${OUTPUT}"
exec python /app/protea_predict.py \
    --query_file "${QUERY}" \
    --frozen_data_dir "${BUNDLE}" \
    --output "${OUTPUT}" \
    --aspect_separated \
    --no_v6 \
    --no_reranker \
    "$@"
