#!/usr/bin/env sh
# protea-knn-v1 LAFA submission entrypoint.
#
# Wraps ``protea-predict`` (from the base ``protea-method-runtime``
# image, ADR-D15) with the v1 KNN + universal-reranker configuration:
#
#   * --aspect_separated   one KNN per GO aspect (P/F/C)
#   * --universal_reranker score with the single universal (pooled,
#                          aspect-conditioned, K-augmented) booster shipped
#                          at /bundle/reranker/universal.txt. Runs v6 feature
#                          enrichment (required) and post-hoc rescores every
#                          candidate; overrides --no_reranker.
#   * --self_prior         inject each target's OWN t0 non-exp annotation
#                          (GOA self-prior; max-combined with neighbour transfer)
#
# Set PROTEA_KNN_V1_NO_UNIVERSAL=1 to fall back to the pure KNN baseline
# (--no_v6 --no_reranker) when no universal booster ships in the bundle.
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

if [ "${PROTEA_KNN_V1_NO_UNIVERSAL:-0}" = "1" ]; then
    # Legacy pure-KNN baseline (no v6, no reranker).
    exec python /app/protea_predict.py \
        --query_file "${QUERY}" \
        --frozen_data_dir "${BUNDLE}" \
        --output "${OUTPUT}" \
        --aspect_separated \
        --no_v6 \
        --no_reranker \
        --self_prior \
        "$@"
fi

exec python /app/protea_predict.py \
    --query_file "${QUERY}" \
    --frozen_data_dir "${BUNDLE}" \
    --output "${OUTPUT}" \
    --aspect_separated \
    --universal_reranker \
    --self_prior \
    "$@"
