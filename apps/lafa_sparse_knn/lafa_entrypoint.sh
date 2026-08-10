#!/usr/bin/env sh
# protea-sparse-knn LAFA submission entrypoint.
#
# Bind-mount layout (LAFA container guide, anphan0828/LAFA_container_guide):
#
#   /bundle               frozen bundle: encoder.pt, the sparse bank,
#                         donor annotations, EMBEDDING_RECIPE.json
#   /input/queries.fasta  FASTA of query proteins (read-only)
#   /input/go-basic.obo   the ontology snapshot at the training cutoff
#   /output/              writable dir for predictions.tsv
#   /hf-cache             HuggingFace cache for the backbone weights
#
# Any positional args appended to ``docker run`` are forwarded to the
# driver verbatim, so the evaluator can override K or the batch size
# without rebuilding.

set -eu

BUNDLE="${PROTEA_SPARSE_BUNDLE:-/bundle}"
QUERY="${PROTEA_SPARSE_QUERY:-/input/queries.fasta}"
OBO="${PROTEA_SPARSE_OBO:-/input/go-basic.obo}"
OUTPUT="${PROTEA_SPARSE_OUTPUT:-/output/predictions.tsv}"

if [ ! -d "${BUNDLE}" ]; then
    echo "[protea-sparse-knn] frozen bundle not bind-mounted at ${BUNDLE}" >&2
    exit 64
fi
if [ ! -f "${QUERY}" ]; then
    echo "[protea-sparse-knn] queries FASTA not bind-mounted at ${QUERY}" >&2
    exit 65
fi
if [ ! -f "${OBO}" ]; then
    echo "[protea-sparse-knn] ontology not bind-mounted at ${OBO}" >&2
    exit 67
fi
OUT_DIR="$(dirname "${OUTPUT}")"
if [ ! -d "${OUT_DIR}" ]; then
    echo "[protea-sparse-knn] output dir not bind-mounted at ${OUT_DIR}" >&2
    exit 66
fi
# Check writability here rather than discovering it at the end. The container
# runs as uid 1000, and a host directory owned by another uid is readable but
# not writable, so without this the run embeds, retrieves and transfers before
# failing on the last line with a bare PermissionError.
if ! touch "${OUT_DIR}/.protea-write-test" 2>/dev/null; then
    echo "[protea-sparse-knn] ${OUT_DIR} is not writable by uid $(id -u)." >&2
    echo "[protea-sparse-knn] Mount a directory this uid can write, or run with" >&2
    echo "[protea-sparse-knn] --user \$(id -u):\$(id -g)." >&2
    exit 68
fi
rm -f "${OUT_DIR}/.protea-write-test"

echo "[protea-sparse-knn] bundle=${BUNDLE} query=${QUERY} obo=${OBO} output=${OUTPUT}"
exec python -m apps.lafa_sparse_knn.sparse_driver \
    --query_file "${QUERY}" \
    --bundle "${BUNDLE}" \
    --obo "${OBO}" \
    --output "${OUTPUT}" \
    "$@"
