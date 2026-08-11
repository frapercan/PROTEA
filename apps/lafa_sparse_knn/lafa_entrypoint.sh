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

# Let the driver answer for itself before any mount is checked. The image
# creates the mount points, so a user with nothing mounted yet still deserves to
# be able to read the arguments.
for arg in "$@"; do
    case "${arg}" in
        -h|--help|--version)
            exec python -m apps.lafa_sparse_knn.sparse_driver "$@"
            ;;
    esac
done

BUNDLE="${PROTEA_SPARSE_BUNDLE:-/bundle}"
QUERY="${PROTEA_SPARSE_QUERY:-/input/queries.fasta}"
OBO="${PROTEA_SPARSE_OBO:-/input/go-basic.obo}"
OUTPUT="${PROTEA_SPARSE_OUTPUT:-/output/predictions.tsv}"

# The image creates these mount points, so testing that the directory exists
# proves nothing: it always does. Test for a file the bundle must contain, or an
# unmounted bundle surfaces as a FileNotFoundError from Python several steps
# later instead of as this exit code.
if [ ! -f "${BUNDLE}/BANK.json" ]; then
    echo "[protea-sparse-knn] no frozen bundle at ${BUNDLE}." >&2
    echo "[protea-sparse-knn] Fetch it once with:" >&2
    echo "[protea-sparse-knn]   hf download XaxiPiruli/protea-sparse-knn --local-dir ./bundle" >&2
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
# The backbone is baked into the image, so this should never fire. It stays as a
# guard in case someone mounts over the cache directory with an empty one.
HF_DIR="${HF_HOME:-/hf-cache}"
if [ ! -d "${HF_DIR}/hub" ]; then
    echo "[protea-sparse-knn] no HuggingFace cache at ${HF_DIR}." >&2
    echo "[protea-sparse-knn] Seed it once on a machine with network:" >&2
    echo "[protea-sparse-knn]   hf download ElnaggarLab/ankh-base" >&2
    echo "[protea-sparse-knn] then mount that cache read-only at ${HF_DIR}." >&2
    exit 70
fi
OUT_DIR="$(dirname "${OUTPUT}")"
if [ ! -d "${OUT_DIR}" ]; then
    echo "[protea-sparse-knn] output dir missing at ${OUT_DIR}" >&2
    exit 66
fi
# The image creates /output, so an unmounted output directory is writable and
# the run succeeds: the predictions are then discarded with the container. That
# is worse than an error, so refuse unless the directory is a real mount.
if ! grep -q " ${OUT_DIR} " /proc/mounts 2>/dev/null; then
    echo "[protea-sparse-knn] ${OUT_DIR} is not a bind mount, so anything written" >&2
    echo "[protea-sparse-knn] there is discarded when the container exits." >&2
    echo "[protea-sparse-knn] Mount a host directory: -v \$PWD/out:${OUT_DIR}" >&2
    exit 69
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
