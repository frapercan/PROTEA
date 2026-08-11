#!/usr/bin/env sh
# protea-sparse-knn LAFA submission entrypoint.
#
# The LAFA container guide asks that input paths arrive as command-line
# arguments rather than being hard-coded, because its file names change
# between releases. So every path here is optional: supply it and it is
# used, omit it and the bind-mount default below fills in.
#
#   /bundle               frozen bundle: encoder.pt, the sparse bank,
#                         donor annotations, EMBEDDING_RECIPE.json
#   /input/queries.fasta  FASTA of query proteins (read-only)
#   /input/go-basic.obo   the ontology snapshot at the training cutoff
#   /output/              writable dir for predictions.tsv
#   /hf-cache             backbone weights, baked in unless built with
#                         BAKE_BACKBONE=0
#
# A default is only injected, and only checked, when the corresponding
# argument is absent. Checking a default the caller has already overridden
# is how this script used to reject the guide's own invocation.

set -eu

DRIVER="apps.lafa_sparse_knn.sparse_driver"

# Answer for the driver before touching any mount. The image creates the
# mount points, so someone with nothing mounted still deserves to read the
# arguments.
for arg in "$@"; do
    case "${arg}" in
        -h|--help|--version) exec python -m "${DRIVER}" "$@" ;;
    esac
done

# Which paths did the caller supply? Both our spelling and the guide's.
has_query=0; has_obo=0; has_output=0; has_bundle=0
for arg in "$@"; do
    case "${arg}" in
        --query_file|-q)                    has_query=1 ;;
        --obo|--graph|-g)                   has_obo=1 ;;
        --output|--output_file|-o)          has_output=1 ;;
        --bundle)                           has_bundle=1 ;;
    esac
done

BUNDLE="${PROTEA_SPARSE_BUNDLE:-/bundle}"
QUERY="${PROTEA_SPARSE_QUERY:-/input/queries.fasta}"
OBO="${PROTEA_SPARSE_OBO:-/input/go-basic.obo}"
OUTPUT="${PROTEA_SPARSE_OUTPUT:-/output/predictions.tsv}"

set --  "$@"

if [ "${has_bundle}" -eq 0 ]; then
    # The image creates /bundle, so testing the directory proves nothing: it
    # always exists. Test for a file the bundle must contain, or an unmounted
    # bundle surfaces as a FileNotFoundError from Python several steps later.
    if [ ! -f "${BUNDLE}/BANK.json" ]; then
        echo "[protea-sparse-knn] no frozen bundle at ${BUNDLE}." >&2
        echo "[protea-sparse-knn] Fetch it once with:" >&2
        echo "[protea-sparse-knn]   hf download XaxiPiruli/protea-sparse-knn --local-dir ./bundle" >&2
        exit 64
    fi
    set -- "$@" --bundle "${BUNDLE}"
fi

if [ "${has_query}" -eq 0 ]; then
    if [ ! -f "${QUERY}" ]; then
        echo "[protea-sparse-knn] no queries at ${QUERY}, and --query_file was not given." >&2
        exit 65
    fi
    set -- "$@" --query_file "${QUERY}"
fi

if [ "${has_obo}" -eq 0 ]; then
    if [ ! -f "${OBO}" ]; then
        echo "[protea-sparse-knn] no ontology at ${OBO}, and --graph was not given." >&2
        exit 67
    fi
    set -- "$@" --obo "${OBO}"
fi

if [ "${has_output}" -eq 0 ]; then
    OUT_DIR="$(dirname "${OUTPUT}")"
    if [ ! -d "${OUT_DIR}" ]; then
        echo "[protea-sparse-knn] output dir missing at ${OUT_DIR}" >&2
        exit 66
    fi
    # The image creates /output, so an unmounted output directory is writable
    # and the run succeeds: the predictions are then discarded with the
    # container. That is worse than an error, so refuse it.
    if ! grep -q " ${OUT_DIR} " /proc/mounts 2>/dev/null; then
        echo "[protea-sparse-knn] ${OUT_DIR} is not a bind mount, so anything written" >&2
        echo "[protea-sparse-knn] there is discarded when the container exits." >&2
        echo "[protea-sparse-knn] Mount a host directory: -v \$PWD/out:${OUT_DIR}" >&2
        exit 69
    fi
    if ! touch "${OUT_DIR}/.protea-write-test" 2>/dev/null; then
        echo "[protea-sparse-knn] ${OUT_DIR} is not writable by uid $(id -u)." >&2
        echo "[protea-sparse-knn] Mount a directory this uid can write, or run with" >&2
        echo "[protea-sparse-knn] --user \$(id -u):\$(id -g)." >&2
        exit 68
    fi
    rm -f "${OUT_DIR}/.protea-write-test"
    set -- "$@" --output "${OUTPUT}"
fi

# The backbone is baked into the image, so this is a guard for the case where
# someone mounts an empty directory over the cache.
HF_DIR="${HF_HOME:-/hf-cache}"
if [ ! -d "${HF_DIR}/hub" ]; then
    echo "[protea-sparse-knn] no HuggingFace cache at ${HF_DIR}." >&2
    echo "[protea-sparse-knn] Seed it once on a machine with network:" >&2
    echo "[protea-sparse-knn]   hf download ElnaggarLab/ankh-base" >&2
    echo "[protea-sparse-knn] then mount that cache read-only at ${HF_DIR}." >&2
    exit 70
fi

exec python -m "${DRIVER}" "$@"
