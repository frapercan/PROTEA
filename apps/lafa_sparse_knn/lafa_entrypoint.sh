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
#
# The output VALUE is captured too, not just its presence. The writability
# check below needs a directory whichever way the path arrived, and a check
# that reads a variable only one of the two branches assigns is how this
# script came to abort on its own first line under `set -u`.
has_query=0; has_obo=0; has_output=0; has_bundle=0
OUT_ARG=""
take_output=0
for arg in "$@"; do
    if [ "${take_output}" -eq 1 ]; then
        OUT_ARG="${arg}"; take_output=0; continue
    fi
    case "${arg}" in
        --query_file|-q)                    has_query=1 ;;
        --obo|--graph|-g)                   has_obo=1 ;;
        --output|--output_file|-o)          has_output=1; take_output=1 ;;
        --output=*|--output_file=*)         has_output=1; OUT_ARG="${arg#*=}" ;;
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

# Where the output will go, whichever way it arrived, and then ONE set of
# checks on it. The two branches used to check different things: the
# caller-supplied path was exempted from the bind-mount test on the reasoning
# that somebody who names a path has chosen where it goes. That reasoning is
# wrong, and the guide's own calling style is where it bites. Running with
# `--output_file /output/predictions.tsv` and no `-v` for /output completes,
# logs "wrote 5,625 predictions", and exits 0 with the file discarded inside
# the container. A silent success that produces nothing is the worst failure
# this script can have, and it was reachable through the documented interface.
OUT_DIR="$(dirname "${OUT_ARG:-${OUTPUT}}")"

if [ ! -d "${OUT_DIR}" ]; then
    echo "[protea-sparse-knn] output dir missing at ${OUT_DIR}" >&2
    exit 66
fi
# Writability first, then whether it is a mount at all. Both orders are
# defensible; this one is chosen because an unwritable directory has a specific
# remedy the caller can act on, while "not a mount" is the diagnosis you want
# once the obvious thing is ruled out. It also keeps each refusal reachable on
# its own, which the reverse order does not: an unmounted directory inside the
# image is always writable, so a mount-first check would answer 69 for a
# permissions problem too.
#
# Checked here rather than left to the driver's final line: the container runs
# as uid 1000, a host directory owned by another uid is readable but not
# writable, and without this the run embeds, retrieves and transfers first and
# dies on a bare PermissionError with the whole computation thrown away.
if ! touch "${OUT_DIR}/.protea-write-test" 2>/dev/null; then
    echo "[protea-sparse-knn] ${OUT_DIR} is not writable by uid $(id -u)." >&2
    echo "[protea-sparse-knn] Mount a directory this uid can write, or run with" >&2
    echo "[protea-sparse-knn] --user \$(id -u):\$(id -g)." >&2
    exit 68
fi
rm -f "${OUT_DIR}/.protea-write-test"
# The image creates /output, so an unmounted output directory is writable and
# the run succeeds: the predictions are then discarded with the container.
if ! grep -q " ${OUT_DIR} " /proc/mounts 2>/dev/null; then
    echo "[protea-sparse-knn] ${OUT_DIR} is not a bind mount, so anything written" >&2
    echo "[protea-sparse-knn] there is discarded when the container exits." >&2
    echo "[protea-sparse-knn] Mount a host directory: -v \$PWD/out:${OUT_DIR}" >&2
    exit 69
fi

if [ "${has_output}" -eq 0 ]; then
    set -- "$@" --output "${OUTPUT}"
fi

# The backbone is NOT in the image unless it was built with BAKE_BACKBONE=1,
# and the published image is not, so for a caller this is not an edge case: the
# cache mount is mandatory and this is the check that says so. The comment here
# used to claim the backbone was baked, which is the kind of false statement
# that survives because nobody runs the path it describes.
HF_DIR="${HF_HOME:-/hf-cache}"
if [ ! -d "${HF_DIR}/hub" ]; then
    echo "[protea-sparse-knn] no HuggingFace cache at ${HF_DIR}." >&2
    echo "[protea-sparse-knn] Seed it once on a machine with network:" >&2
    echo "[protea-sparse-knn]   hf download ElnaggarLab/ankh-base" >&2
    echo "[protea-sparse-knn] then mount that cache read-only at ${HF_DIR}." >&2
    exit 70
fi

exec python -m "${DRIVER}" "$@"
