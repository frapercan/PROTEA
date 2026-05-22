#!/usr/bin/env bash
# scripts/build_all_docs.sh
#
# Build Sphinx HTML for every repo in the PROTEA stack that carries docs,
# placing output under docs/build/<slug>/html/ inside this PROTEA tree.
# FastAPI already mounts those directories at /docs/<slug>/ via
# app.py:_mount_sibling_docs; the /stack page lights the Docs link when
# docs/build/<slug>/html/index.html exists.
#
# Usage:
#   bash scripts/build_all_docs.sh
#
# Environment:
#   SIBLINGS_DIR   parent directory that holds sibling repo clones
#                  (default: ~/Thesis2/repositories)
#
# Behaviour:
#   - Reads slug list from docs/source/_data/stack.yaml via python3.
#   - Slug "protea-core" maps to directory PROTEA; all others match verbatim.
#   - Detects Sphinx source at docs/source/conf.py (primary) or
#     docs/conf.py (cafaeval-protea and similar).
#   - Repos with no conf.py are skipped gracefully (picks them up once
#     docs are added later).
#   - Builds inside each repo's own poetry environment where available;
#     falls back to the ambient sphinx-build binary otherwise.
#   - A single-repo build failure logs a warning and continues to the rest.
#   - Exits 0 if PROTEA's own docs (protea-core) built successfully;
#     exits 1 only if protea-core itself failed.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
STACK_YAML="$ROOT/docs/source/_data/stack.yaml"
BUILD_ROOT="$ROOT/docs/build"
DEFAULT_SIBLINGS="$HOME/Thesis2/repositories"
SIBLINGS_DIR="${SIBLINGS_DIR:-$DEFAULT_SIBLINGS}"

C_RESET=$'\e[0m'; C_BOLD=$'\e[1m'; C_GREEN=$'\e[32m'
C_YELLOW=$'\e[33m'; C_RED=$'\e[31m'

if [[ ! -f "$STACK_YAML" ]]; then
    echo "${C_RED}ERROR${C_RESET}: stack.yaml not found at $STACK_YAML" >&2
    exit 1
fi

# Extract ordered slug list from stack.yaml using the always-available python3.
mapfile -t SLUGS < <(python3 - "$STACK_YAML" <<'PYEOF'
import sys, yaml
data = yaml.safe_load(open(sys.argv[1], encoding="utf-8"))
for r in data.get("repos", []):
    print(r["slug"])
PYEOF
)

if [[ ${#SLUGS[@]} -eq 0 ]]; then
    echo "${C_RED}ERROR${C_RESET}: no slugs found in $STACK_YAML" >&2
    exit 1
fi

_repo_dir_for() {
    local slug="$1"
    # protea-core is the PROTEA monorepo itself (the current tree).
    if [[ "$slug" == "protea-core" ]]; then
        echo "$ROOT"
    else
        echo "$SIBLINGS_DIR/$slug"
    fi
}

_sphinx_src_for() {
    local repo_dir="$1"
    # Primary: docs/source/conf.py (PROTEA, protea-contracts, etc.)
    if [[ -f "$repo_dir/docs/source/conf.py" ]]; then
        echo "$repo_dir/docs/source"
        return 0
    fi
    # Fallback: docs/conf.py (cafaeval-protea)
    if [[ -f "$repo_dir/docs/conf.py" ]]; then
        echo "$repo_dir/docs"
        return 0
    fi
    return 1
}

_build_one() {
    local slug="$1"
    local repo_dir="$2"
    local out_dir="$3"

    local src_dir
    if ! src_dir="$(_sphinx_src_for "$repo_dir")"; then
        echo "  ${C_YELLOW}skip${C_RESET} $slug: no conf.py found (repo has no Sphinx docs yet)"
        return 0
    fi

    echo "  ${C_BOLD}building${C_RESET} $slug: $src_dir -> $out_dir"
    rm -rf "$out_dir"
    mkdir -p "$out_dir"

    local rc=0

    # Prefer building inside the repo's own poetry venv for autodoc imports.
    if [[ -f "$repo_dir/pyproject.toml" ]] && command -v poetry >/dev/null 2>&1; then
        # Use -W-less invocation so import warnings don't abort the build,
        # but preserve real errors via the exit code.
        (cd "$repo_dir" && poetry run sphinx-build -q -b html "$src_dir" "$out_dir") || rc=$?
    else
        # Fallback: ambient sphinx-build (drop -W so one missing autodoc
        # dependency doesn't kill the whole run).
        if command -v sphinx-build >/dev/null 2>&1; then
            sphinx-build -q -b html "$src_dir" "$out_dir" || rc=$?
        else
            echo "  ${C_YELLOW}skip${C_RESET} $slug: sphinx-build not found and no poetry venv"
            return 0
        fi
    fi

    if [[ $rc -ne 0 ]]; then
        echo "  ${C_RED}FAILED${C_RESET} $slug (exit $rc) -- continuing" >&2
        return 1
    fi

    if [[ -f "$out_dir/index.html" ]]; then
        echo "  ${C_GREEN}OK${C_RESET} $slug -> $out_dir/index.html"
    else
        echo "  ${C_YELLOW}WARN${C_RESET} $slug: build exited 0 but index.html missing" >&2
        return 1
    fi
    return 0
}

echo "${C_BOLD}=== build_all_docs.sh ===${C_RESET}"
echo "  SIBLINGS_DIR  = $SIBLINGS_DIR"
echo "  BUILD_ROOT    = $BUILD_ROOT"
echo "  slugs         = ${SLUGS[*]}"
echo ""

protea_core_ok=1
declare -a built=()
declare -a failed=()
declare -a skipped=()

for slug in "${SLUGS[@]}"; do
    repo_dir="$(_repo_dir_for "$slug")"

    if [[ ! -d "$repo_dir" ]]; then
        echo "  ${C_YELLOW}skip${C_RESET} $slug: $repo_dir not found (repo not cloned)"
        skipped+=("$slug")
        continue
    fi

    out_dir="$BUILD_ROOT/$slug/html"

    if _build_one "$slug" "$repo_dir" "$out_dir"; then
        if [[ -f "$out_dir/index.html" ]]; then
            built+=("$slug")
        else
            skipped+=("$slug")
        fi
    else
        failed+=("$slug")
        if [[ "$slug" == "protea-core" ]]; then
            protea_core_ok=0
        fi
    fi
done

# Mirror protea-core into the legacy docs/build/html path so the existing
# /sphinx mount keeps working for callers that hard-coded that URL.
legacy="$BUILD_ROOT/html"
src_legacy="$BUILD_ROOT/protea-core/html"
if [[ -d "$src_legacy" ]]; then
    rm -rf "$legacy"
    cp -r "$src_legacy" "$legacy"
    echo "  ${C_GREEN}mirrored${C_RESET} protea-core -> docs/build/html (legacy /sphinx)"
fi

echo ""
echo "${C_BOLD}Results${C_RESET}"
echo "  built:   ${built[*]:-none}"
echo "  failed:  ${failed[*]:-none}"
echo "  skipped: ${skipped[*]:-none}"

if [[ ${#failed[@]} -gt 0 ]]; then
    echo "${C_YELLOW}WARNING${C_RESET}: some repos failed to build: ${failed[*]}" >&2
fi

# Only fail hard if protea-core itself did not build.
if [[ "$protea_core_ok" -eq 0 ]]; then
    echo "${C_RED}FATAL${C_RESET}: protea-core docs failed to build" >&2
    exit 1
fi

exit 0
