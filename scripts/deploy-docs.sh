#!/usr/bin/env bash
# Publish the documentation OF THE REVISION THIS TREE IS AT, and never leave
# the served build empty.
#
# WHY THIS EXISTS. On 2026-11-10 the API served revision 9f09e2c while
# /sphinx/ served a build from October 7th: a month of documentation, eight
# architecture decision records among it, that the public site did not show.
# Nothing was broken and nothing said anything, because the deploy path this
# machine actually uses (declare, move the tree, restart) has no docs step at
# all, and `scripts/deploy.sh`, which does build them, targets a different
# slot. So the rule is now a script: the docs ship with the revision, every
# time, and the build carries the revision it was made from so a skipped run
# is visible from outside.
#
# THE SERVED BUILD IS PROTECTED THE WAY THE FRONTEND'S IS. `sphinx-build`
# writes into its output directory, and the serving directory is the one
# FastAPI mounted at /sphinx. A build that fails halfway through would
# therefore publish a broken site, which is exactly the outage
# `scripts/deploy-frontend.sh` was written after. So this builds somewhere
# else, checks the result, and only then swaps, keeping the previous build
# until the new one has answered a request.
#
# THE BUILD ENVIRONMENT IS NOT THE PLATFORM'S. Sphinx is not installed in the
# venv that runs the API and the workers, and it must not be installed there:
# that venv holds a torch pinned by hand (see ADR notes on the deploy torch
# trap) and a docs dependency group has no business near it. The build needs
# much less than the platform anyway, because `docs/source/conf.py` mocks
# torch, numpy, transformers, pandas, pyarrow, scipy, sklearn, faiss,
# lightgbm, pika, minio and nine more for autodoc. What it does need is the
# Sphinx side (from the tracked docs/requirements.txt), the light runtime
# packages whose imports are NOT mocked, and the protea-contracts sibling,
# which a documentation extension imports directly. Measured: that set builds
# all 144 pages in about a minute, with no torch anywhere.
#
# Usage:
#   bash scripts/deploy-docs.sh            # build this tree's revision and swap
#   bash scripts/deploy-docs.sh --check    # say what is served, build nothing
#
# Environment:
#   SIBLINGS_DIR   where the sibling clones live (default: the tree's parent)

set -uo pipefail

AQUI="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVIDO="$AQUI/docs/build/html"
SELLO="_static/protea-revision.txt"
# Where the sibling clones live. The default is the parent of the MAIN
# worktree and not the parent of this one: a rehearsal from a linked worktree
# under a scratchpad has no siblings beside it, and the point of rehearsing is
# to exercise this script and not a different one. `--git-common-dir` resolves
# to the real repository's `.git` from either place.
_comun="$(git -C "$AQUI" rev-parse --path-format=absolute --git-common-dir 2>/dev/null)"
if [[ -n "$_comun" ]]; then
  _por_defecto="$(dirname "$(dirname "$_comun")")"
else
  _por_defecto="$(dirname "$AQUI")"
fi
SIBLINGS_DIR="${SIBLINGS_DIR:-$_por_defecto}"
#: The current build has 135 pages and this tree's has 144. A build that
#: produces a handful of pages did not fail, it succeeded at something else,
#: and publishing it would be worse than publishing nothing.
MINIMO_PAGINAS=100

revision_servida() {
  local f="$SERVIDO/$SELLO"
  [[ -f "$f" ]] && tr -d '[:space:]' < "$f" || echo "desconocida"
}

if [[ "${1:-}" == "--check" ]]; then
  printf 'arbol    %s\n' "$(git -C "$AQUI" rev-parse HEAD 2>/dev/null || echo 'no es un repo')"
  printf 'servido  %s\n' "$(revision_servida)"
  printf 'paginas  %s\n' "$(find "$SERVIDO" -name '*.html' 2>/dev/null | wc -l)"
  exit 0
fi

REV="$(git -C "$AQUI" rev-parse HEAD 2>/dev/null)"
if [[ -z "$REV" ]]; then
  echo "deploy-docs: no puedo leer la revision del arbol; no toco lo servido" >&2
  exit 1
fi
if [[ -n "$(git -C "$AQUI" status --porcelain -- docs protea 2>/dev/null)" ]]; then
  echo "deploy-docs: el arbol tiene cambios sin confirmar en docs/ o protea/." >&2
  echo "deploy-docs: publicaria una documentacion que ninguna revision nombra." >&2
  exit 1
fi

if ! command -v uv >/dev/null 2>&1; then
  echo "deploy-docs: uv no esta; no construyo y no toco lo servido" >&2
  exit 1
fi

if [[ ! -d "$SIBLINGS_DIR/protea-contracts" ]]; then
  echo "deploy-docs: no encuentro protea-contracts en $SIBLINGS_DIR." >&2
  echo "deploy-docs: una extension de la documentacion lo importa; pasa SIBLINGS_DIR." >&2
  echo "deploy-docs: no construyo y no toco lo servido" >&2
  exit 1
fi

NUEVO="$(mktemp -d "${TMPDIR:-/tmp}/protea-docs-XXXXXX")"
PREVIO="${TMPDIR:-/tmp}/protea-docs-anterior"
trap 'rm -rf "$NUEVO"' EXIT

echo "deploy-docs: construyendo ${REV:0:12} en $NUEVO"
# --no-project: the environment is assembled from the lists below and not from
# this repo's pyproject, which would drag torch in. `protea` itself is imported
# from the tree by conf.py's sys.path, so it is not installed either.
if ! uv run --quiet --no-project \
      --with-requirements "$AQUI/docs/requirements.txt" \
      --with sqlalchemy --with pydantic --with pydantic-settings --with fastapi \
      --with "$SIBLINGS_DIR/protea-contracts" \
      sphinx-build -b html "$AQUI/docs/source" "$NUEVO"; then
  echo "deploy-docs: el build FALLO; lo servido sigue intacto en $(revision_servida)" >&2
  exit 1
fi

PAGINAS="$(find "$NUEVO" -name '*.html' | wc -l)"
if [[ ! -s "$NUEVO/index.html" ]] || [[ ! -s "$NUEVO/adr/index.html" ]] \
   || (( PAGINAS < MINIMO_PAGINAS )); then
  echo "deploy-docs: el build no pasa la revision ($PAGINAS paginas, index y adr/index exigidos);" >&2
  echo "deploy-docs: lo servido sigue intacto en $(revision_servida)" >&2
  exit 1
fi

mkdir -p "$NUEVO/_static"
printf '%s\n' "$REV" > "$NUEVO/$SELLO"

echo "deploy-docs: $PAGINAS paginas, sellado ${REV:0:12}; cambiando"
rm -rf "$PREVIO"
if [[ -d "$SERVIDO" ]]; then
  mv "$SERVIDO" "$PREVIO"
fi
mkdir -p "$(dirname "$SERVIDO")"
if ! cp -a "$NUEVO" "$SERVIDO"; then
  echo "deploy-docs: la copia fallo; devolviendo el build anterior" >&2
  rm -rf "$SERVIDO"
  [[ -d "$PREVIO" ]] && mv "$PREVIO" "$SERVIDO"
  exit 1
fi

# WHAT THE PROBE CAN AND CANNOT DECIDE. It asks the local API for the stamp it
# is serving, which is the only question with a checkable answer: whether this
# swap changed what that API serves. It does NOT gate the rollback. This script
# restarts nothing and the API may well be serving a different tree -- a
# rehearsal from a linked worktree is exactly that case -- so a non-matching
# answer is information about which tree the API has, not evidence that the
# build is bad. The build was already checked before the swap, which is where
# a decision belongs. (`deploy-frontend.sh` does gate on its probe, and it is
# entitled to: it restarts the very unit that serves what it just built.)
# -f so a 404 gives nothing instead of an RFC 7807 problem document: without
# it the first run of this reported `{"type":"/pr` as the revision the API was
# serving, which is a sentence about a JSON body dressed as a sentence about a
# deployment. And the shape is checked before the value is believed, because
# the only thing a stamp may be is forty hexadecimal characters.
SELLO_REMOTO="$(curl -sf --max-time 20 "http://127.0.0.1:8000/sphinx/$SELLO" 2>/dev/null | tr -d '[:space:]' || true)"
if [[ ! "$SELLO_REMOTO" =~ ^[0-9a-f]{40}$ ]]; then
  SELLO_REMOTO=""
fi
SERVIDO_POR_LA_API="$SELLO_REMOTO"
if [[ "$SERVIDO_POR_LA_API" == "$REV" ]]; then
  SITUACION="la API de esta maquina ya sirve ${REV:0:12}"
elif [[ -z "$SERVIDO_POR_LA_API" ]]; then
  SITUACION="sin API local que preguntar, o sin sello que leer todavia"
else
  SITUACION="OJO: la API local sirve otro arbol, sellado ${SERVIDO_POR_LA_API:0:12}"
fi

echo "deploy-docs: servido ${REV:0:12}, $PAGINAS paginas"
echo "deploy-docs: $SITUACION"
echo "deploy-docs: el build anterior queda en $PREVIO hasta el siguiente despliegue"
exit 0
