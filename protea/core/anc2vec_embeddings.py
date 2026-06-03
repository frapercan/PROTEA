"""Anc2Vec GO-term embedding index — thin shim over ``protea_method.anc2vec``.

The implementation lives in the standalone ``protea-method`` library
(F2C extraction, 2026-05-07). This module is a backwards-compatible
shim so existing PROTEA call sites that import from
``protea.core.anc2vec_embeddings`` keep working without changes; new
code should import directly from ``protea_method.anc2vec``.

Path resolution (priority order)
---------------------------------

The Anc2Vec npz artefact is resolved through two mechanisms, in order:

1. **Env var** ``PROTEA_ANC2VEC_PATH``: if set and points to an existing
   readable file, that path wins. This is the recommended deploy
   configuration; the deploy runbook copies the artefact into the worktree
   and sets this variable.
2. **Repo-relative fallback**: the historical
   ``Path(__file__).resolve().parents[2] / "artifacts" / "anc2vec" /
   "anc2vec_2020-10.npz"`` location. The first time this branch is
   taken in a process a one-line warning is emitted suggesting
   ``PROTEA_ANC2VEC_PATH`` instead.

Note: the anc2vec npz is a static pre-trained model artefact. It is
**not** managed as a :class:`~protea.infrastructure.orm.models.embedding.dataset.Dataset`
row (those track re-ranker training exports). There is therefore no
artifact-store resolution path for this file; use ``PROTEA_ANC2VEC_PATH``
or the repo-relative fallback.

When both mechanisms fail (typical on a fresh deploy worktree where
``artifacts/`` is gitignored) a :class:`FileNotFoundError` is raised
that lists both mechanisms so the operator can pick the right fix.

The repo-relative path stays exported as ``_DEFAULT_PATH`` for
backwards compatibility with code that imported the constant directly.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from protea_method.anc2vec import Anc2VecIndex
from protea_method.anc2vec import get_index as _get_index_lib

_LOGGER = logging.getLogger(__name__)

_ENV_VAR = "PROTEA_ANC2VEC_PATH"

_DEFAULT_PATH = (
    Path(__file__).resolve().parents[2] / "artifacts" / "anc2vec" / "anc2vec_2020-10.npz"
)

# Module-scoped flag so the "consider setting PROTEA_ANC2VEC_PATH" warning
# fires once per process, not once per ``get_index()`` call.
_FALLBACK_WARNED = False


def _resolve_from_env() -> Path | None:
    """Return the env-var path if it points at a readable file, else None.

    Empty strings and whitespace count as unset so an operator can blank
    the variable in a compose file without tripping the existence check.
    """
    raw = os.environ.get(_ENV_VAR)
    if raw is None or not raw.strip():
        return None
    candidate = Path(raw).expanduser()
    if candidate.is_file() and os.access(candidate, os.R_OK):
        return candidate
    return None


def _resolve_from_artifact_store() -> Path | None:
    """Stub kept for call-site compatibility; always returns None.

    The anc2vec npz is a static pre-trained model artefact, not a
    re-ranker export. It is not tracked as a
    :class:`~protea.infrastructure.orm.models.embedding.dataset.Dataset`
    row and therefore has no artifact-store resolution path. Callers
    should set ``PROTEA_ANC2VEC_PATH`` or place the file at the
    repo-relative default path.
    """
    return None


def _resolve_default_path() -> Path:
    """Pick the npz path via env var > repo-relative fallback.

    Raises :class:`FileNotFoundError` when neither mechanism yields an
    existing readable file. The error message names both mechanisms so
    the operator can fix the deployment without grepping source.
    """
    env_path = _resolve_from_env()
    if env_path is not None:
        return env_path

    if _DEFAULT_PATH.is_file() and os.access(_DEFAULT_PATH, os.R_OK):
        global _FALLBACK_WARNED
        if not _FALLBACK_WARNED:
            _LOGGER.warning(
                "anc2vec npz resolved via repo-relative fallback at %s; "
                "set %s to a deploy-managed path to avoid this branch",
                _DEFAULT_PATH,
                _ENV_VAR,
            )
            _FALLBACK_WARNED = True
        return _DEFAULT_PATH

    raise FileNotFoundError(
        "anc2vec npz could not be resolved. Tried (in order):\n"
        f"  1. env var {_ENV_VAR!r} (unset or points at a missing file)\n"
        f"  2. repo-relative fallback at {_DEFAULT_PATH} (file missing)\n"
        f"Set {_ENV_VAR} to the absolute path of anc2vec_2020-10.npz "
        "(see deploy/DEPLOY.md for the recommended worktree setup), "
        "or copy the artefact into the repo-relative location.\n"
        "Note: the anc2vec npz is not a re-ranker Dataset export and "
        "is not tracked in the artifact store."
    )


def get_index(path: str | None = None) -> Anc2VecIndex:
    """Return a process-wide singleton index keyed by path.

    Resolves the default path through env var > repo-relative fallback
    when no path is passed. See the module docstring for the full
    contract and operator-facing failure mode.
    """
    if path is not None:
        return _get_index_lib(path)
    return _get_index_lib(str(_resolve_default_path()))


__all__ = ["Anc2VecIndex", "_DEFAULT_PATH", "get_index"]
