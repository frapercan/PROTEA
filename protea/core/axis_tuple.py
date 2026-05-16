"""Axis-tuple shortid: thin shim that prefers the protea-contracts helper.

The canonical implementation lives in ``protea_contracts.axis_tuple``
(shipped by FARM-EXP.1 companion PR on protea-contracts). PROTEA pins
``protea-contracts@main`` in :file:`pyproject.toml`, so once that PR
merges and we re-lock, the local fallback below disappears via the
``try / except ImportError`` path.

The local fallback is byte-for-byte identical to the upstream helper so
the cross-repo shortid contract holds during the brief window where
PROTEA is merged before the contracts release that ships the helper.

Formula (mirrors ExperimentSpec.hash() in
protea-reranker-lab/src/protea_reranker_lab/experiment.py:108-111):

.. code-block:: python

    sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:12]

When the upstream helper is available, this module re-exports it
verbatim (same symbol, same digest). Callers should import from here
so the swap is invisible::

    from protea.core.axis_tuple import axis_tuple_shortid
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

try:  # pragma: no cover -- exercised once contracts ships the helper
    # The helper lives in protea-contracts on branch
    # ``feat/farm-exp-1-shortid-helper`` (PR #12). PROTEA pins
    # ``protea-contracts@main`` in pyproject.toml, so the attributes
    # below resolve only once that PR merges + a release ships. The
    # type-ignores avoid a mypy ``attr-defined`` while the upstream
    # release is still pending.
    from protea_contracts import CANONICAL_AXIS_KEYS as _UPSTREAM_KEYS  # type: ignore[attr-defined]
    from protea_contracts import SHORTID_HEX_LEN as _UPSTREAM_HEX_LEN  # type: ignore[attr-defined]
    from protea_contracts import (
        axis_tuple_shortid as _UPSTREAM_SHORTID,  # type: ignore[attr-defined]
    )
except ImportError:  # contracts pin not yet bumped past FARM-EXP.1 release
    _UPSTREAM_KEYS = None
    _UPSTREAM_HEX_LEN = None
    _UPSTREAM_SHORTID = None


#: Canonical axis-tuple key set. Falls back to the locally-pinned tuple
#: while the upstream contracts release is being rolled out. Order does
#: NOT affect the digest (``json.dumps(..., sort_keys=True)``).
CANONICAL_AXIS_KEYS: tuple[str, ...] = _UPSTREAM_KEYS or (
    "plm",
    "k",
    "reranker_spec_id",
    "feature_schema_sha",
    "eval_set_name",
    "eval_set_manifest_sha",
    "propagation",
    "ensemble_spec",
)

#: Length of the truncated hex digest. Matches the lab convention.
SHORTID_HEX_LEN: int = _UPSTREAM_HEX_LEN or 12


def _local_axis_tuple_shortid(axis_tuple: Mapping[str, Any]) -> str:
    """Fallback implementation: see module docstring for the formula."""
    try:
        blob = json.dumps(dict(axis_tuple), sort_keys=True).encode()
    except TypeError as exc:
        raise TypeError(
            "axis_tuple values must be JSON-encodable without a custom "
            "default; pre-stringify Path / UUID / datetime before "
            f"calling axis_tuple_shortid. underlying error: {exc}"
        ) from exc
    return hashlib.sha256(blob).hexdigest()[:SHORTID_HEX_LEN]


axis_tuple_shortid = _UPSTREAM_SHORTID or _local_axis_tuple_shortid


__all__ = [
    "CANONICAL_AXIS_KEYS",
    "SHORTID_HEX_LEN",
    "axis_tuple_shortid",
]
