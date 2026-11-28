"""The operating point of a reading: a cell's declared threshold, and the grid it is read on.

Every rung of the literature ladder is read at a threshold declared before the
read, per evaluator cell (knowledge category by aspect), on the scale its
reading lives on. The package holds the pieces every reading shares:

- :mod:`protea.core.operating_point.grid`: the evaluator's per-protein grid,
  reached by one path from a stored set or from in-memory scores; the metrics
  read off it at a threshold; the threshold pooled over fitting windows;
- :mod:`protea.core.operating_point.hierarchy`: hierarchy restoration, with
  the evaluator's own ancestors;
- :mod:`protea.core.operating_point.window_fields`: which fields one arm's sets
  may differ in across windows.

This module holds the declared threshold's document. A weights artifact of a
kind in :data:`DECLARING_WEIGHT_KINDS` is a JSON document carrying, besides
whatever its kind records, the grid step it was declared on (``th_step``) and
one block per cell under ``declared_thresholds``: the threshold (``tau``) and
whether it is the grid's first or last element (``at_grid_edge``). A reading
(``read_operating_point``), a comparison (``compare_paired_panels`` with
declared threshold ids) and a method that applies the threshold read it through
:func:`load_declared_threshold`, which checks the bytes against the registered
sha256 before anything is parsed.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from protea.core.domain.aspect import Aspect

if TYPE_CHECKING:  # pragma: no cover - typing only
    from sqlalchemy.orm import Session

#: The version of the declared-threshold block this package reads and writes.
THRESHOLD_DOCUMENT_VERSION = "1"

#: The weights kinds whose artifact declares per-cell thresholds. Each kind that
#: fits a reading joins this tuple in the pull request that adds it.
DECLARING_WEIGHT_KINDS: tuple[str, ...] = ("operating_threshold",)

#: The evaluator's settings, one per knowledge category.
SETTING_CODES: tuple[str, ...] = ("NK", "LK", "PK")

#: The nine cells, ``<setting>:<CAFA aspect>``, in the report order of the paired comparison.
CELLS: tuple[str, ...] = tuple(
    f"{setting}:{aspect}" for setting in SETTING_CODES for aspect in ("MFO", "BPO", "CCO")
)

_CAFA_FOR_NAMESPACE: dict[str, str] = {aspect.namespace: aspect.cafa for aspect in Aspect}


class DeclaredThresholdUnreadable(ValueError):
    """A weights row that declares no usable per-cell threshold, named with the reason."""


def cell_key(setting: str, namespace: str) -> str:
    """``"NK:BPO"`` from a setting and cafaeval's long namespace (or a CAFA code)."""
    aspect = _CAFA_FOR_NAMESPACE.get(namespace, namespace)
    key = f"{setting}:{aspect}"
    if key not in CELLS:
        raise ValueError(f"{key!r} is not one of the nine cells {list(CELLS)}")
    return key


@dataclass(frozen=True)
class DeclaredThreshold:
    """One weights row's per-cell thresholds, as its artifact declares them."""

    weights_id: str
    kind: str
    th_step: float
    taus: Mapping[str, float]
    at_grid_edge: Mapping[str, bool]

    def tau(self, cell: str) -> float:
        """The threshold declared for ``cell``, or a refusal naming the cells it covers."""
        if cell not in self.taus:
            raise DeclaredThresholdUnreadable(
                f"weights {self.weights_id} ({self.kind}) declares no threshold for cell {cell}; "
                f"it declares {sorted(self.taus)}"
            )
        return float(self.taus[cell])


def declared_block(
    th_step: float, taus: Mapping[str, float], edges: Mapping[str, bool]
) -> dict[str, Any]:
    """The block every declaring artifact carries: its grid step and its thresholds per cell."""
    return {
        "version": THRESHOLD_DOCUMENT_VERSION,
        "th_step": float(th_step),
        "declared_thresholds": {
            cell: {"tau": float(taus[cell]), "at_grid_edge": bool(edges[cell])}
            for cell in CELLS
            if cell in taus
        },
    }


def parse_declared_threshold(
    document: Mapping[str, Any], *, weights_id: str, kind: str
) -> DeclaredThreshold:
    """Read the declared block of a weights artifact, refusing one that does not carry it."""
    block = document.get("declared_thresholds")
    if document.get("version") != THRESHOLD_DOCUMENT_VERSION or not isinstance(block, Mapping):
        raise DeclaredThresholdUnreadable(
            f"weights {weights_id} ({kind}) carries no declared-threshold block of version "
            f"{THRESHOLD_DOCUMENT_VERSION}, so it declares no threshold a reading can be read at"
        )
    unknown = sorted(set(block) - set(CELLS))
    if unknown:
        raise DeclaredThresholdUnreadable(
            f"weights {weights_id} ({kind}) declares thresholds for {unknown}, which are not cells"
        )
    return DeclaredThreshold(
        weights_id=weights_id,
        kind=kind,
        th_step=float(document["th_step"]),
        taus={cell: float(entry["tau"]) for cell, entry in block.items()},
        at_grid_edge={cell: bool(entry["at_grid_edge"]) for cell, entry in block.items()},
    )


def canonical_document(document: Mapping[str, Any]) -> bytes:
    """The bytes a declaring artifact is stored as: sorted keys, no spaces, ASCII."""
    return json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def load_declared_threshold(session: Session, weights_id: str, store: Any) -> DeclaredThreshold:
    """A weights row's declared thresholds, read from its artifact after its bytes are checked."""
    from protea.infrastructure.orm.models.embedding.model_weights import ModelWeights

    try:
        key = uuid.UUID(str(weights_id))
    except ValueError as exc:
        raise DeclaredThresholdUnreadable(f"{weights_id!r} is not a weights id") from exc
    weights = session.get(ModelWeights, key)
    if weights is None:
        raise DeclaredThresholdUnreadable(f"no model_weights row {weights_id}")
    if weights.kind not in DECLARING_WEIGHT_KINDS:
        raise DeclaredThresholdUnreadable(
            f"weights {weights_id} are of kind {weights.kind!r}, which declares no per-cell "
            f"threshold; the kinds that do are {list(DECLARING_WEIGHT_KINDS)}"
        )
    blob = store.get(weights.uri)
    digest = hashlib.sha256(blob).hexdigest()
    if digest != weights.sha256:
        raise DeclaredThresholdUnreadable(
            f"weights {weights_id}: {weights.uri} hashes to {digest}, not to the registered "
            f"{weights.sha256}; a threshold read from other bytes is not the one declared"
        )
    return parse_declared_threshold(json.loads(blob), weights_id=str(key), kind=weights.kind)


__all__ = [
    "CELLS",
    "DECLARING_WEIGHT_KINDS",
    "SETTING_CODES",
    "THRESHOLD_DOCUMENT_VERSION",
    "DeclaredThreshold",
    "DeclaredThresholdUnreadable",
    "canonical_document",
    "cell_key",
    "declared_block",
    "load_declared_threshold",
    "parse_declared_threshold",
]
