"""What the two operating-point operations read: results, their windows and their grids.

``declare_operating_threshold`` pools an arm's fitting-window results into a
declared threshold and ``read_operating_point`` reads one result at a declared
threshold. Both read the same things the same way: the result row, its window,
and the per-protein grids its evaluation stored, fetched from the store into a
scratch folder and loaded through the paired comparison's own loader, whose
gates (the footer, the population, the monotone curves) every reader of a grid
passes through.
"""

from __future__ import annotations

import contextlib
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from protea.core.operating_point import cell_key
from protea.core.operations._paired_panels_artifact import GRID_FILENAME, SETTINGS
from protea.core.operations._paired_panels_bootstrap import PanelArrays
from protea.core.operations._paired_panels_panel import Side, load_side
from protea.core.operations._run_cafa_helpers import eval_artifact_key
from protea.infrastructure.orm.models.annotation.evaluation_result import EvaluationResult
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet

_PROJECT_ROOT = Path(__file__).resolve().parents[3]


class OperatingPointRefused(ValueError):
    """An operating-point read or declaration the operation will not make, with the reason."""


def operating_point_store() -> Any:
    """The artifact store, resolved once per call: where grids are read and documents written."""
    from protea.infrastructure.settings import load_settings
    from protea.infrastructure.storage import get_artifact_store

    return get_artifact_store(load_settings(_PROJECT_ROOT))


@dataclass(frozen=True)
class ResultRead:
    """One evaluation result, its window, and its grids in each weighting it carries."""

    result: EvaluationResult
    window: EvaluationSet
    sides: dict[str, Side]

    @property
    def result_id(self) -> str:
        return str(self.result.id)

    def cells(self, variant: str) -> dict[str, PanelArrays]:
        """Each cell's rows in one weighting, keyed ``"NK:BPO"``."""
        side = self.sides.get(variant)
        if side is None:
            return {}
        return {
            cell_key(setting, namespace): arrays
            for setting, grid in side.grids.items()
            for namespace, arrays in grid.panels.items()
        }

    def tau_grid(self) -> Any:
        """The grid the result was evaluated on, read from its first loaded file."""
        side = self.sides.get("weighted") or self.sides.get("unweighted")
        meta = None if side is None else side.meta()
        if meta is None:  # pragma: no cover - a result with no grid is refused earlier
            raise OperatingPointRefused(f"evaluation result {self.result_id} has no grid")
        return meta


def load_result(session: Session, result_id: str) -> tuple[EvaluationResult, EvaluationSet]:
    """The result row and its window, refusing either one absent."""
    try:
        key = uuid.UUID(str(result_id))
    except ValueError as exc:
        raise OperatingPointRefused(f"{result_id!r} is not an evaluation result id") from exc
    result = session.get(EvaluationResult, key)
    if result is None:
        raise OperatingPointRefused(f"no evaluation result {result_id}")
    window = session.get(EvaluationSet, result.evaluation_set_id)
    if window is None:
        raise OperatingPointRefused(
            f"evaluation result {result_id} names window {result.evaluation_set_id}, which is gone"
        )
    return result, window


def fetch_result_grids(store: Any, result_id: str, root: Path) -> list[str]:
    """Copy the result's per-setting grid files from the store under ``root``; the settings found."""
    found: list[str] = []
    for setting in SETTINGS:
        key = eval_artifact_key(uuid.UUID(result_id), f"{setting}/{GRID_FILENAME}")
        if not store.exists(key):
            continue
        target = root / setting
        target.mkdir(parents=True, exist_ok=True)
        (target / GRID_FILENAME).write_bytes(store.get(key))
        found.append(setting)
    if not found:
        raise OperatingPointRefused(
            f"evaluation result {result_id} stored no {GRID_FILENAME} for any setting, so there "
            "is no per-protein grid to read a threshold on; re-run run_cafa_evaluation"
        )
    return found


def read_result(
    session: Session,
    store: Any,
    result_id: str,
    variants: tuple[str, ...],
    stack: contextlib.ExitStack,
) -> ResultRead:
    """The result, its window and its grids in ``variants``, refusing a weighting it lacks."""
    result, window = load_result(session, result_id)
    root = Path(stack.enter_context(tempfile.TemporaryDirectory(prefix="protea_operating_")))
    settings = tuple(fetch_result_grids(store, str(result.id), root))
    provenance = {"id": str(result.id), "results": result.results or {}}
    sides = {
        variant: load_side(str(result.id), str(root), provenance, stack, (settings, variant))
        for variant in variants
    }
    return ResultRead(result, window, sides)


def score_reading_of(result: EvaluationResult) -> str:
    """How a result read its scores before thresholding; a result before ADR-D55 read them raw."""
    reading = (result.results or {}).get("reading") or {}
    return str(reading.get("score_reading") or "raw")


__all__ = [
    "OperatingPointRefused",
    "ResultRead",
    "fetch_result_grids",
    "load_result",
    "operating_point_store",
    "read_result",
    "score_reading_of",
]
