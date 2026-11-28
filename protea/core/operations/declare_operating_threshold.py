"""Pool an arm's fitting-window grids into a declared threshold per cell.

A reading of the literature ladder is read at a threshold declared before the
read. This declares one from the raw evaluations of one arm on its fitting
windows: per evaluator cell, the per-protein grids of every window are pooled
as distinct (protein, window) units, ``f_micro_w(tau) = 2 sum tp_w / (sum pred_w
+ sum n_gt_w)`` is computed on the evaluator's grid, and the declared threshold
is its argmax, the lowest threshold on ties
(:func:`protea.core.operating_point.grid.pooled_threshold`).

A threshold declared on raw scores is a **transport diagnostic**: F at it on the
selection window shows how a raw scale travels between windows. The threshold a
reading declares on calibrated scores is declared by its fit, which pools its
out-of-fold grids the same way.

The document (``operating_threshold`` weights, JSON, content-addressed in the
store) carries the declared block every reader reads
(:func:`protea.core.operating_point.declared_block`) and, per cell, the pooled
curve, the units pooled, ``F*_fit`` and ``F*_fit / 2``, and each window's own
threshold and value. The weights are registered as fitted on the latest
window's t1 corpus, since the windows' labels chose the threshold.

It refuses before writing anything: a result read on a percentile scale (a
threshold on that scale is relative to each arm and travels nowhere), a window
that is not a fitting window (role ``training``), two results on one window,
results evaluated on different grids or under different evaluator options, and
results that are not one arm at several windows, judged by the one list of
window-varying fields (:mod:`protea.core.operating_point.window_fields`), which
names the field.
"""

from __future__ import annotations

import contextlib
import hashlib
import uuid
from typing import Any, Literal

from pydantic import Field, field_validator
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core._evaluation_leakage import frame_of
from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.operating_point import CELLS, canonical_document, declared_block
from protea.core.operating_point.grid import PooledThreshold, pooled_threshold
from protea.core.operating_point.window_fields import arm_at_window, assert_one_arm
from protea.core.operations._operating_point_reads import (
    OperatingPointRefused,
    ResultRead,
    operating_point_store,
    read_result,
    score_reading_of,
)
from protea.core.operations._paired_panels_artifact import PanelComparabilityError
from protea.core.utils import contract_payload
from protea.infrastructure.orm.models.embedding.model_weights import ModelWeights
from protea.services import model_weights_service as weights_service

#: The window role a threshold may be declared on: the fitting windows.
FITTING_ROLE = "training"

#: The footer keys every pooled grid must share: one grid and one set of evaluator options.
SHARED_GRID_KEYS: tuple[str, ...] = ("tau_grid", "normalization", "prop", "no_orphans", "max_terms")

#: The weights kind this operation registers.
KIND = "operating_threshold"


class DeclareOperatingThresholdPayload(ProteaPayload, frozen=True):
    """The weights' name, the fitting-window results of one arm, and the rule (fixed values)."""

    name: str
    evaluation_result_ids: list[str] = Field(min_length=1)
    variant: Literal["f_micro_w"] = "f_micro_w"
    ties: Literal["lowest_tau"] = "lowest_tau"
    reading: Literal["raw"] = "raw"

    @field_validator("name")
    @classmethod
    def _a_weights_name(cls, value: str) -> str:
        return weights_service.validate_name(value)

    @field_validator("evaluation_result_ids")
    @classmethod
    def _distinct_ids(cls, value: list[str]) -> list[str]:
        for item in value:
            uuid.UUID(item)
        repeated = sorted({item for item in value if value.count(item) > 1})
        if repeated:
            raise ValueError(f"evaluation results {repeated} are listed more than once")
        return value


def _refuse_unpoolable(reads: list[ResultRead]) -> None:
    """Refuse a percentile reading, a non-fitting window, a window twice, or two grids."""
    windows = [str(read.window.id) for read in reads]
    if len(set(windows)) != len(windows):
        raise OperatingPointRefused(
            f"two results are on one window ({windows}); a threshold pools one arm's windows, "
            "each once"
        )
    for read in reads:
        reading = score_reading_of(read.result)
        if reading != "raw":
            raise OperatingPointRefused(
                f"evaluation result {read.result_id} read its scores as {reading!r}; a threshold "
                "on that scale is relative to each arm and window and travels nowhere. Declare "
                "it on raw evaluations (a transport diagnostic) or by the reading's own fit"
            )
        if read.window.window_role != FITTING_ROLE:
            raise OperatingPointRefused(
                f"window {read.window.id} of result {read.result_id} has role "
                f"{read.window.window_role!r}; a threshold is declared on the fitting windows "
                f"(role {FITTING_ROLE!r}) and read elsewhere"
            )
    first = reads[0].tau_grid()
    for read in reads[1:]:
        meta = read.tau_grid()
        differ = [k for k in SHARED_GRID_KEYS if meta.values.get(k) != first.values.get(k)]
        if differ:
            raise PanelComparabilityError(
                f"results {reads[0].result_id} and {read.result_id} were evaluated with "
                f"different {differ}; one threshold is declared on one grid"
            )


def _pooled(reads: list[ResultRead]) -> dict[str, PooledThreshold]:
    """Each cell pooled over the windows that score it."""
    grid = reads[0].tau_grid().tau_grid
    by_cell: dict[str, dict[str, Any]] = {}
    for read in reads:
        for cell, arrays in read.cells("weighted").items():
            by_cell.setdefault(cell, {})[str(read.window.id)] = arrays
    return {cell: pooled_threshold(by_cell[cell], grid) for cell in CELLS if cell in by_cell}


def _document(p: DeclareOperatingThresholdPayload, reads: list[ResultRead], pooled: dict) -> dict:
    meta = reads[0].tau_grid()
    document = declared_block(
        meta.th_step,
        {cell: found.tau for cell, found in pooled.items()},
        {cell: found.at_grid_edge for cell, found in pooled.items()},
    )
    document.update(
        {
            "kind": KIND,
            "variant": p.variant,
            "ties": p.ties,
            "reading": p.reading,
            "tau_grid": [float(t) for t in meta.tau_grid],
            "windows": [
                {
                    "evaluation_result_id": read.result_id,
                    "evaluation_set_id": str(read.window.id),
                    "prediction_set_id": str(read.result.prediction_set_id),
                }
                for read in reads
            ],
            "cells": {
                cell: {
                    "tau": found.tau,
                    "tau_index": found.index,
                    "f_micro_w": found.value,
                    "half_f_micro_w": found.half_value,
                    "units": found.units,
                    "at_grid_edge": found.at_grid_edge,
                    "curve": list(found.curve),
                    "windows": {
                        window: {"tau": tau, "f_micro_w": value}
                        for window, (tau, value) in found.window_taus.items()
                    },
                }
                for cell, found in pooled.items()
            },
            "absent_cells": [cell for cell in CELLS if cell not in pooled],
        }
    )
    return document


def _latest_truth(session: Session, reads: list[ResultRead]) -> uuid.UUID:
    """The t1 corpus of the window that ends last: what the threshold was fitted up to."""
    ends = [(frame_of(session, read.window), read.window) for read in reads]
    dated = [(frame.end, window) for frame, window in ends if frame is not None]
    window = max(dated, key=lambda pair: pair[0])[1] if dated else reads[-1].window
    return window.new_annotation_set_id


def _describe(p: DeclareOperatingThresholdPayload, reads: list[ResultRead], pooled: dict) -> str:
    taus = ", ".join(f"{cell} {found.tau:.2f}" for cell, found in pooled.items())
    return (
        f"A declared threshold per evaluator cell for the arm of prediction set "
        f"{reads[0].result.prediction_set_id}: the grid argmax of f_micro_w pooled over "
        f"{len(reads)} fitting window(s) as (protein, window) units, the lowest threshold on "
        f"ties, on raw scores (a transport diagnostic). Thresholds: {taus}."
    )


def _register(
    session: Session, store: Any, p: DeclareOperatingThresholdPayload, reads: list[ResultRead]
) -> tuple[ModelWeights, dict[str, PooledThreshold]]:
    pooled = _pooled(reads)
    if not pooled:
        raise OperatingPointRefused("no cell is scored by any of the results; nothing to declare")
    blob = canonical_document(_document(p, reads, pooled))
    sha = hashlib.sha256(blob).hexdigest()
    key = weights_service.content_key(KIND, p.name, sha, ".json")
    existing = session.execute(
        select(ModelWeights).where(ModelWeights.uri == key)
    ).scalar_one_or_none()
    if existing is not None:
        return existing, pooled
    store.put(key, blob)
    info = weights_service.WeightsInfo(
        kind=KIND,
        name=p.name,
        description=_describe(p, reads, pooled),
        meta={"declared_thresholds": {cell: found.tau for cell, found in pooled.items()}},
        trained_on_annotation_set_id=_latest_truth(session, reads),
    )
    return weights_service.register_by_reference(session, store, info, key, sha), pooled


class DeclareOperatingThresholdOperation:
    """Declare per-cell thresholds from one arm's fitting-window evaluations."""

    name = "declare_operating_threshold"
    description = (
        "Pool one arm's raw fitting-window evaluations, each (protein, window) a unit, into a "
        "declared threshold per evaluator cell: the grid argmax of f_micro_w, lowest tau on "
        "ties. Registers operating_threshold weights; refuses a percentile reading, a "
        "non-fitting window and results that are not one arm."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        ids = payload.get("evaluation_result_ids") or []
        return f"{payload.get('name', '?')}: pooled over {len(ids)} fitting-window result(s)"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = DeclareOperatingThresholdPayload.model_validate(contract_payload(payload))
        store = operating_point_store()
        with contextlib.ExitStack() as stack:
            reads = [
                read_result(session, store, result_id, ("weighted",), stack)
                for result_id in p.evaluation_result_ids
            ]
            _refuse_unpoolable(reads)
            assert_one_arm(
                {
                    str(read.window.id): arm_at_window(session, read.result.prediction_set_id)
                    for read in reads
                }
            )
            weights, pooled = _register(session, store, p, reads)
        cells = {
            cell: {
                "tau": found.tau,
                "f_micro_w": found.value,
                "half_f_micro_w": found.half_value,
                "units": found.units,
                "at_grid_edge": found.at_grid_edge,
                "window_taus": {w: tau for w, (tau, _) in found.window_taus.items()},
            }
            for cell, found in pooled.items()
        }
        emit(
            "declare_operating_threshold.declared",
            None,
            {"weights_id": str(weights.id), "cells": {c: v["tau"] for c, v in cells.items()}},
            "info",
        )
        edges = sorted(cell for cell, v in cells.items() if v["at_grid_edge"])
        if edges:
            emit(
                "declare_operating_threshold.grid_edge",
                f"cells {edges} declare a threshold at the grid's first or last element",
                {"cells": edges},
                "warning",
            )
        return OperationResult(
            result={
                "weights_id": str(weights.id),
                "sha256": weights.sha256,
                "uri": weights.uri,
                "cells": cells,
            }
        )
