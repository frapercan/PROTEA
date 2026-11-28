"""Read one evaluation result at the per-cell thresholds a weights row declares.

The reading of the literature ladder: an arm's result on a window, read at the
threshold each evaluator cell declared before the read (a reading's fit, or a
transport threshold of ``declare_operating_threshold``). The per-protein grid
the evaluation stored is read at the declared column of each cell with the
evaluator's own formulas (:func:`protea.core.operating_point.grid.metric_curves`),
so a number here and the evaluator's curve at that threshold are one number.

**Per cell** (one ``evaluation_operating_point`` row, ``cells``): the declared
threshold and whether it is the grid's first or last element; ``f_micro_w``,
``f_w``, the micro and macro precisions and recalls, the coverage and S in both
weightings at it; S's minimum in both weightings with its threshold; the oracle
maximum of ``f_micro_w`` (and of ``f_w``) with its threshold and edge, and the
oracle threshold's distance from half the oracle value (a calibrated scale puts
its optimum near half its F). **Per stratum** (the window's identity band by
length band inside each cell, from its placement): the same numbers with the
stratum's size, those under ``min_population`` flagged and never withheld, in a
parquet named by URI and sha256.

**The t0 category's disagreement.** On a window of LAFA's frame the reading
records the 3 x 3 table of the t0 category (what a method may key by,
:mod:`protea.core.t0_category`) against the evaluator's category over the
window's benchmark pairs. An evaluation step may read the truth; a method never
does.

A result and a threshold are read once: asking again returns the row already
written. Refused before anything is written: an unknown result or weights, a
weights row of a kind that declares no threshold or whose bytes do not hash to
its record, a result without the information-accretion weighting, a declared
threshold off the result's grid, and strata asked of a window with no placement.
"""

from __future__ import annotations

import contextlib
import hashlib
import io
import uuid
from collections.abc import Iterator
from typing import Any

import numpy as np
from pydantic import Field, field_validator
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from protea.core import lafa_frame
from protea.core.code_revision import code_revision
from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.operating_point import (
    DeclaredThreshold,
    load_declared_threshold,
)
from protea.core.operating_point.grid import extremum, metric_curves, metrics_at
from protea.core.operations._operating_point_reads import (
    OperatingPointRefused,
    ResultRead,
    operating_point_store,
    read_result,
)
from protea.core.operations._paired_panels_bootstrap import PanelArrays
from protea.core.strata import homology_band_for, length_band_for
from protea.core.t0_category import disagreement_table, disagreements, t0_categories
from protea.core.utils import contract_payload, job_id_from_payload
from protea.core.window_placement import WindowNotPlacedError, placement_of_window
from protea.infrastructure.orm.models.annotation.evaluation_operating_point import (
    EvaluationOperatingPoint,
)

#: The axes a reading can cross. The cell is category by aspect, always; the two
#: stratum axes are the window's placement of each target.
AXES: tuple[str, ...] = ("category", "aspect", "homology", "length")
STRATUM_AXES: tuple[str, ...] = ("homology", "length")

#: The metrics a stratum row carries, in both weightings.
STRATUM_METRICS: tuple[str, ...] = (
    "f_micro_w",
    "f_w",
    "pr_micro_w",
    "rc_micro_w",
    "pr_w",
    "rc_w",
    "cov_w",
    "s_w",
    "f_micro",
    "f",
    "pr_micro",
    "rc_micro",
    "pr",
    "rc",
    "cov",
    "s",
)

#: The truth parquet's scored buckets, as evaluator categories.
_SCORED_BUCKETS: dict[str, str] = {"nk": "NK", "lk": "LK", "pk": "PK"}


class ReadOperatingPointPayload(ProteaPayload, frozen=True):
    """The result, the weights declaring its thresholds, the axes and the population floor."""

    evaluation_result_id: str
    declared_threshold_id: str
    axes: list[str] = Field(default_factory=lambda: list(AXES))
    min_population: int = Field(default=30, ge=1)

    @field_validator("evaluation_result_id", "declared_threshold_id")
    @classmethod
    def _an_id(cls, value: str) -> str:
        return str(uuid.UUID(value))

    @field_validator("axes")
    @classmethod
    def _known_axes(cls, value: list[str]) -> list[str]:
        unknown = sorted(set(value) - set(AXES))
        if unknown:
            raise ValueError(f"unknown axes {unknown}; the axes are {list(AXES)}")
        if len(set(value)) != len(value):
            raise ValueError("an axis is listed more than once")
        if not {"category", "aspect"} <= set(value):
            raise ValueError("a reading is per cell: the axes include category and aspect")
        return value


def _rounded(values: dict[str, float]) -> dict[str, float]:
    return {key: round(float(value), 6) for key, value in values.items()}


def cell_reading(
    weighted: PanelArrays, unweighted: PanelArrays, index: int, grid: np.ndarray
) -> dict[str, Any]:
    """One cell at grid column ``index``, with S's minimum and the oracle maximum beside it."""
    cw, cu = metric_curves(weighted), metric_curves(unweighted)
    covered = cu["n"] > 0
    out: dict[str, Any] = {
        "tau": float(grid[index]),
        "tau_index": int(index),
        "n_targets": unweighted.n,
        "n_targets_w": weighted.n,
        **_rounded(metrics_at(cw, index, suffix="_w")),
        **_rounded(metrics_at(cu, index)),
    }
    for key, curve in (("", cu["s"]), ("_w", cw["s"])):
        low = extremum(curve, covered, grid, lowest=True)
        out[f"smin{key}"] = None if low is None else round(low.value, 6)
        out[f"tau_smin{key}"] = None if low is None else low.tau
    for key, curve in (("f_micro_w", cw["f_micro"]), ("f_w", cw["f"])):
        top = extremum(curve, covered, grid)
        out[f"oracle_{key}"] = None if top is None else round(top.value, 6)
        out[f"oracle_tau_{key}"] = None if top is None else top.tau
        out[f"oracle_edge_{key}"] = None if top is None else top.edge
    if out["oracle_tau_f_micro_w"] is not None:
        out["oracle_distance_from_half"] = round(
            abs(out["oracle_tau_f_micro_w"] - out["oracle_f_micro_w"] / 2.0), 6
        )
    return out


def _subset(arrays: PanelArrays, keep: set[str]) -> PanelArrays:
    order = np.array([i for i, acc in enumerate(arrays.accessions) if acc in keep], dtype=np.int64)
    return arrays.take(order)


def _bands(placement: dict[str, Any], accession: str, axes: list[str]) -> tuple[str, ...]:
    if not axes:
        return ()
    where = placement.get(accession)
    if where is None:
        raise OperatingPointRefused(
            f"protein {accession} is scored in the grid and is not a target of the window's "
            "placement; the grid and the placement describe different windows"
        )
    bands = {
        "homology": homology_band_for(where.identity).value,
        "length": length_band_for(where.residues).value,
    }
    return tuple(bands[axis] for axis in STRATUM_AXES if axis in axes)


def strata_rows(
    read: ResultRead, declared: DeclaredThreshold, placement: dict[str, Any], p: Any
) -> Iterator[dict[str, Any]]:
    """Each cell's strata at its declared threshold, one row per nonempty stratum."""
    axes = [axis for axis in STRATUM_AXES if axis in p.axes]
    weighted, unweighted = read.cells("weighted"), read.cells("unweighted")
    meta = read.tau_grid()
    for cell in sorted(set(weighted) & set(unweighted)):
        if cell not in declared.taus:
            continue
        index = meta.tau_index(declared.tau(cell))
        groups: dict[tuple[str, ...], set[str]] = {}
        for accession in unweighted[cell].accessions:
            groups.setdefault(_bands(placement, accession, axes), set()).add(accession)
        for bands, members in sorted(groups.items()):
            reading = cell_reading(
                _subset(weighted[cell], members),
                _subset(unweighted[cell], members),
                index,
                meta.tau_grid,
            )
            setting, aspect = cell.split(":")
            placed: dict[str, str] = {}
            for axis, band in zip(axes, bands, strict=True):
                placed[axis] = band
            yield {
                "cell": cell,
                "setting": setting,
                "aspect": aspect,
                **placed,
                "n": reading["n_targets"],
                "n_w": reading["n_targets_w"],
                "small": reading["n_targets"] < p.min_population,
                "tau": reading["tau"],
                **{name: reading[name] for name in STRATUM_METRICS},
            }


def _parquet(rows: list[dict[str, Any]]) -> bytes:
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.Table.from_pylist(rows) if rows else pa.table({"cell": pa.array([], pa.string())})
    sink = io.BytesIO()
    pq.write_table(table, sink)
    return sink.getvalue()


def _benchmark_pairs(session: Session, window: Any) -> list[tuple[str, str, str]]:
    """``(accession, aspect, evaluator category)`` of the window's benchmark pairs, from its truth."""
    from protea.core.operations.declare_window_sample import scored_truth

    aspects = dict(
        session.execute(
            text("SELECT go_id, aspect FROM go_term WHERE ontology_snapshot_id = :s"),
            {"s": window.pivot_snapshot_id},
        ).all()
    )
    pairs: dict[tuple[str, str], str] = {}
    for accession, go_id, category in scored_truth(window):
        aspect = aspects.get(go_id)
        if aspect is not None:
            pairs[(accession, aspect)] = category
    return [
        (accession, aspect, category) for (accession, aspect), category in sorted(pairs.items())
    ]


def _disagreement(session: Session, window: Any) -> dict[str, Any] | None:
    """The t0 category against the evaluator's on the window's benchmark pairs, or None."""
    if window.truth_profile != lafa_frame.PROFILE:
        return None
    pairs = _benchmark_pairs(session, window)
    t0 = t0_categories(session, window, {accession for accession, _, _ in pairs})
    table = disagreement_table(t0, pairs)
    return {"table": table, "pairs": len(pairs), "disagreements": disagreements(table)}


def _placement(session: Session, read: ResultRead, p: Any) -> dict[str, Any]:
    if not set(STRATUM_AXES) & set(p.axes):
        return {}
    try:
        return placement_of_window(session, str(read.window.id))
    except WindowNotPlacedError as exc:
        raise OperatingPointRefused(str(exc)) from exc


def _cells(read: ResultRead, declared: DeclaredThreshold) -> dict[str, Any]:
    weighted, unweighted = read.cells("weighted"), read.cells("unweighted")
    meta = read.tau_grid()
    out: dict[str, Any] = {}
    for cell in sorted(set(weighted) & set(unweighted)):
        if cell not in declared.taus:
            out[cell] = {"read": False, "reason": "the weights declare no threshold for this cell"}
            continue
        index = meta.tau_index(declared.tau(cell))
        reading = cell_reading(weighted[cell], unweighted[cell], index, meta.tau_grid)
        out[cell] = {"read": True, "at_grid_edge": declared.at_grid_edge[cell], **reading}
    return out


def describe(read: ResultRead, declared: DeclaredThreshold, cells: dict[str, Any]) -> str:
    """The row's description, derived from what was read."""
    read_cells = [cell for cell, block in cells.items() if block.get("read")]
    return (
        f"Evaluation result {read.result_id} (prediction set {read.result.prediction_set_id} on "
        f"window {read.window.id}) read at the per-cell thresholds declared by "
        f"{declared.kind} weights {declared.weights_id}: {len(read_cells)} cell(s) read at "
        "their declared threshold, with f_micro_w, f_w, precision, recall, coverage and S in "
        "both weightings, S's minimum and the oracle maximum, and every identity by length "
        "stratum inside each cell."
    )


class ReadOperatingPointOperation:
    """A result read at a declared threshold per cell, and per stratum inside each cell."""

    name = "read_operating_point"
    description = (
        "Read one evaluation result at the per-cell thresholds a weights row declares: per "
        "cell f_micro_w, f_w, precision, recall, coverage and S in both weightings, S's "
        "minimum and the oracle maximum; per identity by length stratum the same with its "
        "size, small strata flagged; and the t0 category's disagreement table."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        result = str(payload.get("evaluation_result_id", "?"))[:8]
        weights = str(payload.get("declared_threshold_id", "?"))[:8]
        return f"result {result} at the thresholds of weights {weights}"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = ReadOperatingPointPayload.model_validate(contract_payload(payload))
        store = operating_point_store()
        declared = load_declared_threshold(session, p.declared_threshold_id, store)
        existing = self._existing(session, p.evaluation_result_id, declared.weights_id)
        if existing is not None:
            emit("read_operating_point.already_read", None, {"id": str(existing.id)}, "info")
            return OperationResult(result=self._result(existing))
        with contextlib.ExitStack() as stack:
            read = read_result(
                session, store, p.evaluation_result_id, ("weighted", "unweighted"), stack
            )
            cells = _cells(read, declared)
            rows = list(strata_rows(read, declared, _placement(session, read, p), p))
        disagreement = _disagreement(session, read.window)
        blob = _parquet(rows)
        sha = hashlib.sha256(blob).hexdigest()
        uri = str(
            store.put(
                f"operating-points/{read.result_id}/{declared.weights_id}/{sha}/strata.parquet",
                blob,
            )
        )
        row = EvaluationOperatingPoint(
            evaluation_result_id=read.result.id,
            declared_threshold_id=uuid.UUID(declared.weights_id),
            cells=cells,
            t0_category_disagreement=disagreement,
            strata_uri=uri,
            strata_sha256=sha,
            description=describe(read, declared, cells),
            job_id=job_id_from_payload(payload),
            code_revision=code_revision(),
        )
        session.add(row)
        session.flush()
        self._emit(emit, row, rows)
        return OperationResult(result=self._result(row))

    @staticmethod
    def _existing(
        session: Session, result_id: str, weights_id: str
    ) -> EvaluationOperatingPoint | None:
        """The row already read for this result and these weights, if any."""
        return session.execute(
            select(EvaluationOperatingPoint).where(
                EvaluationOperatingPoint.evaluation_result_id == uuid.UUID(result_id),
                EvaluationOperatingPoint.declared_threshold_id == uuid.UUID(weights_id),
            )
        ).scalar_one_or_none()

    @staticmethod
    def _emit(emit: EmitFn, row: EvaluationOperatingPoint, rows: list[dict[str, Any]]) -> None:
        small = sum(1 for r in rows if r["small"])
        emit(
            "read_operating_point.read",
            None,
            {
                "id": str(row.id),
                "cells": {cell: block.get("f_micro_w") for cell, block in row.cells.items()},
                "strata": len(rows),
                "small_strata": small,
            },
            "info",
        )
        edges = sorted(c for c, b in row.cells.items() if b.get("at_grid_edge"))
        if edges:
            emit(
                "read_operating_point.threshold_at_grid_edge",
                f"cells {edges} are read at a threshold at the grid's first or last element",
                {"cells": edges},
                "warning",
            )

    @staticmethod
    def _result(row: EvaluationOperatingPoint) -> dict[str, Any]:
        return {
            "evaluation_operating_point_id": str(row.id),
            "evaluation_result_id": str(row.evaluation_result_id),
            "declared_threshold_id": str(row.declared_threshold_id),
            "cells": row.cells,
            "t0_category_disagreement": row.t0_category_disagreement,
            "strata": {"uri": row.strata_uri, "sha256": row.strata_sha256},
        }
