"""Turn a finished evaluation into cells, one number per stratum.

`run_cafa_evaluation` writes `per_protein.parquet` per setting; this reads it
back, places every protein on the axes in :mod:`protea.core.strata`, and pools
each cell. It is a separate operation rather than a step inside the evaluation
driver for three reasons: the driver has no database session and should not
grow one, the evaluation path is long and should not get longer, and the
evaluations already on disk deserve to be stratified without being rerun.

Pooling is micro. The mean of per-protein F is a macro average and would be a
different number under the same name; :func:`micro_cells` does the summing.
"""

from __future__ import annotations

import tempfile
import uuid
from pathlib import Path
from typing import Annotated, Any

from pydantic import Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, Operation, OperationResult, ProteaPayload
from protea.core.operations._run_cafa_helpers import eval_artifact_key
from protea.core.operations._run_cafa_strata import micro_cells, neighbourhoods_for, project
from protea.core.strata import (
    Aspect,
    Category,
    Stratum,
    stratum_for,
)

#: cafaeval reports the long namespace; the strata vocabulary speaks CAFA codes.
_ASPECT_FOR_NAMESPACE: dict[str, Aspect] = {
    "biological_process": Aspect.BIOLOGICAL_PROCESS,
    "molecular_function": Aspect.MOLECULAR_FUNCTION,
    "cellular_component": Aspect.CELLULAR_COMPONENT,
}

_LENGTHS = "SELECT accession, length FROM protein WHERE length IS NOT NULL"


class StratifyEvaluationPayload(ProteaPayload):
    """What to stratify, and how finely."""

    prediction_set_id: Annotated[str, Field(description="the run whose neighbourhood is read")]
    artifacts_root: Annotated[
        str | None,
        Field(default=None, description="local directory holding <setting>/per_protein.parquet"),
    ]
    evaluation_result_id: Annotated[
        str | None,
        Field(
            default=None,
            description=(
                "read and write the artefacts in the store under this result's "
                "prefix; the normal path, since a finished evaluation's files "
                "live there and its temporary directory is already gone"
            ),
        ),
    ]
    axes: Annotated[
        list[str],
        Field(
            default_factory=lambda: ["category", "aspect", "length", "homology"],
            description="stratum fields to cross; all seven leaves most cells empty",
        ),
    ]
    min_population: Annotated[
        int, Field(default=30, ge=1, description="cells below this are withheld, not dropped")
    ]


def _protein_lengths(session: Session) -> dict[str, int]:
    return {r[0]: int(r[1]) for r in session.execute(text(_LENGTHS))}


def _category_for(setting: str) -> Category | None:
    """cafaeval's setting names carry the knowledge category as a prefix."""
    head = setting.split("_", 1)[0].upper()
    try:
        return Category.from_code(head)
    except (ValueError, KeyError):
        return None


def _strata_for_rows(
    rows: list[dict[str, Any]],
    *,
    category: Category,
    lengths: dict[str, int],
    neighbourhoods: dict[str, Any],
) -> dict[int, Stratum]:
    """Index of row position -> stratum, skipping rows that cannot be placed.

    A protein with no length, no resolvable aspect or no non-self donor is
    skipped rather than defaulted into a band, because every default here would
    move a published number by an amount nobody chose.
    """
    placed: dict[int, Stratum] = {}
    for i, row in enumerate(rows):
        aspect = _ASPECT_FOR_NAMESPACE.get(str(row.get("namespace")))
        acc = str(row.get("protein_accession"))
        residues = lengths.get(acc)
        neighbourhood = neighbourhoods.get(acc)
        if aspect is None or not residues or neighbourhood is None:
            continue
        placed[i] = stratum_for(
            category=category, aspect=aspect, residues=residues, neighbourhood=neighbourhood
        )
    return placed


def _settings_from_store(store: Any, result_id: str, work: Path) -> list[str]:
    """Fetch every ``<setting>/per_protein.parquet`` under a result's prefix.

    The settings are not listed from the store: the store protocol has no list,
    and the three CAFA knowledge categories are the only settings an evaluation
    writes. A setting whose file is absent is simply not returned.
    """
    found: list[str] = []
    for setting in ("NK", "LK", "PK"):
        key = eval_artifact_key(uuid.UUID(result_id), f"{setting}/per_protein.parquet")
        if not store.exists(key):
            continue
        target = work / setting
        target.mkdir(parents=True, exist_ok=True)
        (target / "per_protein.parquet").write_bytes(store.get(key))
        found.append(setting)
    return found


def _resolve_source(
    p: StratifyEvaluationPayload, emit: EmitFn
) -> tuple[Path, Any, Any]:
    """Where the per-protein tables are read from, and where results go back.

    Two modes on purpose. A local directory is what a test or an ad-hoc run
    has; a result id is what a finished evaluation has, since its temporary
    directory is long gone and the files live in the store.
    """
    if not p.artifacts_root and not p.evaluation_result_id:
        raise ValueError(
            "give either artifacts_root (a local directory) or "
            "evaluation_result_id (the store prefix); with neither there is "
            "nothing to read"
        )
    if not p.evaluation_result_id:
        return Path(str(p.artifacts_root)), None, None

    from protea.infrastructure.settings import load_settings
    from protea.infrastructure.storage.factory import get_artifact_store

    # protea/core/operations/<this>.py -> parents[3] is the repo root, the same
    # walk export_evaluation_targets and compute_information_accretion do.
    store = get_artifact_store(load_settings(Path(__file__).resolve().parents[3]))
    tmp = tempfile.TemporaryDirectory(prefix="protea_strata_")
    root = Path(tmp.name)
    emit(
        "stratify_evaluation.fetched",
        None,
        {"settings": _settings_from_store(store, p.evaluation_result_id, root)},
        "info",
    )
    return root, store, tmp


class StratifyEvaluationOperation(Operation):
    """Pool a finished evaluation into per-stratum cells."""

    name = "stratify_evaluation"
    description = (
        "Pool a finished evaluation's per-protein scores into one micro-averaged "
        "cell per stratum, withholding cells below the population floor."
    )
    payload_model = StratifyEvaluationPayload

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        """One line naming what is being crossed, which is the thing that
        distinguishes two otherwise identical runs of this operation."""
        axes = payload.get("axes") or ["category", "aspect", "length", "homology"]
        pset = str(payload.get("prediction_set_id", "?"))[:8]
        return f"{pset} by {' x '.join(axes)} (floor {payload.get('min_population', 30)})"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = StratifyEvaluationPayload.model_validate(payload)
        root, store, tmp = _resolve_source(p, emit)
        neighbourhoods = neighbourhoods_for(session, p.prediction_set_id)
        lengths = _protein_lengths(session)
        emit(
            "stratify_evaluation.inputs",
            None,
            {"neighbourhoods": len(neighbourhoods), "lengths": len(lengths)},
            "info",
        )

        import pandas as pd

        summary: dict[str, Any] = {"settings": {}, "axes": list(p.axes)}
        for parquet in sorted(root.glob("*/per_protein.parquet")):
            setting = parquet.parent.name
            category = _category_for(setting)
            if category is None:
                emit(
                    "stratify_evaluation.setting_skipped",
                    None,
                    {"setting": setting, "reason": "no knowledge category in the name"},
                    "warning",
                )
                continue
            rows = pd.read_parquet(parquet).to_dict("records")
            placed = _strata_for_rows(
                rows, category=category, lengths=lengths, neighbourhoods=neighbourhoods
            )
            # The stratum rides on the row rather than being looked up by
            # position: resolving it per row through list.index would be
            # quadratic and would compare dicts by value, which is both slow
            # and wrong when two proteins happen to score identically.
            placed_rows = [{**rows[i], "_stratum": st} for i, st in placed.items()]
            cells = micro_cells(placed_rows, lambda row: project(row["_stratum"], p.axes))
            summary["settings"][setting] = self._write(
                parquet.parent, cells, p, emit, len(rows), len(placed)
            )
            if store is not None and p.evaluation_result_id:
                key = eval_artifact_key(
                    uuid.UUID(p.evaluation_result_id), f"{setting}/strata.parquet"
                )
                store.put(key, parquet.parent / "strata.parquet")
                summary["settings"][setting]["key"] = key
        if tmp is not None:
            tmp.cleanup()
        return OperationResult(result=summary)

    @staticmethod
    def _write(
        setting_dir: Path,
        cells: dict[Any, Any],
        p: StratifyEvaluationPayload,
        emit: EmitFn,
        n_rows: int,
        n_placed: int,
    ) -> dict[str, Any]:
        import pandas as pd

        populations = {k: c.n_proteins for k, c in cells.items()}
        reportable = [k for k, n in populations.items() if n >= p.min_population]
        withheld = [k for k, n in populations.items() if n < p.min_population]
        frame = pd.DataFrame(
            [
                {
                    # ``.value``, not ``str()``. Category and Aspect are plain
                    # Enums, so str() yields "Aspect.MOLECULAR_FUNCTION" and the
                    # column stops matching the vocabulary every other table
                    # uses. The bands are StrEnum and agree either way.
                    **{
                        axis: getattr(value, "value", str(value))
                        for axis, value in zip(p.axes, key, strict=True)
                    },
                    "n_proteins": c.n_proteins,
                    "precision_w": c.precision_w,
                    "recall_w": c.recall_w,
                    "f_micro_w": c.f_micro_w,
                    # Withheld cells are WRITTEN and flagged, never dropped: a
                    # table that prints only what survived looks identical to a
                    # table that covered everything.
                    "reportable": populations[key] >= p.min_population,
                }
                for key, c in cells.items()
            ]
        )
        setting = setting_dir.name
        setting_dir.mkdir(parents=True, exist_ok=True)
        out = setting_dir / "strata.parquet"
        frame.to_parquet(out, index=False)
        counts = {
            "rows_read": n_rows,
            "rows_placed": n_placed,
            "cells": len(cells),
            "reportable": len(reportable),
            "withheld": len(withheld),
        }
        emit("stratify_evaluation.written", None,
             {"setting": setting, "path": str(out), **counts}, "info")
        return counts
