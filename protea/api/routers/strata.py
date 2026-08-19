"""Per-stratum cells for a finished evaluation.

`stratify_evaluation` writes one `strata.parquet` per knowledge setting beside
the evaluation's other artefacts. This serves them, so a reader can ask what a
model scored on short sequences in the twilight zone rather than only what it
scored overall.

Read-only and computes nothing: if a cell is missing here it is missing in the
artefact, and the fix is to run the operation, not to widen this endpoint.
"""

from __future__ import annotations

import io
import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from protea.api.deps import get_settings
from protea.core.operations._run_cafa_helpers import eval_artifact_key
from protea.infrastructure.storage.factory import ArtifactStoreUnavailable, get_artifact_store

router = APIRouter(prefix="/strata", tags=["strata"])

#: The knowledge categories an evaluation writes. Probed by name because the
#: artifact store has no list operation.
_SETTINGS = ("NK", "LK", "PK")

#: Columns the parquet always carries, whatever axes were crossed.
_MEASURES = ("n_proteins", "precision_w", "recall_w", "f_micro_w", "reportable")


def _cells(raw: bytes) -> list[dict[str, Any]]:
    import pandas as pd

    frame = pd.read_parquet(io.BytesIO(raw))
    return [
        {k: (v.item() if hasattr(v, "item") else v) for k, v in row.items()}
        for row in frame.to_dict("records")
    ]


@router.get(
    "/{evaluation_result_id}",
    summary="Per-stratum cells for one evaluation result",
)
def get_strata(
    evaluation_result_id: uuid.UUID,
    reportable_only: bool = Query(
        default=False,
        description=(
            "Drop cells below the population floor. Off by default: a table "
            "that shows only what survived looks identical to one that covered "
            "everything, so the caller has to ask for the narrower view."
        ),
    ),
    settings: Any = Depends(get_settings),
) -> dict[str, Any]:
    """Return the cells per knowledge setting, plus which axes were crossed.

    404 when no setting has been stratified, because an empty body and a run
    nobody stratified would otherwise look the same.
    """
    try:
        store = get_artifact_store(settings)
    except ArtifactStoreUnavailable as exc:  # pragma: no cover - config failure
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    out: dict[str, list[dict[str, Any]]] = {}
    for setting in _SETTINGS:
        key = eval_artifact_key(evaluation_result_id, f"{setting}/strata.parquet")
        if not store.exists(key):
            continue
        cells = _cells(store.get(key))
        if reportable_only:
            cells = [c for c in cells if c.get("reportable")]
        out[setting] = cells

    if not out:
        raise HTTPException(
            status_code=404,
            detail=(
                f"no strata artefact for evaluation result {evaluation_result_id}; "
                f"run the stratify_evaluation operation for it"
            ),
        )

    axes = [k for k in next(iter(out.values()))[0] if k not in _MEASURES] if any(out.values()) else []
    return {
        "evaluation_result_id": str(evaluation_result_id),
        "axes": axes,
        "settings": out,
    }
