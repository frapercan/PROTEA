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
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from protea.api.cache import cached
from protea.api.deps import get_session_factory, get_settings
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

    axes = (
        [k for k in next(iter(out.values()))[0] if k not in _MEASURES] if any(out.values()) else []
    )
    return {
        "evaluation_result_id": str(evaluation_result_id),
        "axes": axes,
        "settings": out,
    }


#: Five minutes. A stratum comparison only changes when a new arm is
#: stratified, which is minutes-to-hours apart, and the read costs one
#: parquet per arm.
_COMPARE_TTL = 300.0

#: The arms of an evaluation set, with the identity a reader compares by.
#: prediction_set.meta carries the receipt, so the model and K come from
#: the row that produced the score rather than from a label.
#:
#: The scoring configuration and the donor policy travel with them. Eight arms
#: of this campaign share a prediction set and differ only downstream of it, so
#: a table naming an arm by its model alone prints eight rows reading
#: ``esm2_650m`` at eight different scores. A reader comparing two of those rows
#: believes they differ in the field the column names, which is the defect this
#: project has already hit and named: a single-field comparison that was not.
_ARMS = text(
    """
    SELECT er.id            AS evaluation_result_id,
           ec.id            AS embedding_config_id,
           ec.model_name    AS model,
           ec.display_name  AS display_name,
           -- Both sides of this merge added identity to the arm, and both are
           -- needed: an arm is told apart by every field that varied, and a
           -- comparison whose rows cannot be told apart is the defect this
           -- endpoint exists to avoid.
           ec.layer_indices AS layer_indices,
           ps.limit_per_entry AS k,
           sc.name          AS scoring_name,
           CASE
               WHEN ps.meta -> 'donor_policy' ->> 'evidence_codes' IS NULL
                   THEN 'permissive'
               ELSE 'evidence-gated'
           END              AS donor_policy,
           ps.meta ->> 'metric' AS metric
    FROM evaluation_result er
    JOIN prediction_set ps ON ps.id = er.prediction_set_id
    JOIN embedding_config ec ON ec.id = ps.embedding_config_id
    LEFT JOIN scoring_config sc ON sc.id = er.scoring_config_id
    WHERE er.evaluation_set_id = :esid
    ORDER BY ec.model_name, ec.layer_indices, ps.limit_per_entry, sc.name
    """
)


def _load_arms(session: Any, evaluation_set_id: str) -> list[dict[str, Any]]:
    """The arms of an evaluation set, each labelled so no two collide."""
    return _label(
        [
            {
                "evaluation_result_id": str(r["evaluation_result_id"]),
                "embedding_config_id": str(r["embedding_config_id"]),
                "model": r["model"],
                "display_name": r["display_name"] or r["model"],
                "layer_indices": r["layer_indices"],
                "k": r["k"],
            }
            for r in session.execute(_ARMS, {"esid": evaluation_set_id}).mappings().all()
        ]
    )


def _label(arms: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Make each arm's label name the configuration, not just the model.

    An arm is an EmbeddingConfig, and two configs of one model differ in
    every axis this campaign varies except the model itself. Comparing by
    display name silently merged them: rung 2's layer axis put ankh-base at
    layers 48 and 10 into the same evaluation set, and they stayed apart
    only because nobody had set display_name on the second, so the fallback
    to model_name happened to differ. With the name set on both, two layers
    would have averaged into one series and the comparison would have shown
    a number belonging to neither.

    So the row now carries embedding_config_id, which is the identity, and
    the label disambiguates when a model appears under more than one
    configuration in this evaluation set. Models with a single config keep
    the label they had, because renaming every arm to fix a collision that
    is not there makes the common case worse.
    """
    by_model: dict[str, set[str]] = {}
    for a in arms:
        by_model.setdefault(a["model"], set()).add(a["embedding_config_id"])
    for a in arms:
        if len(by_model[a["model"]]) < 2:
            continue
        layers = a.get("layer_indices")
        # Layer first when it is what differs, since it is the axis rung 2
        # declares. Otherwise the config id, which always distinguishes.
        suffix = f"L{layers}" if layers is not None else a["embedding_config_id"][:8]
        a["display_name"] = f'{a["display_name"]} [{suffix}]'
    return arms


def _matches(cell: dict[str, Any], where: dict[str, str]) -> bool:
    """Does this cell sit at the requested coordinates on every named axis?"""
    return all(str(cell.get(axis, "")) == value for axis, value in where.items())


def _arm_rows(
    store: Any,
    arms: list[dict[str, Any]],
    setting: str,
    where: dict[str, str],
) -> list[dict[str, Any]]:
    """One row per arm that has this stratum, dropping arms that do not.

    An arm with no artefact is absent rather than zero: it was never
    stratified, which is a different fact from scoring nothing there, and
    the two must not be drawn the same way.
    """
    rows = []
    for arm in arms:
        key = eval_artifact_key(arm["evaluation_result_id"], f"{setting}/strata.parquet")
        if not store.exists(key):
            continue
        for cell in _cells(store.get(key)):
            if not _matches(cell, where):
                continue
            rows.append({**arm, **cell})
    return rows


@dataclass
class _Coordinates:
    """Where in the crossing to read, as FastAPI query parameters.

    Grouped rather than listed on the endpoint because they are one idea:
    a point on the axes the caller is NOT comparing along. Passing them
    loose put the signature over the argument ceiling and read as four
    unrelated filters.
    """

    setting: str = Query(
        default="NK",
        description=(
            "Knowledge category to read: NK, LK or PK. One at a time, because "
            "the three are different populations and a table mixing them is "
            "not comparable down a column."
        ),
    )
    category: str | None = Query(
        default=None, description="Stratum coordinate on the category axis."
    )
    aspect: str | None = Query(default=None, description="Stratum coordinate on the aspect axis.")
    length: str | None = Query(default=None, description="Stratum coordinate on the length axis.")
    homology: str | None = Query(
        default=None, description="Stratum coordinate on the homology axis."
    )

    def where(self) -> dict[str, str]:
        """Only the axes actually pinned; an unset axis is not a filter."""
        return {
            axis: value
            for axis, value in (
                ("category", self.category),
                ("aspect", self.aspect),
                ("length", self.length),
                ("homology", self.homology),
            )
            if value is not None
        }


@router.get(
    "/compare/{evaluation_set_id}",
    summary="One stratum across every arm of an evaluation set",
)
def compare_strata(
    evaluation_set_id: uuid.UUID,
    at: _Coordinates = Depends(),
    factory: sessionmaker[Session] = Depends(get_session_factory),
    settings: Any = Depends(get_settings),
) -> dict[str, Any]:
    """Compare every arm of a rung inside one stratum.

    The existing endpoint answers "how did this arm do across strata". This
    answers the transpose, "who wins inside this stratum", which is the
    question a reader has once they know the strata differ more than the
    arms do. In rung 1 the twilight band scores 0.0535 against 0.1978 for
    the close band, a factor of nearly four, while the whole spread across
    eight backbones is 0.022.

    Coordinates left unset are not filtered, so omitting all of them
    returns every cell of every arm; a caller wanting a table pins the
    axes it is not comparing along.
    """
    try:
        store = get_artifact_store(settings)
    except ArtifactStoreUnavailable as exc:  # pragma: no cover - config failure
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    where = at.where()

    with factory() as session:
<<<<<<< HEAD
        arms = _load_arms(session, evaluation_set_id)
=======
        arms = [
            {
                "evaluation_result_id": str(r["evaluation_result_id"]),
                "model": r["model"],
                "display_name": r["display_name"] or r["model"],
                "k": r["k"],
                "scoring_name": r["scoring_name"],
                "donor_policy": r["donor_policy"],
                "metric": r["metric"],
            }
            for r in session.execute(_ARMS, {"esid": evaluation_set_id}).mappings().all()
        ]
>>>>>>> origin/feat/the-graph-surface-reads-its-strata

    if not arms:
        raise HTTPException(
            status_code=404,
            detail=f"no arms for evaluation set {evaluation_set_id}",
        )

    key = f"strata:compare:{evaluation_set_id}:{at.setting}:{sorted(where.items())}"
    rows = cached(
        key,
        _COMPARE_TTL,
        lambda: _arm_rows(store, arms, at.setting, where),
        serve_stale_on_error=True,
    )
    return {
        "evaluation_set_id": str(evaluation_set_id),
        "setting": at.setting,
        "where": where,
        "arms_total": len(arms),
        # Named separately so a caller can tell "this stratum is empty" from
        # "these arms were never stratified".
        "arms_with_strata": len({r["evaluation_result_id"] for r in rows}),
        "rows": rows,
    }
