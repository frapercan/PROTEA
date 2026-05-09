"""Scoring configuration management and analytical endpoints.

Provides full CRUD for :class:`ScoringConfig` and two read-only analytical
endpoints that apply a stored config to an existing ``PredictionSet``:

``GET /scoring/prediction-sets/{id}/score.tsv``
    Stream a TSV of scored predictions.  The score column is computed on-the-fly
    by applying the selected ``ScoringConfig`` formula to the raw signals stored
    in ``GOPrediction`` rows — no re-running of the KNN pipeline is required.

``GET /scoring/prediction-sets/{id}/metrics``
    Compute CAFA Fmax / AUC-PR for a (PredictionSet, ScoringConfig, category)
    triple.  Requires two ``AnnotationSet`` IDs to build the NK/LK ground truth
    following the CAFA4 protocol.

Evidence weights
----------------
Each ``ScoringConfig`` may carry an optional ``evidence_weights`` dict that
overrides the system-default per-GO-evidence-code quality multipliers.  The
API validates that:

- Every key in the dict is a known GO evidence code (one of the codes in
  :data:`DEFAULT_EVIDENCE_WEIGHTS`).
- Every value is a float in [0, 1].

Partial overrides are allowed: codes absent from the submitted dict will
continue to use the system default at score-computation time.
"""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from protea.api.deps import get_session_factory
from protea.core.evaluation import EvalContext
from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel
from protea.infrastructure.session import session_scope
from protea.services.scoring_service import (
    BoosterUnavailableError,
    EntityNotFoundError,
    RerankerResponse,
    ScoringConfigCreate,
    ScoringConfigResponse,
    SignalCoverageError,
    compute_prediction_metrics,
    compute_reranker_metrics_data,
    create_preset_configs_data,
    create_scoring_config_data,
    delete_scoring_config_data,
    get_scoring_config_data,
    iter_reranked_predictions_tsv,
    iter_scored_predictions,
    iter_training_data,
    list_scoring_configs_data,
    prepare_training_data_request,
    score_predictions_with_reranker,
    to_reranker_response,
    validate_scoring_request,
)

router = APIRouter(prefix="/scoring", tags=["scoring"])

# ScoringConfig CRUD ---------------------------------------------------------


@router.get("/configs", response_model=list[ScoringConfigResponse])
def list_scoring_configs(factory=Depends(get_session_factory)):
    """Return all stored ScoringConfigs ordered by creation time."""
    with session_scope(factory) as session:
        return list_scoring_configs_data(session)


@router.post("/configs", response_model=ScoringConfigResponse, status_code=201)
def create_scoring_config(
    body: ScoringConfigCreate,
    factory=Depends(get_session_factory),
):
    """Create a new ScoringConfig. Pydantic validators on ``ScoringConfigCreate``
    already enforce ``formula`` membership + signal-name allowlist (422)."""
    with session_scope(factory) as session:
        return create_scoring_config_data(session, body)


@router.post("/configs/presets", status_code=201)
def create_preset_configs(factory=Depends(get_session_factory)):
    """Seed the database with the four built-in preset ScoringConfigs.

    Idempotent — presets matched by name are skipped silently. Returns
    the list of preset names that were actually created.
    """
    with session_scope(factory) as session:
        return {"created": create_preset_configs_data(session)}


@router.get("/configs/{config_id}", response_model=ScoringConfigResponse)
def get_scoring_config(
    config_id: uuid.UUID,
    factory=Depends(get_session_factory),
):
    """Retrieve a single ScoringConfig by UUID."""
    try:
        with session_scope(factory) as session:
            return get_scoring_config_data(session, config_id)
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.delete("/configs/{config_id}", status_code=204)
def delete_scoring_config(
    config_id: uuid.UUID,
    factory=Depends(get_session_factory),
):
    """Delete a ScoringConfig by UUID."""
    try:
        with session_scope(factory) as session:
            delete_scoring_config_data(session, config_id)
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


# Scored TSV endpoint ----------------------------------------

@router.get("/prediction-sets/{set_id}/score.tsv")
def download_scored_predictions(
    set_id: uuid.UUID,
    scoring_config_id: uuid.UUID = Query(
        ...,
        description="UUID of the ``ScoringConfig`` whose formula computes the score column.",
    ),
    min_score: float | None = Query(
        None,
        ge=0.0,
        le=1.0,
        description="Optional score threshold in [0, 1]; rows below are omitted.",
    ),
    accession: str | None = Query(
        None,
        description="Optional protein accession filter; only rows for this protein streamed.",
    ),
    factory=Depends(get_session_factory),
):
    """Stream a TSV of predictions with computed confidence scores.

    The score is computed on-the-fly for every row using the selected
    ``ScoringConfig``, including any custom evidence-weight overrides stored
    in that config.  The session is closed before the generator starts so
    the response is streamed without holding a DB connection open.

    Query parameters
    ----------------
    scoring_config_id:
        UUID of the ``ScoringConfig`` to apply.
    min_score:
        Optional score threshold — rows below this value are omitted.
    accession:
        Optional protein accession filter.

    TSV columns
    -----------
    protein_accession, go_id, score, distance, ref_protein_accession,
    evidence_code, qualifier, identity_nw, identity_sw, taxonomic_distance.
    """
    try:
        with session_scope(factory) as session:
            config_snap = validate_scoring_request(session, set_id, scoring_config_id)
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except SignalCoverageError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    filename = f"scored_{set_id}_{scoring_config_id}.tsv"
    return StreamingResponse(
        iter_scored_predictions(
            factory,
            prediction_set_id=set_id,
            config_snap=config_snap,
            min_score=min_score,
            accession=accession,
        ),
        media_type="text/tab-separated-values",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# CAFA metrics endpoint ----------------------------------------

def _eval_context_dep(
    old_annotation_set_id: uuid.UUID = Query(
        ...,
        description=(
            "UUID of the OLD ``AnnotationSet`` (the t0 baseline) used "
            "to derive NK / LK / PK protein partitions."
        ),
    ),
    new_annotation_set_id: uuid.UUID = Query(
        ...,
        description=(
            "UUID of the NEW ``AnnotationSet`` (the t1 target) used as "
            "the ground-truth delta in CAFA scoring."
        ),
    ),
    ontology_snapshot_id: uuid.UUID = Query(
        ...,
        description=(
            "UUID of the ``OntologySnapshot`` to use for DAG ancestor "
            "propagation. Should be aligned with the OLD annotation "
            "set's snapshot for valid scoring."
        ),
    ),
) -> EvalContext:
    """FastAPI dependency that bundles the (old, new, snapshot) triple."""
    return EvalContext(
        old_annotation_set_id=old_annotation_set_id,
        new_annotation_set_id=new_annotation_set_id,
        ontology_snapshot_id=ontology_snapshot_id,
    )


@router.get("/prediction-sets/{set_id}/metrics")
def compute_metrics(
    set_id: uuid.UUID,
    scoring_config_id: uuid.UUID = Query(
        ...,
        description=(
            "UUID of the ``ScoringConfig`` whose formula and evidence "
            "weights are applied to every prediction before computing "
            "the curve."
        ),
    ),
    eval_context: EvalContext = Depends(_eval_context_dep),
    category: str = Query(
        "nk",
        pattern="^(nk|lk)$",
        description=(
            "CAFA category to score against: ``nk`` (no-knowledge "
            "proteins, default) or ``lk`` (limited-knowledge)."
        ),
    ),
    factory=Depends(get_session_factory),
):
    """Compute CAFA Fmax and AUC-PR for a PredictionSet under a ScoringConfig.

    Ground truth is the NK or LK delta between the old and new annotation sets
    in ``eval_context``, following the CAFA4 protocol: only experimental
    evidence codes, NOT-qualifier annotations excluded with full DAG propagation.

    The selected ``ScoringConfig`` — including any custom ``evidence_weights``
    — is applied to every ``GOPrediction`` row before computing the
    precision-recall curve.

    Parameters
    ----------
    scoring_config_id:
        Which stored ScoringConfig formula (and evidence weights) to apply.
    eval_context:
        Bundled ``(old_annotation_set_id, new_annotation_set_id,
        ontology_snapshot_id)`` carried through the metrics pipeline. The
        FastAPI dependency exposes the three fields as discrete query
        parameters on the wire.
    category:
        ``"nk"`` (no-knowledge) or ``"lk"`` (limited-knowledge) protein set.
    """
    try:
        with session_scope(factory) as session:
            return compute_prediction_metrics(
                session,
                prediction_set_id=set_id,
                scoring_config_id=scoring_config_id,
                eval_context=eval_context,
                category=category,
            )
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except SignalCoverageError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


# Training data endpoint (re-ranker) ----------------------------------------
@router.get(
    "/prediction-sets/{set_id}/training-data.tsv",
    summary="Export labeled training data for the re-ranker",
    response_class=StreamingResponse,
)
def download_training_data(
    set_id: uuid.UUID,
    evaluation_set_id: uuid.UUID = Query(
        ..., description="EvaluationSet to derive ground-truth labels from"
    ),
    category: str = Query(
        "nk", pattern="^(nk|lk|pk)$", description="Ground-truth category: nk, lk, or pk"
    ),
    factory=Depends(get_session_factory),
) -> StreamingResponse:
    """Stream labeled training data for the re-ranker model.

    Joins all GOPrediction feature columns with a binary ``label`` derived
    from the temporal ground-truth delta of the given EvaluationSet.

    A prediction is labeled **1** if the (protein_accession, go_id) pair
    appears in the selected category's ground truth, **0** otherwise.

    Parameters
    ----------
    evaluation_set_id:
        UUID of the EvaluationSet (old → new annotation sets).
    category:
        ``"nk"`` (no-knowledge), ``"lk"`` (limited-knowledge), or
        ``"pk"`` (partial-knowledge).
    """
    try:
        with session_scope(factory) as session:
            gt_pairs = prepare_training_data_request(
                session,
                prediction_set_id=set_id,
                evaluation_set_id=evaluation_set_id,
                category=category,
            )
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    filename = f"training_data_{set_id}_{category}.tsv"
    return StreamingResponse(
        iter_training_data(factory, prediction_set_id=set_id, gt_pairs=gt_pairs),
        media_type="text/tab-separated-values",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# Re-ranker model CRUD + train + apply ----------------------------------------

# RerankerResponse + to_reranker_response are exported by the service module.


@router.get("/rerankers", response_model=list[RerankerResponse])
def list_rerankers(factory=Depends(get_session_factory)):
    """Return all stored re-ranker models ordered by creation time."""
    with session_scope(factory) as session:
        models = session.query(RerankerModel).order_by(RerankerModel.created_at).all()
        return [to_reranker_response(m) for m in models]


@router.get("/rerankers/{reranker_id}", response_model=RerankerResponse)
def get_reranker(reranker_id: uuid.UUID, factory=Depends(get_session_factory)):
    """Retrieve a single re-ranker model by UUID."""
    with session_scope(factory) as session:
        model = session.get(RerankerModel, reranker_id)
        if model is None:
            raise HTTPException(status_code=404, detail="RerankerModel not found")
        return to_reranker_response(model)


@router.delete("/rerankers/{reranker_id}", status_code=204)
def delete_reranker(reranker_id: uuid.UUID, factory=Depends(get_session_factory)):
    """Delete a re-ranker model by UUID."""
    with session_scope(factory) as session:
        model = session.get(RerankerModel, reranker_id)
        if model is None:
            raise HTTPException(status_code=404, detail="RerankerModel not found")
        session.delete(model)


@router.get(
    "/prediction-sets/{set_id}/rerank.tsv",
    summary="Apply a trained re-ranker to predictions",
    response_class=StreamingResponse,
)
def download_reranked_predictions(
    set_id: uuid.UUID,
    reranker_id: uuid.UUID = Query(..., description="UUID of the trained RerankerModel to apply"),
    min_score: float | None = Query(
        None, ge=0.0, le=1.0, description="Minimum re-ranker score threshold"
    ),
    factory=Depends(get_session_factory),
) -> StreamingResponse:
    """Stream predictions re-scored by a trained LightGBM model.

    Each row includes the original prediction data plus a ``reranker_score``
    column (probability 0–1, higher = more likely correct).  Rows are sorted
    by descending score within each protein.
    """
    try:
        with session_scope(factory) as session:
            df = score_predictions_with_reranker(
                session,
                prediction_set_id=set_id,
                reranker_id=reranker_id,
            )
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except BoosterUnavailableError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    filename = f"reranked_{set_id}.tsv"
    return StreamingResponse(
        iter_reranked_predictions_tsv(df, min_score=min_score),
        media_type="text/tab-separated-values",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/prediction-sets/{set_id}/reranker-metrics")
def compute_reranker_metrics(
    set_id: uuid.UUID,
    reranker_id: uuid.UUID = Query(..., description="UUID of the trained RerankerModel"),
    evaluation_set_id: uuid.UUID = Query(..., description="UUID of the EvaluationSet"),
    category: str = Query(
        "nk",
        pattern="^(nk|lk|pk)$",
        description=(
            "CAFA category to score against: ``nk`` (no-knowledge, "
            "default), ``lk`` (limited-knowledge), or ``pk`` "
            "(partial-knowledge)."
        ),
    ),
    factory=Depends(get_session_factory),
):
    """Compute CAFA Fmax and AUC-PR using re-ranker scores instead of ScoringConfig.

    Applies the trained LightGBM model to all predictions in the PredictionSet,
    then evaluates against the temporal ground truth of the EvaluationSet.

    This closes the full re-ranker loop: train → apply → evaluate.
    """
    try:
        with session_scope(factory) as session:
            return compute_reranker_metrics_data(
                session,
                prediction_set_id=set_id,
                reranker_id=reranker_id,
                evaluation_set_id=evaluation_set_id,
                category=category,
            )
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except BoosterUnavailableError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
