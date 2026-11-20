"""Scoring configurations, the reranker registry and the reranker's training data.

Provides full CRUD for :class:`ScoringConfig`, the registry of reranker models,
and the labelled export a reranker is trained on in the laboratory.

Nothing here scores a set or measures one. A set's scores are made by its
method when it predicts (``score_prediction_set`` applies a ScoringConfig to a
candidate set and stores the result; a reranker stores its score as the method
does) and read as stored, by the evaluator and by the CAFA export of
``/embeddings/prediction-sets/{id}/predictions-cafa.tsv``; its numbers are the
evaluator's. The endpoints that scored, reranked and measured on the fly
(``score.tsv``, ``rerank.tsv``, ``score``, ``metrics`` and ``reranker-metrics``)
were retired on 2026-11-20 with the closure's single path: each was a parallel
door, and ``metrics`` computed a Fmax of its own, with exact term matching and
no propagation, that no other number in the platform agreed with.

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
from protea.api.roles import ROLE_OPERATOR, require_role
from protea.infrastructure.orm.models.embedding.dataset import Dataset
from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel
from protea.infrastructure.session import session_scope
from protea.services.scoring_service import (
    EntityNotFoundError,
    RerankerResponse,
    ScoringConfigCreate,
    ScoringConfigResponse,
    create_preset_configs_data,
    create_scoring_config_data,
    delete_scoring_config_data,
    get_scoring_config_data,
    iter_training_data,
    list_scoring_configs_data,
    prepare_training_data_request,
    to_reranker_response,
)

router = APIRouter(prefix="/scoring", tags=["scoring"])

# ScoringConfig CRUD ---------------------------------------------------------


@router.get("/configs", response_model=list[ScoringConfigResponse])
def list_scoring_configs(factory=Depends(get_session_factory)):
    """Return all stored ScoringConfigs ordered by creation time."""
    with session_scope(factory) as session:
        return list_scoring_configs_data(session)


@router.post(
    "/configs",
    response_model=ScoringConfigResponse,
    status_code=201,
    dependencies=[Depends(require_role(ROLE_OPERATOR))],
)
def create_scoring_config(
    body: ScoringConfigCreate,
    factory=Depends(get_session_factory),
):
    """Create a new ScoringConfig. Pydantic validators on ``ScoringConfigCreate``
    already enforce ``formula`` membership + signal-name allowlist (422)."""
    with session_scope(factory) as session:
        return create_scoring_config_data(session, body)


@router.post(
    "/configs/presets", status_code=201, dependencies=[Depends(require_role(ROLE_OPERATOR))]
)
def create_preset_configs(factory=Depends(get_session_factory)):
    """Seed the database with the four built-in preset ScoringConfigs.

    Idempotent: presets matched by name are skipped silently. Returns
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


@router.delete(
    "/configs/{config_id}", status_code=204, dependencies=[Depends(require_role(ROLE_OPERATOR))]
)
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
        datasets = {
            d.id: d
            for d in session.query(Dataset)
            .filter(Dataset.id.in_([m.dataset_id for m in models if m.dataset_id]))
            .all()
        }
        return [to_reranker_response(m, datasets.get(m.dataset_id)) for m in models]


@router.get("/rerankers/{reranker_id}", response_model=RerankerResponse)
def get_reranker(reranker_id: uuid.UUID, factory=Depends(get_session_factory)):
    """Retrieve a single re-ranker model by UUID."""
    with session_scope(factory) as session:
        model = session.get(RerankerModel, reranker_id)
        if model is None:
            raise HTTPException(status_code=404, detail="RerankerModel not found")
        dataset = session.get(Dataset, model.dataset_id) if model.dataset_id else None
        return to_reranker_response(model, dataset)


@router.delete(
    "/rerankers/{reranker_id}", status_code=204, dependencies=[Depends(require_role(ROLE_OPERATOR))]
)
def delete_reranker(reranker_id: uuid.UUID, factory=Depends(get_session_factory)):
    """Delete a re-ranker model by UUID."""
    with session_scope(factory) as session:
        model = session.get(RerankerModel, reranker_id)
        if model is None:
            raise HTTPException(status_code=404, detail="RerankerModel not found")
        session.delete(model)
