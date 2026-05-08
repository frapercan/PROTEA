from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import func
from sqlalchemy.orm import Session, sessionmaker

from protea.api.cache import cached
from protea.api.deps import get_amqp_url, get_session_factory
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
from protea.infrastructure.queue.publisher import publish_job
from protea.infrastructure.session import session_scope
from protea.services.embeddings_service import (
    EntityNotFoundError,
    InvalidEmbeddingConfigError,
    assert_prediction_set_exists,
    config_to_dict,
    delete_embedding_config_cascade,
    delete_prediction_set_cascade,
    get_go_term_distribution_data,
    get_prediction_set_data,
    get_predictions_for_protein,
    iter_predictions_cafa_tsv,
    iter_predictions_tsv,
    list_prediction_sets_data,
    list_proteins_in_prediction_set,
    prepare_cafa_export,
    validate_embedding_config_body,
)
from protea.services.jobs_service import enqueue_job

router = APIRouter(prefix="/embeddings", tags=["embeddings"])

_PREDICTIONS_QUEUE = "protea.predictions"


def _validate_embedding_config_body(body: dict[str, Any]) -> dict[str, Any]:
    """Translate :class:`InvalidEmbeddingConfigError` to HTTP 422.

    Thin shim over
    :func:`protea.services.embeddings_service.validate_embedding_config_body`
    so existing call sites in this router keep working unchanged.
    """
    try:
        return validate_embedding_config_body(body)
    except InvalidEmbeddingConfigError as exc:
        raise HTTPException(status_code=422, detail=exc.errors) from exc


def _config_to_dict(c: EmbeddingConfig, embedding_count: int | None = None) -> dict[str, Any]:
    """Backwards-compatible alias for ``embeddings_service.config_to_dict``."""
    return config_to_dict(c, embedding_count)


# ── Embedding Configs ─────────────────────────────────────────────────────────


@router.get("/configs", summary="List embedding configs")
def list_embedding_configs(
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    """List all embedding configurations with their stored embedding counts, newest first.

    The per-config GROUP BY over a 4M-row table is cached 5 minutes — new
    configs still appear immediately (they have 0 embeddings), only the
    counts are stale.
    """

    def _compute() -> list[dict[str, Any]]:
        with session_scope(factory) as session:
            rows = session.query(EmbeddingConfig).order_by(EmbeddingConfig.created_at.desc()).all()
            counts = {
                config_id: cnt
                for config_id, cnt in session.query(
                    SequenceEmbedding.embedding_config_id,
                    func.count(SequenceEmbedding.id),
                )
                .group_by(SequenceEmbedding.embedding_config_id)
                .all()
            }
            return [_config_to_dict(c, embedding_count=counts.get(c.id, 0)) for c in rows]

    return cached("embeddings:configs", 300.0, _compute)


@router.post("/configs", summary="Create an embedding config")
def create_embedding_config(
    body: dict[str, Any],
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Create a new EmbeddingConfig that defines the model, layer selection, pooling strategy, and chunking.

    This config is referenced by `compute_embeddings` jobs and `predict_go_terms` jobs to ensure
    query and reference embeddings were produced under identical settings.
    """
    validated = _validate_embedding_config_body(body)

    with session_scope(factory) as session:
        config = EmbeddingConfig(**validated)
        session.add(config)
        session.flush()
        result = _config_to_dict(config)

    return result


@router.get("/configs/{config_id}", summary="Get embedding config details")
def get_embedding_config(
    config_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Retrieve a single EmbeddingConfig with its total stored embedding count."""
    with session_scope(factory) as session:
        c = session.get(EmbeddingConfig, config_id)
        if c is None:
            raise HTTPException(status_code=404, detail="EmbeddingConfig not found")

        embedding_count = (
            session.query(func.count(SequenceEmbedding.id))
            .filter(SequenceEmbedding.embedding_config_id == config_id)
            .scalar()
        )

        return _config_to_dict(c, embedding_count=embedding_count)


@router.delete("/configs/{config_id}", summary="Delete an embedding config")
def delete_embedding_config(
    config_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Delete an EmbeddingConfig and cascade-delete all linked embeddings, prediction sets, and predictions."""
    try:
        with session_scope(factory) as session:
            return delete_embedding_config_cascade(session, config_id)
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


# ── Predict ───────────────────────────────────────────────────────────────────


@router.post("/predict", summary="Trigger GO term prediction")
def predict_go_terms(
    body: dict[str, Any],
    factory: sessionmaker[Session] = Depends(get_session_factory),
    amqp_url: str = Depends(get_amqp_url),
) -> dict[str, Any]:
    """Queue a `predict_go_terms` job that runs KNN-based GO term transfer.

    The coordinator partitions query proteins into batches, each dispatched to
    `protea.predictions.batch` workers for KNN search (numpy or FAISS) + GO annotation transfer.
    Results are written to a new `PredictionSet` via `protea.predictions.write` workers.

    Required body fields: `embedding_config_id`, `annotation_set_id`, `ontology_snapshot_id`.
    Optional: `query_set_id` (FASTA upload), `limit_per_entry`, `distance_threshold`,
    `batch_size`, `search_backend`. Feature-engineering flags default to True —
    `compute_alignments`, `compute_taxonomy`, `compute_reranker_features` all run
    unless explicitly set to false. `aspect_separated_knn` defaults to true
    (one KNN index per GO aspect to guarantee BPO/MFO/CCO coverage even when
    unified nearest neighbours carry only one aspect).
    """

    def _parse_uuid(key: str) -> UUID:
        raw = body.get(key)
        try:
            return UUID(str(raw))
        except (ValueError, AttributeError):
            raise HTTPException(status_code=422, detail=f"{key} must be a valid UUID") from None

    config_id = _parse_uuid("embedding_config_id")
    annotation_set_id = _parse_uuid("annotation_set_id")
    ontology_snapshot_id = _parse_uuid("ontology_snapshot_id")

    with session_scope(factory) as session:
        if session.get(EmbeddingConfig, config_id) is None:
            raise HTTPException(status_code=404, detail="EmbeddingConfig not found")
        if session.get(AnnotationSet, annotation_set_id) is None:
            raise HTTPException(status_code=404, detail="AnnotationSet not found")
        if session.get(OntologySnapshot, ontology_snapshot_id) is None:
            raise HTTPException(status_code=404, detail="OntologySnapshot not found")

        job_id = enqueue_job(
            session,
            operation="predict_go_terms",
            queue_name=_PREDICTIONS_QUEUE,
            payload=body,
        )

    publish_job(amqp_url, _PREDICTIONS_QUEUE, job_id)
    return {"id": str(job_id), "status": "queued"}


# ── Prediction Sets ───────────────────────────────────────────────────────────


@router.get("/prediction-sets", summary="List prediction sets")
def list_prediction_sets(
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    """List the 100 most recent prediction sets, cached 5 min."""

    def _compute() -> list[dict[str, Any]]:
        with session_scope(factory) as session:
            return list_prediction_sets_data(session)

    return cached("embeddings:prediction-sets", 300.0, _compute)


@router.get("/prediction-sets/{set_id}", summary="Get prediction set details")
def get_prediction_set(
    set_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Retrieve a prediction set with total prediction count and per-protein GO term counts."""
    try:
        with session_scope(factory) as session:
            return get_prediction_set_data(session, set_id)
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/prediction-sets/{set_id}/proteins", summary="List proteins in a prediction set")
def list_prediction_set_proteins(
    set_id: UUID,
    search: str | None = None,
    limit: int = 50,
    offset: int = 0,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Paginated list of proteins in a prediction set with their predicted GO count, minimum distance,
    known annotation count, and how many predictions match known annotations (precision proxy)."""
    try:
        with session_scope(factory) as session:
            return list_proteins_in_prediction_set(
                session,
                prediction_set_id=set_id,
                search=search,
                limit=limit,
                offset=offset,
            )
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get(
    "/prediction-sets/{set_id}/proteins/{accession}", summary="Get predictions for one protein"
)
def get_protein_predictions(
    set_id: UUID,
    accession: str,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    """Return all predicted GO terms for a protein in a prediction set, sorted by distance (nearest first).
    Includes GO term details plus optional alignment (NW/SW) and taxonomy fields when computed."""
    try:
        with session_scope(factory) as session:
            return get_predictions_for_protein(
                session, prediction_set_id=set_id, accession=accession
            )
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get(
    "/prediction-sets/{set_id}/go-terms", summary="GO term distribution in a prediction set"
)
def get_go_term_distribution(
    set_id: UUID,
    limit: int = 50,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Return the most frequently predicted GO terms grouped by aspect (F/P/C)
    and the total prediction counts per aspect."""
    try:
        with session_scope(factory) as session:
            return get_go_term_distribution_data(
                session, prediction_set_id=set_id, limit=limit
            )
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get(
    "/prediction-sets/{set_id}/predictions.tsv",
    summary="Download predictions as TSV",
    response_class=StreamingResponse,
)
def download_predictions_tsv(
    set_id: UUID,
    accession: str | None = Query(None, description="Filter to a single query protein accession"),
    aspect: str | None = Query(None, description="Filter by GO aspect: F, P, or C"),
    max_distance: float | None = Query(
        None, description="Only include predictions with distance ≤ this value"
    ),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> StreamingResponse:
    """Stream all GO predictions for a prediction set as a tab-separated file.

    Each row is one (protein, GO term, reference protein) triple. Columns include
    embedding distance, GO term metadata, annotation fields, and optional alignment
    and taxonomy features (columns are present but empty when not computed).

    Optional filters: ``accession``, ``aspect`` (F/P/C), ``max_distance``.

    The response streams rows directly from the database — suitable for large
    prediction sets without loading everything into memory.
    """
    try:
        with session_scope(factory) as _check:
            assert_prediction_set_exists(_check, set_id)
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    filename = f"predictions_{set_id}.tsv"
    return StreamingResponse(
        iter_predictions_tsv(
            factory,
            prediction_set_id=set_id,
            accession=accession,
            aspect=aspect,
            max_distance=max_distance,
        ),
        media_type="text/tab-separated-values",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get(
    "/prediction-sets/{set_id}/predictions-cafa.tsv",
    summary="Download predictions in CAFA submission format",
    response_class=StreamingResponse,
)
def download_predictions_cafa(
    set_id: UUID,
    eval_id: UUID | None = Query(
        None, description="Filter to delta proteins from this EvaluationSet."
    ),
    aspect: str | None = Query(None, description="Filter by GO aspect: F, P, or C"),
    max_distance: float | None = Query(
        None, description="Only include predictions with distance ≤ this value"
    ),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> StreamingResponse:
    """Stream predictions in CAFA format: ``protein_accession\\tgo_id\\tscore``.

    Score is computed as ``max(0.0, 1.0 - distance)`` so that closer neighbours
    receive higher confidence scores in the [0, 1] range expected by the
    CAFA evaluator.  One row per (protein, GO term) pair — duplicate GO terms
    for the same protein are deduplicated keeping the highest score (lowest distance).

    Pass ``eval_id`` to restrict output to delta proteins only (NK + LK targets),
    which is required for a valid CAFA evaluation.
    """
    try:
        with session_scope(factory) as _check:
            delta_proteins = prepare_cafa_export(
                _check, prediction_set_id=set_id, eval_id=eval_id
            )
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    filename = f"predictions_cafa_{set_id}.tsv"
    return StreamingResponse(
        iter_predictions_cafa_tsv(
            factory,
            prediction_set_id=set_id,
            aspect=aspect,
            max_distance=max_distance,
            delta_proteins=delta_proteins,
        ),
        media_type="text/tab-separated-values",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.delete("/prediction-sets/{set_id}", summary="Delete a prediction set")
def delete_prediction_set(
    set_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Delete a prediction set and all its GOPrediction rows."""
    try:
        with session_scope(factory) as session:
            return delete_prediction_set_cascade(session, set_id)
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
