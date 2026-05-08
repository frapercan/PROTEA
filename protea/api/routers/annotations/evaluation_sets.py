"""Evaluation-set endpoints: generate / list / get / delete + GT TSV streams + delta FASTA + run-cafa."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import ValidationError
from sqlalchemy.orm import Session, sessionmaker

from protea.api.deps import get_amqp_url, get_benchmark_config, get_session_factory
from protea.core.operations.generate_evaluation_set import GenerateEvaluationSetPayload
from protea.core.operations.run_cafa_evaluation import RunCafaEvaluationPayload
from protea.infrastructure.benchmark_config import BenchmarkConfig
from protea.infrastructure.queue.publisher import publish_job
from protea.infrastructure.session import session_scope
from protea.services.annotations_service import (
    EntityNotFoundError,
    apply_baseline_scoring_default,
    assert_evaluation_set_exists,
    delete_evaluation_set_collect_keys,
    get_evaluation_set_data,
    iter_delta_proteins_fasta,
    list_evaluation_sets_data,
)
from protea.services.jobs_service import (
    InvalidJobPayloadError,
    dispatch_validated_job,
    enqueue_job,
)

from ._common import EVALUATIONS_QUEUE, JOBS_QUEUE, stream_groundtruth

router = APIRouter()


@router.post("/evaluation-sets/generate", summary="Queue a generate_evaluation_set job")
def generate_evaluation_set(
    body: dict[str, Any],
    factory: sessionmaker[Session] = Depends(get_session_factory),
    amqp_url: str = Depends(get_amqp_url),
) -> dict[str, Any]:
    """Queue a job that computes the CAFA delta between two annotation sets.

    Applies experimental evidence filtering, NOT-qualifier propagation through
    the GO DAG, and classifies delta proteins into NK/LK.  Stats are stored in
    a new EvaluationSet row; ground-truth TSVs are streamed on demand.
    """
    try:
        return dispatch_validated_job(
            factory, amqp_url, body, GenerateEvaluationSetPayload,
            operation="generate_evaluation_set", queue_name=JOBS_QUEUE,
        )
    except InvalidJobPayloadError as exc:
        raise HTTPException(status_code=422, detail=exc.errors) from exc


@router.get("/evaluation-sets", summary="List evaluation sets")
def list_evaluation_sets(
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    """List all evaluation sets, newest first."""
    with session_scope(factory) as session:
        return list_evaluation_sets_data(session)


@router.delete("/evaluation-sets/{eval_id}", summary="Delete an evaluation set", status_code=204)
def delete_evaluation_set(
    eval_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> None:
    """Delete an evaluation set and all its results, cascading to EvaluationResult
    rows and removing their artifact-store objects (ground-truth + per-result
    cafaeval outputs)."""
    from protea.core.evaluation import groundtruth_key_for
    from protea.infrastructure.settings import load_settings
    from protea.infrastructure.storage import get_artifact_store

    try:
        with session_scope(factory) as session:
            result_keys = delete_evaluation_set_collect_keys(session, eval_id)
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    project_root = Path(__file__).resolve().parents[4]
    store = get_artifact_store(load_settings(project_root))
    store.delete(groundtruth_key_for(eval_id))
    for key in result_keys:
        store.delete(key)


@router.get("/evaluation-sets/{eval_id}", summary="Get evaluation set details")
def get_evaluation_set(
    eval_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    try:
        with session_scope(factory) as session:
            return get_evaluation_set_data(session, eval_id)
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get(
    "/evaluation-sets/{eval_id}/ground-truth-NK.tsv",
    response_class=StreamingResponse,
    summary="Download NK ground truth (CAFA format)",
)
def download_gt_nk(
    eval_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> StreamingResponse:
    """Download No-Knowledge ground truth: delta proteins with zero prior experimental annotations."""
    return stream_groundtruth(factory, eval_id, "nk", "ground_truth_NK.tsv")


@router.get(
    "/evaluation-sets/{eval_id}/ground-truth-LK.tsv",
    response_class=StreamingResponse,
    summary="Download LK ground truth (CAFA format)",
)
def download_gt_lk(
    eval_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> StreamingResponse:
    """Download Limited-Knowledge ground truth: delta proteins with prior experimental annotations."""
    return stream_groundtruth(factory, eval_id, "lk", "ground_truth_LK.tsv")


@router.get(
    "/evaluation-sets/{eval_id}/ground-truth-PK.tsv",
    response_class=StreamingResponse,
    summary="Download PK ground truth (CAFA format)",
)
def download_gt_pk(
    eval_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> StreamingResponse:
    """Download Partial-Knowledge ground truth: proteins that gained new terms in a
    namespace where they already had experimental annotations at t0."""
    return stream_groundtruth(factory, eval_id, "pk", "ground_truth_PK.tsv")


@router.get(
    "/evaluation-sets/{eval_id}/known-terms.tsv",
    response_class=StreamingResponse,
    summary="Download known-terms from OLD annotation set (for CAFA PK evaluation)",
)
def download_known_terms(
    eval_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> StreamingResponse:
    """Download ALL experimental annotations from the OLD set. Pass as ``-known`` to enable PK scoring."""
    return stream_groundtruth(factory, eval_id, "known", "known_terms.tsv")


@router.get(
    "/evaluation-sets/{eval_id}/delta-proteins.fasta",
    response_class=StreamingResponse,
    summary="Download delta proteins as FASTA",
)
def download_delta_fasta(
    eval_id: UUID,
    category: str = Query(
        default="all", description="Which proteins to include: `nk`, `lk`, or `all` (default)."
    ),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> StreamingResponse:
    """Download the amino-acid sequences of delta proteins (NK and/or LK) as FASTA.

    Header format: ``>ACCESSION entry_name OS=organism OX=taxonomy_id (NK|LK)``
    """
    try:
        with session_scope(factory) as session:
            lines = iter_delta_proteins_fasta(session, eval_id, category)
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return StreamingResponse(
        iter(lines),
        media_type="text/plain",
        headers={"Content-Disposition": f'attachment; filename="delta_proteins_{category}.fasta"'},
    )


@router.post("/evaluation-sets/{eval_id}/run", summary="Queue a run_cafa_evaluation job")
def run_cafa_evaluation(
    eval_id: UUID,
    body: dict[str, Any],
    factory: sessionmaker[Session] = Depends(get_session_factory),
    amqp_url: str = Depends(get_amqp_url),
    cfg: BenchmarkConfig = Depends(get_benchmark_config),
) -> dict[str, Any]:
    """Queue a job that runs the CAFA evaluator (NK / LK / PK) for a prediction set.

    Body must contain ``prediction_set_id`` (required) and optionally
    ``max_distance`` (float).
    """
    body = {**body, "evaluation_set_id": str(eval_id)}
    try:
        RunCafaEvaluationPayload.model_validate(body)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors()) from exc

    try:
        with session_scope(factory) as session:
            assert_evaluation_set_exists(session, eval_id)
            body = apply_baseline_scoring_default(session, body, cfg.baseline_scoring_name)
            job_id = enqueue_job(
                session,
                operation="run_cafa_evaluation",
                queue_name=EVALUATIONS_QUEUE,
                payload=body,
            )
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    publish_job(amqp_url, EVALUATIONS_QUEUE, job_id)
    return {"id": str(job_id), "status": "queued"}
