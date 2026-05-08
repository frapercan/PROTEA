from __future__ import annotations

import io
import zipfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from protea.api.cache import cached
from protea.api.deps import get_amqp_url, get_benchmark_config, get_session_factory
from protea.core.domain.aspect import ASPECT_CAFA_CODES
from protea.core.operations.generate_evaluation_set import GenerateEvaluationSetPayload
from protea.core.operations.load_goa_annotations import LoadGOAAnnotationsPayload
from protea.core.operations.load_ontology_snapshot import LoadOntologySnapshotPayload
from protea.core.operations.load_quickgo_annotations import LoadQuickGOAnnotationsPayload
from protea.core.operations.run_cafa_evaluation import RunCafaEvaluationPayload
from protea.infrastructure.benchmark_config import BenchmarkConfig
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig
from protea.infrastructure.queue.publisher import publish_job
from protea.infrastructure.session import session_scope
from protea.services.annotations_service import (
    AnnotationSetReferencedError,
    EntityNotFoundError,
    delete_annotation_set_data,
    delete_eval_result_collect_keys,
    delete_evaluation_set_collect_keys,
    get_annotation_set_data,
    get_eval_result_with_keys,
    get_evaluation_set_data,
    get_go_subgraph_data,
    get_snapshot_data,
    iter_delta_proteins_fasta,
    iter_groundtruth_tsv,
    list_annotation_sets_data,
    list_evaluation_results_data,
    list_evaluation_sets_data,
    list_snapshots_data,
    render_evaluation_metrics_tsv,
)
from protea.services.annotations_service import (
    set_snapshot_ia_url as _set_snapshot_ia_url_service,
)
from protea.services.jobs_service import enqueue_job

router = APIRouter(prefix="/annotations", tags=["annotations"])

_JOBS_QUEUE = "protea.jobs"
_EVALUATIONS_QUEUE = "protea.evaluations"


# ── Ontology Snapshots ────────────────────────────────────────────────────────


@router.get("/snapshots", summary="List ontology snapshots")
def list_snapshots(
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    """List all loaded GO ontology snapshots with their GO term counts, newest first.

    Cached 5 minutes — the GROUP BY over go_term (N million rows per snapshot)
    takes multiple seconds, and snapshots are effectively immutable once loaded.
    """

    def _compute() -> list[dict[str, Any]]:
        with session_scope(factory) as session:
            return list_snapshots_data(session)

    return cached("annotations:snapshots", 300.0, _compute)


@router.get("/snapshots/{snapshot_id}", summary="Get ontology snapshot details")
def get_snapshot(
    snapshot_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Retrieve a single ontology snapshot with its GO term count."""
    try:
        with session_scope(factory) as session:
            return get_snapshot_data(session, snapshot_id)
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.patch("/snapshots/{snapshot_id}/ia-url", summary="Set IA URL on an ontology snapshot")
def set_snapshot_ia_url(
    snapshot_id: UUID,
    body: dict[str, Any],
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Associate an Information Accretion (IA) file URL with an existing ontology snapshot.

    The IA file contains per-term information-content weights (two columns:
    ``go_id``, ``ia_value``) and is published alongside each CAFA benchmark
    (e.g. ``IA_cafa6.tsv``).  Once set, ``run_cafa_evaluation`` picks it up
    automatically for every evaluation that uses this snapshot — no need to
    pass ``ia_file`` in the job payload.

    Pass ``{"ia_url": null}`` to clear the association (evaluations will fall
    back to uniform IC=1).

    This endpoint only touches ``ia_url``; the OBO file and GO term data are
    not affected.
    """
    if "ia_url" not in body:
        raise HTTPException(
            status_code=422, detail="Body must contain 'ia_url' key (string or null)"
        )
    try:
        with session_scope(factory) as session:
            return _set_snapshot_ia_url_service(session, snapshot_id, body.get("ia_url"))
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/snapshots/load", summary="Trigger ontology snapshot load")
def load_ontology_snapshot(
    body: dict[str, Any],
    factory: sessionmaker[Session] = Depends(get_session_factory),
    amqp_url: str = Depends(get_amqp_url),
) -> dict[str, Any]:
    """Queue a `load_ontology_snapshot` job that downloads and parses a GO OBO file.

    The job is idempotent by `obo_version`: if the snapshot already exists with relationships
    it will be skipped; if relationships are missing they will be backfilled.
    """
    try:
        LoadOntologySnapshotPayload.model_validate(body)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors()) from exc

    with session_scope(factory) as session:
        job_id = enqueue_job(
            session,
            operation="load_ontology_snapshot",
            queue_name=_JOBS_QUEUE,
            payload=body,
        )

    publish_job(amqp_url, _JOBS_QUEUE, job_id)
    return {"id": str(job_id), "status": "queued"}


# ── Annotation Sets ───────────────────────────────────────────────────────────


@router.get("/sets", summary="List annotation sets")
def list_annotation_sets(
    source: str | None = Query(default=None, description="Filter by source: `goa` or `quickgo`."),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    """List annotation sets with their annotation counts, newest first. Optionally filter by source.

    Cached 5 minutes — GROUP BY over protein_go_annotation (80M rows) takes
    6+ seconds. Per-source views are cached independently.
    """

    def _compute() -> list[dict[str, Any]]:
        with session_scope(factory) as session:
            return list_annotation_sets_data(session, source)

    return cached(f"annotations:sets:{source or '*'}", 300.0, _compute)


@router.get("/sets/{set_id}", summary="Get annotation set details")
def get_annotation_set(
    set_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Retrieve a single annotation set with its total annotation count."""
    try:
        with session_scope(factory) as session:
            return get_annotation_set_data(session, set_id)
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.delete("/sets/{set_id}", summary="Delete an annotation set")
def delete_annotation_set(
    set_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Delete an annotation set and all its annotations. Returns 409 if referenced by a prediction set."""
    try:
        with session_scope(factory) as session:
            return delete_annotation_set_data(session, set_id)
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except AnnotationSetReferencedError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/sets/load-goa", summary="Trigger GOA annotation load")
def load_goa_annotations(
    body: dict[str, Any],
    factory: sessionmaker[Session] = Depends(get_session_factory),
    amqp_url: str = Depends(get_amqp_url),
) -> dict[str, Any]:
    """Queue a `load_goa_annotations` job that streams a GAF file (gzip or plain) and upserts
    GO annotations into an AnnotationSet. Only proteins already in the DB are annotated."""
    try:
        LoadGOAAnnotationsPayload.model_validate(body)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors()) from exc

    with session_scope(factory) as session:
        job_id = enqueue_job(
            session,
            operation="load_goa_annotations",
            queue_name=_JOBS_QUEUE,
            payload=body,
        )

    publish_job(amqp_url, _JOBS_QUEUE, job_id)
    return {"id": str(job_id), "status": "queued"}


@router.post("/sets/load-quickgo", summary="Trigger QuickGO annotation load")
def load_quickgo_annotations(
    body: dict[str, Any],
    factory: sessionmaker[Session] = Depends(get_session_factory),
    amqp_url: str = Depends(get_amqp_url),
) -> dict[str, Any]:
    """Queue a `load_quickgo_annotations` job that streams GO annotations from the QuickGO
    bulk download API with optional taxon, aspect, and evidence code filtering."""
    try:
        LoadQuickGOAnnotationsPayload.model_validate(body)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors()) from exc

    with session_scope(factory) as session:
        job_id = enqueue_job(
            session,
            operation="load_quickgo_annotations",
            queue_name=_JOBS_QUEUE,
            payload=body,
        )

    publish_job(amqp_url, _JOBS_QUEUE, job_id)
    return {"id": str(job_id), "status": "queued"}


# ── CAFA Evaluation Sets ──────────────────────────────────────────────────────


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
        GenerateEvaluationSetPayload.model_validate(body)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors()) from exc

    with session_scope(factory) as session:
        job_id = enqueue_job(
            session,
            operation="generate_evaluation_set",
            queue_name=_JOBS_QUEUE,
            payload=body,
        )

    publish_job(amqp_url, _JOBS_QUEUE, job_id)
    return {"id": str(job_id), "status": "queued"}


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

    project_root = Path(__file__).resolve().parents[3]
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


def _eval_set_or_404(session: Session, eval_id: UUID) -> EvaluationSet:
    e = session.get(EvaluationSet, eval_id)
    if e is None:
        raise HTTPException(status_code=404, detail="EvaluationSet not found")
    return e


def _stream_groundtruth(
    factory: sessionmaker[Session],
    eval_id: UUID,
    category: str,
    filename: str,
) -> StreamingResponse:
    """Wrap :func:`iter_groundtruth_tsv` in a TSV streaming response.

    Translates ``EntityNotFoundError`` to HTTP 404 at the boundary;
    same shape used by the four GT/known-terms download endpoints.
    """
    try:
        with session_scope(factory) as session:
            lines = iter_groundtruth_tsv(session, eval_id, category)
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return StreamingResponse(
        iter(lines),
        media_type="text/tab-separated-values",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get(
    "/evaluation-sets/{eval_id}/ground-truth-NK.tsv",
    response_class=StreamingResponse,
    summary="Download NK ground truth (CAFA format)",
)
def download_gt_nk(
    eval_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> StreamingResponse:
    """Download No-Knowledge ground truth: delta proteins with zero prior experimental annotations.
    Format: ``protein_accession\\tgo_id`` (no header, 2 columns).
    """
    return _stream_groundtruth(factory, eval_id, "nk", "ground_truth_NK.tsv")


@router.get(
    "/evaluation-sets/{eval_id}/ground-truth-LK.tsv",
    response_class=StreamingResponse,
    summary="Download LK ground truth (CAFA format)",
)
def download_gt_lk(
    eval_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> StreamingResponse:
    """Download Limited-Knowledge ground truth: delta proteins with prior experimental annotations.
    Format: ``protein_accession\\tgo_id`` (no header, 2 columns).
    """
    return _stream_groundtruth(factory, eval_id, "lk", "ground_truth_LK.tsv")


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
    namespace where they already had experimental annotations at t0.
    Use together with ``known-terms.tsv`` passed as ``-known`` to the CAFA evaluator.
    Format: ``protein_accession\\tgo_id`` (no header, 2 columns).
    """
    return _stream_groundtruth(factory, eval_id, "pk", "ground_truth_PK.tsv")


@router.get(
    "/evaluation-sets/{eval_id}/known-terms.tsv",
    response_class=StreamingResponse,
    summary="Download known-terms from OLD annotation set (for CAFA PK evaluation)",
)
def download_known_terms(
    eval_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> StreamingResponse:
    """Download ALL experimental annotations from the OLD set (not delta-filtered).
    Format: ``protein_accession\\tgo_id`` (no header, 2 columns).
    Pass this as ``-known`` to the CAFA evaluator to enable PK scoring.
    """
    return _stream_groundtruth(factory, eval_id, "known", "known_terms.tsv")


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

    Only proteins whose sequence is already stored in the database are included.
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


# ── CAFA Evaluation Results ───────────────────────────────────────────────────


@router.post(
    "/evaluation-sets/{eval_id}/run",
    summary="Queue a run_cafa_evaluation job",
)
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

    with session_scope(factory) as session:
        if session.get(EvaluationSet, eval_id) is None:
            raise HTTPException(status_code=404, detail="EvaluationSet not found")
        # Auto-apply baseline scoring_config so every eval_result lands in the
        # benchmark matrix. Without this, unclassified rows (scoring_config_id
        # and reranker_model_id both NULL) are filtered out by _stage_of().
        if not body.get("scoring_config_id") and not body.get("reranker_model_id") \
                and not body.get("rerankers") and cfg.baseline_scoring_name:
            baseline = session.execute(
                select(ScoringConfig).where(ScoringConfig.name == cfg.baseline_scoring_name)
            ).scalar_one_or_none()
            if baseline is not None:
                body = {**body, "scoring_config_id": str(baseline.id)}
        job_id = enqueue_job(
            session,
            operation="run_cafa_evaluation",
            queue_name=_EVALUATIONS_QUEUE,
            payload=body,
        )

    publish_job(amqp_url, _EVALUATIONS_QUEUE, job_id)
    return {"id": str(job_id), "status": "queued"}


@router.get(
    "/evaluation-sets/{eval_id}/results/{result_id}/metrics.tsv",
    summary="Download evaluation metrics as TSV",
)
def download_evaluation_metrics(
    eval_id: UUID,
    result_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> StreamingResponse:
    try:
        with session_scope(factory) as session:
            result, _ = get_eval_result_with_keys(session, eval_id, result_id)
            return StreamingResponse(
                render_evaluation_metrics_tsv(result, ASPECT_CAFA_CODES),
                media_type="text/tab-separated-values",
                headers={
                    "Content-Disposition": f'attachment; filename="metrics_{result_id}.tsv"'
                },
            )
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get(
    "/evaluation-sets/{eval_id}/results/{result_id}/artifacts.zip",
    summary="Download all cafaeval artifacts for an evaluation result as a zip",
)
def download_evaluation_artifacts(
    eval_id: UUID,
    result_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> StreamingResponse:
    try:
        with session_scope(factory) as session:
            _, keys = get_eval_result_with_keys(session, eval_id, result_id)
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    if not keys:
        raise HTTPException(status_code=404, detail="No artifacts found for this result")

    from protea.infrastructure.settings import load_settings
    from protea.infrastructure.storage import get_artifact_store

    project_root = Path(__file__).resolve().parents[3]
    store = get_artifact_store(load_settings(project_root))
    prefix = f"eval_artifacts/{result_id}/"

    def _zip_stream() -> Iterator[bytes]:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            for key in sorted(keys):
                rel = key[len(prefix):] if key.startswith(prefix) else key
                zf.writestr(rel, store.get(key))
        yield buf.getvalue()

    return StreamingResponse(
        _zip_stream(),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="artifacts_{result_id}.zip"'},
    )


@router.get(
    "/evaluation-sets/{eval_id}/results",
    summary="List evaluation results for an evaluation set",
)
def list_evaluation_results(
    eval_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    try:
        with session_scope(factory) as session:
            return list_evaluation_results_data(session, eval_id)
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.delete(
    "/evaluation-sets/{eval_id}/results/{result_id}",
    summary="Delete an evaluation result",
    status_code=204,
)
def delete_evaluation_result(
    eval_id: UUID,
    result_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> None:
    from protea.infrastructure.settings import load_settings
    from protea.infrastructure.storage import get_artifact_store

    try:
        with session_scope(factory) as session:
            keys = delete_eval_result_collect_keys(session, eval_id, result_id)
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    project_root = Path(__file__).resolve().parents[3]
    store = get_artifact_store(load_settings(project_root))
    for key in keys:
        store.delete(key)


# ── GO subgraph ───────────────────────────────────────────────────────────────


@router.get("/snapshots/{snapshot_id}/subgraph")
def get_go_subgraph(
    snapshot_id: UUID,
    go_ids: str = Query(..., description="Comma-separated GO IDs, e.g. GO:0003674,GO:0008150"),
    depth: int = Query(default=3, ge=1, le=6),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Return a subgraph of the GO DAG containing the requested terms and their ancestors up to ``depth`` levels."""
    try:
        with session_scope(factory) as session:
            return get_go_subgraph_data(session, snapshot_id, go_ids, depth)
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
