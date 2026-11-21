"""Evaluation-result endpoints: list / metrics TSV / artifacts ZIP / one artifact / delete."""

from __future__ import annotations

from pathlib import Path, PurePosixPath
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session, sessionmaker

from protea.api.deps import get_session_factory
from protea.api.roles import ROLE_OPERATOR, require_role
from protea.core.domain.aspect import ASPECT_CAFA_CODES
from protea.infrastructure.session import session_scope
from protea.services.annotations_service import (
    EntityNotFoundError,
    delete_eval_result_collect_keys,
    get_eval_result_artifact,
    get_eval_result_with_keys,
    iter_evaluation_artifact,
    iter_evaluation_artifacts_zip,
    list_evaluation_results_data,
    list_paired_comparisons_data,
    render_evaluation_metrics_tsv,
)

router = APIRouter()

#: How each kind of file an evaluation writes is served; anything else is bytes.
_ARTIFACT_MEDIA_TYPES = {
    ".tsv": "text/tab-separated-values",
    ".txt": "text/plain",
    ".parquet": "application/vnd.apache.parquet",
}


@router.get(
    "/evaluation-sets/{eval_id}/results/{result_id}/metrics.tsv",
    summary="Download evaluation metrics as TSV",
)
def download_evaluation_metrics(
    eval_id: UUID,
    result_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> StreamingResponse:
    """Stream the per-aspect metrics table for one evaluation result.

    Renders the cafaeval-style summary (Fmax, Smin, AUPRC) as TSV with one
    row per CAFA aspect (``BPO``/``MFO``/``CCO``). Returns ``404`` if the
    ``(eval_id, result_id)`` pair does not match.
    """
    try:
        with session_scope(factory) as session:
            result, _ = get_eval_result_with_keys(session, eval_id, result_id)
            return StreamingResponse(
                render_evaluation_metrics_tsv(result, ASPECT_CAFA_CODES),
                media_type="text/tab-separated-values",
                headers={"Content-Disposition": f'attachment; filename="metrics_{result_id}.tsv"'},
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
    """Stream all stored cafaeval artefacts for one evaluation result as a ZIP.

    Bundles the per-result outputs (raw predictions, CAFA scoring TSVs,
    plot images) that the artifact store keeps under the result's prefix.
    Returns ``404`` if the result is unknown or has no artefacts persisted.
    """
    try:
        with session_scope(factory) as session:
            _, keys = get_eval_result_with_keys(session, eval_id, result_id)
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    if not keys:
        raise HTTPException(status_code=404, detail="No artifacts found for this result")

    return StreamingResponse(
        iter_evaluation_artifacts_zip(keys, result_id),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="artifacts_{result_id}.zip"'},
    )


@router.get(
    "/evaluation-sets/{eval_id}/results/{result_id}/artifacts/{relpath:path}",
    summary="Download one file an evaluation result wrote",
)
def download_evaluation_artifact(
    eval_id: UUID,
    result_id: UUID,
    relpath: str,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> StreamingResponse:
    """Stream one file the result recorded, by its path under the result.

    ``relpath`` is the path the evaluator wrote, such as
    ``PK/evaluation_best_f_micro_w.tsv``, the file a cell's ``f_micro_w`` and
    ``cov_w`` are read from. When the result recorded the file's sha256, the
    ``X-Recorded-SHA256`` header carries it: the sha256 of the body is that
    value, or the stored bytes are not the ones the figures were read from.
    Returns ``404`` when the result does not belong to ``eval_id`` or recorded
    no file at ``relpath``.
    """
    try:
        with session_scope(factory) as session:
            key, digest = get_eval_result_artifact(session, eval_id, result_id, relpath)
    except EntityNotFoundError as exc:
        detail = (
            f"result {result_id} recorded no file {relpath!r}"
            if exc.entity == "EvaluationArtifact"
            else str(exc)
        )
        raise HTTPException(status_code=404, detail=detail) from exc

    name = PurePosixPath(relpath)
    headers = {"Content-Disposition": f'attachment; filename="{name.name}"'}
    if digest is not None:
        headers["X-Recorded-SHA256"] = digest
    return StreamingResponse(
        iter_evaluation_artifact(key),
        media_type=_ARTIFACT_MEDIA_TYPES.get(name.suffix, "application/octet-stream"),
        headers=headers,
    )


@router.get(
    "/evaluation-sets/{eval_id}/results",
    summary="List evaluation results for an evaluation set",
)
def list_evaluation_results(
    eval_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    """List every cafaeval result row attached to one evaluation set.

    Each row carries the prediction-set / scoring-config / reranker
    triple used to produce it plus the cached metrics summary, so the UI
    benchmark matrix can render without per-row drilldowns. Returns
    ``404`` if the evaluation set itself is missing.
    """
    try:
        with session_scope(factory) as session:
            return list_evaluation_results_data(session, eval_id)
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get(
    "/evaluation-sets/{eval_id}/paired-comparisons",
    summary="List the paired comparisons of an evaluation set's results",
)
def list_paired_comparisons(
    eval_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    """The latest whole-panel ``compare_paired_panels`` of each pair of the window's results.

    Each one names its job, the result under test and the baseline, the method
    axis it declared, its operating point and resampling, and per panel the
    difference, its interval, the interval's method, the verdict and the number
    of paired proteins. Returns ``404`` if the evaluation set is missing.
    """
    try:
        with session_scope(factory) as session:
            return list_paired_comparisons_data(session, eval_id)
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.delete(
    "/evaluation-sets/{eval_id}/results/{result_id}",
    summary="Delete an evaluation result",
    status_code=204,
    dependencies=[Depends(require_role(ROLE_OPERATOR))],
)
def delete_evaluation_result(
    eval_id: UUID,
    result_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> None:
    """Delete one evaluation result row plus its stored artefacts.

    Two-step: the ORM cascade clears the DB row first (collecting the
    artifact keys); the artifact store ``delete()`` is then issued for
    each key outside the session so a network failure here does not
    leave the DB inconsistent. Returns ``204`` on success, ``404`` if
    the result is unknown.
    """
    from protea.infrastructure.settings import load_settings
    from protea.infrastructure.storage import get_artifact_store

    try:
        with session_scope(factory) as session:
            keys = delete_eval_result_collect_keys(session, eval_id, result_id)
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    project_root = Path(__file__).resolve().parents[4]
    store = get_artifact_store(load_settings(project_root))
    for key in keys:
        store.delete(key)
