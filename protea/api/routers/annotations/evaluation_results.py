"""Evaluation-result endpoints: list / metrics TSV / artifacts ZIP / delete."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session, sessionmaker

from protea.api.deps import get_session_factory
from protea.core.domain.aspect import ASPECT_CAFA_CODES
from protea.infrastructure.session import session_scope
from protea.services.annotations_service import (
    EntityNotFoundError,
    delete_eval_result_collect_keys,
    get_eval_result_with_keys,
    iter_evaluation_artifacts_zip,
    list_evaluation_results_data,
    render_evaluation_metrics_tsv,
)

router = APIRouter()


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
