"""Annotation-set endpoints: list / get / delete / load (GOA + QuickGO)."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session, sessionmaker

from protea.api.cache import cached, invalidate
from protea.api.deps import get_amqp_url, get_session_factory
from protea.api.roles import ROLE_OPERATOR, require_role
from protea.core.operations.load_goa_annotations import LoadGOAAnnotationsPayload
from protea.core.operations.load_quickgo_annotations import LoadQuickGOAnnotationsPayload
from protea.infrastructure.session import session_scope
from protea.services.annotations_service import (
    AnnotationSetReferencedError,
    EntityNotFoundError,
    delete_annotation_set_data,
    get_annotation_set_data,
    list_annotation_sets_data,
)
from protea.services.jobs_service import (
    InvalidJobPayloadError,
    dispatch_validated_job,
)

from ._common import JOBS_QUEUE

router = APIRouter()

ANNOTATION_SETS_TTL_SECONDS = 300.0


def _annotation_sets_cache_key(source: str | None) -> str:
    return f"annotations:sets:{source or '*'}"


def _compute_annotation_sets(
    factory: sessionmaker[Session],
    source: str | None,
) -> list[dict[str, Any]]:
    with session_scope(factory) as session:
        return list_annotation_sets_data(session, source)


@router.get("/sets", summary="List annotation sets")
def list_annotation_sets(
    source: str | None = Query(default=None, description="Filter by source: `goa` or `quickgo`."),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    """List annotation sets with their annotation counts, newest first.

    Cached 5 minutes: GROUP BY over protein_go_annotation (80M rows) takes
    6+ seconds. Per-source views are cached independently. Serves the
    last-known payload on producer failure so a transient DB blip does
    not surface as a 500.
    """
    return cached(
        _annotation_sets_cache_key(source),
        ANNOTATION_SETS_TTL_SECONDS,
        lambda: _compute_annotation_sets(factory, source),
        serve_stale_on_error=True,
    )


def prewarm_annotation_sets(
    factory: sessionmaker[Session],
) -> list[dict[str, Any]]:
    """Recompute and store the unfiltered ``annotations:sets:*`` view; used by
    the app startup hook and the background refresh loop. Only the unfiltered
    variant is prewarmed because the per-source views are computed on demand
    and rarely hit cold."""
    key = _annotation_sets_cache_key(None)
    invalidate(key)
    return cached(
        key,
        ANNOTATION_SETS_TTL_SECONDS,
        lambda: _compute_annotation_sets(factory, None),
    )


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


@router.delete("/sets/{set_id}", summary="Delete an annotation set", dependencies=[Depends(require_role(ROLE_OPERATOR))])
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


@router.post("/sets/load-goa", summary="Trigger GOA annotation load", dependencies=[Depends(require_role(ROLE_OPERATOR))])
def load_goa_annotations(
    body: dict[str, Any],
    factory: sessionmaker[Session] = Depends(get_session_factory),
    amqp_url: str = Depends(get_amqp_url),
) -> dict[str, Any]:
    """Queue a `load_goa_annotations` job that streams a GAF file (gzip or plain) and upserts
    GO annotations into an AnnotationSet. Only proteins already in the DB are annotated."""
    try:
        return dispatch_validated_job(
            factory,
            amqp_url,
            body,
            LoadGOAAnnotationsPayload,
            operation="load_goa_annotations",
            queue_name=JOBS_QUEUE,
        )
    except InvalidJobPayloadError as exc:
        raise HTTPException(status_code=422, detail=exc.errors) from exc


@router.post("/sets/load-quickgo", summary="Trigger QuickGO annotation load", dependencies=[Depends(require_role(ROLE_OPERATOR))])
def load_quickgo_annotations(
    body: dict[str, Any],
    factory: sessionmaker[Session] = Depends(get_session_factory),
    amqp_url: str = Depends(get_amqp_url),
) -> dict[str, Any]:
    """Queue a `load_quickgo_annotations` job that streams GO annotations from the QuickGO
    bulk download API with optional taxon, aspect, and evidence code filtering."""
    try:
        return dispatch_validated_job(
            factory,
            amqp_url,
            body,
            LoadQuickGOAnnotationsPayload,
            operation="load_quickgo_annotations",
            queue_name=JOBS_QUEUE,
        )
    except InvalidJobPayloadError as exc:
        raise HTTPException(status_code=422, detail=exc.errors) from exc
