"""Ontology-snapshot endpoints: list / get / IA-URL / load / GO subgraph."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session, sessionmaker

from protea.api.cache import cached, invalidate
from protea.api.deps import get_amqp_url, get_session_factory
from protea.api.roles import ROLE_OPERATOR, require_role
from protea.core.operations.load_ontology_snapshot import LoadOntologySnapshotPayload
from protea.infrastructure.session import session_scope
from protea.services.annotations_service import (
    EntityNotFoundError,
    get_go_subgraph_data,
    get_snapshot_data,
    list_snapshots_data,
)
from protea.services.annotations_service import (
    set_snapshot_ia_url as _set_snapshot_ia_url_service,
)
from protea.services.jobs_service import (
    InvalidJobPayloadError,
    dispatch_validated_job,
)

from ._common import JOBS_QUEUE

router = APIRouter()

SNAPSHOTS_CACHE_KEY = "annotations:snapshots"
SNAPSHOTS_TTL_SECONDS = 300.0


def _compute_snapshots(
    factory: sessionmaker[Session],
) -> list[dict[str, Any]]:
    with session_scope(factory) as session:
        return list_snapshots_data(session)


@router.get("/snapshots", summary="List ontology snapshots")
def list_snapshots(
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    """List all loaded GO ontology snapshots with their GO term counts, newest first.

    Cached 5 minutes: the GROUP BY over go_term (N million rows per snapshot)
    takes multiple seconds, and snapshots are effectively immutable once loaded.
    Serves the last-known payload on producer failure so a transient DB blip
    does not surface as a 500.
    """
    return cached(
        SNAPSHOTS_CACHE_KEY,
        SNAPSHOTS_TTL_SECONDS,
        lambda: _compute_snapshots(factory),
        serve_stale_on_error=True,
    )


def prewarm_snapshots(
    factory: sessionmaker[Session],
) -> list[dict[str, Any]]:
    """Recompute and store ``annotations:snapshots``; used by the app startup
    hook and the background refresh loop."""
    invalidate(SNAPSHOTS_CACHE_KEY)
    return cached(
        SNAPSHOTS_CACHE_KEY,
        SNAPSHOTS_TTL_SECONDS,
        lambda: _compute_snapshots(factory),
    )


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


@router.patch(
    "/snapshots/{snapshot_id}/ia-url",
    summary="Set IA URL on an ontology snapshot",
    dependencies=[Depends(require_role(ROLE_OPERATOR))],
)
def set_snapshot_ia_url(
    snapshot_id: UUID,
    body: dict[str, Any],
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Associate an Information Accretion (IA) file URL with an existing ontology snapshot.

    The IA file contains per-term information-content weights (two columns:
    ``go_id``, ``ia_value``) and is published alongside each CAFA benchmark
    (e.g. ``IA_cafa6.tsv``).  Once set, ``run_cafa_evaluation`` picks it up
    automatically for every evaluation that uses this snapshot.

    Pass ``{"ia_url": null}`` to clear the association (evaluations will fall
    back to uniform IC=1).
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


@router.post(
    "/snapshots/load",
    summary="Trigger ontology snapshot load",
    dependencies=[Depends(require_role(ROLE_OPERATOR))],
)
def load_ontology_snapshot(
    body: dict[str, Any],
    factory: sessionmaker[Session] = Depends(get_session_factory),
    amqp_url: str = Depends(get_amqp_url),
) -> dict[str, Any]:
    """Queue a `load_ontology_snapshot` job that downloads and parses a GO OBO file.

    Idempotent by ``obo_version``: existing snapshots are skipped or backfilled.
    """
    try:
        return dispatch_validated_job(
            factory,
            amqp_url,
            body,
            LoadOntologySnapshotPayload,
            operation="load_ontology_snapshot",
            queue_name=JOBS_QUEUE,
        )
    except InvalidJobPayloadError as exc:
        raise HTTPException(status_code=422, detail=exc.errors) from exc


@router.get("/snapshots/{snapshot_id}/subgraph")
def get_go_subgraph(
    snapshot_id: UUID,
    go_ids: str = Query(..., description="Comma-separated GO IDs, e.g. GO:0003674,GO:0008150"),
    depth: int = Query(
        default=3,
        ge=1,
        le=6,
        description=(
            "Number of ancestor hops to walk up the GO DAG from each "
            "seed term; capped at 6 to keep the response bounded."
        ),
    ),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Return a subgraph of the GO DAG containing the requested terms and their ancestors up to ``depth`` levels."""
    try:
        with session_scope(factory) as session:
            return get_go_subgraph_data(session, snapshot_id, go_ids, depth)
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
