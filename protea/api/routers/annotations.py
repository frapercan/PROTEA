from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import ValidationError
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker
from starlette.requests import Request

from protea.core.operations.load_goa_annotations import LoadGOAAnnotationsPayload
from protea.core.operations.load_ontology_snapshot import LoadOntologySnapshotPayload
from protea.core.operations.load_quickgo_annotations import LoadQuickGOAnnotationsPayload
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.annotation.protein_go_annotation import ProteinGOAnnotation
from protea.infrastructure.orm.models.job import Job, JobEvent
from protea.infrastructure.queue.publisher import publish_job
from protea.infrastructure.session import session_scope

router = APIRouter(prefix="/annotations", tags=["annotations"])

_JOBS_QUEUE = "protea.jobs"


def get_session_factory(request: Request) -> sessionmaker[Session]:
    factory = getattr(request.app.state, "session_factory", None)
    if factory is None:
        raise RuntimeError("app.state.session_factory is not set")
    return factory  # type: ignore[no-any-return]


def get_amqp_url(request: Request) -> str:
    url = getattr(request.app.state, "amqp_url", None)
    if url is None:
        raise RuntimeError("app.state.amqp_url is not set")
    return url  # type: ignore[no-any-return]


# ── Ontology Snapshots ────────────────────────────────────────────────────────

@router.get("/snapshots")
def list_snapshots(
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    with session_scope(factory) as session:
        count_sub = (
            session.query(
                GOTerm.ontology_snapshot_id,
                func.count(GOTerm.id).label("cnt"),
            )
            .group_by(GOTerm.ontology_snapshot_id)
            .subquery()
        )
        rows = (
            session.query(OntologySnapshot, count_sub.c.cnt)
            .outerjoin(count_sub, OntologySnapshot.id == count_sub.c.ontology_snapshot_id)
            .order_by(OntologySnapshot.loaded_at.desc())
            .all()
        )
        return [
            {
                "id": str(s.id),
                "obo_url": s.obo_url,
                "obo_version": s.obo_version,
                "loaded_at": s.loaded_at.isoformat(),
                "go_term_count": cnt or 0,
            }
            for s, cnt in rows
        ]


@router.get("/snapshots/{snapshot_id}")
def get_snapshot(
    snapshot_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    with session_scope(factory) as session:
        s = session.get(OntologySnapshot, snapshot_id)
        if s is None:
            raise HTTPException(status_code=404, detail="OntologySnapshot not found")

        term_count = (
            session.query(func.count(GOTerm.id))
            .filter(GOTerm.ontology_snapshot_id == snapshot_id)
            .scalar()
        )

        return {
            "id": str(s.id),
            "obo_url": s.obo_url,
            "obo_version": s.obo_version,
            "loaded_at": s.loaded_at.isoformat(),
            "go_term_count": term_count,
        }


@router.post("/snapshots/load")
def load_ontology_snapshot(
    body: dict[str, Any],
    factory: sessionmaker[Session] = Depends(get_session_factory),
    amqp_url: str = Depends(get_amqp_url),
) -> dict[str, Any]:
    try:
        LoadOntologySnapshotPayload.model_validate(body)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors()) from exc

    with session_scope(factory) as session:
        job = Job(operation="load_ontology_snapshot", queue_name=_JOBS_QUEUE, payload=body)
        session.add(job)
        session.flush()
        job_id = job.id
        session.add(JobEvent(
            job_id=job_id,
            event="job.created",
            fields={"operation": "load_ontology_snapshot", "queue": _JOBS_QUEUE},
        ))

    publish_job(amqp_url, _JOBS_QUEUE, job_id)
    return {"id": str(job_id), "status": "queued"}


# ── Annotation Sets ───────────────────────────────────────────────────────────

@router.get("/sets")
def list_annotation_sets(
    source: str | None = Query(default=None),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    with session_scope(factory) as session:
        count_sub = (
            session.query(
                ProteinGOAnnotation.annotation_set_id,
                func.count(ProteinGOAnnotation.id).label("cnt"),
            )
            .group_by(ProteinGOAnnotation.annotation_set_id)
            .subquery()
        )
        q = session.query(AnnotationSet, count_sub.c.cnt).outerjoin(
            count_sub, AnnotationSet.id == count_sub.c.annotation_set_id
        )
        if source is not None:
            q = q.filter(AnnotationSet.source == source)
        rows = q.order_by(AnnotationSet.created_at.desc()).all()
        return [
            {
                "id": str(a.id),
                "source": a.source,
                "source_version": a.source_version,
                "ontology_snapshot_id": str(a.ontology_snapshot_id),
                "job_id": str(a.job_id) if a.job_id else None,
                "created_at": a.created_at.isoformat(),
                "meta": a.meta,
                "annotation_count": cnt or 0,
            }
            for a, cnt in rows
        ]


@router.get("/sets/{set_id}")
def get_annotation_set(
    set_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    with session_scope(factory) as session:
        a = session.get(AnnotationSet, set_id)
        if a is None:
            raise HTTPException(status_code=404, detail="AnnotationSet not found")

        annotation_count = (
            session.query(func.count(ProteinGOAnnotation.id))
            .filter(ProteinGOAnnotation.annotation_set_id == set_id)
            .scalar()
        )

        return {
            "id": str(a.id),
            "source": a.source,
            "source_version": a.source_version,
            "ontology_snapshot_id": str(a.ontology_snapshot_id),
            "job_id": str(a.job_id) if a.job_id else None,
            "created_at": a.created_at.isoformat(),
            "meta": a.meta,
            "annotation_count": annotation_count,
        }


@router.delete("/sets/{set_id}")
def delete_annotation_set(
    set_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    with session_scope(factory) as session:
        a = session.get(AnnotationSet, set_id)
        if a is None:
            raise HTTPException(status_code=404, detail="AnnotationSet not found")
        annotation_count = (
            session.query(func.count(ProteinGOAnnotation.id))
            .filter(ProteinGOAnnotation.annotation_set_id == set_id)
            .scalar()
        )
        try:
            session.delete(a)
            session.flush()
        except IntegrityError:
            raise HTTPException(
                status_code=409,
                detail="This annotation set is referenced by one or more prediction sets. Delete those first.",
            )
        return {"deleted": str(set_id), "annotations_deleted": annotation_count}


@router.post("/sets/load-goa")
def load_goa_annotations(
    body: dict[str, Any],
    factory: sessionmaker[Session] = Depends(get_session_factory),
    amqp_url: str = Depends(get_amqp_url),
) -> dict[str, Any]:
    try:
        LoadGOAAnnotationsPayload.model_validate(body)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors()) from exc

    with session_scope(factory) as session:
        job = Job(operation="load_goa_annotations", queue_name=_JOBS_QUEUE, payload=body)
        session.add(job)
        session.flush()
        job_id = job.id
        session.add(JobEvent(
            job_id=job_id,
            event="job.created",
            fields={"operation": "load_goa_annotations", "queue": _JOBS_QUEUE},
        ))

    publish_job(amqp_url, _JOBS_QUEUE, job_id)
    return {"id": str(job_id), "status": "queued"}


@router.post("/sets/load-quickgo")
def load_quickgo_annotations(
    body: dict[str, Any],
    factory: sessionmaker[Session] = Depends(get_session_factory),
    amqp_url: str = Depends(get_amqp_url),
) -> dict[str, Any]:
    try:
        LoadQuickGOAnnotationsPayload.model_validate(body)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors()) from exc

    with session_scope(factory) as session:
        job = Job(operation="load_quickgo_annotations", queue_name=_JOBS_QUEUE, payload=body)
        session.add(job)
        session.flush()
        job_id = job.id
        session.add(JobEvent(
            job_id=job_id,
            event="job.created",
            fields={"operation": "load_quickgo_annotations", "queue": _JOBS_QUEUE},
        ))

    publish_job(amqp_url, _JOBS_QUEUE, job_id)
    return {"id": str(job_id), "status": "queued"}
