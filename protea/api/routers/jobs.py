# protea/api/routers/jobs.py
from __future__ import annotations

from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session, sessionmaker
from starlette.requests import Request

from protea.infrastructure.orm.models.job import Job, JobEvent, JobStatus
from protea.infrastructure.queue.publisher import publish_job
from protea.infrastructure.session import session_scope

router = APIRouter(prefix="/jobs", tags=["jobs"])


# --- Dependency hook (wire this in your app factory) ---

def get_session_factory(request: Request) -> sessionmaker[Session]:
    factory = getattr(request.app.state, "session_factory", None)
    if factory is None:
        raise RuntimeError("app.state.session_factory is not set")
    return factory


def get_amqp_url(request: Request) -> str:
    url = getattr(request.app.state, "amqp_url", None)
    if url is None:
        raise RuntimeError("app.state.amqp_url is not set")
    return url


@router.post("")
def create_job(
    body: Dict[str, Any],
    factory: sessionmaker[Session] = Depends(get_session_factory),
    amqp_url: str = Depends(get_amqp_url),
) -> Dict[str, Any]:
    operation = body.get("operation")
    queue_name = body.get("queue_name")
    payload = body.get("payload") or {}
    meta = body.get("meta") or {}

    if not operation or not queue_name:
        raise HTTPException(status_code=400, detail="operation and queue_name are required")

    with session_scope(factory) as session:
        job = Job(operation=operation, queue_name=queue_name, payload=payload, meta=meta)
        session.add(job)
        session.flush()
        job_id = job.id
        session.add(JobEvent(job_id=job_id, event="job.created", fields={"operation": operation, "queue": queue_name}))

    # Publish after commit so the worker always finds the row.
    publish_job(amqp_url, queue_name, job_id)
    return {"id": str(job_id), "status": "queued"}


@router.get("")
def list_jobs(
    status: Optional[str] = Query(default=None),
    operation: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> List[Dict[str, Any]]:
    with session_scope(factory) as session:
        q = session.query(Job)

        if status is not None:
            try:
                st = JobStatus(status)
            except Exception:
                raise HTTPException(status_code=400, detail=f"Unknown status: {status}")
            q = q.filter(Job.status == st)

        if operation is not None:
            q = q.filter(Job.operation == operation)

        rows = q.order_by(Job.created_at.desc()).limit(limit).all()
        return [
            {
                "id": str(j.id),
                "operation": j.operation,
                "queue_name": j.queue_name,
                "status": j.status.value,
                "created_at": j.created_at.isoformat(),
                "started_at": j.started_at.isoformat() if j.started_at else None,
                "finished_at": j.finished_at.isoformat() if j.finished_at else None,
                "progress_current": j.progress_current,
                "progress_total": j.progress_total,
                "error_code": j.error_code,
                "error_message": j.error_message,
            }
            for j in rows
        ]


@router.get("/{job_id}")
def get_job(
    job_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> Dict[str, Any]:
    with session_scope(factory) as session:
        j = session.get(Job, job_id)
        if j is None:
            raise HTTPException(status_code=404, detail="Job not found")

        return {
            "id": str(j.id),
            "operation": j.operation,
            "queue_name": j.queue_name,
            "status": j.status.value,
            "payload": j.payload,
            "meta": j.meta,
            "created_at": j.created_at.isoformat(),
            "started_at": j.started_at.isoformat() if j.started_at else None,
            "finished_at": j.finished_at.isoformat() if j.finished_at else None,
            "progress_current": j.progress_current,
            "progress_total": j.progress_total,
            "error_code": j.error_code,
            "error_message": j.error_message,
        }


@router.get("/{job_id}/events")
def get_job_events(
    job_id: UUID,
    limit: int = Query(default=200, ge=1, le=2000),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> List[Dict[str, Any]]:
    with session_scope(factory) as session:
        # quick existence check
        j = session.get(Job, job_id)
        if j is None:
            raise HTTPException(status_code=404, detail="Job not found")

        events = (
            session.query(JobEvent)
            .filter(JobEvent.job_id == job_id)
            .order_by(JobEvent.ts.desc())
            .limit(limit)
            .all()
        )

        return [
            {
                "id": e.id,
                "ts": e.ts.isoformat(),
                "level": e.level,
                "event": e.event,
                "message": e.message,
                "fields": e.fields,
            }
            for e in events
        ]


@router.post("/{job_id}/cancel")
def cancel_job(
    job_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> Dict[str, Any]:
    with session_scope(factory) as session:
        j = session.get(Job, job_id)
        if j is None:
            raise HTTPException(status_code=404, detail="Job not found")

        if j.status in (JobStatus.SUCCEEDED, JobStatus.FAILED):
            return {"id": str(j.id), "status": j.status.value}

        j.status = JobStatus.CANCELLED
        session.add(JobEvent(job_id=job_id, event="job.cancelled", fields={}))
        return {"id": str(j.id), "status": j.status.value}
