# protea/api/routers/jobs.py
from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field, field_validator
from sqlalchemy.orm import Session, sessionmaker
from starlette.requests import Request

from protea.infrastructure.orm.models.job import Job, JobEvent, JobStatus
from protea.infrastructure.queue.publisher import publish_job
from protea.infrastructure.session import session_scope

router = APIRouter(prefix="/jobs", tags=["jobs"])


class CreateJobRequest(BaseModel):
    operation: str = Field(..., min_length=1, description="Registered operation name, e.g. `insert_proteins`.")
    queue_name: str = Field(..., min_length=1, description="RabbitMQ queue to publish the job to, e.g. `protea.jobs`.")
    payload: dict[str, Any] = Field(default_factory=dict, description="Operation-specific configuration object.")
    meta: dict[str, Any] = Field(default_factory=dict, description="Optional free-form metadata stored alongside the job.")

    @field_validator("operation", "queue_name", mode="before")
    @classmethod
    def strip_and_require(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()


# --- Dependency hook (wire this in your app factory) ---

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


@router.post("", summary="Create and enqueue a job")
def create_job(
    body: CreateJobRequest,
    factory: sessionmaker[Session] = Depends(get_session_factory),
    amqp_url: str = Depends(get_amqp_url),
) -> dict[str, Any]:
    """Create a Job row and publish its ID to the specified RabbitMQ queue.

    The job transitions `QUEUED → RUNNING → SUCCEEDED/FAILED` as the worker processes it.
    Use `GET /jobs/{id}/events` to poll structured progress events in real time.
    """
    with session_scope(factory) as session:
        job = Job(
            operation=body.operation,
            queue_name=body.queue_name,
            payload=body.payload,
            meta=body.meta,
        )
        session.add(job)
        session.flush()
        job_id = job.id
        session.add(JobEvent(
            job_id=job_id,
            event="job.created",
            fields={"operation": body.operation, "queue": body.queue_name},
        ))

    # Publish after commit so the worker always finds the row.
    publish_job(amqp_url, body.queue_name, job_id)
    return {"id": str(job_id), "status": "queued"}


@router.get("", summary="List jobs")
def list_jobs(
    status: str | None = Query(default=None),
    operation: str | None = Query(default=None),
    include_children: bool = Query(default=False),
    parent_job_id: UUID | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    """List jobs with optional filtering.

    By default only top-level jobs (no parent) are returned. Set `include_children=true`
    or filter by `parent_job_id` to see batch sub-jobs from distributed pipelines.
    """
    with session_scope(factory) as session:
        q = session.query(Job)

        if parent_job_id is not None:
            q = q.filter(Job.parent_job_id == parent_job_id)
        elif not include_children:
            q = q.filter(Job.parent_job_id.is_(None))

        if status is not None:
            try:
                st = JobStatus(status)
            except Exception as exc:
                raise HTTPException(status_code=400, detail=f"Unknown status: {status}") from exc
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
                "parent_job_id": str(j.parent_job_id) if j.parent_job_id else None,
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


@router.get("/{job_id}", summary="Get job details")
def get_job(
    job_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Retrieve full details for a single job including its payload, meta, and progress counters."""
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


@router.get("/{job_id}/events", summary="List job events")
def get_job_events(
    job_id: UUID,
    limit: int = Query(default=200, ge=1, le=2000),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    """Return the structured event log for a job (newest first).

    Events include progress milestones, warnings, HTTP retries, and errors.
    Useful for monitoring long-running operations such as `compute_embeddings` or `predict_go_terms`.
    """
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


@router.delete("/{job_id}", summary="Delete a job")
def delete_job(
    job_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Permanently delete a job and its event log. Running jobs cannot be deleted (409)."""
    with session_scope(factory) as session:
        j = session.get(Job, job_id)
        if j is None:
            raise HTTPException(status_code=404, detail="Job not found")
        if j.status == JobStatus.RUNNING:
            raise HTTPException(status_code=409, detail="Cannot delete a running job")
        session.delete(j)
    return {"deleted": str(job_id)}


@router.post("/{job_id}/cancel", summary="Cancel a job")
def cancel_job(
    job_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Mark a job (and any queued child jobs) as CANCELLED.

    Already-finished jobs (SUCCEEDED/FAILED) are returned as-is with no state change.
    Note: workers processing a batch mid-flight will complete their current message before stopping.
    """
    with session_scope(factory) as session:
        j = session.get(Job, job_id)
        if j is None:
            raise HTTPException(status_code=404, detail="Job not found")

        if j.status in (JobStatus.SUCCEEDED, JobStatus.FAILED):
            return {"id": str(j.id), "status": j.status.value}

        j.status = JobStatus.CANCELLED
        session.add(JobEvent(job_id=job_id, event="job.cancelled", fields={}))

        # Cancel any queued children so they are not picked up by a worker.
        children = (
            session.query(Job)
            .filter(Job.parent_job_id == job_id, Job.status == JobStatus.QUEUED)
            .all()
        )
        for child in children:
            child.status = JobStatus.CANCELLED
            session.add(JobEvent(
                job_id=child.id,
                event="job.cancelled",
                fields={"reason": "parent_cancelled"},
            ))

        return {"id": str(j.id), "status": j.status.value, "children_cancelled": len(children)}
