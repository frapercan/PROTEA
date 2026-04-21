"""Tools de escritura. Todas con dry_run y audit trail."""

from __future__ import annotations

from typing import Optional
from uuid import UUID

from sqlalchemy import select

from protea.core.utils import utcnow
from protea.infrastructure.orm.models import Job, JobEvent
from protea.infrastructure.orm.models.job import JobStatus
from protea.infrastructure.queue.publisher import publish_job
from protea_mcp.audit import audited
from protea_mcp.db import get_session_factory, get_settings, session_scope
from protea_mcp.schemas import CancelRequest, EnqueueRequest
from protea_mcp.server import mcp


@mcp.tool()
def enqueue_operation(req: EnqueueRequest, session_id: Optional[str] = None) -> dict:
    """Encola una operación. dry_run=True (default) devuelve el plan sin ejecutar."""
    if req.dry_run:
        return {
            "dry_run": True,
            "would_enqueue": {
                "operation": req.operation,
                "queue_name": req.queue_name,
                "payload": req.payload,
                "meta": req.meta,
            },
        }
    with audited("enqueue_operation", req.model_dump(), session_id=session_id):
        with session_scope(get_session_factory()) as s:
            job = Job(
                operation=req.operation,
                queue_name=req.queue_name,
                payload=req.payload,
                meta=req.meta,
            )
            s.add(job)
            s.flush()
            job_id = job.id
            s.add(
                JobEvent(
                    job_id=job_id,
                    event="job.created",
                    fields={
                        "operation": req.operation,
                        "queue": req.queue_name,
                        "source": "mcp",
                    },
                )
            )
        publish_job(get_settings().amqp_url, req.queue_name, job_id)
    return {"id": str(job_id), "status": "queued"}


@mcp.tool()
def cancel_job(req: CancelRequest, session_id: Optional[str] = None) -> dict:
    """Cancela un job (y sus hijos no-terminales)."""
    with audited("cancel_job", req.model_dump(mode="json"), session_id=session_id):
        with session_scope(get_session_factory()) as s:
            j = s.get(Job, req.job_id)
            if j is None:
                return {"error": "not found", "id": str(req.job_id)}
            if j.status in (JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED):
                return {"id": str(j.id), "status": j.status.value, "noop": True}
            j.status = JobStatus.CANCELLED
            j.finished_at = utcnow()
            s.add(
                JobEvent(
                    job_id=j.id,
                    event="job.cancelled",
                    fields={"reason": req.reason, "source": "mcp"},
                )
            )
            children = s.execute(
                select(Job).where(
                    Job.parent_job_id == req.job_id,
                    Job.status.in_((JobStatus.QUEUED, JobStatus.RUNNING)),
                )
            ).scalars().all()
            for c in children:
                c.status = JobStatus.CANCELLED
                c.finished_at = utcnow()
                s.add(
                    JobEvent(
                        job_id=c.id,
                        event="job.cancelled",
                        fields={"reason": "parent_cancelled", "source": "mcp"},
                    )
                )
            return {
                "id": str(j.id),
                "status": j.status.value,
                "children_cancelled": len(children),
            }


@mcp.tool()
def retry_failed_jobs(
    queue_name: str,
    max_n: int = 10,
    dry_run: bool = True,
    session_id: Optional[str] = None,
) -> dict:
    """Re-encola jobs FAILED de una cola (crea nuevas filas Job, no muta las originales)."""
    with session_scope(get_session_factory()) as s:
        failed = s.execute(
            select(Job)
            .where(Job.queue_name == queue_name, Job.status == JobStatus.FAILED)
            .order_by(Job.finished_at.desc().nullslast())
            .limit(max_n)
        ).scalars().all()
        plan = [{"id": str(j.id), "operation": j.operation} for j in failed]
    if dry_run:
        return {"dry_run": True, "would_retry": plan}
    new_ids: list[str] = []
    with audited(
        "retry_failed_jobs",
        {"queue_name": queue_name, "max_n": max_n, "source_ids": [p["id"] for p in plan]},
        session_id=session_id,
    ):
        for src in plan:
            with session_scope(get_session_factory()) as s:
                original = s.get(Job, UUID(src["id"]))
                if original is None:
                    continue
                new_job = Job(
                    operation=original.operation,
                    queue_name=original.queue_name,
                    payload=original.payload,
                    meta={**(original.meta or {}), "retry_of": str(original.id)},
                )
                s.add(new_job)
                s.flush()
                new_id = new_job.id
                s.add(
                    JobEvent(
                        job_id=new_id,
                        event="job.created",
                        fields={"source": "mcp.retry", "retry_of": str(original.id)},
                    )
                )
            publish_job(get_settings().amqp_url, queue_name, new_id)
            new_ids.append(str(new_id))
    return {"retried": len(new_ids), "new_job_ids": new_ids}
