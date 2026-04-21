"""Tools sobre jobs individuales (cada Job row = una operación)."""

from __future__ import annotations

import asyncio
from typing import Optional
from uuid import UUID

from sqlalchemy import select

from protea.infrastructure.orm.models import Job, JobEvent
from protea.infrastructure.orm.models.job import JobStatus
from protea_mcp.db import get_session_factory, session_scope
from protea_mcp.server import mcp


def _job_to_dict(j: Job) -> dict:
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
        "parent_job_id": str(j.parent_job_id) if j.parent_job_id else None,
        "error_code": j.error_code,
        "error_message": j.error_message,
    }


@mcp.tool()
def get_job(job_id: UUID) -> dict:
    """Detalle completo de un job (equivalente a GET /jobs/{id})."""
    with session_scope(get_session_factory()) as s:
        j = s.get(Job, job_id)
        return _job_to_dict(j) if j else {"error": "not found", "id": str(job_id)}


@mcp.tool()
def list_recent_jobs(
    operation: Optional[str] = None,
    status: Optional[str] = None,
    include_children: bool = False,
    parent_job_id: Optional[UUID] = None,
    limit: int = 50,
) -> list[dict]:
    """Jobs recientes filtrables — replica la lógica de GET /jobs."""
    with session_scope(get_session_factory()) as s:
        q = select(Job)
        if parent_job_id is not None:
            q = q.where(Job.parent_job_id == parent_job_id)
        elif not include_children:
            q = q.where(Job.parent_job_id.is_(None))
        if status is not None:
            q = q.where(Job.status == JobStatus(status))
        if operation is not None:
            q = q.where(Job.operation == operation)
        q = q.order_by(Job.created_at.desc()).limit(limit)
        return [_job_to_dict(j) for j in s.execute(q).scalars().all()]


@mcp.tool()
def job_events(job_id: UUID, limit: int = 200) -> list[dict]:
    """Log de eventos estructurados de un job (el `emit` de la operación)."""
    with session_scope(get_session_factory()) as s:
        rows = s.execute(
            select(JobEvent)
            .where(JobEvent.job_id == job_id)
            .order_by(JobEvent.ts.desc())
            .limit(limit)
        ).scalars().all()
        return [
            {
                "id": e.id,
                "ts": e.ts.isoformat(),
                "level": e.level,
                "event": e.event,
                "message": e.message,
                "fields": e.fields,
            }
            for e in rows
        ]


@mcp.tool()
async def watch_job(job_id: UUID, poll_interval_seconds: float = 2.0, timeout_seconds: int = 600) -> dict:
    """Poll del estado de un job hasta que termine o se agote el timeout."""
    terminal = {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED}
    elapsed = 0.0
    while elapsed < timeout_seconds:
        with session_scope(get_session_factory()) as s:
            j = s.get(Job, job_id)
            if j is None:
                return {"error": "not found", "id": str(job_id)}
            if j.status in terminal:
                return _job_to_dict(j)
        await asyncio.sleep(poll_interval_seconds)
        elapsed += poll_interval_seconds
    return {"error": "timeout", "id": str(job_id), "elapsed": elapsed}
