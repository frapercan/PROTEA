"""Tools sobre colas y workers — derivadas de la tabla Job (no hay worker registry)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import func, select

from protea.infrastructure.orm.models import Job
from protea.infrastructure.orm.models.job import JobStatus
from protea_mcp.db import get_session_factory, session_scope
from protea_mcp.server import mcp

_EMPTY_STATUSES = {s.value: 0 for s in JobStatus}


@mcp.tool()
def queue_status(queue_name: Optional[str] = None) -> list[dict]:
    """Agregado por cola: counts de jobs queued/running/succeeded/failed/cancelled."""
    with session_scope(get_session_factory()) as s:
        q = select(Job.queue_name, Job.status, func.count(Job.id)).group_by(
            Job.queue_name, Job.status
        )
        if queue_name is not None:
            q = q.where(Job.queue_name == queue_name)
        agg: dict[str, dict[str, int]] = {}
        for qn, st, n in s.execute(q).all():
            d = agg.setdefault(qn, dict(_EMPTY_STATUSES))
            d[st.value] = n
        return [{"queue_name": qn, **counts} for qn, counts in sorted(agg.items())]


@mcp.tool()
def list_running_jobs(queue_name: Optional[str] = None, limit: int = 50) -> list[dict]:
    """Jobs actualmente RUNNING — proxy para 'qué están haciendo los workers ahora'."""
    with session_scope(get_session_factory()) as s:
        q = (
            select(Job)
            .where(Job.status == JobStatus.RUNNING)
            .order_by(Job.started_at.desc().nullslast())
            .limit(limit)
        )
        if queue_name is not None:
            q = q.where(Job.queue_name == queue_name)
        return [
            {
                "id": str(j.id),
                "operation": j.operation,
                "queue_name": j.queue_name,
                "started_at": j.started_at.isoformat() if j.started_at else None,
                "progress_current": j.progress_current,
                "progress_total": j.progress_total,
                "parent_job_id": str(j.parent_job_id) if j.parent_job_id else None,
            }
            for j in s.execute(q).scalars().all()
        ]


@mcp.tool()
def stalled_jobs(older_than_minutes: int = 30, queue_name: Optional[str] = None) -> list[dict]:
    """Jobs RUNNING cuyo started_at supera el umbral — candidatos para el reaper."""
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=older_than_minutes)
    with session_scope(get_session_factory()) as s:
        q = (
            select(Job)
            .where(Job.status == JobStatus.RUNNING, Job.started_at < cutoff)
            .order_by(Job.started_at.asc())
        )
        if queue_name is not None:
            q = q.where(Job.queue_name == queue_name)
        return [
            {
                "id": str(j.id),
                "operation": j.operation,
                "queue_name": j.queue_name,
                "started_at": j.started_at.isoformat() if j.started_at else None,
                "age_minutes": int(
                    (datetime.now(timezone.utc) - j.started_at).total_seconds() / 60
                )
                if j.started_at
                else None,
            }
            for j in s.execute(q).scalars().all()
        ]


@mcp.tool()
def recent_failures(limit: int = 20, queue_name: Optional[str] = None) -> list[dict]:
    """Últimos jobs FAILED — equivalente práctico a peek de DLQ."""
    with session_scope(get_session_factory()) as s:
        q = (
            select(Job)
            .where(Job.status == JobStatus.FAILED)
            .order_by(Job.finished_at.desc().nullslast())
            .limit(limit)
        )
        if queue_name is not None:
            q = q.where(Job.queue_name == queue_name)
        return [
            {
                "id": str(j.id),
                "operation": j.operation,
                "queue_name": j.queue_name,
                "finished_at": j.finished_at.isoformat() if j.finished_at else None,
                "error_code": j.error_code,
                "error_message": j.error_message,
            }
            for j in s.execute(q).scalars().all()
        ]
