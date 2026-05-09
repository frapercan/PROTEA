# protea/api/routers/jobs.py
from __future__ import annotations

from datetime import datetime
from typing import Any, NamedTuple
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field, field_validator
from sqlalchemy.orm import Session, sessionmaker

from protea.api.deps import get_amqp_url, get_operation_registry, get_session_factory
from protea.core.contracts.registry import OperationRegistry
from protea.core.utils import utcnow
from protea.infrastructure.orm.models.job import Job, JobComment, JobEvent, JobStatus
from protea.infrastructure.queue.publisher import publish_job
from protea.infrastructure.session import session_scope


class JobListFilters(NamedTuple):
    """Bundle of query-string filters consumed by ``GET /jobs``.

    Carries the user-visible knobs so the route handler signature stays
    under the §3 6-param ceiling. The FastAPI dep ``_job_list_filters_dep``
    exposes each field as a discrete query parameter on the wire.

    ``after`` is the cursor token for pagination (T4.2): when set, the
    list only returns rows strictly older than the given UTC timestamp.
    Clients page forward by reading the ``created_at`` of the last row
    and feeding it back as ``after``. Microsecond resolution on
    ``Job.created_at`` keeps tie collisions astronomically rare.
    """

    status: str | None
    operation: str | None
    include_children: bool
    parent_job_id: UUID | None
    limit: int
    after: datetime | None


def _job_list_filters_dep(
    status: str | None = Query(
        default=None,
        description=(
            "Filter by ``JobStatus`` enum value (``queued``, "
            "``running``, ``succeeded``, ``failed``, ``cancelled``). "
            "Unknown values return 400."
        ),
    ),
    operation: str | None = Query(
        default=None,
        description=(
            "Filter by registered operation name "
            "(e.g. ``compute_embeddings``, ``predict_go_terms``)."
        ),
    ),
    include_children: bool = Query(
        default=False,
        description=(
            "When true, include batch sub-jobs (rows with a "
            "non-null ``parent_job_id``). Default false keeps the "
            "list focused on top-level work."
        ),
    ),
    parent_job_id: UUID | None = Query(
        default=None,
        description=(
            "Filter to sub-jobs of a specific parent (overrides "
            "``include_children``). Useful for inspecting one batch."
        ),
    ),
    limit: int = Query(
        default=50,
        ge=1,
        le=500,
        description="Max rows per page; capped at 500.",
    ),
    after: datetime | None = Query(
        default=None,
        description=(
            "Cursor for pagination; return rows with "
            "``created_at < after`` only. Use the ``created_at`` "
            "of the last row from the previous page to walk forward."
        ),
    ),
) -> JobListFilters:
    """FastAPI dependency that gathers the ``GET /jobs`` query filters."""
    return JobListFilters(
        status=status,
        operation=operation,
        include_children=include_children,
        parent_job_id=parent_job_id,
        limit=limit,
        after=after,
    )


def _operation_metadata(
    registry: OperationRegistry,
    operation_name: str,
    payload: dict[str, Any] | None,
    session: Session | None = None,
) -> tuple[str | None, str | None]:
    """Look up an operation's static description and dynamic payload summary.

    Returns ``(description, summary)``. Both are ``None`` if the operation
    is no longer registered (e.g. renamed/removed). ``summary`` is ``None``
    if the operation does not implement ``summarize_payload`` or raises while
    rendering it; we never want metadata enrichment to break the jobs API.

    If ``summarize_payload`` accepts a ``session`` keyword (introspected via
    ``inspect.signature``), the active SQLAlchemy session is forwarded so the
    operation can resolve foreign keys (e.g. an EmbeddingConfig) and render
    human-readable details. Otherwise it is called with payload only.
    """
    import inspect

    try:
        op = registry.get(operation_name)
    except KeyError:
        return None, None
    description = getattr(op, "description", None) or None
    summary: str | None = None
    summarize = getattr(op, "summarize_payload", None)
    if callable(summarize):
        try:
            sig = inspect.signature(summarize)
            if "session" in sig.parameters:
                rendered = summarize(payload or {}, session=session)
            else:
                rendered = summarize(payload or {})
        except Exception:
            rendered = ""
        summary = rendered or None
    return description, summary


router = APIRouter(prefix="/jobs", tags=["jobs"])


class CreateJobRequest(BaseModel):
    """Body for ``POST /jobs``.

    Tells PROTEA which registered operation to run (``operation``) and
    which RabbitMQ queue to publish the work onto (``queue_name``). The
    ``payload`` blob is op-specific; the operation registry validates it
    on dequeue. ``description`` / ``tags`` are the D11 narrative fields
    surfaced in the UI run detail.
    """

    model_config = {
        "extra": "forbid",
        "json_schema_extra": {
            "example": {
                "operation": "compute_embeddings",
                "queue_name": "protea.embedding",
                "payload": {
                    "embedding_config_id": "00000000-0000-0000-0000-000000000001",
                    "annotation_set_id": "00000000-0000-0000-0000-000000000002",
                    "batch_size": 1,
                },
                "meta": {},
                "description": "Recompute ESM-2 embeddings for the GOA 2024-04 set.",
                "tags": ["ablation", "benchmark-v1"],
            }
        },
    }

    operation: str = Field(
        ..., min_length=1, description="Registered operation name, e.g. `insert_proteins`."
    )
    queue_name: str = Field(
        ..., min_length=1, description="RabbitMQ queue to publish the job to, e.g. `protea.jobs`."
    )
    payload: dict[str, Any] = Field(
        default_factory=dict, description="Operation-specific configuration object."
    )
    meta: dict[str, Any] = Field(
        default_factory=dict, description="Optional free-form metadata stored alongside the job."
    )
    description: str | None = Field(
        default=None,
        description="Human-readable intent text shown on the run (D11 narrative field).",
    )
    tags: list[str] = Field(
        default_factory=list,
        description="Lightweight grouping tokens (e.g. `ablation`, `benchmark-v1`).",
    )

    @field_validator("operation", "queue_name", mode="before")
    @classmethod
    def strip_and_require(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()


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
            description=body.description,
            tags=list(body.tags),
        )
        session.add(job)
        session.flush()
        job_id = job.id
        session.add(
            JobEvent(
                job_id=job_id,
                event="job.created",
                fields={"operation": body.operation, "queue": body.queue_name},
            )
        )

    # Publish after commit so the worker always finds the row.
    publish_job(amqp_url, body.queue_name, job_id)
    return {"id": str(job_id), "status": "queued"}


@router.get("", summary="List jobs")
def list_jobs(
    filters: JobListFilters = Depends(_job_list_filters_dep),
    factory: sessionmaker[Session] = Depends(get_session_factory),
    registry: OperationRegistry = Depends(get_operation_registry),
) -> list[dict[str, Any]]:
    """List jobs with optional filtering.

    By default only top-level jobs (no parent) are returned. Set
    ``include_children=true`` or filter by ``parent_job_id`` to see batch
    sub-jobs from distributed pipelines. Filters travel as discrete query
    parameters on the wire; the dependency bundles them into
    :class:`JobListFilters` for the handler.
    """
    with session_scope(factory) as session:
        q = session.query(Job)

        if filters.parent_job_id is not None:
            q = q.filter(Job.parent_job_id == filters.parent_job_id)
        elif not filters.include_children:
            q = q.filter(Job.parent_job_id.is_(None))

        if filters.status is not None:
            try:
                st = JobStatus(filters.status)
            except Exception as exc:
                raise HTTPException(
                    status_code=400, detail=f"Unknown status: {filters.status}"
                ) from exc
            q = q.filter(Job.status == st)

        if filters.operation is not None:
            q = q.filter(Job.operation == filters.operation)

        if filters.after is not None:
            q = q.filter(Job.created_at < filters.after)

        rows = q.order_by(Job.created_at.desc()).limit(filters.limit).all()
        return [_serialise_job_summary(j, registry, session) for j in rows]


def _serialise_job_summary(
    j: Job, registry: OperationRegistry, session: Session
) -> dict[str, Any]:
    """Shape a Job ORM row for the list endpoint (no payload / meta)."""
    description, summary = _operation_metadata(
        registry, j.operation, j.payload, session=session
    )
    return {
        "id": str(j.id),
        "operation": j.operation,
        "operation_description": description,
        "operation_summary": summary,
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
        "description": j.description,
        "findings": j.findings,
        "tags": list(j.tags or []),
    }


@router.get("/{job_id}", summary="Get job details")
def get_job(
    job_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
    registry: OperationRegistry = Depends(get_operation_registry),
) -> dict[str, Any]:
    """Retrieve full details for a single job including its payload, meta, and progress counters."""
    with session_scope(factory) as session:
        j = session.get(Job, job_id)
        if j is None:
            raise HTTPException(status_code=404, detail="Job not found")

        description, summary = _operation_metadata(
            registry, j.operation, j.payload, session=session
        )
        return {
            "id": str(j.id),
            "operation": j.operation,
            "operation_description": description,
            "operation_summary": summary,
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
            "description": j.description,
            "findings": j.findings,
            "tags": list(j.tags or []),
        }


@router.get("/{job_id}/events", summary="List job events")
def get_job_events(
    job_id: UUID,
    limit: int = Query(
        default=200,
        ge=1,
        le=2000,
        description="Max events per page; capped at 2000.",
    ),
    after: datetime | None = Query(
        default=None,
        description=(
            "Cursor for pagination (T4.2); return events with "
            "``ts < after`` only. Walks the newest-first stream "
            "backward by feeding the oldest visible event's ``ts``."
        ),
    ),
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

        q = session.query(JobEvent).filter(JobEvent.job_id == job_id)
        if after is not None:
            q = q.filter(JobEvent.ts < after)
        events = q.order_by(JobEvent.ts.desc()).limit(limit).all()

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


# ---------------------------------------------------------------------------
# Job comments (T3.10 / decision D11 thread)
# ---------------------------------------------------------------------------


class CreateJobCommentRequest(BaseModel):
    """Body for ``POST /jobs/{job_id}/comments``.

    Curator/operator note attached to a Job (D11 narrative thread).
    Distinct from machine-emitted ``JobEvent`` rows: comments carry an
    opinionated message and an optional ``author`` tag. Markdown is
    permitted in ``body``; the UI renders the thread chronologically.
    """

    model_config = {
        "extra": "forbid",
        "json_schema_extra": {
            "example": {
                "body": "Re-running with k=10; k=5 hit the variance ceiling on PK.",
                "author": "frapercan",
            }
        },
    }

    body: str = Field(..., min_length=1, description="Comment payload (markdown allowed).")
    author: str | None = Field(
        default=None,
        description="Free-form actor tag (e.g. user login, bot id, cron name).",
    )

    @field_validator("body", mode="before")
    @classmethod
    def strip_body(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("body must be a non-empty string")
        return v.strip()


def _serialise_job_comment(c: JobComment) -> dict[str, Any]:
    """Shape one ``JobComment`` row for the API response."""
    return {
        "id": c.id,
        "job_id": str(c.job_id),
        "author": c.author,
        "body": c.body,
        "created_at": c.created_at.isoformat(),
    }


@router.post(
    "/{job_id}/comments",
    status_code=201,
    summary="Append a comment to a job",
)
def create_job_comment(
    job_id: UUID,
    body: CreateJobCommentRequest,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Append a free-form comment to a Job.

    Curators / operators use this thread to record observations,
    follow-ups, or post-mortems; the worker fleet keeps writing to
    ``JobEvent`` for machine-emitted progress.
    """
    with session_scope(factory) as session:
        if session.get(Job, job_id) is None:
            raise HTTPException(status_code=404, detail="Job not found")
        # Stamp created_at explicitly so the response carries it even when
        # the test fixture replaces session.flush() with a no-op (the ORM-
        # level ``default=utcnow`` only fires at the SQL INSERT boundary).
        comment = JobComment(
            job_id=job_id, body=body.body, author=body.author, created_at=utcnow()
        )
        session.add(comment)
        session.flush()
        return _serialise_job_comment(comment)


@router.get("/{job_id}/comments", summary="List job comments")
def list_job_comments(
    job_id: UUID,
    limit: int = Query(
        default=200,
        ge=1,
        le=2000,
        description="Max comments per page; capped at 2000.",
    ),
    after: datetime | None = Query(
        default=None,
        description=(
            "Cursor for pagination (T4.2); return comments with "
            "``created_at > after`` only. Walks the oldest-first "
            "stream forward by feeding the newest visible comment's "
            "``created_at``."
        ),
    ),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    """Return ``JobComment`` rows for a Job, oldest first.

    Use ``after`` to page forward (the comment thread grows oldest →
    newest, so cursor semantics flip vs. the newest-first lists).
    ``limit`` caps each page at 2000 to keep payloads bounded.
    """
    with session_scope(factory) as session:
        if session.get(Job, job_id) is None:
            raise HTTPException(status_code=404, detail="Job not found")
        q = session.query(JobComment).filter(JobComment.job_id == job_id)
        if after is not None:
            q = q.filter(JobComment.created_at > after)
        rows = (
            q.order_by(JobComment.created_at.asc(), JobComment.id.asc())
            .limit(limit)
            .all()
        )
        return [_serialise_job_comment(c) for c in rows]


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
    """Mark a job (and any non-terminal child jobs) as CANCELLED.

    Already-finished jobs (SUCCEEDED/FAILED) are returned as-is with no state change.
    Children in QUEUED are cancelled immediately.  Children in RUNNING are also
    marked CANCELLED; the worker's parent-check in BaseWorker.handle_job() will
    detect the cancelled parent on the next iteration and stop gracefully.
    """
    with session_scope(factory) as session:
        j = session.get(Job, job_id)
        if j is None:
            raise HTTPException(status_code=404, detail="Job not found")

        if j.status in (JobStatus.SUCCEEDED, JobStatus.FAILED):
            return {"id": str(j.id), "status": j.status.value}

        j.status = JobStatus.CANCELLED
        j.finished_at = utcnow()
        session.add(JobEvent(job_id=job_id, event="job.cancelled", fields={}))

        # Cancel all non-terminal children (QUEUED and RUNNING).
        children = (
            session.query(Job)
            .filter(
                Job.parent_job_id == job_id,
                Job.status.in_((JobStatus.QUEUED, JobStatus.RUNNING)),
            )
            .all()
        )
        for child in children:
            child.status = JobStatus.CANCELLED
            child.finished_at = utcnow()
            session.add(
                JobEvent(
                    job_id=child.id,
                    event="job.cancelled",
                    fields={"reason": "parent_cancelled"},
                )
            )

        return {"id": str(j.id), "status": j.status.value, "children_cancelled": len(children)}
