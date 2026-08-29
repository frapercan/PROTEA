"""Shared helper for child operations that report progress to a parent job.

Used by ``store_embeddings`` and ``store_predictions``: each child
batch increments the parent's ``progress_current`` and, if it was
the last batch, transitions the parent from RUNNING to SUCCEEDED.
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import func
from sqlalchemy import update as sa_update
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn
from protea.infrastructure.orm.models.job import Job, JobEvent, JobStatus


def update_parent_progress(
    session: Session,
    parent_job_id: UUID,
    emit: EmitFn,
    *,
    event_name: str,
) -> None:
    """Atomically increment parent progress; mark SUCCEEDED if last batch.

    Returns silently if the parent has not yet seen all its batches
    (``progress_current < progress_total``) or if no longer RUNNING.

    The ``event_name`` is the operation-specific suffix used in the
    ``emit`` call when the parent transitions to SUCCEEDED, e.g.
    ``"store_embeddings.parent_succeeded"`` or
    ``"store_predictions.parent_succeeded"``. The DB-level event row is
    always written as ``job.succeeded`` so downstream consumers see a
    uniform name regardless of which child closed the parent.
    """
    row = session.execute(
        sa_update(Job)
        .where(Job.id == parent_job_id, Job.status == JobStatus.RUNNING)
        .values(progress_current=Job.progress_current + 1)
        .returning(Job.progress_current, Job.progress_total)
    ).fetchone()

    if row is None or row.progress_current != row.progress_total:
        return

    closed = session.execute(
        sa_update(Job)
        .where(Job.id == parent_job_id, Job.status == JobStatus.RUNNING)
        .values(status=JobStatus.SUCCEEDED, finished_at=func.now())
        .returning(Job.id)
    ).fetchone()

    if closed:
        session.add(
            JobEvent(
                job_id=parent_job_id,
                event="job.succeeded",
                fields={"via": "last_batch_stored"},
                level="info",
            )
        )
        emit(
            event_name,
            None,
            {"parent_job_id": str(parent_job_id)},
            "info",
        )
