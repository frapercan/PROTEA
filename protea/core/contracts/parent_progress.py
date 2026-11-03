"""Shared helper for child operations that report progress to a parent job.

Used by ``store_embeddings`` and ``store_predictions``: each child
batch increments the parent's ``progress_current`` and, if it was
the last batch, transitions the parent from RUNNING to SUCCEEDED.
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import and_, func, or_
from sqlalchemy import update as sa_update
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn
from protea.infrastructure.orm.models.job import Job, JobEvent, JobStatus

#: The ``error_code`` ``stale_job_reaper`` leaves on a job it gave up on.
#:
#: A parent carrying it did not fail. A deferred coordinator clears its lease
#: once its own work is done, and the reaper marks it FAILED when its children
#: stop emitting for longer than its window; the children can and do arrive
#: afterwards. Measured: ``b185a6d0`` was failed this way at exactly six hours,
#: with progress ``0/7``. Any OTHER code on a FAILED parent is a real failure,
#: and the work of its children is not wanted.
REAPED = "lease_expired"

#: A parent whose children's progress still counts: it is running, or the reaper
#: giving up is the only reason it is not.
_STILL_COUNTS = or_(
    Job.status == JobStatus.RUNNING,
    and_(Job.status == JobStatus.FAILED, Job.error_code == REAPED),
)

#: The same predicate under a public name, for the modules outside this contract that
#: need it.
STILL_COUNTS = _STILL_COUNTS

#: A coordinator whose children are still in flight: its progress still counts and it has
#: not received all of them. A RUNNING coordinator is in flight whatever its count; a
#: reaped one only while its count is short of its total (``coalesce``, because a count
#: never advanced is NULL, and NULL < total would read as "not in flight"). Derived from
#: ``STILL_COUNTS`` so a change in what the reaper's FAILED means reaches every reader:
#: the card guard (``protea.core.operations._card_guard``) is one.
IN_FLIGHT = and_(
    _STILL_COUNTS,
    or_(
        Job.status == JobStatus.RUNNING,
        func.coalesce(Job.progress_current, 0) < Job.progress_total,
    ),
)


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
        .where(Job.id == parent_job_id, _STILL_COUNTS)
        .values(progress_current=Job.progress_current + 1)
        .returning(Job.progress_current, Job.progress_total, Job.status)
    ).fetchone()

    if row is None or row.progress_current != row.progress_total:
        return

    # ``returning`` gave the status as it WAS, because that statement does not
    # touch it. FAILED here can only be the reaper: ``_STILL_COUNTS`` admits no
    # other failure.
    recovered = row.status == JobStatus.FAILED
    closed = session.execute(
        sa_update(Job)
        .where(Job.id == parent_job_id, _STILL_COUNTS)
        .values(status=JobStatus.SUCCEEDED, finished_at=func.now(), error_code=None)
        .returning(Job.id)
    ).fetchone()

    if closed:
        fields: dict[str, object] = {"via": "last_batch_stored"}
        if recovered:
            # Cleared on the ROW, because a SUCCEEDED job carrying the reason it
            # failed contradicts itself and anything reading ``error_code`` as
            # "why this failed" would be wrong about it. Kept HERE so the
            # recovery stays on the record.
            fields |= {"recovered_from": REAPED, "was": JobStatus.FAILED.value}
        session.add(
            JobEvent(
                job_id=parent_job_id,
                event="job.succeeded",
                fields=fields,
                level="info",
            )
        )
        reported = {"parent_job_id": str(parent_job_id)}
        if recovered:
            reported["recovered_from"] = REAPED
        emit(event_name, None, reported, "info")
