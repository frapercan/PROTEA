"""When a child of a coordinator stops: only when a person cancelled the coordinator.

A coordinator that fans its work out as operation messages returns early (``deferred``) and
holds no lease while its children run, so the reaper judges it by its events alone. Past the
hard limit, a row silent for the stall window is read as dead: it is requeued and, once its
requeue budget is spent, marked FAILED. If a child then stopped on FAILED, every batch the
queue still held would be discarded and acknowledged, with nothing in the dead-letter queue
to show it: computed work thrown away while the pass reports no error. That is the failure
found on 2026-11-03 for the per-residue codes, whose card yields to other reads for hours.

So FAILED does not stop a child: the work is still wanted and its writes are idempotent. The
child says so with a warning and runs. CANCELLED does stop it, because a cancellation is a
person saying stop. The consumer already dead-letters a message whose parent is cancelled
before it runs; this check covers a cancellation that lands after that.
"""

from __future__ import annotations

import uuid

from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn
from protea.infrastructure.orm.models.job import Job, JobStatus


def parent_cancelled(
    session: Session, parent_job_id: str | None, emit: EmitFn, prefix: str
) -> bool:
    """True when the coordinator was cancelled, in which case the child does nothing."""
    if not parent_job_id:
        return False
    parent = session.get(Job, uuid.UUID(parent_job_id))
    if parent is None:
        return False
    if parent.status == JobStatus.CANCELLED:
        emit(f"{prefix}.skipped", None, {"reason": "parent_cancelled"}, "warning")
        return True
    if parent.status == JobStatus.FAILED:
        emit(
            f"{prefix}.parent_failed",
            "the coordinator is FAILED; the child runs anyway, only a cancellation stops it",
            {"parent_error_code": parent.error_code},
            "warning",
        )
    return False
