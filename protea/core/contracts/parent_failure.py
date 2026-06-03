"""Helper for child operations that propagate failure to a parent job.

Companion to ``parent_progress.py``. While the latter handles the
happy-path SUCCEEDED transition, this module handles the failure-path
FAILED transition for a coordinator parent whose dispatched children
do not have their own ``Job`` rows (e.g. ``OperationConsumer`` mini-jobs
published via ``OperationResult.publish_operations``).

Use case (F-OPS-COORD-FAIL-PROPAGATE, incident 2026-05-26):

The ``export_coordinator`` dispatches per-snapshot KNN minijobs which
run as ``OperationConsumer`` deliveries (no DB Job row of their own).
If all of those children raise the same non-retryable ``ValueError``
(e.g. unsupported ``search_backend="torch"``), the coordinator's
parent job previously stayed ``RUNNING`` for hours because
``_maybe_fail_parent`` in ``base_worker`` only inspects child ``Job``
rows. This helper closes that gap by aggregating failure counts in
the parent's ``meta`` JSONB and transitioning the parent to ``FAILED``
when the configurable threshold (default: every dispatched child
failed) is met.

Aggregation policy (default):

- ``failed_children_count`` is stored in ``parent.meta`` and
  incremented atomically on each call.
- Up to three distinct error messages are kept in
  ``parent.meta.top_error_messages`` (insertion order; duplicates are
  dropped) so the post-mortem can see the dominant cause.
- The parent transitions to ``FAILED`` as soon as
  ``failed_children_count >= dispatched_total`` OR
  ``failed_children_count / dispatched_total >= failure_ratio``.

The default ``failure_ratio=1.0`` keeps the policy conservative: only
when 100% of dispatched children fail does the parent flip. Callers
that want a stricter policy (e.g. fail at 50%) can pass a lower
ratio.

This helper is safe to call repeatedly: once the parent leaves the
``RUNNING`` state it becomes a no-op.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from sqlalchemy import update as sa_update
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn
from protea.core.utils import utcnow
from protea.infrastructure.orm.models.job import Job, JobEvent, JobStatus

_MAX_TOP_ERRORS = 3
_TRUNC_ERROR_MESSAGE = 500


@dataclass(frozen=True)
class ChildFailure:
    """One child-failure observation, ready for aggregation on the parent."""

    error_message: str | None
    error_code: str | None
    dispatched_total: int
    failure_ratio: float = 1.0
    event_name: str = "export_coordinator.failed"


def _truncate(message: str | None) -> str:
    if not message:
        return ""
    text = str(message)
    if len(text) <= _TRUNC_ERROR_MESSAGE:
        return text
    return text[: _TRUNC_ERROR_MESSAGE - 1] + "…"


def _bump_meta(meta: dict[str, Any], failure: ChildFailure) -> tuple[int, list[str]]:
    """Increment counters + top-3 message list on a ``meta`` dict in place."""
    failed = int(meta.get("failed_children_count", 0)) + 1
    meta["failed_children_count"] = failed
    meta["dispatched_total"] = failure.dispatched_total

    msg = _truncate(failure.error_message) or (failure.error_code or "UnknownChildError")
    top = list(meta.get("top_error_messages", []))
    if msg not in top:
        top.append(msg)
        top = top[:_MAX_TOP_ERRORS]
    meta["top_error_messages"] = top
    return failed, top


def _aggregated_message(
    failed: int, top: list[str], failure: ChildFailure
) -> str:
    if failed > 1:
        return _truncate(
            f"all {failed} dispatched children failed; "
            f"top causes: {' | '.join(top)}"
        )
    cause = top[0] if top else (failure.error_code or "")
    return _truncate(f"first child failed with non-retryable error: {cause}")


def _close_parent(
    session: Session,
    parent_job_id: UUID,
    error_code: str | None,
    error_message: str,
) -> bool:
    """Atomically flip parent RUNNING → FAILED. Returns True if we won the race."""
    row = session.execute(
        sa_update(Job)
        .where(Job.id == parent_job_id, Job.status == JobStatus.RUNNING)
        .values(
            status=JobStatus.FAILED,
            finished_at=utcnow(),
            error_code=error_code or "AllChildrenFailed",
            error_message=error_message,
        )
        .returning(Job.id)
    ).fetchone()
    return row is not None


def report_child_failure(
    session: Session,
    parent_job_id: UUID,
    failure: ChildFailure,
    emit: EmitFn,
) -> bool:
    """Record one child failure on the parent and maybe transition to FAILED.

    Returns ``True`` if this call closed the parent (RUNNING -> FAILED),
    ``False`` otherwise (still RUNNING, already terminal, or row missing).
    Callers are responsible for opening and committing the session.
    """
    if failure.dispatched_total <= 0:
        return False
    parent = session.get(Job, parent_job_id)
    if parent is None or parent.status != JobStatus.RUNNING:
        return False

    meta = dict(parent.meta or {})
    failed, top = _bump_meta(meta, failure)
    parent.meta = meta

    ratio = failed / failure.dispatched_total
    if not (failed >= failure.dispatched_total or ratio >= failure.failure_ratio):
        session.flush()
        return False

    aggregated = _aggregated_message(failed, top, failure)
    if not _close_parent(session, parent_job_id, failure.error_code, aggregated):
        session.flush()
        return False

    fields: dict[str, Any] = {
        "failed_children": failed,
        "total_children": failure.dispatched_total,
        "top_error_messages": top,
    }
    session.add(
        JobEvent(
            job_id=parent_job_id,
            event="job.failed",
            message=aggregated,
            fields={"via": "child_failure_aggregate", **fields},
            level="error",
        )
    )
    emit(failure.event_name, aggregated, fields, "error")
    session.flush()
    return True
