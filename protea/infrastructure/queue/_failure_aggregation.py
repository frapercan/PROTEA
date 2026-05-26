"""Aggregate-failure helper for ``OperationConsumer`` deliveries.

Extracted from ``consumer.py`` so that module stays under the ~800 LOC
file budget while still implementing F-OPS-COORD-FAIL-PROPAGATE.

A child operation that fails inside an ``OperationConsumer`` has no
DB ``Job`` row of its own, so the parent coordinator job never sees
the failure and can stay ``RUNNING`` indefinitely. This module owns
the side-effecting lookup that bridges the gap: it reads the parent's
``progress_total`` (the dispatched count stamped by the coordinator
at dispatch time) and feeds each child failure into
``report_child_failure``, which transitions the parent to ``FAILED``
once the threshold is reached.
"""

from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from protea.core.contracts.operation import make_safe_emit
from protea.core.contracts.parent_failure import ChildFailure, report_child_failure
from protea.infrastructure.orm.models.job import Job, JobStatus

logger = logging.getLogger(__name__)


def maybe_aggregate_parent_failure(
    parent_job_id: UUID | None,
    exc: BaseException,
    *,
    session_factory: Any,
    make_raw_emit: Any,
) -> None:
    """Best-effort aggregate-failure pass for one child delivery failure.

    Looks up the parent ``Job`` row, reads its ``progress_total`` and
    delegates to ``report_child_failure``. Any error (DB hiccup, missing
    parent row, etc.) is logged at WARNING and swallowed so the calling
    failure-handler can still nack the delivery.
    """
    if parent_job_id is None:
        return
    session = session_factory()
    try:
        parent = session.get(Job, parent_job_id)
        if parent is None or parent.status != JobStatus.RUNNING:
            return
        dispatched_total = int(parent.progress_total or 0)
        if dispatched_total <= 0:
            return
        emit = make_safe_emit(make_raw_emit(parent_job_id))
        failure = ChildFailure(
            error_message=str(exc),
            error_code=exc.__class__.__name__,
            dispatched_total=dispatched_total,
        )
        report_child_failure(session, parent_job_id, failure, emit)
        session.commit()
    except Exception as agg_exc:
        logger.warning(
            "Aggregate parent-failure update failed. parent_job_id=%s error=%s",
            parent_job_id,
            agg_exc,
        )
        try:
            session.rollback()
        except Exception:
            pass
    finally:
        session.close()
