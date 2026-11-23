"""A cancelled job's message is acknowledged with an event, not dead-lettered.

ADR-D60, phase 3 (the executor rules). Both consumers drop a delivery whose job
(or parent job) the API already cancelled. They used to nack it without requeue,
which sent it to the dead-letter queue, where a cancellation is indistinguishable
from a failure and a person reading the queue has to tell them apart by hand.
They now acknowledge it and write one event on the job saying so.
"""

from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from protea.core.host import compute_host
from protea.infrastructure.orm.models.job import JobEvent

logger = logging.getLogger(__name__)


def record_skip(factory: Any, job_id: UUID | None, event: str, queue_name: str) -> None:
    """Write the event that says a cancelled job's message was acked unrun; never raises."""
    if job_id is None:
        return
    session = factory()
    try:
        session.add(
            JobEvent(
                job_id=job_id,
                event=event,
                message="the job was cancelled, so its message was acknowledged without running",
                fields={"queue": queue_name, "host": compute_host()},
                level="info",
            )
        )
        session.commit()
    except Exception as exc:
        logger.warning("Could not record a skipped message. job_id=%s error=%s", job_id, exc)
        try:
            session.rollback()
        except Exception:
            pass
    finally:
        session.close()
