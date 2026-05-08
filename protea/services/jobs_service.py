"""Jobs service — shared helper for the queue-dispatch pattern.

Multiple routers (annotations, embeddings, predictions) follow the
same shape when an HTTP endpoint queues background work:

1. Pydantic-validate the request body.
2. Insert a ``Job`` row with the canonical operation name + queue.
3. Insert a matching ``JobEvent`` (``job.created``) for the audit log.
4. Publish to RabbitMQ via :func:`protea.infrastructure.queue.publisher.publish_job`.
5. Return ``{"id": ..., "status": "queued"}``.

This module centralises step 2-3 so the router can collapse to::

    job_id = enqueue_job(session, operation="...", queue_name="...", payload=body)
    publish_job(amqp_url, queue_name, job_id)
    return {"id": str(job_id), "status": "queued"}

The ORM ``Job`` model owns the row schema; this service just
threads the payload through with a consistent ``JobEvent`` shape.
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.job import Job, JobEvent


def enqueue_job(
    session: Session,
    *,
    operation: str,
    queue_name: str,
    payload: dict[str, Any],
) -> uuid.UUID:
    """Insert ``Job`` + ``JobEvent`` rows for a background task.

    Returns the new job's UUID. The caller is responsible for
    publishing to the queue (via
    :func:`protea.infrastructure.queue.publisher.publish_job`)
    after committing this session, and for any payload validation
    that should happen before the row hits the database.

    Both rows are flushed but not committed; the caller's
    ``session_scope`` context manager owns the transaction.
    """
    job = Job(operation=operation, queue_name=queue_name, payload=payload)
    session.add(job)
    session.flush()
    job_id = job.id
    session.add(
        JobEvent(
            job_id=job_id,
            event="job.created",
            fields={"operation": operation, "queue": queue_name},
        )
    )
    return job_id


__all__ = ["enqueue_job"]
