"""Jobs service — shared helpers for the queue-dispatch pattern.

Multiple routers (annotations, embeddings, predictions) follow the
same shape when an HTTP endpoint queues background work:

1. Pydantic-validate the request body.
2. Insert a ``Job`` row with the canonical operation name + queue.
3. Insert a matching ``JobEvent`` (``job.created``) for the audit log.
4. Publish to RabbitMQ via :func:`protea.infrastructure.queue.publisher.publish_job`.
5. Return ``{"id": ..., "status": "queued"}``.

This module exposes :func:`enqueue_job` (steps 2–3) and the higher-
level :func:`dispatch_validated_job` (steps 1–5) so routers collapse
to a single try/except.
"""

from __future__ import annotations

import uuid
from typing import Any

from pydantic import BaseModel, ValidationError
from sqlalchemy.orm import Session, sessionmaker

from protea.infrastructure.orm.models.job import Job, JobEvent
from protea.infrastructure.queue.publisher import publish_job
from protea.infrastructure.session import session_scope


class InvalidJobPayloadError(Exception):
    """Pydantic validation failed for a queue-dispatch request body.

    Carries the structured ``errors`` list produced by Pydantic so
    the router can pass it through verbatim as the HTTP 422 detail.
    """

    def __init__(self, errors: Any) -> None:
        self.errors = errors
        super().__init__("invalid job payload")


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


def dispatch_validated_job(
    factory: sessionmaker[Session],
    amqp_url: str,
    body: dict[str, Any],
    payload_model: type[BaseModel],
    *,
    operation: str,
    queue_name: str,
) -> dict[str, Any]:
    """End-to-end queue dispatch: validate, persist, publish, respond.

    Pydantic-validates ``body`` against ``payload_model`` (raising
    :class:`InvalidJobPayloadError` on failure for the router to map
    to a 422), inserts a ``Job`` + ``JobEvent`` pair inside a
    fresh session, then publishes to RabbitMQ. Returns the canonical
    ``{"id": <uuid>, "status": "queued"}`` response shape used by
    every dispatch endpoint.
    """
    try:
        payload_model.model_validate(body)
    except ValidationError as exc:
        raise InvalidJobPayloadError(exc.errors()) from exc

    with session_scope(factory) as session:
        job_id = enqueue_job(
            session, operation=operation, queue_name=queue_name, payload=body
        )

    publish_job(amqp_url, queue_name, job_id)
    return {"id": str(job_id), "status": "queued"}


__all__ = [
    "InvalidJobPayloadError",
    "dispatch_validated_job",
    "enqueue_job",
]
