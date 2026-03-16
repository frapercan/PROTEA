from __future__ import annotations

import json
import logging
import time
from typing import Any
from uuid import UUID

import pika

logger = logging.getLogger(__name__)

_RETRY_DELAYS = (1, 3, 10)  # seconds between attempts (3 attempts total)


def _publish(amqp_url: str, queue_name: str, body: bytes) -> None:
    """Core publish logic with retries. Used by both publish_job and publish_operation."""
    last_exc: Exception | None = None

    for attempt, delay in enumerate((*_RETRY_DELAYS, None), start=1):
        try:
            connection = pika.BlockingConnection(pika.URLParameters(amqp_url))
            try:
                channel = connection.channel()
                channel.queue_declare(queue=queue_name, durable=True)
                channel.basic_publish(
                    exchange="",
                    routing_key=queue_name,
                    body=body,
                    properties=pika.BasicProperties(
                        delivery_mode=pika.DeliveryMode.Persistent,
                    ),
                )
                return
            finally:
                if connection.is_open:
                    connection.close()
        except Exception as exc:
            last_exc = exc
            if delay is not None:
                logger.warning(
                    "publish failed (attempt %d), retrying in %ds. queue=%s error=%s",
                    attempt,
                    delay,
                    queue_name,
                    exc,
                )
                time.sleep(delay)
            else:
                logger.error(
                    "publish failed after %d attempts. queue=%s error=%s",
                    attempt,
                    queue_name,
                    exc,
                )

    raise RuntimeError(
        f"Failed to publish to queue {queue_name!r} after {len(_RETRY_DELAYS) + 1} attempts"
    ) from last_exc


def publish_job(amqp_url: str, queue_name: str, job_id: UUID) -> None:
    """Publish a job dispatch message ``{"job_id": "<uuid>"}`` to a queue."""
    _publish(amqp_url, queue_name, json.dumps({"job_id": str(job_id)}).encode("utf-8"))


def publish_operation(amqp_url: str, queue_name: str, payload: dict[str, Any]) -> None:
    """Publish an ephemeral operation message to a queue.

    Unlike publish_job, publishes a full payload dict consumed by
    OperationConsumer.  Expected format::

        {
            "operation": "<name>",
            "job_id":    "<parent-uuid>",
            "payload":   { ... operation-specific fields ... },
        }
    """
    _publish(amqp_url, queue_name, json.dumps(payload).encode("utf-8"))
