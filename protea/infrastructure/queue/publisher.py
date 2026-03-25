from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any
from uuid import UUID

import pika

logger = logging.getLogger(__name__)

_MAX_ATTEMPTS = 5
_BASE_DELAY = 1  # seconds; exponential backoff: 1, 2, 4, 8, 16 (capped at 30)

# Thread-local persistent connection to avoid opening/closing per publish.
_local = threading.local()


def _get_connection(amqp_url: str) -> pika.BlockingConnection:
    """Return a reusable connection, creating one if needed."""
    conn: pika.BlockingConnection | None = getattr(_local, "connection", None)
    if conn is not None and conn.is_open:
        return conn
    _local.connection = pika.BlockingConnection(pika.URLParameters(amqp_url))
    return _local.connection


def _close_cached_connection() -> None:
    conn: pika.BlockingConnection | None = getattr(_local, "connection", None)
    if conn is not None and conn.is_open:
        try:
            conn.close()
        except Exception:
            pass
    _local.connection = None


def _publish(amqp_url: str, queue_name: str, body: bytes) -> None:
    """Core publish logic with retries and connection reuse."""
    last_exc: Exception | None = None

    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            connection = _get_connection(amqp_url)
            channel = connection.channel()
            channel.queue_declare(
                queue=queue_name,
                durable=True,
                arguments={"x-dead-letter-exchange": "protea.dlx"},
            )
            channel.basic_publish(
                exchange="",
                routing_key=queue_name,
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent,
                ),
            )
            return
        except Exception as exc:
            last_exc = exc
            # Connection is stale — discard it so next attempt creates a fresh one.
            _close_cached_connection()
            if attempt < _MAX_ATTEMPTS:
                delay = min(_BASE_DELAY * (2 ** (attempt - 1)), 30)
                logger.warning(
                    "publish failed (attempt %d/%d), retrying in %ds. queue=%s error=%s",
                    attempt,
                    _MAX_ATTEMPTS,
                    delay,
                    queue_name,
                    exc,
                )
                time.sleep(delay)
            else:
                logger.error(
                    "publish failed after %d attempts. queue=%s error=%s",
                    _MAX_ATTEMPTS,
                    queue_name,
                    exc,
                )

    raise RuntimeError(
        f"Failed to publish to queue {queue_name!r} after {_MAX_ATTEMPTS} attempts"
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
