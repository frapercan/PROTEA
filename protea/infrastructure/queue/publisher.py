from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any
from uuid import UUID

import pika

from protea.config.tuning import get_tuning
from protea.infrastructure.telemetry import get_tracer, inject_trace_context

logger = logging.getLogger(__name__)
_TRACER = get_tracer(__name__)

# Thread-local persistent connection to avoid opening/closing per publish.
_local = threading.local()


def _get_connection(amqp_url: str) -> pika.BlockingConnection:
    """Return a reusable connection, creating one if needed.

    Applies the configured AMQP heartbeat (default 600s, see
    ``QueueTuning.amqp_heartbeat``) so the publisher side does not get
    closed mid-publish after a long idle window between batches. Pika's
    60s default is too short for jobs that publish bursts hours apart.
    """
    conn: pika.BlockingConnection | None = getattr(_local, "connection", None)
    if conn is not None and conn.is_open:
        return conn
    params = pika.URLParameters(amqp_url)
    params.heartbeat = get_tuning().queue.amqp_heartbeat
    _local.connection = pika.BlockingConnection(params)
    return _local.connection


def _close_cached_connection() -> None:
    conn: pika.BlockingConnection | None = getattr(_local, "connection", None)
    if conn is not None and conn.is_open:
        try:
            conn.close()
        except Exception:
            pass
    _local.connection = None


def _publish_with_span(channel: Any, queue_name: str, body: bytes) -> None:
    """Wrap a single ``basic_publish`` in an OTel CLIENT span and inject
    the ``traceparent`` header so consumers can stitch the trace.

    Extracted from :func:`_publish` to keep that function under the §3
    60-LOC method ceiling without losing the retry loop's clarity.
    """
    with _TRACER.start_as_current_span(f"amqp.publish {queue_name}") as span:
        span.set_attribute("messaging.system", "rabbitmq")
        span.set_attribute("messaging.destination", queue_name)
        span.set_attribute("messaging.destination_kind", "queue")
        headers: dict[str, Any] = {}
        inject_trace_context(headers)
        channel.basic_publish(
            exchange="",
            routing_key=queue_name,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                headers=headers or None,
            ),
        )


def _publish(amqp_url: str, queue_name: str, body: bytes) -> None:
    """Core publish logic with retries and connection reuse.

    T5.1b: each attempt is wrapped in a CLIENT span via
    :func:`_publish_with_span` so the W3C ``traceparent`` flows onto the
    outbound AMQP message.
    """
    settings = get_tuning().queue
    max_attempts = settings.publisher_max_attempts
    base_delay = settings.publisher_base_delay
    last_exc: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            connection = _get_connection(amqp_url)
            channel = connection.channel()
            channel.queue_declare(
                queue=queue_name,
                durable=True,
                arguments={"x-dead-letter-exchange": "protea.dlx"},
            )
            _publish_with_span(channel, queue_name, body)
            return
        except Exception as exc:
            last_exc = exc
            # Connection is stale: discard it so next attempt creates a fresh one.
            _close_cached_connection()
            if attempt < max_attempts:
                delay = min(base_delay * (2 ** (attempt - 1)), 30)
                logger.warning(
                    "publish failed (attempt %d/%d), retrying in %ss. queue=%s error=%s",
                    attempt,
                    max_attempts,
                    delay,
                    queue_name,
                    exc,
                )
                time.sleep(delay)
            else:
                logger.error(
                    "publish failed after %d attempts. queue=%s error=%s",
                    max_attempts,
                    queue_name,
                    exc,
                )

    raise RuntimeError(
        f"Failed to publish to queue {queue_name!r} after {max_attempts} attempts"
    ) from last_exc


def publish_job(amqp_url: str, queue_name: str, job_id: UUID) -> None:
    """Publish a job dispatch message ``{"job_id": "<uuid>"}`` to a queue."""
    _publish(amqp_url, queue_name, json.dumps({"job_id": str(job_id)}).encode("utf-8"))


def safe_republish_job(amqp_url: str, queue_name: str, job_id: UUID) -> None:
    """Re-publish a re-queued job; logs but does NOT raise on failure.

    Used by the SIGTERM grace watchdog after
    :meth:`BaseWorker.requeue_on_shutdown` flips the row back to QUEUED.
    Failures fall through to the StaleJobReaper via the lease-expiry
    path so a flaky RabbitMQ never leaves the row stranded.
    """
    try:
        publish_job(amqp_url, queue_name, job_id)
    except Exception as exc:
        logger.error(
            "Re-publish after shutdown re-queue failed; reaper will "
            "recover via lease expiry. job_id=%s error=%s",
            job_id,
            exc,
        )


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
