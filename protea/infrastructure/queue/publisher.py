from __future__ import annotations

import json
import logging
import time
from uuid import UUID

import pika

logger = logging.getLogger(__name__)

_RETRY_DELAYS = (1, 3, 10)  # seconds between attempts (3 attempts total)


def publish_job(amqp_url: str, queue_name: str, job_id: UUID) -> None:
    """Publish a job dispatch message to a RabbitMQ queue.

    Opens a connection, publishes a single persistent message of the form
    ``{"job_id": "<uuid>"}``, then closes the connection.

    Retries up to 3 times with increasing delays if the broker is temporarily
    unavailable.  Raises the last exception if all attempts fail, which will
    cause the caller to handle the error (e.g. nack the message or log it).

    Intended to be called immediately after a Job row is committed so that
    a QueueConsumer can pick it up and call BaseWorker.handle_job().
    """
    body = json.dumps({"job_id": str(job_id)}).encode("utf-8")
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
                return  # success
            finally:
                if connection.is_open:
                    connection.close()
        except Exception as exc:
            last_exc = exc
            if delay is not None:
                logger.warning(
                    "publish_job failed (attempt %d), retrying in %ds. job_id=%s error=%s",
                    attempt, delay, job_id, exc,
                )
                time.sleep(delay)
            else:
                logger.error(
                    "publish_job failed after %d attempts. job_id=%s error=%s",
                    attempt, job_id, exc,
                )

    raise RuntimeError(
        f"Failed to publish job {job_id} to queue {queue_name!r} after {len(_RETRY_DELAYS) + 1} attempts"
    ) from last_exc
