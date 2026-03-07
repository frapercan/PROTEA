from __future__ import annotations

import json
import logging
import signal
from uuid import UUID

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from protea.workers.base_worker import BaseWorker

logger = logging.getLogger(__name__)


class QueueConsumer:
    """
    Thin RabbitMQ consumer that delegates job execution to BaseWorker.

    Responsibilities are strictly limited to transport concerns:
    - Connect to RabbitMQ and declare the queue.
    - Receive messages containing a JSON ``{"job_id": "<uuid>"}`` body.
    - Call ``BaseWorker.handle_job(job_id)`` for each valid message.
    - Ack on success, nack on failure or invalid message.
    - Graceful shutdown on SIGINT / SIGTERM.

    All business logic, DB state transitions, and event emission happen
    inside BaseWorker — this class knows nothing about operations.
    """

    def __init__(
        self,
        amqp_url: str,
        queue_name: str,
        worker: BaseWorker,
        *,
        prefetch_count: int = 1,
        requeue_on_failure: bool = False,
    ) -> None:
        self._amqp_url = amqp_url
        self._queue_name = queue_name
        self._worker = worker
        self._prefetch_count = prefetch_count
        self._requeue_on_failure = requeue_on_failure
        self._stop = False

    def run(self) -> None:
        signal.signal(signal.SIGINT, self._handle_stop)
        signal.signal(signal.SIGTERM, self._handle_stop)

        connection = pika.BlockingConnection(pika.URLParameters(self._amqp_url))
        channel = connection.channel()

        channel.queue_declare(queue=self._queue_name, durable=True)
        channel.basic_qos(prefetch_count=self._prefetch_count)
        channel.basic_consume(
            queue=self._queue_name,
            on_message_callback=self._on_message,
            auto_ack=False,
        )

        logger.info("Consumer started. queue=%s", self._queue_name)
        try:
            channel.start_consuming()
        finally:
            try:
                if channel.is_open:
                    channel.stop_consuming()
            except Exception:
                pass
            try:
                if connection.is_open:
                    connection.close()
            except Exception:
                pass
            logger.info("Consumer stopped. queue=%s", self._queue_name)

    def _handle_stop(self, *_) -> None:
        self._stop = True
        logger.info("Stop signal received. queue=%s", self._queue_name)

    def _on_message(
        self,
        channel: BlockingChannel,
        method: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
    ) -> None:
        # Drain remaining messages gracefully on shutdown.
        if self._stop:
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            return

        # Parse message.
        try:
            data = json.loads(body.decode("utf-8"))
            job_id = UUID(data["job_id"])
        except Exception as exc:
            logger.error("Unparseable message, discarding. body=%r error=%s", body, exc)
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        logger.info("Dispatching job. job_id=%s queue=%s", job_id, self._queue_name)

        # Delegate entirely to the worker.
        try:
            self._worker.handle_job(job_id)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            logger.info("Job acked. job_id=%s", job_id)
        except Exception as exc:
            logger.error("Job failed. job_id=%s error=%s", job_id, exc)
            channel.basic_nack(
                delivery_tag=method.delivery_tag,
                requeue=self._requeue_on_failure,
            )
