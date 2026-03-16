from __future__ import annotations

import json
import logging
import signal
from typing import Any
from uuid import UUID

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties
from sqlalchemy.orm import Session, sessionmaker

from protea.core.contracts.operation import RetryLaterError
from protea.core.contracts.registry import OperationRegistry
from protea.infrastructure.queue.publisher import publish_operation
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

        # Use a long heartbeat so RabbitMQ does not close the connection
        # while the worker is blocked inside a long operation (QuickGO, embeddings…).
        # BlockingConnection cannot send heartbeats during op.execute(), so we
        # give the broker up to 1 hour before it considers this consumer dead.
        params = pika.URLParameters(self._amqp_url)
        params.heartbeat = 3600
        connection = pika.BlockingConnection(params)
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

    def _handle_stop(self, *_: object) -> None:
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

        # ACK before execution so long-running jobs don't hit RabbitMQ's
        # consumer_timeout. The job is already recorded as RUNNING in the DB,
        # so a worker crash can be detected and recovered externally.
        channel.basic_ack(delivery_tag=method.delivery_tag)
        logger.info("Job acked. job_id=%s", job_id)

        try:
            self._worker.handle_job(job_id)
        except RetryLaterError as exc:
            delay = exc.delay_seconds
            logger.info("Job will retry in %ss. job_id=%s reason=%s", delay, job_id, exc)
            channel.connection.sleep(delay)
            channel.basic_publish(
                exchange="",
                routing_key=self._queue_name,
                body=json.dumps({"job_id": str(job_id)}).encode(),
                properties=pika.BasicProperties(delivery_mode=2),
            )
            logger.info("Job re-published. job_id=%s queue=%s", job_id, self._queue_name)
        except Exception as exc:
            logger.error("Job failed. job_id=%s error=%s", job_id, exc)


class OperationConsumer:
    """
    RabbitMQ consumer for ephemeral operation messages.

    Unlike QueueConsumer (which manages the full Job lifecycle via BaseWorker),
    this consumer handles lightweight operation messages that have no DB Job row
    of their own.  Workers process the operation, write results directly to the
    DB, and atomically update the parent Job's progress counter.

    Expected message format::

        {
            "operation": "<operation-name>",
            "job_id":    "<parent-job-uuid>",
            "payload":   { ... operation-specific fields ... }
        }
    """

    def __init__(
        self,
        amqp_url: str,
        queue_name: str,
        registry: OperationRegistry,
        session_factory: sessionmaker[Session],
        *,
        prefetch_count: int = 1,
        requeue_on_failure: bool = False,
    ) -> None:
        self._amqp_url = amqp_url
        self._queue_name = queue_name
        self._registry = registry
        self._factory = session_factory
        self._prefetch_count = prefetch_count
        self._requeue_on_failure = requeue_on_failure
        self._stop = False

    def run(self) -> None:
        signal.signal(signal.SIGINT, self._handle_stop)
        signal.signal(signal.SIGTERM, self._handle_stop)

        params = pika.URLParameters(self._amqp_url)
        params.heartbeat = 3600
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        channel.queue_declare(queue=self._queue_name, durable=True)
        channel.basic_qos(prefetch_count=self._prefetch_count)
        channel.basic_consume(
            queue=self._queue_name,
            on_message_callback=self._on_message,
            auto_ack=False,
        )

        logger.info("OperationConsumer started. queue=%s", self._queue_name)
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
            logger.info("OperationConsumer stopped. queue=%s", self._queue_name)

    def _handle_stop(self, *_: object) -> None:
        self._stop = True
        logger.info("Stop signal received. queue=%s", self._queue_name)

    def _on_message(
        self,
        channel: BlockingChannel,
        method: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
    ) -> None:
        if self._stop:
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            return

        try:
            data = json.loads(body.decode("utf-8"))
            operation_name: str = data["operation"]
            payload: dict[str, Any] = data["payload"]
        except Exception as exc:
            logger.error("Unparseable operation message, discarding. body=%r error=%s", body, exc)
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        logger.info(
            "Dispatching operation. operation=%s queue=%s", operation_name, self._queue_name
        )

        op = self._registry.get(operation_name)
        session = self._factory()
        try:

            def emit(
                event: str,
                message: str | None = None,
                fields: dict[str, Any] | None = None,
                level: str = "info",
            ) -> None:
                logger.info("operation.%s fields=%s", event, fields or {})

            result = op.execute(session, payload, emit=emit)
            session.commit()
            # Forward any downstream operation messages (e.g. GPU→write worker).
            for queue_name, op_payload in result.publish_operations or []:
                publish_operation(self._amqp_url, queue_name, op_payload)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            logger.info("Operation acked. operation=%s", operation_name)
        except Exception as exc:
            requeue = self._requeue_on_failure
            # CUDA OOM: free the GPU cache and requeue so the batch is retried
            # once memory is available (e.g. after other workers release theirs).
            if "CUDA out of memory" in str(exc):
                try:
                    import torch

                    torch.cuda.empty_cache()
                except Exception:
                    pass
                requeue = True
                logger.warning(
                    "CUDA OOM — cache cleared, message requeued. operation=%s", operation_name
                )
            else:
                logger.error("Operation failed. operation=%s error=%s", operation_name, exc)
            try:
                session.rollback()
            except Exception:
                pass
            channel.basic_nack(
                delivery_tag=method.delivery_tag,
                requeue=requeue,
            )
        finally:
            session.close()
