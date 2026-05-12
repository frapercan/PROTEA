from __future__ import annotations

import json
import logging
import signal
from typing import Any, NamedTuple
from uuid import UUID

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties
from sqlalchemy.orm import Session, sessionmaker

from protea.config.tuning import get_tuning
from protea.core.contracts.operation import EmitFn, RetryLaterError, make_safe_emit
from protea.core.contracts.registry import OperationRegistry
from protea.infrastructure.orm.models.job import JobEvent
from protea.infrastructure.queue.publisher import publish_operation
from protea.infrastructure.telemetry import extract_trace_context, get_tracer
from protea.workers.base_worker import BaseWorker

logger = logging.getLogger(__name__)
_TRACER = get_tracer(__name__)


def _consumer_span(
    queue_name: str,
    properties: BasicProperties,
    operation: str | None = None,
) -> Any:
    """Start a CONSUMER span linked to the producer via ``traceparent``.

    Returns a context manager whose span is the current span for the
    duration of message handling. When OTel is not installed the
    underlying tracer is a no-op stand-in (see telemetry.get_tracer).
    """
    ctx = extract_trace_context(properties.headers)
    span_name = f"amqp.process {operation}" if operation else f"amqp.process {queue_name}"
    return _TRACER.start_as_current_span(span_name, context=ctx)

_DLX_NAME = "protea.dlx"
_DLQ_NAME = "protea.dead-letter"

# CUDA OOM retry policy for OperationConsumer.
# Configured via QueueTuning (oom_max_retries / oom_base_delay /
# oom_max_delay). Defaults: 5 retries, 5s base, 300s cap, backoff
# 5/10/20/40/80s. ~155s wait budget before dead-letter.
_OOM_RETRY_HEADER = "x-oom-retry"


class ConsumerOptions(NamedTuple):
    """Tunable knobs shared by ``QueueConsumer`` / ``OperationConsumer``.

    Bundles the two AMQP-side tunables (``prefetch_count`` and
    ``requeue_on_failure``) so consumer constructors stay under the §3
    6-param ceiling. Call sites can pass ``ConsumerOptions(...)`` or omit
    it to accept the defaults (prefetch=1, no requeue on failure).
    """

    prefetch_count: int = 1
    requeue_on_failure: bool = False


class _DecodedMessage(NamedTuple):
    """Validated header + payload bundle parsed from one AMQP delivery."""

    operation_name: str
    payload: dict[str, Any]
    parent_job_id: UUID | None
    headers: dict[str, Any]
    oom_retry_count: int


def _setup_dead_letter(channel: BlockingChannel) -> None:
    """Declare the dead-letter exchange and queue (idempotent)."""
    channel.exchange_declare(exchange=_DLX_NAME, exchange_type="fanout", durable=True)
    channel.queue_declare(queue=_DLQ_NAME, durable=True)
    channel.queue_bind(queue=_DLQ_NAME, exchange=_DLX_NAME)


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
        options: ConsumerOptions = ConsumerOptions(),
    ) -> None:
        self._amqp_url = amqp_url
        self._queue_name = queue_name
        self._worker = worker
        self._prefetch_count = options.prefetch_count
        self._requeue_on_failure = options.requeue_on_failure
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

        _setup_dead_letter(channel)
        channel.queue_declare(
            queue=self._queue_name,
            durable=True,
            arguments={"x-dead-letter-exchange": _DLX_NAME},
        )
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

        # T5.1b: open a CONSUMER span linked to the producer via the
        # ``traceparent`` header so the job span stitches under the
        # originating HTTP request.
        with _consumer_span(self._queue_name, properties) as span:
            span.set_attribute("messaging.system", "rabbitmq")
            span.set_attribute("messaging.destination", self._queue_name)
            span.set_attribute("messaging.operation", "process")
            span.set_attribute("protea.job_id", str(job_id))

            # ACK before execution so long-running jobs don't hit RabbitMQ's
            # consumer_timeout. The job is already recorded as RUNNING in the DB,
            # so a worker crash can be detected and recovered externally.
            channel.basic_ack(delivery_tag=method.delivery_tag)
            logger.info("Job acked. job_id=%s", job_id)

            try:
                self._worker.handle_job(job_id)
            except RetryLaterError as exc:
                delay = exc.delay_seconds
                logger.info(
                    "Job will retry in %ss. job_id=%s reason=%s", delay, job_id, exc
                )
                channel.connection.sleep(delay)
                channel.basic_publish(
                    exchange="",
                    routing_key=self._queue_name,
                    body=json.dumps({"job_id": str(job_id)}).encode(),
                    properties=pika.BasicProperties(delivery_mode=2),
                )
                logger.info(
                    "Job re-published. job_id=%s queue=%s", job_id, self._queue_name
                )
            except Exception as exc:
                span.record_exception(exc)
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
        options: ConsumerOptions = ConsumerOptions(),
    ) -> None:
        self._amqp_url = amqp_url
        self._queue_name = queue_name
        self._registry = registry
        self._factory = session_factory
        self._prefetch_count = options.prefetch_count
        self._requeue_on_failure = options.requeue_on_failure
        self._stop = False

    def run(self) -> None:
        signal.signal(signal.SIGINT, self._handle_stop)
        signal.signal(signal.SIGTERM, self._handle_stop)

        params = pika.URLParameters(self._amqp_url)
        params.heartbeat = 3600
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        _setup_dead_letter(channel)
        channel.queue_declare(
            queue=self._queue_name,
            durable=True,
            arguments={"x-dead-letter-exchange": _DLX_NAME},
        )
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

        decoded = self._decode_message(body, properties)
        if decoded is None:
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return
        logger.info(
            "Dispatching operation. operation=%s queue=%s",
            decoded.operation_name,
            self._queue_name,
        )

        # T5.1b: open a CONSUMER span using the inbound traceparent so
        # the operation span chains under the dispatching HTTP/worker
        # span. ``decoded.operation_name`` is part of the span name so
        # OTel UIs can filter directly by op.
        with _consumer_span(
            self._queue_name, properties, operation=decoded.operation_name
        ) as span:
            span.set_attribute("messaging.system", "rabbitmq")
            span.set_attribute("messaging.destination", self._queue_name)
            span.set_attribute("messaging.operation", "process")
            span.set_attribute("protea.operation", decoded.operation_name)
            if decoded.parent_job_id is not None:
                span.set_attribute("protea.parent_job_id", str(decoded.parent_job_id))

            op = self._registry.get(decoded.operation_name)
            session = self._factory()
            try:
                emit = make_safe_emit(self._make_raw_emit(decoded.parent_job_id))
                result = op.execute(session, decoded.payload, emit=emit)
                session.commit()
                # Forward any downstream operation messages (e.g. GPU→write worker).
                for queue_name, op_payload in result.publish_operations or []:
                    publish_operation(self._amqp_url, queue_name, op_payload)
                channel.basic_ack(delivery_tag=method.delivery_tag)
                logger.info("Operation acked. operation=%s", decoded.operation_name)
            except Exception as exc:
                span.record_exception(exc)
                try:
                    session.rollback()
                except Exception:
                    pass
                if "CUDA out of memory" in str(exc):
                    self._handle_cuda_oom(channel, method, body, decoded, exc)
                else:
                    self._handle_general_failure(channel, method, decoded, exc)
            finally:
                session.close()

    @staticmethod
    def _decode_message(
        body: bytes, properties: BasicProperties
    ) -> _DecodedMessage | None:
        """Parse the AMQP delivery into a validated bundle.

        Returns ``None`` when the body is not valid JSON or is missing
        ``operation`` / ``payload``; the caller dead-letters in that case.
        """
        try:
            data = json.loads(body.decode("utf-8"))
            operation_name: str = data["operation"]
            payload: dict[str, Any] = data["payload"]
        except Exception as exc:
            logger.error(
                "Unparseable operation message, discarding. body=%r error=%s", body, exc
            )
            return None
        parent_job_id: UUID | None = None
        raw_job_id = data.get("job_id")
        if raw_job_id:
            try:
                parent_job_id = UUID(raw_job_id)
            except (ValueError, TypeError):
                pass
        headers = dict(properties.headers or {})
        return _DecodedMessage(
            operation_name=operation_name,
            payload=payload,
            parent_job_id=parent_job_id,
            headers=headers,
            oom_retry_count=int(headers.get(_OOM_RETRY_HEADER, 0)),
        )

    def _make_raw_emit(self, parent_job_id: UUID | None) -> EmitFn:
        """Build the raw emit closure that streams operation events to the
        parent job's ``JobEvent`` log via fresh per-event sessions."""

        def raw_emit(
            event: str,
            message: str | None = None,
            fields: dict[str, Any] | None = None,
            level: str = "info",
        ) -> None:
            logger.info("operation.%s fields=%s", event, fields or {})
            if parent_job_id is None:
                return
            event_session = self._factory()
            try:
                event_session.add(
                    JobEvent(
                        job_id=parent_job_id,
                        event=f"child.{event}",
                        message=message,
                        fields=fields or {},
                        level=level,
                    )
                )
                event_session.commit()
            finally:
                event_session.close()

        return raw_emit  # type: ignore[return-value]

    def _handle_cuda_oom(
        self,
        channel: BlockingChannel,
        method: Basic.Deliver,
        body: bytes,
        decoded: _DecodedMessage,
        exc: BaseException,
    ) -> None:
        """Free the GPU cache and either republish with backoff or dead-letter.

        While ``decoded.oom_retry_count`` is below ``oom_max_retries`` the
        message is republished with an incremented header after an
        exponential backoff (heartbeat-safe sleep). Past the cap the
        message is dead-lettered so an impossible batch size cannot burn
        the GPU forever.
        """
        try:
            import torch

            torch.cuda.empty_cache()
        except Exception:
            pass

        qsettings = get_tuning().queue
        operation_name = decoded.operation_name
        if decoded.oom_retry_count < qsettings.oom_max_retries:
            if self._republish_oom(channel, method, body, decoded, qsettings):
                return
            # republish failed → fall through to dead-letter path

        logger.error(
            "CUDA OOM retries exhausted — dead-lettering. operation=%s retries=%d",
            operation_name,
            decoded.oom_retry_count,
        )
        self._emit_parent_event(
            decoded.parent_job_id,
            "child.cuda_oom_dead_letter",
            f"CUDA OOM on {operation_name} after {decoded.oom_retry_count} retries; "
            f"message dead-lettered",
            {
                "operation": operation_name,
                "retries": decoded.oom_retry_count,
                "error": str(exc)[:500],
            },
            level="error",
        )
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def _republish_oom(
        self,
        channel: BlockingChannel,
        method: Basic.Deliver,
        body: bytes,
        decoded: _DecodedMessage,
        qsettings: Any,
    ) -> bool:
        """Emit retry event, sleep with heartbeats, republish with bumped header.

        Returns ``True`` on republish+ack, ``False`` on republish failure
        (caller falls through to dead-letter).
        """
        next_count = decoded.oom_retry_count + 1
        delay = min(
            qsettings.oom_base_delay * (2**decoded.oom_retry_count),
            qsettings.oom_max_delay,
        )
        operation_name = decoded.operation_name
        logger.warning(
            "CUDA OOM: backing off %ds (retry %d/%d). operation=%s",
            delay,
            next_count,
            qsettings.oom_max_retries,
            operation_name,
        )
        self._emit_parent_event(
            decoded.parent_job_id,
            "child.cuda_oom_retry",
            f"CUDA OOM on {operation_name}; retry {next_count}/{qsettings.oom_max_retries} "
            f"after {delay}s backoff",
            {
                "operation": operation_name,
                "retry_count": next_count,
                "max_retries": qsettings.oom_max_retries,
                "delay_seconds": delay,
            },
            level="warning",
        )
        try:
            channel.connection.sleep(delay)
        except Exception:
            pass
        new_headers = {**decoded.headers, _OOM_RETRY_HEADER: next_count}
        try:
            self._publish_persistent(channel, body, new_headers, method.delivery_tag)
            return True
        except Exception as republish_exc:
            logger.error(
                "Failed to republish OOM message; dead-lettering. operation=%s error=%s",
                operation_name, republish_exc,
            )
            return False

    def _publish_persistent(
        self,
        channel: BlockingChannel,
        body: bytes,
        headers: dict[str, Any],
        delivery_tag: int,
    ) -> None:
        """Republish ``body`` with persistent delivery + ``headers`` and ack
        the original delivery."""
        channel.basic_publish(
            exchange="",
            routing_key=self._queue_name,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                headers=headers,
            ),
        )
        channel.basic_ack(delivery_tag=delivery_tag)

    def _handle_general_failure(
        self,
        channel: BlockingChannel,
        method: Basic.Deliver,
        decoded: _DecodedMessage,
        exc: BaseException,
    ) -> None:
        """Non-OOM failure: emit ``child.failed`` and nack with the configured
        requeue flag."""
        operation_name = decoded.operation_name
        logger.error("Operation failed. operation=%s error=%s", operation_name, exc)
        self._emit_parent_event(
            decoded.parent_job_id,
            "child.failed",
            str(exc)[:2000],
            {
                "operation": operation_name,
                "error_code": exc.__class__.__name__,
            },
            level="error",
        )
        channel.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue=self._requeue_on_failure,
        )

    def _emit_parent_event(
        self,
        parent_job_id: UUID | None,
        event: str,
        message: str | None,
        fields: dict[str, Any],
        *,
        level: str = "info",
    ) -> None:
        """Write a ``JobEvent`` row against the parent job (best-effort)."""
        if parent_job_id is None:
            return
        session = self._factory()
        try:
            session.add(
                JobEvent(
                    job_id=parent_job_id,
                    event=event,
                    message=message,
                    fields=fields,
                    level=level,
                )
            )
            session.commit()
        except Exception as exc:
            logger.warning(
                "Failed to write event to parent job. parent_job_id=%s event=%s error=%s",
                parent_job_id,
                event,
                exc,
            )
            try:
                session.rollback()
            except Exception:
                pass
        finally:
            session.close()
