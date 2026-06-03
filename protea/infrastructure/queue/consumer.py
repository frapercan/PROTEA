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
from protea.infrastructure.orm.models.job import Job, JobEvent, JobStatus
from protea.infrastructure.queue import _failure_aggregation as _agg
from protea.infrastructure.queue.publisher import publish_operation, safe_republish_job
from protea.infrastructure.telemetry import extract_trace_context, get_tracer
from protea.workers.base_worker import BaseWorker
from protea.workers.shutdown import HeartbeatLoop, ShutdownGuard

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

# CUDA OOM retry policy for OperationConsumer. Configured via QueueTuning
# (oom_max_retries / oom_base_delay / oom_max_delay). Defaults: 5 retries,
# 5s base, 300s cap, backoff 5/10/20/40/80s, ~155s budget before DLQ.
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
        self._channel: BlockingChannel | None = None
        # F-OPS-JOBS.1: SIGTERM mid-job re-queues + republishes;
        # heartbeat keeps the lease alive while the worker is healthy.
        wt = get_tuning().worker
        self._guard = ShutdownGuard(
            self._worker._force_fail_job,
            grace_seconds=wt.worker_shutdown_grace_seconds,
            requeue=self._worker.requeue_on_shutdown,
            on_requeue=lambda jid: safe_republish_job(self._amqp_url, self._queue_name, jid),
        )
        self._heartbeat = HeartbeatLoop(
            self._worker.extend_lease,
            interval_seconds=wt.job_heartbeat_interval_seconds,
        )

    def run(self) -> None:
        signal.signal(signal.SIGINT, self._handle_stop)
        signal.signal(signal.SIGTERM, self._handle_stop)

        # Pika's default heartbeat (60s) is too short for ops that hold
        # the AMQP connection through long compute without yielding to
        # the select() loop (QuickGO crawl, embeddings, KNN). 600s gives
        # a 10x safety margin while still detecting genuinely dead peers
        # within minutes. Tunable via PROTEA_AMQP_HEARTBEAT or
        # PROTEA_TUNING__queue__amqp_heartbeat (see config/tuning.py).
        params = pika.URLParameters(self._amqp_url)
        params.heartbeat = get_tuning().queue.amqp_heartbeat
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        self._channel = channel

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
            self._guard.cancel()
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
            self._channel = None
            logger.info("Consumer stopped. queue=%s", self._queue_name)

    def _handle_stop(self, *_: object) -> None:
        """Mark the consumer stopping and arm the shutdown watchdog.

        Idempotent. If a job is in flight when the signal arrives the
        guard schedules a force-fail+exit after
        ``worker_shutdown_grace_seconds`` so the DB row never gets
        stuck in RUNNING after deploy-keeper kills the process.
        """
        if self._stop:
            return
        self._stop = True
        in_flight = self._guard.current_job_id
        logger.info(
            "Stop signal received. queue=%s in_flight=%s",
            self._queue_name,
            in_flight,
        )
        # add_callback_threadsafe queues stop_consuming on the IO loop
        # so it works whether or not a callback is mid-execution.
        channel = self._channel
        if channel is not None:
            try:
                channel.connection.add_callback_threadsafe(self._stop_consuming_safely)
            except Exception:
                try:
                    channel.stop_consuming()
                except Exception:
                    pass
        if in_flight is not None:
            self._guard.arm(in_flight)

    def _stop_consuming_safely(self) -> None:
        if self._channel is None:
            return
        try:
            self._channel.stop_consuming()
        except Exception:
            pass

    def _is_job_cancelled(self, job_id: UUID) -> bool:
        """Return True when the Job row exists and is in CANCELLED state.

        Missing jobs or transient DB failures return False so the regular
        dispatch path continues to handle them (the worker's _claim_job
        already short-circuits on missing/non-QUEUED rows).
        """
        session = self._worker._factory()
        try:
            job = session.get(Job, job_id)
            return job is not None and job.status == JobStatus.CANCELLED
        except Exception as exc:
            logger.warning(
                "Cancellation check failed; proceeding with dispatch. job_id=%s error=%s",
                job_id,
                exc,
            )
            return False
        finally:
            session.close()

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

        # T-INFRA.NACK: drop deliveries for jobs that the API/UI already
        # cancelled. Without this check the message would be pre-acked and
        # the worker would pointlessly observe the CANCELLED status inside
        # _claim_job. Nacking with requeue=False also drains stale messages
        # that survived a restart so prefetch=1 cannot deadlock on a queue
        # of orphaned cancellations.
        if self._is_job_cancelled(job_id):
            logger.info("Skipping cancelled job. job_id=%s queue=%s", job_id, self._queue_name)
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        logger.info("Dispatching job. job_id=%s queue=%s", job_id, self._queue_name)
        self._dispatch_job(channel, method, properties, job_id)

    def _dispatch_job(
        self,
        channel: BlockingChannel,
        method: Basic.Deliver,
        properties: BasicProperties,
        job_id: UUID,
    ) -> None:
        """Pre-ack the delivery, invoke the worker, and handle terminal errors.

        Keeps the pre-ack pattern that protects long-running jobs from
        RabbitMQ's ``consumer_timeout``. ``RetryLaterError`` is converted
        into an explicit republish after ``delay_seconds``; unhandled
        exceptions are logged (the message is already acked, so the queue
        cannot deadlock — failure bookkeeping lives in the Job row).
        """
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

            # F-OPS-JOBS.1: track in-flight + start the lease heartbeat.
            self._guard.track(job_id)
            self._heartbeat.start(job_id)
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
                span.record_exception(exc)
                logger.error("Job failed. job_id=%s error=%s", job_id, exc)
            finally:
                self._heartbeat.stop()
                self._guard.untrack()


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

        # See QueueConsumer.run for the heartbeat rationale: default 600s
        # tolerates long blocking ops without losing dead-peer detection.
        params = pika.URLParameters(self._amqp_url)
        params.heartbeat = get_tuning().queue.amqp_heartbeat
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

    def _is_parent_job_cancelled(self, parent_job_id: UUID | None) -> bool:
        """Return True when the parent job row exists and is CANCELLED.

        Used by ``_on_message`` to drop operation deliveries whose parent
        job has been cancelled, instead of running them and recording a
        wasted child.failed event. Transient DB errors degrade safely to
        ``False`` so a flaky lookup never blocks dispatch.
        """
        if parent_job_id is None:
            return False
        session = self._factory()
        try:
            job = session.get(Job, parent_job_id)
            return job is not None and job.status == JobStatus.CANCELLED
        except Exception as exc:
            logger.warning(
                "Parent cancellation check failed; proceeding with dispatch. "
                "parent_job_id=%s error=%s",
                parent_job_id,
                exc,
            )
            return False
        finally:
            session.close()

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

        # T-INFRA.NACK: short-circuit cancelled parent jobs. Without this
        # the operation would run, commit half-results, then write a
        # child.failed event. Nacking with requeue=False evicts the
        # message from the queue so prefetch=1 cannot deadlock on a
        # backlog of cancelled-job deliveries.
        if self._is_parent_job_cancelled(decoded.parent_job_id):
            logger.info(
                "Skipping operation for cancelled parent job. operation=%s parent_job_id=%s",
                decoded.operation_name,
                decoded.parent_job_id,
            )
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        logger.info(
            "Dispatching operation. operation=%s queue=%s",
            decoded.operation_name,
            self._queue_name,
        )
        self._dispatch_operation(channel, method, properties, body, decoded)

    def _dispatch_operation(
        self,
        channel: BlockingChannel,
        method: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
        decoded: _DecodedMessage,
    ) -> None:
        """Execute the operation inside a CONSUMER span and route the result.

        ACKs on success, nacks with requeue=True on ``RetryLaterError``
        (T-INFRA.NACK: the broker retries instead of an in-process sleep),
        delegates CUDA OOM to the backoff handler, and routes other
        exceptions to ``_handle_general_failure`` (which nacks with the
        configured requeue flag).
        """
        # T5.1b: open a CONSUMER span using the inbound traceparent so
        # the operation span chains under the dispatching HTTP/worker
        # span. ``decoded.operation_name`` is part of the span name so
        # OTel UIs can filter directly by op.
        with _consumer_span(self._queue_name, properties, operation=decoded.operation_name) as span:
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
            except RetryLaterError as exc:
                self._handle_retry_later(channel, method, session, decoded, span, exc)
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
    def _handle_retry_later(
        channel: BlockingChannel,
        method: Basic.Deliver,
        session: Session,
        decoded: _DecodedMessage,
        span: Any,
        exc: RetryLaterError,
    ) -> None:
        """Nack with requeue=True on ``RetryLaterError``.

        T-INFRA.NACK: an operation raising ``RetryLaterError`` is an
        explicit retry signal (e.g. GPU temporarily busy), not a failure.
        The broker is asked to redeliver instead of running an
        in-process sleep/republish that would hold the prefetch slot.
        """
        span.record_exception(exc)
        try:
            session.rollback()
        except Exception:
            pass
        logger.info(
            "Operation requested retry. operation=%s reason=%s",
            decoded.operation_name,
            exc,
        )
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    @staticmethod
    def _decode_message(body: bytes, properties: BasicProperties) -> _DecodedMessage | None:
        """Parse the AMQP delivery into a validated bundle.

        Returns ``None`` when the body is not valid JSON or is missing
        ``operation`` / ``payload``; the caller dead-letters in that case.
        """
        try:
            data = json.loads(body.decode("utf-8"))
            operation_name: str = data["operation"]
            payload: dict[str, Any] = data["payload"]
        except Exception as exc:
            logger.error("Unparseable operation message, discarding. body=%r error=%s", body, exc)
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
                operation_name,
                republish_exc,
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
        """F-OPS-CHILD-FAILED-EMIT (2026-05-27): emit structured
        ``child.failed`` (pair_id + error_class + truncated message)
        on a fresh session via ``self._factory``, aggregate, nack."""
        logger.error("Operation failed. operation=%s error=%s", decoded.operation_name, exc)
        error_message = str(exc)[:500]
        fields = _agg.build_child_failed_fields(
            decoded.operation_name,
            decoded.payload,
            exc,
            error_message,
        )
        self._emit_parent_event(
            decoded.parent_job_id,
            "child.failed",
            error_message,
            fields,
            level="error",
        )
        _agg.maybe_aggregate_parent_failure(
            decoded.parent_job_id,
            exc,
            session_factory=self._factory,
            make_raw_emit=self._make_raw_emit,
        )
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=self._requeue_on_failure)

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
