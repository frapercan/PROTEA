"""
Unit tests for the queue consumer and publisher.
Pika is fully mocked — no RabbitMQ server required.
"""
from __future__ import annotations

import json
import threading
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest

from protea.infrastructure.queue.consumer import QueueConsumer
from protea.infrastructure.queue.publisher import publish_job

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_worker(raises=None):
    worker = MagicMock()
    if raises:
        worker.handle_job.side_effect = raises
    return worker


def _make_method(delivery_tag=1):
    method = MagicMock()
    method.delivery_tag = delivery_tag
    return method


def _encode(job_id: UUID) -> bytes:
    return json.dumps({"job_id": str(job_id)}).encode("utf-8")


def _consumer(worker=None, requeue_on_failure=False):
    return QueueConsumer(
        amqp_url="amqp://guest:guest@localhost/",
        queue_name="test.jobs",
        worker=worker or _make_worker(),
        requeue_on_failure=requeue_on_failure,
    )


# ---------------------------------------------------------------------------
# QueueConsumer._on_message
# ---------------------------------------------------------------------------

class TestOnMessage:
    def setup_method(self):
        self.channel = MagicMock()
        self.properties = MagicMock()

    def test_valid_message_calls_handle_job(self):
        job_id = uuid4()
        worker = _make_worker()
        consumer = _consumer(worker)

        consumer._on_message(self.channel, _make_method(), self.properties, _encode(job_id))

        worker.handle_job.assert_called_once_with(job_id)

    def test_valid_message_is_acked_on_success(self):
        job_id = uuid4()
        consumer = _consumer()

        consumer._on_message(self.channel, _make_method(42), self.properties, _encode(job_id))

        self.channel.basic_ack.assert_called_once_with(delivery_tag=42)
        self.channel.basic_nack.assert_not_called()

    def test_worker_failure_acks_before_execution(self):
        # QueueConsumer ACKs before execution to avoid RabbitMQ consumer_timeout
        # on long-running jobs. Failed jobs are recorded in the DB; no nack is sent.
        consumer = _consumer(_make_worker(raises=RuntimeError("boom")), requeue_on_failure=False)

        consumer._on_message(self.channel, _make_method(7), self.properties, _encode(uuid4()))

        self.channel.basic_ack.assert_called_once_with(delivery_tag=7)
        self.channel.basic_nack.assert_not_called()

    def test_worker_failure_acks_before_execution_regardless_of_requeue_flag(self):
        consumer = _consumer(_make_worker(raises=RuntimeError("boom")), requeue_on_failure=True)

        consumer._on_message(self.channel, _make_method(3), self.properties, _encode(uuid4()))

        self.channel.basic_ack.assert_called_once_with(delivery_tag=3)
        self.channel.basic_nack.assert_not_called()

    def test_invalid_json_body_nacks_without_requeue(self):
        consumer = _consumer()

        consumer._on_message(self.channel, _make_method(5), self.properties, b"not json at all")

        self.channel.basic_nack.assert_called_once_with(delivery_tag=5, requeue=False)
        self.channel.basic_ack.assert_not_called()

    def test_missing_job_id_field_nacks(self):
        consumer = _consumer()
        body = json.dumps({"wrong_key": "value"}).encode()

        consumer._on_message(self.channel, _make_method(9), self.properties, body)

        self.channel.basic_nack.assert_called_once_with(delivery_tag=9, requeue=False)

    def test_invalid_uuid_nacks(self):
        consumer = _consumer()
        body = json.dumps({"job_id": "not-a-uuid"}).encode()

        consumer._on_message(self.channel, _make_method(2), self.properties, body)

        self.channel.basic_nack.assert_called_once_with(delivery_tag=2, requeue=False)

    def test_stop_flag_nacks_with_requeue(self):
        consumer = _consumer()
        consumer._stop = True

        consumer._on_message(self.channel, _make_method(11), self.properties, _encode(uuid4()))

        self.channel.basic_nack.assert_called_once_with(delivery_tag=11, requeue=True)
        self.channel.basic_ack.assert_not_called()


# ---------------------------------------------------------------------------
# QueueConsumer.run (pika connection fully mocked)
# ---------------------------------------------------------------------------

class TestConsumerRun:
    def _mock_pika(self, consumer):
        """
        Patch pika.BlockingConnection so that start_consuming() immediately
        returns (simulates an empty queue / graceful exit).
        """
        conn = MagicMock()
        channel = MagicMock()
        conn.channel.return_value = channel
        conn.is_open = False  # skip close attempt

        with patch("protea.infrastructure.queue.consumer.pika.BlockingConnection", return_value=conn):
            consumer.run()

        return conn, channel

    def test_run_declares_queue_with_dlx(self):
        consumer = _consumer()
        conn, channel = self._mock_pika(consumer)
        # DLQ + main queue
        assert channel.queue_declare.call_count == 2
        channel.queue_declare.assert_any_call(queue="protea.dead-letter", durable=True)
        channel.queue_declare.assert_any_call(
            queue="test.jobs",
            durable=True,
            arguments={"x-dead-letter-exchange": "protea.dlx"},
        )
        channel.exchange_declare.assert_called_once_with(
            exchange="protea.dlx", exchange_type="fanout", durable=True
        )

    def test_run_sets_prefetch(self):
        consumer = _consumer()
        conn, channel = self._mock_pika(consumer)
        channel.basic_qos.assert_called_once_with(prefetch_count=1)

    def test_run_registers_on_message_callback(self):
        consumer = _consumer()
        conn, channel = self._mock_pika(consumer)
        channel.basic_consume.assert_called_once()
        kwargs = channel.basic_consume.call_args.kwargs
        assert kwargs["queue"] == "test.jobs"
        assert kwargs["auto_ack"] is False
        assert callable(kwargs["on_message_callback"])

    def test_handle_stop_sets_flag(self):
        consumer = _consumer()
        assert consumer._stop is False
        consumer._handle_stop()
        assert consumer._stop is True


# ---------------------------------------------------------------------------
# publish_job
# ---------------------------------------------------------------------------

class TestPublishJob:
    def test_publishes_correct_body(self):
        job_id = uuid4()
        conn = MagicMock()
        channel = MagicMock()
        conn.channel.return_value = channel
        conn.is_open = True

        with patch("protea.infrastructure.queue.publisher.pika.BlockingConnection", return_value=conn), \
             patch("protea.infrastructure.queue.publisher._local", threading.local()):
            publish_job("amqp://localhost/", "test.jobs", job_id)

        channel.basic_publish.assert_called_once()
        kwargs = channel.basic_publish.call_args.kwargs
        assert kwargs["routing_key"] == "test.jobs"
        body = json.loads(kwargs["body"].decode())
        assert body["job_id"] == str(job_id)

    def test_reuses_connection_on_success(self):
        """With thread-local connection reuse, conn is NOT closed after a successful publish."""
        conn = MagicMock()
        channel = MagicMock()
        conn.channel.return_value = channel
        conn.is_open = True

        with patch("protea.infrastructure.queue.publisher.pika.BlockingConnection", return_value=conn), \
             patch("protea.infrastructure.queue.publisher._local", threading.local()):
            publish_job("amqp://localhost/", "q", uuid4())

        conn.close.assert_not_called()

    def test_closes_connection_on_exception(self):
        conn = MagicMock()
        channel = MagicMock()
        channel.basic_publish.side_effect = RuntimeError("broker down")
        conn.channel.return_value = channel
        conn.is_open = True

        with patch("protea.infrastructure.queue.publisher.pika.BlockingConnection", return_value=conn), \
             patch("protea.infrastructure.queue.publisher.time.sleep"), \
             patch("protea.infrastructure.queue.publisher._local", threading.local()):
            with pytest.raises(RuntimeError, match="Failed to publish to queue"):
                publish_job("amqp://localhost/", "q", uuid4())

        # _close_cached_connection calls conn.close() once per failed attempt (5 total)
        assert conn.close.call_count == 5

    def test_declares_durable_queue(self):
        conn = MagicMock()
        channel = MagicMock()
        conn.channel.return_value = channel
        conn.is_open = False

        with patch("protea.infrastructure.queue.publisher.pika.BlockingConnection", return_value=conn), \
             patch("protea.infrastructure.queue.publisher._local", threading.local()):
            publish_job("amqp://localhost/", "my.queue", uuid4())

        channel.queue_declare.assert_called_once_with(
            queue="my.queue", durable=True, arguments={"x-dead-letter-exchange": "protea.dlx"}
        )

    def test_exponential_backoff_delays(self):
        """Verify that the publisher uses exponential backoff between retries."""
        conn = MagicMock()
        channel = MagicMock()
        channel.basic_publish.side_effect = RuntimeError("broker down")
        conn.channel.return_value = channel
        conn.is_open = True

        sleep_calls = []
        with patch("protea.infrastructure.queue.publisher.pika.BlockingConnection", return_value=conn), \
             patch("protea.infrastructure.queue.publisher.time.sleep", side_effect=lambda d: sleep_calls.append(d)), \
             patch("protea.infrastructure.queue.publisher._local", threading.local()):
            with pytest.raises(RuntimeError, match="Failed to publish"):
                publish_job("amqp://localhost/", "q", uuid4())

        # 5 attempts → 4 sleeps: 1, 2, 4, 8
        assert sleep_calls == [1, 2, 4, 8]


# ---------------------------------------------------------------------------
# OperationConsumer — emit writes to parent job
# ---------------------------------------------------------------------------

class TestOperationConsumerEmit:
    """Verify that OperationConsumer's emit writes JobEvent rows to the parent job."""

    def test_emit_writes_job_event_on_parent(self):
        from protea.core.contracts.operation import OperationResult
        from protea.infrastructure.queue.consumer import OperationConsumer

        parent_job_id = uuid4()

        # Mock registry and operation
        op = MagicMock()
        op.execute.return_value = OperationResult()
        registry = MagicMock()
        registry.get.return_value = op

        # Track sessions created by the factory
        sessions = []
        def make_session():
            s = MagicMock()
            sessions.append(s)
            return s
        factory = MagicMock(side_effect=make_session)

        consumer = OperationConsumer(
            amqp_url="amqp://localhost/",
            queue_name="test.queue",
            registry=registry,
            session_factory=factory,
        )

        # Build a valid message with a parent job_id
        body = json.dumps({
            "operation": "test_op",
            "job_id": str(parent_job_id),
            "payload": {"key": "value"},
        }).encode()

        channel = MagicMock()
        method = _make_method()
        props = MagicMock()

        consumer._on_message(channel, method, props, body)

        # Operation should have been called
        op.execute.assert_called_once()
        channel.basic_ack.assert_called_once()

    def test_emit_records_failure_on_parent(self):
        from protea.infrastructure.queue.consumer import OperationConsumer

        parent_job_id = uuid4()

        # Operation that raises
        op = MagicMock()
        op.execute.side_effect = ValueError("boom")
        registry = MagicMock()
        registry.get.return_value = op

        sessions = []
        def make_session():
            s = MagicMock()
            sessions.append(s)
            return s
        factory = MagicMock(side_effect=make_session)

        consumer = OperationConsumer(
            amqp_url="amqp://localhost/",
            queue_name="test.queue",
            registry=registry,
            session_factory=factory,
        )

        body = json.dumps({
            "operation": "test_op",
            "job_id": str(parent_job_id),
            "payload": {},
        }).encode()

        channel = MagicMock()
        method = _make_method()
        props = MagicMock()

        consumer._on_message(channel, method, props, body)

        # Should nack (not requeue by default)
        channel.basic_nack.assert_called_once()
        # Should have created a session to write the error event
        # At least: 1 execution session + 1 error event session
        assert len(sessions) >= 2
        # The error event session should have had .add() called with a JobEvent
        error_session = sessions[-1]
        error_session.add.assert_called_once()
        error_session.commit.assert_called_once()


# ---------------------------------------------------------------------------
# OperationConsumer._on_message — extended coverage
# ---------------------------------------------------------------------------

class TestOperationConsumerOnMessage:
    """Cover uncovered lines in OperationConsumer._on_message."""

    def _make_consumer(self, op=None, raises=None, requeue_on_failure=False):
        from protea.core.contracts.operation import OperationResult
        from protea.infrastructure.queue.consumer import OperationConsumer

        if op is None:
            op = MagicMock()
            if raises:
                op.execute.side_effect = raises
            else:
                op.execute.return_value = OperationResult()

        registry = MagicMock()
        registry.get.return_value = op

        sessions = []
        def make_session():
            s = MagicMock()
            sessions.append(s)
            return s

        factory = MagicMock(side_effect=make_session)

        consumer = OperationConsumer(
            amqp_url="amqp://localhost/",
            queue_name="test.ops",
            registry=registry,
            session_factory=factory,
            requeue_on_failure=requeue_on_failure,
        )
        return consumer, sessions, factory, op

    def _body(self, operation="test_op", job_id=None, payload=None):
        msg = {
            "operation": operation,
            "payload": payload or {},
        }
        if job_id is not None:
            msg["job_id"] = str(job_id)
        return json.dumps(msg).encode()

    def test_successful_operation_acks(self):
        consumer, sessions, _, op = self._make_consumer()
        channel = MagicMock()
        method = _make_method(10)

        consumer._on_message(channel, method, MagicMock(), self._body())

        op.execute.assert_called_once()
        channel.basic_ack.assert_called_once_with(delivery_tag=10)
        channel.basic_nack.assert_not_called()

    def test_failed_operation_nacks_without_requeue(self):
        consumer, sessions, _, _ = self._make_consumer(raises=ValueError("oops"))
        channel = MagicMock()
        method = _make_method(20)

        consumer._on_message(channel, method, MagicMock(), self._body())

        channel.basic_nack.assert_called_once_with(delivery_tag=20, requeue=False)
        channel.basic_ack.assert_not_called()

    def test_failed_operation_nacks_with_requeue_when_flag_set(self):
        consumer, sessions, _, _ = self._make_consumer(
            raises=ValueError("oops"), requeue_on_failure=True
        )
        channel = MagicMock()
        method = _make_method(21)

        consumer._on_message(channel, method, MagicMock(), self._body())

        channel.basic_nack.assert_called_once_with(delivery_tag=21, requeue=True)

    def test_cuda_oom_clears_cache_and_requeues(self):
        exc = RuntimeError("CUDA out of memory. Tried to allocate 2 GiB")
        consumer, sessions, _, _ = self._make_consumer(raises=exc)
        channel = MagicMock()
        method = _make_method(30)

        with patch("protea.infrastructure.queue.consumer.torch", create=True):
            # Import torch inside the handler — we patch at module level
            import sys
            mock_module = MagicMock()
            with patch.dict(sys.modules, {"torch": mock_module}):
                consumer._on_message(channel, method, MagicMock(), self._body())

        # Should requeue regardless of requeue_on_failure flag
        channel.basic_nack.assert_called_once()
        call_kwargs = channel.basic_nack.call_args.kwargs
        assert call_kwargs["requeue"] is True

    def test_unparseable_message_nacks_without_requeue(self):
        consumer, _, _, _ = self._make_consumer()
        channel = MagicMock()
        method = _make_method(40)

        consumer._on_message(channel, method, MagicMock(), b"not json")

        channel.basic_nack.assert_called_once_with(delivery_tag=40, requeue=False)
        channel.basic_ack.assert_not_called()

    def test_missing_operation_key_nacks(self):
        consumer, _, _, _ = self._make_consumer()
        channel = MagicMock()
        method = _make_method(41)
        body = json.dumps({"payload": {}}).encode()

        consumer._on_message(channel, method, MagicMock(), body)

        channel.basic_nack.assert_called_once_with(delivery_tag=41, requeue=False)

    def test_stop_flag_nacks_with_requeue(self):
        consumer, _, _, _ = self._make_consumer()
        consumer._stop = True
        channel = MagicMock()
        method = _make_method(50)

        consumer._on_message(channel, method, MagicMock(), self._body())

        channel.basic_nack.assert_called_once_with(delivery_tag=50, requeue=True)
        channel.basic_ack.assert_not_called()

    def test_emit_writes_job_event_to_parent_session(self):
        """When operation calls emit, a JobEvent is written to a separate session."""
        from protea.core.contracts.operation import OperationResult

        parent_id = uuid4()

        def _execute(session, payload, *, emit):
            emit("progress", "doing stuff", {"step": 1}, "info")
            return OperationResult()

        op = MagicMock()
        op.execute.side_effect = _execute

        consumer, sessions, _, _ = self._make_consumer(op=op)
        channel = MagicMock()
        method = _make_method()

        consumer._on_message(channel, method, MagicMock(), self._body(job_id=parent_id))

        # sessions: [0]=execution session, [1]=emit event session
        assert len(sessions) >= 2
        emit_session = sessions[1]
        emit_session.add.assert_called_once()
        emit_session.commit.assert_called_once()
        emit_session.close.assert_called_once()

    def test_emit_without_parent_job_id_only_logs(self):
        """When no job_id in message, emit should not create an event session."""
        from protea.core.contracts.operation import OperationResult

        def _execute(session, payload, *, emit):
            emit("progress", "no parent", {}, "info")
            return OperationResult()

        op = MagicMock()
        op.execute.side_effect = _execute

        consumer, sessions, _, _ = self._make_consumer(op=op)
        channel = MagicMock()
        method = _make_method()

        # Message without job_id
        body = json.dumps({"operation": "test_op", "payload": {}}).encode()
        consumer._on_message(channel, method, MagicMock(), body)

        # Only the execution session should have been created (no event session)
        assert len(sessions) == 1

    def test_emit_session_failure_is_handled_gracefully(self):
        """If writing the event to DB fails, the operation should still complete."""
        from protea.core.contracts.operation import OperationResult

        parent_id = uuid4()

        def _execute(session, payload, *, emit):
            emit("progress", "msg", {}, "info")
            return OperationResult()

        op = MagicMock()
        op.execute.side_effect = _execute

        sessions_created = []
        def make_session():
            s = MagicMock()
            sessions_created.append(s)
            # Make the second session (emit session) fail on commit
            if len(sessions_created) == 2:
                s.commit.side_effect = RuntimeError("DB down")
            return s

        from protea.infrastructure.queue.consumer import OperationConsumer
        registry = MagicMock()
        registry.get.return_value = op
        factory = MagicMock(side_effect=make_session)

        consumer = OperationConsumer(
            amqp_url="amqp://localhost/",
            queue_name="test.ops",
            registry=registry,
            session_factory=factory,
        )
        channel = MagicMock()
        method = _make_method()

        consumer._on_message(channel, method, MagicMock(), self._body(job_id=parent_id))

        # Should still ack despite emit failure
        channel.basic_ack.assert_called_once()

    def test_publish_operations_forwarded(self):
        """Downstream publish_operations from result are forwarded via publish_operation."""
        from protea.core.contracts.operation import OperationResult

        result = OperationResult(
            publish_operations=[
                ("protea.embeddings.write", {"batch": [1, 2]}),
                ("protea.predictions.write", {"batch": [3, 4]}),
            ]
        )
        op = MagicMock()
        op.execute.return_value = result

        consumer, sessions, _, _ = self._make_consumer(op=op)
        channel = MagicMock()
        method = _make_method()

        with patch("protea.infrastructure.queue.consumer.publish_operation") as mock_pub:
            consumer._on_message(channel, method, MagicMock(), self._body())

        assert mock_pub.call_count == 2
        mock_pub.assert_any_call("amqp://localhost/", "protea.embeddings.write", {"batch": [1, 2]})
        mock_pub.assert_any_call("amqp://localhost/", "protea.predictions.write", {"batch": [3, 4]})

    def test_failed_operation_writes_error_event_to_parent(self):
        """On failure with parent_job_id, a child.failed event is written."""
        parent_id = uuid4()
        consumer, sessions, _, _ = self._make_consumer(raises=TypeError("bad type"))
        channel = MagicMock()
        method = _make_method()

        consumer._on_message(channel, method, MagicMock(), self._body(job_id=parent_id))

        # Find the error event session (last one created besides execution session)
        # sessions: [0]=execution, [1]=error event
        assert len(sessions) >= 2
        err_session = sessions[-1]
        err_session.add.assert_called_once()
        added_event = err_session.add.call_args[0][0]
        assert added_event.job_id == parent_id
        assert added_event.event == "child.failed"
        assert added_event.level == "error"
        assert "bad type" in added_event.message

    def test_invalid_job_id_in_message_is_ignored(self):
        """If job_id is not a valid UUID, parent_job_id should be None (no crash)."""
        from protea.core.contracts.operation import OperationResult

        op = MagicMock()
        op.execute.return_value = OperationResult()

        consumer, sessions, _, _ = self._make_consumer(op=op)
        channel = MagicMock()
        method = _make_method()

        body = json.dumps({
            "operation": "test_op",
            "job_id": "not-a-uuid",
            "payload": {},
        }).encode()

        consumer._on_message(channel, method, MagicMock(), body)

        # Should still succeed — only 1 session (execution), no event sessions
        channel.basic_ack.assert_called_once()
        assert len(sessions) == 1

    def test_error_event_session_rollback_on_commit_failure(self):
        """If the error event session commit fails, rollback is called."""
        parent_id = uuid4()

        sessions_created = []
        def make_session():
            s = MagicMock()
            sessions_created.append(s)
            # Make the error event session (3rd: exec + err_event) fail
            if len(sessions_created) == 2:
                s.commit.side_effect = RuntimeError("DB gone")
            return s

        from protea.infrastructure.queue.consumer import OperationConsumer
        op = MagicMock()
        op.execute.side_effect = ValueError("boom")
        registry = MagicMock()
        registry.get.return_value = op
        factory = MagicMock(side_effect=make_session)

        consumer = OperationConsumer(
            amqp_url="amqp://localhost/",
            queue_name="test.ops",
            registry=registry,
            session_factory=factory,
        )
        channel = MagicMock()
        method = _make_method()

        consumer._on_message(channel, method, MagicMock(), self._body(job_id=parent_id))

        # Error event session should have rollback called
        err_session = sessions_created[1]
        err_session.rollback.assert_called_once()
        err_session.close.assert_called_once()


# ---------------------------------------------------------------------------
# QueueConsumer._on_message — RetryLaterError handling
# ---------------------------------------------------------------------------

class TestQueueConsumerRetryLater:
    """Cover RetryLaterError handling in QueueConsumer._on_message (lines 142-151)."""

    def test_retry_later_sleeps_and_republishes(self):
        from protea.core.contracts.operation import RetryLaterError

        job_id = uuid4()
        worker = _make_worker(raises=RetryLaterError("GPU busy", delay_seconds=30))
        consumer = _consumer(worker)

        channel = MagicMock()
        method = _make_method(99)
        props = MagicMock()

        consumer._on_message(channel, method, props, _encode(job_id))

        # Should ack before execution
        channel.basic_ack.assert_called_once_with(delivery_tag=99)
        # Should sleep on the connection
        channel.connection.sleep.assert_called_once_with(30)
        # Should re-publish
        channel.basic_publish.assert_called_once()
        pub_kwargs = channel.basic_publish.call_args.kwargs
        assert pub_kwargs["routing_key"] == "test.jobs"
        body = json.loads(pub_kwargs["body"].decode())
        assert body["job_id"] == str(job_id)

    def test_shutdown_draining_nacks_with_requeue(self):
        """When _stop is set, messages are nacked with requeue=True."""
        consumer = _consumer()
        consumer._stop = True

        channel = MagicMock()
        method = _make_method(77)

        consumer._on_message(channel, method, MagicMock(), _encode(uuid4()))

        channel.basic_nack.assert_called_once_with(delivery_tag=77, requeue=True)
        channel.basic_ack.assert_not_called()


# ---------------------------------------------------------------------------
# OperationConsumer._handle_stop
# ---------------------------------------------------------------------------

class TestOperationConsumerHandleStop:
    def test_handle_stop_sets_flag(self):
        from protea.infrastructure.queue.consumer import OperationConsumer

        consumer = OperationConsumer(
            amqp_url="amqp://localhost/",
            queue_name="test.ops",
            registry=MagicMock(),
            session_factory=MagicMock(),
        )
        assert consumer._stop is False
        consumer._handle_stop()
        assert consumer._stop is True


# ---------------------------------------------------------------------------
# OperationConsumer.run (pika fully mocked)
# ---------------------------------------------------------------------------

class TestOperationConsumerRun:
    def test_run_declares_queue_and_starts_consuming(self):
        from protea.infrastructure.queue.consumer import OperationConsumer

        consumer = OperationConsumer(
            amqp_url="amqp://localhost/",
            queue_name="test.ops",
            registry=MagicMock(),
            session_factory=MagicMock(),
            prefetch_count=4,
        )

        conn = MagicMock()
        channel = MagicMock()
        conn.channel.return_value = channel
        conn.is_open = False

        with patch("protea.infrastructure.queue.consumer.pika.BlockingConnection", return_value=conn):
            consumer.run()

        channel.queue_declare.assert_any_call(
            queue="test.ops",
            durable=True,
            arguments={"x-dead-letter-exchange": "protea.dlx"},
        )
        channel.basic_qos.assert_called_once_with(prefetch_count=4)
        channel.basic_consume.assert_called_once()
        channel.start_consuming.assert_called_once()
