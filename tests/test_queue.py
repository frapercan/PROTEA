"""
Unit tests for the queue consumer and publisher.
Pika is fully mocked — no RabbitMQ server required.
"""
from __future__ import annotations

import json
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

    def test_worker_failure_nacks_without_requeue_by_default(self):
        consumer = _consumer(_make_worker(raises=RuntimeError("boom")), requeue_on_failure=False)

        consumer._on_message(self.channel, _make_method(7), self.properties, _encode(uuid4()))

        self.channel.basic_nack.assert_called_once_with(delivery_tag=7, requeue=False)
        self.channel.basic_ack.assert_not_called()

    def test_worker_failure_nacks_with_requeue_when_configured(self):
        consumer = _consumer(_make_worker(raises=RuntimeError("boom")), requeue_on_failure=True)

        consumer._on_message(self.channel, _make_method(3), self.properties, _encode(uuid4()))

        self.channel.basic_nack.assert_called_once_with(delivery_tag=3, requeue=True)

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

    def test_run_declares_queue(self):
        consumer = _consumer()
        conn, channel = self._mock_pika(consumer)
        channel.queue_declare.assert_called_once_with(queue="test.jobs", durable=True)

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

        with patch("protea.infrastructure.queue.publisher.pika.BlockingConnection", return_value=conn):
            publish_job("amqp://localhost/", "test.jobs", job_id)

        channel.basic_publish.assert_called_once()
        kwargs = channel.basic_publish.call_args.kwargs
        assert kwargs["routing_key"] == "test.jobs"
        body = json.loads(kwargs["body"].decode())
        assert body["job_id"] == str(job_id)

    def test_closes_connection_on_success(self):
        conn = MagicMock()
        channel = MagicMock()
        conn.channel.return_value = channel
        conn.is_open = True

        with patch("protea.infrastructure.queue.publisher.pika.BlockingConnection", return_value=conn):
            publish_job("amqp://localhost/", "q", uuid4())

        conn.close.assert_called_once()

    def test_closes_connection_on_exception(self):
        conn = MagicMock()
        channel = MagicMock()
        channel.basic_publish.side_effect = RuntimeError("broker down")
        conn.channel.return_value = channel
        conn.is_open = True

        with patch("protea.infrastructure.queue.publisher.pika.BlockingConnection", return_value=conn):
            with pytest.raises(RuntimeError, match="broker down"):
                publish_job("amqp://localhost/", "q", uuid4())

        conn.close.assert_called_once()

    def test_declares_durable_queue(self):
        conn = MagicMock()
        channel = MagicMock()
        conn.channel.return_value = channel
        conn.is_open = False

        with patch("protea.infrastructure.queue.publisher.pika.BlockingConnection", return_value=conn):
            publish_job("amqp://localhost/", "my.queue", uuid4())

        channel.queue_declare.assert_called_once_with(queue="my.queue", durable=True)
