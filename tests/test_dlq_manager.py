"""Unit tests for protea.services.dlq_manager (F-OPS-JOBS.3).

All tests use MagicMock to stub pika so no real RabbitMQ is required.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_props(headers: dict | None = None) -> MagicMock:
    props = MagicMock()
    props.headers = headers or {}
    return props


def _make_method(delivery_tag: int = 1) -> MagicMock:
    m = MagicMock()
    m.delivery_tag = delivery_tag
    return m


def _make_body(operation: str | None = None, job_id: str | None = None) -> bytes:
    d: dict = {}
    if operation:
        d["operation"] = operation
    if job_id:
        d["job_id"] = job_id
    return json.dumps(d).encode()


def _death_header(queue: str = "protea.embeddings.write", days_ago: int = 8) -> dict:
    ts = datetime(2026, 5, 1, 0, 0, 0, tzinfo=UTC)
    return {
        "x-death": [
            {
                "queue": queue,
                "time": ts,
                "count": 1,
                "exchange": "protea.dlx",
                "reason": "rejected",
                "routing-keys": [queue],
            }
        ]
    }


# ---------------------------------------------------------------------------
# dlq_summary
# ---------------------------------------------------------------------------


class TestDlqSummary:
    @patch("protea.services.dlq_manager.pika.BlockingConnection")
    def test_summary_groups_by_operation(self, mock_conn_cls):
        from protea.services.dlq_manager import dlq_summary

        channel = MagicMock()
        conn = MagicMock()
        mock_conn_cls.return_value = conn
        conn.channel.return_value = channel

        q_ok = MagicMock()
        q_ok.method.message_count = 3
        channel.queue_declare.return_value = q_ok

        headers = _death_header("protea.embeddings.write")
        # Return 2 store_embeddings + 1 job_dispatch, then None to stop
        channel.basic_get.side_effect = [
            (_make_method(1), _make_props(headers), _make_body("store_embeddings")),
            (_make_method(2), _make_props(headers), _make_body("store_embeddings")),
            (_make_method(3), _make_props({}), _make_body(job_id="abc-123")),
            (None, None, None),
        ]

        result = dlq_summary("amqp://localhost", max_peek=10)

        assert result["total_peeked"] == 3
        assert result["queue_message_count"] == 3
        groups = result["groups"]
        # store_embeddings should be first (highest count)
        assert groups[0]["operation"] == "store_embeddings"
        assert groups[0]["count"] == 2
        assert groups[1]["operation"] == "job_dispatch"
        assert groups[1]["count"] == 1
        # All messages should be nacked with requeue=True
        assert channel.basic_nack.call_count == 3
        for c in channel.basic_nack.call_args_list:
            assert c.kwargs.get("requeue") is True

    @patch("protea.services.dlq_manager.pika.BlockingConnection")
    def test_summary_empty_queue(self, mock_conn_cls):
        from protea.services.dlq_manager import dlq_summary

        channel = MagicMock()
        conn = MagicMock()
        mock_conn_cls.return_value = conn
        conn.channel.return_value = channel

        q_ok = MagicMock()
        q_ok.method.message_count = 0
        channel.queue_declare.return_value = q_ok

        result = dlq_summary("amqp://localhost", max_peek=100)

        assert result["total_peeked"] == 0
        assert result["queue_message_count"] == 0
        assert result["groups"] == []
        channel.basic_get.assert_not_called()

    @patch("protea.services.dlq_manager.pika.BlockingConnection")
    def test_summary_connection_closed_on_exit(self, mock_conn_cls):
        from protea.services.dlq_manager import dlq_summary

        channel = MagicMock()
        conn = MagicMock()
        mock_conn_cls.return_value = conn
        conn.channel.return_value = channel

        q_ok = MagicMock()
        q_ok.method.message_count = 0
        channel.queue_declare.return_value = q_ok

        dlq_summary("amqp://localhost")

        conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# dlq_replay
# ---------------------------------------------------------------------------


class TestDlqReplay:
    @patch("protea.services.dlq_manager.pika.BlockingConnection")
    def test_replay_matching_message(self, mock_conn_cls):
        from protea.services.dlq_manager import dlq_replay

        channel = MagicMock()
        conn = MagicMock()
        mock_conn_cls.return_value = conn
        conn.channel.return_value = channel

        q_ok = MagicMock()
        q_ok.method.message_count = 2
        channel.queue_declare.return_value = q_ok

        headers = _death_header("protea.embeddings.write")
        body = _make_body("store_embeddings")
        channel.basic_get.side_effect = [
            (_make_method(1), _make_props(headers), body),
            (_make_method(2), _make_props({}), _make_body("other_op")),
            (None, None, None),
        ]

        result = dlq_replay(
            "amqp://localhost",
            operation_filter="store_embeddings",
            dry_run=False,
        )

        assert result["replayed"] == 1
        assert result["skipped"] == 1
        assert result["dry_run"] is False
        # The matching message should be published and acked
        channel.basic_publish.assert_called_once()
        assert channel.basic_ack.call_count == 1
        # The non-matching message should be nacked with requeue
        assert channel.basic_nack.call_count == 1

    @patch("protea.services.dlq_manager.pika.BlockingConnection")
    def test_replay_dry_run_no_publish(self, mock_conn_cls):
        from protea.services.dlq_manager import dlq_replay

        channel = MagicMock()
        conn = MagicMock()
        mock_conn_cls.return_value = conn
        conn.channel.return_value = channel

        q_ok = MagicMock()
        q_ok.method.message_count = 1
        channel.queue_declare.return_value = q_ok

        headers = _death_header()
        channel.basic_get.side_effect = [
            (_make_method(1), _make_props(headers), _make_body("store_embeddings")),
            (None, None, None),
        ]

        result = dlq_replay(
            "amqp://localhost",
            operation_filter="store_embeddings",
            dry_run=True,
        )

        assert result["replayed"] == 0
        assert result["would_replay"] == 1
        assert result["dry_run"] is True
        channel.basic_publish.assert_not_called()
        channel.basic_ack.assert_not_called()
        # Dry-run: matched message still nacked with requeue
        channel.basic_nack.assert_called_once()


# ---------------------------------------------------------------------------
# dlq_purge
# ---------------------------------------------------------------------------


class TestDlqPurge:
    @patch("protea.services.dlq_manager.pika.BlockingConnection")
    def test_purge_matching_message(self, mock_conn_cls):
        from protea.services.dlq_manager import dlq_purge

        channel = MagicMock()
        conn = MagicMock()
        mock_conn_cls.return_value = conn
        conn.channel.return_value = channel

        q_ok = MagicMock()
        q_ok.method.message_count = 2
        channel.queue_declare.return_value = q_ok

        headers = _death_header()
        channel.basic_get.side_effect = [
            (_make_method(1), _make_props(headers), _make_body("store_embeddings")),
            (_make_method(2), _make_props({}), _make_body("other_op")),
            (None, None, None),
        ]

        result = dlq_purge(
            "amqp://localhost",
            operation_filter="store_embeddings",
            dry_run=False,
        )

        assert result["purged"] == 1
        assert result["skipped"] == 1
        assert result["dry_run"] is False
        # The matching message should be acked (deleted)
        assert channel.basic_ack.call_count == 1
        # The non-matching should be nacked with requeue
        assert channel.basic_nack.call_count == 1

    @patch("protea.services.dlq_manager.pika.BlockingConnection")
    def test_purge_dry_run(self, mock_conn_cls):
        from protea.services.dlq_manager import dlq_purge

        channel = MagicMock()
        conn = MagicMock()
        mock_conn_cls.return_value = conn
        conn.channel.return_value = channel

        q_ok = MagicMock()
        q_ok.method.message_count = 1
        channel.queue_declare.return_value = q_ok

        headers = _death_header()
        channel.basic_get.side_effect = [
            (_make_method(1), _make_props(headers), _make_body("store_embeddings")),
            (None, None, None),
        ]

        result = dlq_purge(
            "amqp://localhost",
            operation_filter="store_embeddings",
            dry_run=True,
        )

        assert result["purged"] == 0
        assert result["would_purge"] == 1
        assert result["dry_run"] is True
        channel.basic_ack.assert_not_called()
        # Dry-run: nacked with requeue even for matching
        channel.basic_nack.assert_called_once()

    @patch("protea.services.dlq_manager.pika.BlockingConnection")
    def test_purge_no_filter_purges_all(self, mock_conn_cls):
        from protea.services.dlq_manager import dlq_purge

        channel = MagicMock()
        conn = MagicMock()
        mock_conn_cls.return_value = conn
        conn.channel.return_value = channel

        q_ok = MagicMock()
        q_ok.method.message_count = 3
        channel.queue_declare.return_value = q_ok

        headers = _death_header()
        channel.basic_get.side_effect = [
            (_make_method(1), _make_props(headers), _make_body("op_a")),
            (_make_method(2), _make_props(headers), _make_body("op_b")),
            (_make_method(3), _make_props(headers), _make_body("op_c")),
            (None, None, None),
        ]

        result = dlq_purge("amqp://localhost", dry_run=False)

        assert result["purged"] == 3
        assert result["skipped"] == 0
