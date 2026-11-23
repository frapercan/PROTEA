"""A message that failed for a passing reason is retried a bounded number of times.

ADR-D60, phase 3 (the executor rules). ``RetryLaterError`` used to be requeued at
once and forever, and a lost connection used to dead-letter a message that would
have run a minute later. Both are transient now: republished after a wait with
``x-protea-retry`` raised, and dead-lettered with ``child.retry_exhausted`` at the
cap. These tests drive the operation consumer with a fake channel.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
import sqlalchemy.exc as sa_exc

from protea.config.tuning import get_tuning
from protea.core.contracts.operation import RetryLaterError
from protea.infrastructure.queue import _retry
from protea.infrastructure.queue._retry import (
    RETRY_HEADER,
    is_connection_error,
    retry_count,
    transient_delay,
)


def _consumer(raises: BaseException):
    from protea.infrastructure.queue.consumer import OperationConsumer

    op = MagicMock()
    op.execute.side_effect = raises
    registry = MagicMock()
    registry.get.return_value = op
    events: list[tuple[str, dict]] = []

    def make_session():
        session = MagicMock()
        session.get.return_value = SimpleNamespace(status="RUNNING")

        def add(row):
            if hasattr(row, "event") and row.event.startswith(("child.retry", "child.failed")):
                events.append((row.event, row.fields))

        session.add.side_effect = add
        return session

    # Only the retry and failure events: the consumer also stamps the library
    # versions on every delivery before it runs the operation.

    consumer = OperationConsumer(
        amqp_url="amqp://localhost/",
        queue_name="test.ops",
        registry=registry,
        session_factory=MagicMock(side_effect=make_session),
    )
    return consumer, events


def _deliver(consumer, headers=None, tag=7):
    channel = MagicMock()
    method = MagicMock(delivery_tag=tag)
    body = json.dumps({"operation": "op", "job_id": str(uuid4()), "payload": {}}).encode()
    consumer._on_message(channel, method, MagicMock(headers=headers), body)
    return channel


class TestTheyAreRetriedWithACount:
    def test_a_lost_database_connection_is_retried_after_a_backoff(self) -> None:
        lost = sa_exc.OperationalError("SELECT 1", {}, Exception("server closed the connection"))
        consumer, events = _consumer(lost)
        channel = _deliver(consumer, headers={RETRY_HEADER: 2})
        tuning = get_tuning().queue
        expected = min(tuning.transient_base_delay * 4, tuning.transient_max_delay)
        channel.connection.sleep.assert_called_once_with(expected)
        headers = channel.basic_publish.call_args.kwargs["properties"].headers
        assert headers[RETRY_HEADER] == 3
        channel.basic_ack.assert_called_once_with(delivery_tag=7)
        assert [name for name, _ in events] == ["child.retry"]
        assert events[0][1]["kind"] == "connection"

    def test_the_cap_dead_letters_with_an_event(self) -> None:
        consumer, events = _consumer(RetryLaterError("card busy", delay_seconds=5))
        cap = get_tuning().queue.transient_max_retries
        channel = _deliver(consumer, headers={RETRY_HEADER: cap})
        channel.basic_publish.assert_not_called()
        channel.basic_nack.assert_called_once_with(delivery_tag=7, requeue=False)
        assert [name for name, _ in events] == ["child.retry_exhausted"]
        assert events[0][1]["kind"] == "retry_later"

    def test_a_republish_that_fails_dead_letters_rather_than_holding_the_message(self) -> None:
        consumer, events = _consumer(RetryLaterError("card busy", delay_seconds=0))
        channel = MagicMock()
        channel.basic_publish.side_effect = RuntimeError("channel closed")
        body = json.dumps({"operation": "op", "job_id": str(uuid4()), "payload": {}}).encode()
        consumer._on_message(channel, MagicMock(delivery_tag=9), MagicMock(headers=None), body)
        channel.basic_nack.assert_called_once_with(delivery_tag=9, requeue=False)
        assert [name for name, _ in events] == ["child.retry", "child.retry_exhausted"]

    def test_a_real_failure_is_still_a_failure(self) -> None:
        consumer, events = _consumer(ValueError("bad payload"))
        channel = _deliver(consumer)
        channel.basic_publish.assert_not_called()
        channel.basic_nack.assert_called_once_with(delivery_tag=7, requeue=False)
        assert "child.failed" in [name for name, _ in events]


class TestWhatCountsAsTransient:
    @pytest.mark.parametrize(
        "exc",
        [
            ConnectionRefusedError("refused"),
            TimeoutError("slow"),
            sa_exc.OperationalError("x", {}, Exception("could not connect")),
            sa_exc.InterfaceError("x", {}, Exception("connection already closed")),
        ],
        ids=["refused", "timeout", "operational", "interface"],
    )
    def test_lost_connections(self, exc: BaseException) -> None:
        assert is_connection_error(exc)

    def test_a_wrapped_lost_connection(self) -> None:
        try:
            try:
                raise ConnectionResetError("reset by peer")
            except ConnectionResetError as inner:
                raise RuntimeError("upload failed") from inner
        except RuntimeError as outer:
            assert is_connection_error(outer)

    def test_a_defect_is_not(self) -> None:
        assert not is_connection_error(ValueError("bad payload"))
        assert not is_connection_error(KeyError("missing"))


class TestTheCountAndTheWait:
    def test_the_count_reads_the_header_and_tolerates_garbage(self) -> None:
        assert retry_count(None) == 0
        assert retry_count({RETRY_HEADER: 3}) == 3
        assert retry_count({RETRY_HEADER: "nonsense"}) == 0

    def test_retry_later_waits_its_own_delay_capped(self) -> None:
        tuning = SimpleNamespace(transient_base_delay=30, transient_max_delay=600)
        assert transient_delay(RetryLaterError("x", delay_seconds=45), 3, tuning) == 45
        assert transient_delay(RetryLaterError("x", delay_seconds=9000), 0, tuning) == 600

    def test_a_lost_connection_backs_off_exponentially_capped(self) -> None:
        tuning = SimpleNamespace(transient_base_delay=30, transient_max_delay=600)
        waits = [transient_delay(ConnectionError(), n, tuning) for n in range(6)]
        assert waits == [30, 60, 120, 240, 480, 600]

    def test_the_header_is_its_own_name(self) -> None:
        # Kept apart from the CUDA out-of-memory count, which has its own cap.
        assert RETRY_HEADER == "x-protea-retry" and _retry.RETRY_HEADER != "x-oom-retry"
