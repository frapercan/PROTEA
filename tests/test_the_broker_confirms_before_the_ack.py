"""A publish returns only once the broker holds the message (ADR-D60, phase 3).

A consumer that publishes downstream messages and then acks its own input must not
ack anything the broker could still lose. The publisher therefore runs its channel
in confirm mode and publishes ``mandatory``, and closes the channel each attempt
instead of leaving one more open on the cached connection per publish.

The first half fakes the connection; the second talks to a real RabbitMQ, the one
the integration workflow starts, and is skipped where no broker is offered.
"""

from __future__ import annotations

import json
import os
import uuid
from unittest.mock import MagicMock, patch

import pytest

from protea.infrastructure.queue import publisher


class TestTheChannelConfirms:
    def _published(self):
        connection = MagicMock()
        channel = connection.channel.return_value
        channel.is_open = True
        with patch.object(publisher, "_get_connection", return_value=connection):
            publisher.publish_operation("amqp://x/", "q.test", {"operation": "op", "payload": {}})
        return channel

    def test_confirm_mode_is_on_before_the_publish(self) -> None:
        channel = self._published()
        names = [call[0] for call in channel.method_calls]
        assert names.index("confirm_delivery") < names.index("basic_publish")

    def test_the_message_is_mandatory(self) -> None:
        channel = self._published()
        assert channel.basic_publish.call_args.kwargs["mandatory"] is True

    def test_the_channel_is_closed_after_the_attempt(self) -> None:
        channel = self._published()
        channel.close.assert_called_once()

    def test_a_refused_publish_is_retried_and_its_channel_still_closed(self) -> None:
        import pika.exceptions

        connection = MagicMock()
        first, second = MagicMock(is_open=True), MagicMock(is_open=True)
        first.basic_publish.side_effect = pika.exceptions.NackError([])
        connection.channel.side_effect = [first, second]
        with (
            patch.object(publisher, "_get_connection", return_value=connection),
            patch.object(publisher.time, "sleep"),
        ):
            publisher.publish_operation("amqp://x/", "q.test", {"operation": "op", "payload": {}})
        first.close.assert_called_once()
        second.basic_publish.assert_called_once()


_BROKER = os.environ.get("PROTEA_TEST_AMQP_URL")


@pytest.mark.broker
@pytest.mark.skipif(not _BROKER, reason="no broker offered (PROTEA_TEST_AMQP_URL)")
def test_a_confirmed_publish_is_on_the_queue_when_it_returns() -> None:
    import pika

    queue = f"protea.test.confirm.{uuid.uuid4().hex[:8]}"
    message = {"operation": "op", "job_id": None, "payload": {"n": 1}}
    publisher.publish_operation(_BROKER, queue, message)
    publisher._close_cached_connection()
    connection = pika.BlockingConnection(pika.URLParameters(_BROKER))
    try:
        channel = connection.channel()
        method, _properties, body = channel.basic_get(queue=queue, auto_ack=True)
        assert method is not None, "the broker did not hold the message the publish confirmed"
        assert json.loads(body) == message
        channel.queue_delete(queue=queue)
    finally:
        connection.close()
