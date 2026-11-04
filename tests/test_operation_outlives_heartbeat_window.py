"""An operation that outlives the broker's heartbeat window ends its worker.

WHY. ``OperationConsumer`` runs an operation on pika's IO thread, so no heartbeat
leaves while it runs. On 2026-11-04 a compute node streamed a reference pool for
35 minutes, past the two-heartbeat window. The broker had requeued all 40
messages of the batch, and the worker stayed alive, consuming nothing, invisible
to systemd and silent in its log. Had the load finished, the worker would have
committed and published a result another consumer was already producing.

WHAT THESE PIN. The window is two heartbeats less a margin, and there is none
without heartbeats. Past it, the expiry names the operation; the default expiry
logs why and exits with ``EX_TEMPFAIL`` so the unit restarts the worker. And the
consumer arms the watchdog around the operation alone: it disarms before it
commits, publishes or acks, and also when the operation raises. The timers here
are fired by hand, so no test sleeps.
"""

from __future__ import annotations

import json
import logging
from typing import Any
from unittest.mock import MagicMock

import pytest

from protea.infrastructure.queue import _operation_window as window
from protea.infrastructure.queue.consumer import OperationConsumer


class _HandTimer:
    """A timer that only fires when the test says so."""

    def __init__(self, seconds: float, fn: Any, args: tuple[str, float]) -> None:
        self.seconds, self.fn, self.args = seconds, fn, args
        self.started = self.cancelled = False

    def start(self) -> None:
        self.started = True

    def cancel(self) -> None:
        self.cancelled = True

    def fire(self) -> None:
        self.fn(*self.args)


def _watchdog(
    window_seconds: float | None,
) -> tuple[window.OperationWatchdog, list[_HandTimer], list[Any]]:
    timers: list[_HandTimer] = []
    expired: list[Any] = []

    def factory(seconds: float, fn: Any, args: tuple[str, float]) -> _HandTimer:
        timers.append(_HandTimer(seconds, fn, args))
        return timers[-1]

    dog = window.OperationWatchdog(
        window_seconds, on_expire=lambda op, s: expired.append((op, s)), timer_factory=factory
    )
    return dog, timers, expired


def test_the_window_is_two_heartbeats_less_the_margin() -> None:
    assert window.OperationWatchdog.for_heartbeat(600).window_seconds == 1140.0
    assert window.OperationWatchdog.for_heartbeat(10).window_seconds == 10.0, (
        "never shorter than one heartbeat"
    )
    assert window.OperationWatchdog.for_heartbeat(0).window_seconds is None, (
        "no heartbeats, no window: the broker never drops a silent connection"
    )


def test_an_operation_past_the_window_expires_with_its_name() -> None:
    dog, timers, expired = _watchdog(1140.0)
    dog.arm("predict_go_terms_batch")
    assert timers[0].started and timers[0].seconds == 1140.0
    timers[0].fire()
    assert expired == [("predict_go_terms_batch", 1140.0)]


def test_an_operation_that_returns_in_time_disarms_it() -> None:
    dog, timers, expired = _watchdog(1140.0)
    dog.arm("predict_go_terms_batch")
    dog.disarm()
    assert timers[0].cancelled and expired == []


def test_arming_again_replaces_the_timer_still_armed() -> None:
    dog, timers, _ = _watchdog(1140.0)
    dog.arm("first")
    dog.arm("second")
    assert timers[0].cancelled and timers[1].started and not timers[1].cancelled


def test_without_heartbeats_nothing_is_armed() -> None:
    dog, timers, _ = _watchdog(None)
    dog.arm("predict_go_terms_batch")
    dog.disarm()
    assert timers == []


def test_the_default_expiry_logs_why_and_exits_for_a_restart(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    codes: list[int] = []
    monkeypatch.setattr(window.os, "_exit", codes.append)
    with caplog.at_level(logging.ERROR, logger=window.__name__):
        window.exit_after_window("predict_go_terms_batch", 1140.0)
    assert codes == [window.EX_TEMPFAIL] == [75], (
        "a failure status every unit restarts; the node's units skip only 78 (EX_CONFIG)"
    )
    assert "outlived the broker heartbeat window" in caplog.text
    assert "operation=predict_go_terms_batch" in caplog.text


def _consumer(
    order: list[str], *, raises: Exception | None = None
) -> tuple[OperationConsumer, MagicMock]:
    op = MagicMock()
    result = MagicMock()
    result.publish_operations = []

    def execute(*_a: Any, **_k: Any) -> Any:
        order.append("execute")
        if raises is not None:
            raise raises
        return result

    op.execute.side_effect = execute
    registry = MagicMock()
    registry.get.return_value = op
    session = MagicMock()
    session.get.return_value = None
    session.commit.side_effect = lambda: order.append("commit")
    consumer = OperationConsumer(
        amqp_url="amqp://localhost/",
        queue_name="protea.predictions.batch",
        registry=registry,
        session_factory=MagicMock(return_value=session),
    )
    timers: list[_HandTimer] = []

    def factory(seconds: float, fn: Any, args: tuple[str, float]) -> _HandTimer:
        order.append(f"arm {args[0]}")
        timers.append(_HandTimer(seconds, fn, args))
        timers[-1].cancel = lambda: order.append("disarm")  # type: ignore[method-assign]
        return timers[-1]

    consumer._watchdog = window.OperationWatchdog(1140.0, timer_factory=factory)
    channel = MagicMock()
    channel.basic_ack.side_effect = lambda **_k: order.append("ack")
    return consumer, channel


def _deliver(consumer: OperationConsumer, channel: MagicMock) -> None:
    method = MagicMock()
    method.delivery_tag = 1
    props = MagicMock()
    props.headers = None
    body = json.dumps({"operation": "predict_go_terms_batch", "payload": {}}).encode()
    consumer._on_message(channel, method, props, body)


def test_the_consumer_times_the_operation_alone() -> None:
    order: list[str] = []
    consumer, channel = _consumer(order)
    _deliver(consumer, channel)
    assert order == ["arm predict_go_terms_batch", "execute", "disarm", "commit", "ack"], (
        "disarmed before the commit, the publishes and the ack, so a result is only "
        "written by a worker whose message is still its own"
    )


def test_an_operation_that_raises_disarms_the_watchdog_too() -> None:
    order: list[str] = []
    consumer, channel = _consumer(order, raises=RuntimeError("boom"))
    _deliver(consumer, channel)
    assert order[:3] == ["arm predict_go_terms_batch", "execute", "disarm"]
    assert "commit" not in order and "ack" not in order
    channel.basic_nack.assert_called_once()


def test_a_new_consumer_takes_its_window_from_the_heartbeat_tuning() -> None:
    consumer = OperationConsumer(
        amqp_url="amqp://localhost/",
        queue_name="protea.predictions.batch",
        registry=MagicMock(),
        session_factory=MagicMock(),
    )
    from protea.config.tuning import get_tuning

    heartbeat = get_tuning().queue.amqp_heartbeat
    expected = window.OperationWatchdog.for_heartbeat(heartbeat).window_seconds
    assert consumer._watchdog.window_seconds == expected
