"""Setting the stop flag is not stopping; the IO loop has to be woken.

``test_worker_honours_sigterm`` already covers this incident, and states it
exactly: eleven of twelve workers ignored SIGTERM and had to be killed. What
it pins is that both consumers inherit ``Stoppable`` and expose ``stopped``.
Both did. ``OperationConsumer`` went on being unkillable for months anyway,
because carrying the flag was never the property in question.

``_on_message`` is the only reader of ``_stop``, so it is consulted exactly
when a message is delivered. A consumer with an empty queue is parked inside
``start_consuming`` and never reaches it: the signal sets a flag, the handler
returns, and nothing else happens until systemd escalates to SIGKILL. The
consumer that hangs is therefore the IDLE one, which is why this looked like
slow drain rather than a failure to stop.

``QueueConsumer`` never had the defect -- its handler queues
``stop_consuming`` onto the IO loop with ``add_callback_threadsafe``. So the
invariant these pin is the one that distinguished the two classes and that no
test asked for: after a stop signal, a consumer must have asked its IO loop to
return. Parametrised over both, so a third consumer inherits the requirement
instead of the defect.

Cost, 2026-09-07: four hard kills on the compute node and one on this host,
each paying its full stop timeout first. ``_OPERATION_QUEUES`` in
``scripts/worker.py`` is every compute queue in the system, and all of them
ran the broken handler.
"""

from __future__ import annotations

import pytest

from protea.core.contracts.registry import OperationRegistry
from protea.infrastructure.queue.consumer import OperationConsumer, QueueConsumer


class _FakeConnection:
    def __init__(self, *, raises: bool = False) -> None:
        self.woken: list[object] = []
        self._raises = raises

    def add_callback_threadsafe(self, cb: object) -> None:
        if self._raises:
            raise RuntimeError("connection is already gone")
        self.woken.append(cb)


class _FakeChannel:
    def __init__(self, *, raises: bool = False) -> None:
        self.connection = _FakeConnection(raises=raises)
        self.direct_stops = 0

    def stop_consuming(self) -> None:
        self.direct_stops += 1


class _StubWorker:
    """QueueConsumer reads these off the worker at construction time."""

    requeue_on_shutdown = False

    def _force_fail_job(self, *_: object) -> None: ...
    def extend_lease(self, *_: object) -> None: ...


def _operation_consumer() -> OperationConsumer:
    return OperationConsumer(
        "amqp://guest:guest@localhost:5672/",
        "protea.predictions.batch",
        OperationRegistry(),
        None,  # type: ignore[arg-type]
    )


def _queue_consumer() -> QueueConsumer:
    return QueueConsumer(
        "amqp://guest:guest@localhost:5672/",
        "protea.jobs",
        _StubWorker(),  # type: ignore[arg-type]
    )


BOTH = pytest.mark.parametrize(
    "make", [_operation_consumer, _queue_consumer], ids=["operation", "queue"]
)


@BOTH
class TestAStopSignalReachesTheIOLoop:
    def test_the_handler_asks_the_loop_to_return(self, make) -> None:
        """The invariant the old test could not see: not the flag, the wake-up."""
        c = make()
        c._channel = _FakeChannel()
        c._handle_stop()
        assert c.stopped is True
        assert c._channel.connection.woken == [c._stop_consuming_safely]

    def test_the_wake_up_actually_stops_consuming(self, make) -> None:
        """Whatever was queued has to be the thing that ends the loop."""
        c = make()
        c._channel = _FakeChannel()
        c._handle_stop()
        (queued,) = c._channel.connection.woken
        queued()
        assert c._channel.direct_stops == 1

    def test_a_second_signal_is_not_a_second_wake_up(self, make) -> None:
        """systemd sends SIGTERM then SIGKILL, and a shell adds SIGINT."""
        c = make()
        c._channel = _FakeChannel()
        c._handle_stop()
        c._handle_stop()
        assert len(c._channel.connection.woken) == 1

    def test_a_dead_connection_falls_back_to_stopping_directly(self, make) -> None:
        """The broker OOM on 2026-09-05 is exactly this: signal arrives after
        the connection is gone, and the consumer must still stop."""
        c = make()
        c._channel = _FakeChannel(raises=True)
        c._handle_stop()
        assert c.stopped is True
        assert c._channel.direct_stops == 1

    def test_a_signal_before_the_channel_exists_does_not_raise(self, make) -> None:
        """SIGTERM between process start and basic_consume is a real window."""
        c = make()
        assert c._channel is None
        c._handle_stop()
        assert c.stopped is True

    def test_stopping_safely_with_no_channel_is_a_no_op(self, make) -> None:
        c = make()
        c._channel = None
        c._stop_consuming_safely()


@BOTH
def test_every_consumer_exposes_the_wake_up(make) -> None:
    """Structural, so a third consumer cannot quietly ship without one. The
    old test asserted ``issubclass(..., Stoppable)``, which both classes
    satisfied throughout the outage."""
    c = make()
    assert callable(c._stop_consuming_safely)
