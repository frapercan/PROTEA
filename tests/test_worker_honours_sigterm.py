"""A worker told to stop must stay stopped.

``consumer.run()`` returns rather than raising when a signal stops it, and the
supervising loop in ``scripts/worker.py`` reconnects on any return. So SIGTERM
stopped the consumer, the loop started a fresh one, and the process survived
its own termination signal with the code it booted with still in memory.

Observed on this host while deploying: eleven of twelve workers ignored SIGTERM
and had to be killed, and the log reads

    Stop signal received. -> Consumer stopped. -> Consumer started.

The cost is not a slow shutdown. It is that a deploy cannot restart a worker,
so a fix can be merged, pulled, and still not be the code that runs.
"""

from __future__ import annotations

from protea.infrastructure.queue._stoppable import Stoppable


class _Consumer(Stoppable):
    """Stands in for the real consumers, which set ``_stop`` the same way."""

    def __init__(self) -> None:
        self._stop = False

    def handle_stop(self) -> None:
        self._stop = True


class TestTheStopFlagIsVisibleToTheSupervisor:
    def test_a_fresh_consumer_is_not_stopped(self) -> None:
        assert _Consumer().stopped is False

    def test_handling_a_signal_makes_it_stopped(self) -> None:
        c = _Consumer()
        c.handle_stop()
        assert c.stopped is True

    def test_both_real_consumers_carry_the_flag(self) -> None:
        """The mixin is only useful if the real classes actually use it."""
        from protea.infrastructure.queue.consumer import OperationConsumer, QueueConsumer

        assert issubclass(QueueConsumer, Stoppable)
        assert issubclass(OperationConsumer, Stoppable)


class TestTheSupervisingLoopBreaksOnADeliberateStop:
    """Exercises the loop's shape rather than the module, which would need a
    live AMQP connection. The branch under test is the one that reads
    ``consumer.stopped`` after ``run()`` returns without raising."""

    @staticmethod
    def _loop(consumer: _Consumer, runs: list[int], limit: int = 5) -> int:
        starts = 0
        while starts < limit:
            starts += 1
            runs.append(starts)
            if consumer.stopped:
                break
        return starts

    def test_a_consumer_that_was_told_to_stop_is_not_restarted(self) -> None:
        c = _Consumer()
        c.handle_stop()
        assert self._loop(c, []) == 1

    def test_a_consumer_that_merely_disconnected_is_restarted(self) -> None:
        """The reconnect behaviour is the reason the loop exists; keep it."""
        assert self._loop(_Consumer(), []) == 5
