"""A watchdog for an operation that outlives the broker's heartbeat window.

``OperationConsumer`` runs ``op.execute`` on pika's IO thread, so the
connection sends no heartbeat while an operation runs. After two missed
heartbeats the broker closes the connection and requeues the message, and the
process does not notice: nothing touches the connection until the operation
returns. On 2026-11-04 a compute node spent 35 minutes in that state while it
streamed a 1,536-dimension reference pool over a slow link. It was alive,
consumed nothing, was invisible to its supervisor and wrote no line to its
log, while the broker showed the queue's 40 messages back with no consumer.

A worker in that state is useless and unsafe. Its message is back in the
queue, so finishing would commit and publish a result that another consumer
may already be producing; the publisher keeps a connection per thread and
retries a failed publish on a new one, so the dead channel would not stop it.
And its IO thread is blocked, so nothing can be nacked or closed from here.
Ending the process is the only clean exit. The watchdog logs why and exits with
``EX_TEMPFAIL``, which both the node's units (``Restart=always``, which skips
only 78, ``EX_CONFIG``) and the server's (``Restart=on-failure``) restart. A
unit whose ``ExecStartPre`` guard refuses a tree that is not the declared
revision stays down instead, which is what that guard is for.

An operation that legitimately runs longer belongs on a ``QueueConsumer``
queue, which pumps the connection while its job runs (``_job_pump``).
"""

from __future__ import annotations

import logging
import os
import sys
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Protocol

logger = logging.getLogger(__name__)

#: Seconds before the end of the window at which the watchdog fires, so the
#: worker leaves before the broker drops it rather than after.
WINDOW_MARGIN_SECONDS = 60

#: sysexits ``EX_TEMPFAIL``: a temporary failure, which ``Restart=`` restarts.
EX_TEMPFAIL = 75

ExpireFn = Callable[[str, float], None]


class Timer(Protocol):
    """The two methods of ``threading.Timer`` the watchdog uses."""

    def start(self) -> None: ...

    def cancel(self) -> None: ...


TimerFactory = Callable[[float, ExpireFn, tuple[str, float]], Timer]


def thread_timer(seconds: float, fn: ExpireFn, args: tuple[str, float]) -> Timer:
    """A daemon ``threading.Timer``, so an armed watchdog never holds the process up."""
    timer = threading.Timer(seconds, fn, args=args)
    timer.daemon = True
    return timer


def exit_after_window(operation: str, window_seconds: float) -> None:
    """Log why, flush the logs, then end the process with ``EX_TEMPFAIL``."""
    logger.error(
        "Operation outlived the broker heartbeat window, so the broker has requeued "
        "its message; exiting for the supervisor to restart this worker. "
        "operation=%s window_seconds=%.0f",
        operation,
        window_seconds,
    )
    for handler in logging.getLogger().handlers:
        handler.flush()
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(EX_TEMPFAIL)


class OperationWatchdog:
    """One timer per running operation; a window of ``None`` disables it."""

    def __init__(
        self,
        window_seconds: float | None,
        *,
        on_expire: ExpireFn = exit_after_window,
        timer_factory: TimerFactory = thread_timer,
    ) -> None:
        self.window_seconds = window_seconds
        self._on_expire = on_expire
        self._timer_factory = timer_factory
        self._timer: Timer | None = None

    @classmethod
    def for_heartbeat(
        cls,
        heartbeat_seconds: int,
        *,
        on_expire: ExpireFn = exit_after_window,
        timer_factory: TimerFactory = thread_timer,
    ) -> OperationWatchdog:
        """The window two heartbeats leave, less the margin; none without heartbeats."""
        if heartbeat_seconds <= 0:
            return cls(None, on_expire=on_expire, timer_factory=timer_factory)
        window = max(2 * heartbeat_seconds - WINDOW_MARGIN_SECONDS, heartbeat_seconds)
        return cls(float(window), on_expire=on_expire, timer_factory=timer_factory)

    def arm(self, operation: str) -> None:
        """Start the timer for ``operation``, replacing any timer still armed."""
        self.disarm()
        if self.window_seconds is None:
            return
        timer = self._timer_factory(
            self.window_seconds, self._on_expire, (operation, self.window_seconds)
        )
        timer.start()
        self._timer = timer

    def disarm(self) -> None:
        """Cancel the armed timer, if there is one."""
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    @contextmanager
    def armed(self, operation: str) -> Iterator[None]:
        """Armed for the body of the ``with``; disarmed when it ends, raised or not."""
        self.arm(operation)
        try:
            yield
        finally:
            self.disarm()
