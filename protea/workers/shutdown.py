# protea/workers/shutdown.py
"""SIGTERM/SIGINT grace-period helper for queue consumers.

When deploy-keeper redeploys the stack it sends SIGTERM to running
workers. If a job is in flight, the worker process dies before the
FAILED transition is committed and the DB row stays in RUNNING forever.

``ShutdownGuard`` encapsulates the watchdog timer that turns this into
a graceful shutdown:

1. The consumer reports the in-flight job id via ``track(job_id)`` /
   ``untrack()`` around its callback.
2. The consumer calls ``arm(job_id)`` from its signal handler.
3. The guard schedules a daemon ``threading.Timer``; if the job has not
   finished within ``grace_seconds`` the timer fires, marks the job
   FAILED with ``error_code=WorkerShutdown`` through the supplied
   ``force_fail`` callable, and exits the process with ``os._exit(143)``
   (128 + SIGTERM by convention) so init/systemd does not need to
   escalate to SIGKILL.
4. If the callback finishes naturally before the timer fires,
   ``cancel()`` releases the watchdog.

Extracted from ``infrastructure/queue/consumer.py`` to keep that file
under the file-LOC smell threshold (>800 LOC).
"""

from __future__ import annotations

import logging
import os
import threading
from collections.abc import Callable
from uuid import UUID

from protea.workers.base_worker import WorkerShutdown

logger = logging.getLogger(__name__)


class ShutdownGuard:
    """Coordinate force-fail-on-shutdown for a queue consumer.

    Thread-safety: ``track``/``untrack`` run on the main (pika IO)
    thread; ``arm``/``cancel`` may run on the signal-handler frame (also
    main thread). ``_fire`` runs on the watchdog Timer thread. The
    Python signal-delivery model only switches between bytecodes so
    reads/writes of ``_current_job_id`` from the main thread do not race
    with their own signal-handler equivalents. The Timer thread only
    reads ``_current_job_id``, so a stale read is safe (the worst case
    is a no-op force-fail).
    """

    def __init__(
        self,
        force_fail: Callable[[UUID, Exception], None],
        *,
        grace_seconds: int,
    ) -> None:
        self._force_fail = force_fail
        self._grace_seconds = grace_seconds
        self._current_job_id: UUID | None = None
        self._timer: threading.Timer | None = None

    # -- in-flight tracking ------------------------------------------------

    def track(self, job_id: UUID) -> None:
        """Mark ``job_id`` as the in-flight job."""
        self._current_job_id = job_id

    def untrack(self) -> None:
        """Clear the in-flight tracker after the callback finishes."""
        self._current_job_id = None

    @property
    def current_job_id(self) -> UUID | None:
        return self._current_job_id

    # -- watchdog ----------------------------------------------------------

    def arm(self, job_id: UUID) -> None:
        """Schedule the force-fail watchdog if one is not already armed.

        Idempotent: a second SIGTERM during shutdown does not arm a
        second timer.
        """
        if self._timer is not None:
            return
        logger.info(
            "Arming shutdown grace timer. job_id=%s grace_seconds=%d",
            job_id,
            self._grace_seconds,
        )
        timer = threading.Timer(self._grace_seconds, self._fire, args=(job_id,))
        timer.daemon = True
        self._timer = timer
        timer.start()

    def cancel(self) -> None:
        """Cancel the pending watchdog (if any). Safe to call repeatedly."""
        timer = self._timer
        if timer is None:
            return
        try:
            timer.cancel()
        except Exception:
            pass
        self._timer = None

    @property
    def timer(self) -> threading.Timer | None:
        return self._timer

    def _fire(self, job_id: UUID) -> None:
        """Watchdog callback: force-fail the in-flight job and exit.

        Runs on the Timer thread. If ``_current_job_id`` no longer
        matches ``job_id`` the callback already finished — nothing to
        do. Otherwise call the injected ``force_fail`` to mark the job
        FAILED via the fresh-session fallback and exit the process so
        deploy-keeper can restart cleanly.
        """
        if self._current_job_id != job_id:
            logger.info(
                "Shutdown timer fired but job already finished. job_id=%s",
                job_id,
            )
            return
        logger.warning(
            "Job did not finish within grace period — force-failing. job_id=%s",
            job_id,
        )
        try:
            self._force_fail(job_id, WorkerShutdown("SIGTERM received during job"))
        except Exception as exc:
            logger.error(
                "Force-fail during shutdown raised. job_id=%s error=%s",
                job_id,
                exc,
            )
        # 128 + SIGTERM(15) = 143 by convention.
        os._exit(143)
