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


class HeartbeatLoop:
    """Background thread that periodically extends a job's lease.

    Started by ``QueueConsumer`` around ``BaseWorker.handle_job`` so the
    ``leased_until`` column tracks process liveness rather than the
    initial claim wall-clock. ``StaleJobReaper`` uses the same column to
    decide when an abandoned job is safe to re-enqueue. If the heartbeat
    thread itself dies (unhandled exception during ``extend_lease``) the
    job will simply expire at the next reaper cycle, which is the
    intended failure mode.

    Thread-safety: ``start``/``stop`` are called from the main pika IO
    thread; the worker callback (``extend_lease``) is invoked from the
    daemon worker thread but uses its own short-lived DB session, so
    there is no shared mutable state with the IO thread.
    """

    def __init__(
        self,
        extend_lease: Callable[[UUID], None],
        *,
        interval_seconds: int,
    ) -> None:
        self._extend_lease = extend_lease
        self._interval = max(1, int(interval_seconds))
        self._stop_event: threading.Event | None = None
        self._thread: threading.Thread | None = None

    def start(self, job_id: UUID) -> None:
        """Begin extending the lease for ``job_id`` every interval seconds.

        Idempotent: a second start without an intervening stop is a no-op
        so consumers that re-enter the dispatch path (e.g. RetryLater)
        do not stack heartbeat threads.
        """
        if self._thread is not None:
            return
        stop_event = threading.Event()
        thread = threading.Thread(
            target=self._loop,
            args=(job_id, stop_event),
            name=f"heartbeat-{job_id}",
            daemon=True,
        )
        self._stop_event = stop_event
        self._thread = thread
        thread.start()

    def stop(self) -> None:
        """Signal the heartbeat thread to exit and wait briefly for it."""
        stop_event = self._stop_event
        thread = self._thread
        self._stop_event = None
        self._thread = None
        if stop_event is None or thread is None:
            return
        stop_event.set()
        try:
            thread.join(timeout=self._interval + 1)
        except Exception:
            pass

    def _loop(self, job_id: UUID, stop_event: threading.Event) -> None:
        # First extension happens after the interval, not immediately:
        # ``BaseWorker._claim_job`` already set the initial lease.
        while not stop_event.wait(self._interval):
            try:
                self._extend_lease(job_id)
            except Exception as exc:
                logger.warning(
                    "Heartbeat extend_lease failed (non-fatal). job_id=%s error=%s",
                    job_id,
                    exc,
                )


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

    Re-queue-first policy (F-OPS-JOBS.1 SIGTERM handler): when an
    optional ``requeue`` callable is supplied, the watchdog tries it
    before falling back to ``force_fail``. ``requeue`` returns ``True``
    when it successfully flipped the row back to QUEUED, in which case
    the caller is expected to republish the AMQP delivery so a sibling
    worker can pick it up. ``force_fail`` remains the safety net for
    rows that no longer match (already terminal) or sessions that
    cannot commit.
    """

    def __init__(
        self,
        force_fail: Callable[[UUID, Exception], None],
        *,
        grace_seconds: int,
        requeue: Callable[[UUID], bool] | None = None,
        on_requeue: Callable[[UUID], None] | None = None,
    ) -> None:
        self._force_fail = force_fail
        self._requeue = requeue
        self._on_requeue = on_requeue
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
        if self._try_requeue(job_id):
            # 128 + SIGTERM(15) = 143 by convention.
            os._exit(143)
            return  # pragma: no cover - unreachable in prod; guards tests
        logger.warning(
            "Job did not finish within grace period; force-failing. job_id=%s",
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

    def _try_requeue(self, job_id: UUID) -> bool:
        """Attempt the non-destructive re-queue path before force-fail.

        Returns True when the row was flipped back to QUEUED AND the
        ``on_requeue`` callback (typically publishes the delivery back
        onto the queue) completed without raising. False otherwise; the
        caller falls back to ``force_fail`` so the row never stays in
        RUNNING.
        """
        if self._requeue is None:
            return False
        try:
            requeued = self._requeue(job_id)
        except Exception as exc:
            logger.error(
                "Re-queue-on-shutdown callable raised. job_id=%s error=%s",
                job_id,
                exc,
            )
            return False
        if not requeued:
            return False
        logger.info(
            "Re-queued in-flight job on shutdown grace expiry. job_id=%s",
            job_id,
        )
        if self._on_requeue is not None:
            try:
                self._on_requeue(job_id)
            except Exception as exc:
                logger.error(
                    "on_requeue callback raised; sibling consumer may miss "
                    "the re-queue notification. job_id=%s error=%s",
                    job_id,
                    exc,
                )
        return True
