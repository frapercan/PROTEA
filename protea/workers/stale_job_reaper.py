# protea/workers/stale_job_reaper.py
"""Periodic reaper that marks long-running jobs as FAILED.

Workers are single-threaded and cannot be interrupted mid-operation without
risking data corruption.  Instead, this lightweight reaper runs on a timer
and transitions any job that has been in RUNNING status for longer than
``timeout_seconds`` to FAILED with error_code ``JobTimeout``.

Progress-aware grace period
---------------------------
Deferred coordinator jobs (e.g. ``compute_embeddings``, ``predict_go_terms``)
may legitimately run longer than ``timeout_seconds`` because the actual work
is done by child batch messages on downstream queues.  Killing these jobs
at the global wall-clock threshold would drop hours of successful work on
the floor.

To avoid that, the reaper applies a second check to every candidate: if the
job has produced any ``JobEvent`` within the last ``stall_seconds`` window,
it is considered *alive* and left in place.  Only truly stalled jobs (no
events for ``stall_seconds``) are marked FAILED.  The hard ``timeout_seconds``
still acts as the lower bound (a job under the timeout is never touched),
so this is strictly more permissive than the previous behaviour.

Event-based liveness backstop (C4 / NFR-INFRA)
----------------------------------------------
The lease heartbeat that keeps ``leased_until`` in the future runs in a
daemon thread.  During long single-threaded GPU/numpy splits (multi-hour
``export_research_dataset`` / ``predict_go_terms`` work whose progress
events are 30-40 min apart) that thread can starve under sustained GIL
contention, so the 120s claim lease lapses on a job that is plainly still
computing.  To stop the reaper re-enqueuing such a healthy job as a phantom
duplicate (an idle worker could then claim the duplicate and run a
CONCURRENT export, risking OOM), a leased candidate whose lease has expired
is still treated as ALIVE when it emitted any ``JobEvent`` within the wider
``event_grace_seconds`` window.  The same grace guards the orphaned-QUEUED
re-publish path.  The ``max_lease_requeues`` / ``max_queue_requeues`` budgets
stay the real backstop for jobs that have genuinely stopped emitting.

Usage::

    reaper = StaleJobReaper(session_factory, timeout_seconds=21600)
    reaper.run(interval_seconds=60)  # checks every minute
"""

from __future__ import annotations

import logging
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import func
from sqlalchemy.orm import Session, sessionmaker

from protea.core.utils import utcnow
from protea.infrastructure.orm.models.job import Job, JobEvent, JobStatus
from protea.infrastructure.queue.publisher import publish_job

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StaleJobReaperConfig:
    """Configuration for StaleJobReaper timeout windows and requeue budgets."""

    timeout_seconds: int = 3600
    stall_seconds: int = 1800
    queued_stall_seconds: int = 600
    max_lease_requeues: int = 3
    max_queue_requeues: int = 5
    #: Event-based liveness grace. A RUNNING job whose lease has expired is
    #: still treated as ALIVE (never requeued or failed) if it emitted a
    #: JobEvent within this window. The lease heartbeat is a daemon thread
    #: that can starve under sustained GIL contention during long
    #: single-threaded GPU/numpy splits (events arrive 30-40 min apart),
    #: so a fresh JobEvent is a reliable secondary proof the operation is
    #: still doing work. Defaults to 2700s (45 min) so a job that emits a
    #: progress event every 30-40 min is never mistaken for dead. The
    #: requeue budget (``max_lease_requeues``) remains the real backstop
    #: for genuinely dead jobs that stop emitting entirely.
    event_grace_seconds: int = 2700

    def to_timedeltas(self) -> tuple[timedelta, timedelta, timedelta, timedelta]:
        """Return (timeout, stall, queued_stall, event_grace) as timedeltas."""
        return (
            timedelta(seconds=self.timeout_seconds),
            timedelta(seconds=self.stall_seconds),
            timedelta(seconds=self.queued_stall_seconds),
            timedelta(seconds=self.event_grace_seconds),
        )


#: Event name written every time the reaper re-enqueues a lease-expired job.
#: Counted by :meth:`StaleJobReaper._lease_requeue_attempts` to honour the
#: ``max_lease_requeues`` budget before falling back to ``lease_expired``.
LEASE_REQUEUE_EVENT = "job.lease_expired_requeue"

#: Event written when the reaper gives up after exhausting requeue attempts.
LEASE_EXPIRED_FAILED_EVENT = "job.lease_expired"

#: Event written each time the reaper re-publishes a job orphaned in QUEUED
#: (DB row QUEUED but no live broker message, e.g. a delayed-retry message
#: lost when a worker was killed). Counted to honour ``max_queue_requeues``.
QUEUE_STALL_REQUEUE_EVENT = "job.queue_stall_requeue"


class StaleJobReaper:
    def __init__(
        self,
        session_factory: sessionmaker[Session],
        amqp_url: str | None = None,
        config: StaleJobReaperConfig | None = None,
    ) -> None:
        self._factory = session_factory
        self._amqp_url = amqp_url
        cfg = config or StaleJobReaperConfig()
        (
            self._timeout,
            self._stall,
            self._queued_stall,
            self._event_grace,
        ) = cfg.to_timedeltas()
        self._max_lease_requeues = max(0, int(cfg.max_lease_requeues))
        self._max_queue_requeues = max(0, int(cfg.max_queue_requeues))
        self._stop = False

    def run(self, interval_seconds: int = 60) -> None:
        signal.signal(signal.SIGINT, self._handle_stop)
        signal.signal(signal.SIGTERM, self._handle_stop)

        logger.info(
            "StaleJobReaper started. timeout=%ss stall=%ss interval=%ss",
            self._timeout.total_seconds(),
            self._stall.total_seconds(),
            interval_seconds,
        )
        while not self._stop:
            try:
                reaped = self._reap()
                if reaped:
                    logger.info("Reaped %d stale job(s).", reaped)
            except Exception as exc:
                logger.error("Reaper cycle failed: %s", exc)
            time.sleep(interval_seconds)

        logger.info("StaleJobReaper stopped.")

    def _handle_stop(self, *_: object) -> None:
        self._stop = True

    def _reap(self) -> int:
        now = utcnow()
        cutoff = now - self._timeout
        stall_cutoff = now - self._stall
        session = self._factory()
        try:
            # F-OPS-JOBS.1: liveness is tracked by the lease heartbeat
            # column. A RUNNING job whose lease is in the future is
            # treated as alive even if started_at is older than the
            # global timeout (long exports legitimately heartbeat for
            # many hours). Legacy rows that pre-date the leasing column
            # (leased_until IS NULL) still fall back to the started_at
            # cutoff so the reaper keeps recovering them.
            candidates = (
                session.query(Job)
                .filter(
                    Job.status == JobStatus.RUNNING,
                    ((Job.leased_until.isnot(None)) & (Job.leased_until < now))
                    | ((Job.leased_until.is_(None)) & (Job.started_at < cutoff)),
                )
                .all()
            )
            reaped = sum(
                self._reap_one_candidate(session, job, now, stall_cutoff) for job in candidates
            )
            reaped += self._reap_orphaned_queued(session, now)
            session.commit()
            return reaped
        except Exception:
            try:
                session.rollback()
            except Exception:
                pass
            raise
        finally:
            session.close()

    def _reap_one_candidate(
        self,
        session: Session,
        job: Job,
        now: datetime,
        stall_cutoff: datetime,
    ) -> int:
        """Recover one stalled candidate. Returns 1 if recovered, 0 if alive."""
        last_event_ts = (
            session.query(func.max(JobEvent.ts)).filter(JobEvent.job_id == job.id).scalar()
        )
        # Legacy ``stall_seconds`` grace: only honoured when the row has
        # no lease (pre-F-OPS-JOBS.1). A legacy row that has not emitted any
        # event inside the stall window is stalled.
        if job.leased_until is None and last_event_ts is not None and last_event_ts > stall_cutoff:
            logger.debug(
                "Reaper skipped live legacy job. job_id=%s operation=%s last_event=%s",
                job.id,
                job.operation,
                last_event_ts,
            )
            return 0

        # Event-based liveness backstop for LEASED jobs (C4 / NFR-INFRA).
        # The lease heartbeat runs in a daemon thread that can starve under
        # sustained GIL contention during long single-threaded GPU/numpy
        # splits, so ``leased_until`` can lapse on a job that is plainly
        # still working. A JobEvent emitted within ``event_grace`` is a
        # reliable secondary liveness signal: a job that is emitting
        # progress is alive and must NOT be requeued (a duplicate message
        # could be claimed by an idle worker -> concurrent export -> OOM).
        # The requeue budget below stays the backstop for jobs that have
        # genuinely stopped emitting.
        event_grace_cutoff = now - self._event_grace
        if (
            job.leased_until is not None
            and last_event_ts is not None
            and (last_event_ts > event_grace_cutoff)
        ):
            logger.info(
                "Reaper skipped lease-expired job with recent event "
                "(heartbeat starved but operation alive). "
                "job_id=%s operation=%s last_event=%s leased_until=%s",
                job.id,
                job.operation,
                last_event_ts,
                job.leased_until,
            )
            return 0

        prior_requeues = self._lease_requeue_attempts(session, job.id)
        if self._can_requeue(prior_requeues):
            self._requeue_lease_expired(session, job, now, prior_requeues, last_event_ts)
            return 1
        self._fail_lease_expired(session, job, now, prior_requeues, last_event_ts)
        return 1

    def _reap_orphaned_queued(self, session: Session, now: datetime) -> int:
        """Re-publish jobs orphaned in QUEUED with no live broker message.

        A job can end up QUEUED in the DB with nothing to pick it up: a
        delayed-retry message lost when a worker was killed, or a re-publish
        that failed after a lease expiry. ``_claim_job`` only ever flips a
        row out of QUEUED via an atomic ``WHERE status='queued'`` UPDATE, so
        re-publishing such a job can never double-execute it (a duplicate
        message simply finds the row already claimed and returns).

        Only rows stalled for ``queued_stall`` with no recent JobEvent are
        touched, so actively-retrying jobs (which keep emitting
        ``job.retry_later``) are left alone. A per-job cap stops a genuinely
        unrunnable job from being re-published forever; once spent it is left
        QUEUED for an operator rather than failed (failing a recoverable job
        is worse than leaving it parked).
        """
        if self._amqp_url is None:
            return 0
        queued_cutoff = now - self._queued_stall
        candidates = (
            session.query(Job)
            .filter(Job.status == JobStatus.QUEUED, Job.created_at < queued_cutoff)
            .all()
        )
        recovered = sum(
            self._try_republish_orphaned_queued_job(session, job, queued_cutoff, now)
            for job in candidates
        )
        return recovered

    def _queued_job_is_alive(
        self,
        job: Job,
        last_event_ts: datetime | None,
        queued_cutoff: datetime,
        now: datetime,
    ) -> bool:
        """Return True when an orphaned-QUEUED candidate is still alive.

        Two guards, either of which spares the row from re-publish:

        - ``queued_stall``: the row was created (or last emitted) inside the
          short queued-stall window, so it is freshly queued / still emitting.
        - ``event_grace`` (C4 / NFR-INFRA): a job flipped back to QUEUED by the
          lease-requeue path (or one whose real worker is still mid-flight
          after a heartbeat starve) can keep emitting JobEvents 30-40 min
          apart. The much shorter ``queued_stall`` window would otherwise
          re-publish a DUPLICATE message while the original worker is still
          computing, risking a concurrent export and OOM. Honour the wider
          ``event_grace`` window so a job with ANY recent event is left alone;
          only genuinely silent QUEUED rows are re-published.
        """
        ref_ts = last_event_ts or job.created_at
        if ref_ts > queued_cutoff:
            return True  # still emitting / freshly created: not orphaned
        if last_event_ts is not None and last_event_ts > (now - self._event_grace):
            logger.debug(
                "Reaper skipped QUEUED job with recent event (still alive). "
                "job_id=%s operation=%s last_event=%s",
                job.id,
                job.operation,
                last_event_ts,
            )
            return True
        return False

    def _try_republish_orphaned_queued_job(
        self, session: Session, job: Job, queued_cutoff: datetime, now: datetime
    ) -> int:
        """Try to republish one orphaned QUEUED job. Returns 1 if published, 0 if skipped."""
        assert self._amqp_url is not None  # guarded by _reap_orphaned_queued caller
        last_event_ts = (
            session.query(func.max(JobEvent.ts)).filter(JobEvent.job_id == job.id).scalar()
        )
        if self._queued_job_is_alive(job, last_event_ts, queued_cutoff, now):
            return 0

        prior = (
            session.query(func.count(JobEvent.id))
            .filter(
                JobEvent.job_id == job.id,
                JobEvent.event == QUEUE_STALL_REQUEUE_EVENT,
            )
            .scalar()
            or 0
        )
        if prior >= self._max_queue_requeues:
            return 0  # budget spent; leave QUEUED for manual recovery

        session.add(
            JobEvent(
                job_id=job.id,
                event=QUEUE_STALL_REQUEUE_EVENT,
                message=(
                    f"Orphaned in QUEUED; re-published attempt "
                    f"{int(prior) + 1}/{self._max_queue_requeues}"
                ),
                fields={
                    "prior_requeues": int(prior),
                    "queue": job.queue_name,
                    "last_event_ts": (last_event_ts.isoformat() if last_event_ts else None),
                },
                level="warning",
            )
        )
        try:
            publish_job(self._amqp_url, job.queue_name, job.id)
            logger.warning(
                "Re-published orphaned QUEUED job. job_id=%s operation=%s queue=%s attempt=%d/%d",
                job.id,
                job.operation,
                job.queue_name,
                int(prior) + 1,
                self._max_queue_requeues,
            )
            return 1
        except Exception as exc:
            logger.error(
                "Re-publish of orphaned QUEUED job failed. job_id=%s error=%s", job.id, exc
            )
            return 0

    def _lease_requeue_attempts(self, session: Session, job_id) -> int:
        """Count prior lease-expiry re-enqueues for ``job_id``."""
        count = (
            session.query(func.count(JobEvent.id))
            .filter(JobEvent.job_id == job_id, JobEvent.event == LEASE_REQUEUE_EVENT)
            .scalar()
            or 0
        )
        return int(count)

    def _can_requeue(self, prior_requeues: int) -> bool:
        """Return True when re-enqueue should be tried for this attempt.

        Re-enqueue requires (a) an AMQP URL so the publisher can put the
        job back on its source queue, and (b) the prior re-enqueue count
        being strictly below ``max_lease_requeues``. The cap prevents an
        infinitely flapping job from looping through reap/dispatch
        forever; once exhausted the job lands in FAILED with error_code
        ``lease_expired`` so an operator can intervene.
        """
        if self._amqp_url is None:
            return False
        return prior_requeues < self._max_lease_requeues

    def _requeue_lease_expired(
        self,
        session: Session,
        job: Job,
        now: datetime,
        prior_requeues: int,
        last_event_ts: datetime | None,
    ) -> None:
        """Flip a lease-expired job back to QUEUED and re-publish it."""
        assert self._amqp_url is not None  # guarded by ``_can_requeue``
        session.add(
            JobEvent(
                job_id=job.id,
                event=LEASE_REQUEUE_EVENT,
                message=(
                    f"Lease expired; re-enqueued attempt "
                    f"{prior_requeues + 1}/{self._max_lease_requeues}"
                ),
                fields={
                    "prior_requeues": prior_requeues,
                    "max_requeues": self._max_lease_requeues,
                    "last_event_ts": (last_event_ts.isoformat() if last_event_ts else None),
                },
                level="warning",
            )
        )
        job.status = JobStatus.QUEUED
        job.started_at = None
        job.leased_until = None
        logger.warning(
            "Re-enqueuing lease-expired job. job_id=%s operation=%s queue=%s attempt=%d/%d",
            job.id,
            job.operation,
            job.queue_name,
            prior_requeues + 1,
            self._max_lease_requeues,
        )
        try:
            publish_job(self._amqp_url, job.queue_name, job.id)
        except Exception as exc:
            logger.error(
                "Re-publish after lease expiry failed; leaving row QUEUED "
                "for a subsequent reaper cycle. job_id=%s error=%s",
                job.id,
                exc,
            )
            # The row stays QUEUED with no leased_until; the next reaper
            # cycle will see ``Job.status == QUEUED`` and skip it (only
            # RUNNING is candidate). A manual ``publish_job`` call or a
            # sibling consumer that already had the message will recover.

    def _fail_lease_expired(
        self,
        session: Session,
        job: Job,
        now: datetime,
        prior_requeues: int,
        last_event_ts: datetime | None,
    ) -> None:
        """Mark a job FAILED with ``lease_expired`` after the requeue budget is spent."""
        job.status = JobStatus.FAILED
        job.finished_at = now
        job.error_code = "lease_expired"
        job.error_message = (
            f"Lease expired after {prior_requeues} re-enqueue attempt(s) "
            f"(max={self._max_lease_requeues}); no heartbeat in "
            f"{self._timeout.total_seconds():.0f}s window."
        )
        session.add(
            JobEvent(
                job_id=job.id,
                event=LEASE_EXPIRED_FAILED_EVENT,
                message=job.error_message,
                fields={
                    "prior_requeues": prior_requeues,
                    "max_requeues": self._max_lease_requeues,
                    "timeout_seconds": self._timeout.total_seconds(),
                    "stall_seconds": self._stall.total_seconds(),
                    "last_event_ts": (last_event_ts.isoformat() if last_event_ts else None),
                },
                level="error",
            )
        )
        logger.warning(
            "Marking lease-expired job FAILED. job_id=%s operation=%s started_at=%s last_event=%s",
            job.id,
            job.operation,
            job.started_at,
            last_event_ts,
        )
