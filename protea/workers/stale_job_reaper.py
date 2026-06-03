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
it is considered *alive* and left in place.  Only truly stalled jobs — no
events for ``stall_seconds`` — are marked FAILED.  The hard ``timeout_seconds``
still acts as the lower bound (a job under the timeout is never touched),
so this is strictly more permissive than the previous behaviour.

Usage::

    reaper = StaleJobReaper(session_factory, timeout_seconds=21600)
    reaper.run(interval_seconds=60)  # checks every minute
"""

from __future__ import annotations

import logging
import signal
import time
from datetime import datetime, timedelta

from sqlalchemy import func
from sqlalchemy.orm import Session, sessionmaker

from protea.core.utils import utcnow
from protea.infrastructure.orm.models.job import Job, JobEvent, JobStatus
from protea.infrastructure.queue.publisher import publish_job

logger = logging.getLogger(__name__)

#: Event name written every time the reaper re-enqueues a lease-expired job.
#: Counted by :meth:`StaleJobReaper._lease_requeue_attempts` to honour the
#: ``max_lease_requeues`` budget before falling back to ``lease_expired``.
LEASE_REQUEUE_EVENT = "job.lease_expired_requeue"

#: Event written when the reaper gives up after exhausting requeue attempts.
LEASE_EXPIRED_FAILED_EVENT = "job.lease_expired"


class StaleJobReaper:
    def __init__(
        self,
        session_factory: sessionmaker[Session],
        timeout_seconds: int = 3600,
        *,
        stall_seconds: int = 1800,
        amqp_url: str | None = None,
        max_lease_requeues: int = 3,
    ) -> None:
        self._factory = session_factory
        self._timeout = timedelta(seconds=timeout_seconds)
        self._stall = timedelta(seconds=stall_seconds)
        self._amqp_url = amqp_url
        self._max_lease_requeues = max(0, int(max_lease_requeues))
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
        # no lease (pre-F-OPS-JOBS.1). With a lease, the heartbeat already
        # encodes liveness; a missing-heartbeat row is unconditionally
        # stalled regardless of whether the operation kept emitting
        # JobEvents (a sub-process can crash mid-batch and still leave
        # recent events).
        if job.leased_until is None and last_event_ts is not None and last_event_ts > stall_cutoff:
            logger.debug(
                "Reaper skipped live legacy job. job_id=%s operation=%s last_event=%s",
                job.id,
                job.operation,
                last_event_ts,
            )
            return 0

        prior_requeues = self._lease_requeue_attempts(session, job.id)
        if self._can_requeue(prior_requeues):
            self._requeue_lease_expired(session, job, now, prior_requeues, last_event_ts)
            return 1
        self._fail_lease_expired(session, job, now, prior_requeues, last_event_ts)
        return 1

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
