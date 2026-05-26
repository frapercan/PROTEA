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

logger = logging.getLogger(__name__)


class StaleJobReaper:
    def __init__(
        self,
        session_factory: sessionmaker[Session],
        timeout_seconds: int = 3600,
        *,
        stall_seconds: int = 1800,
    ) -> None:
        self._factory = session_factory
        self._timeout = timedelta(seconds=timeout_seconds)
        self._stall = timedelta(seconds=stall_seconds)
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
            # F-OPS-JOBS.1: jobs that set leased_until are considered alive
            # as long as the lease has not expired. Only consider candidates
            # where the lease is also expired (or NULL, for legacy rows that
            # pre-date the leasing column).
            candidates = (
                session.query(Job)
                .filter(
                    Job.status == JobStatus.RUNNING,
                    Job.started_at < cutoff,
                    (Job.leased_until.is_(None)) | (Job.leased_until < now),
                )
                .all()
            )
            reaped = sum(
                self._reap_one_candidate(session, job, now, stall_cutoff)
                for job in candidates
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
        """Mark one stalled candidate FAILED. Returns 1 if reaped, 0 if alive."""
        last_event_ts = (
            session.query(func.max(JobEvent.ts)).filter(JobEvent.job_id == job.id).scalar()
        )
        if last_event_ts is not None and last_event_ts > stall_cutoff:
            logger.debug(
                "Reaper skipped live job. job_id=%s operation=%s last_event=%s",
                job.id,
                job.operation,
                last_event_ts,
            )
            return 0

        job.status = JobStatus.FAILED
        job.finished_at = now
        job.error_code = "JobTimeout"
        job.error_message = (
            f"Job stalled: no activity in {self._stall.total_seconds():.0f}s "
            f"(started >{self._timeout.total_seconds():.0f}s ago)"
        )
        session.add(
            JobEvent(
                job_id=job.id,
                event="job.timeout",
                message=job.error_message,
                fields={
                    "timeout_seconds": self._timeout.total_seconds(),
                    "stall_seconds": self._stall.total_seconds(),
                    "last_event_ts": (last_event_ts.isoformat() if last_event_ts else None),
                },
                level="error",
            )
        )
        logger.warning(
            "Marking stalled job FAILED. job_id=%s operation=%s started_at=%s last_event=%s",
            job.id,
            job.operation,
            job.started_at,
            last_event_ts,
        )
        return 1
