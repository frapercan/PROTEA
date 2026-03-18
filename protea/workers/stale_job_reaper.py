# protea/workers/stale_job_reaper.py
"""Periodic reaper that marks long-running jobs as FAILED.

Workers are single-threaded and cannot be interrupted mid-operation without
risking data corruption.  Instead, this lightweight reaper runs on a timer
and transitions any job that has been in RUNNING status for longer than
``timeout_seconds`` to FAILED with error_code ``JobTimeout``.

Usage::

    reaper = StaleJobReaper(session_factory, timeout_seconds=3600)
    reaper.run(interval_seconds=60)  # checks every minute
"""
from __future__ import annotations

import logging
import signal
import time
from datetime import timedelta

from sqlalchemy import update as sa_update
from sqlalchemy.orm import Session, sessionmaker

from protea.core.utils import utcnow
from protea.infrastructure.orm.models.job import Job, JobEvent, JobStatus

logger = logging.getLogger(__name__)


class StaleJobReaper:
    def __init__(
        self,
        session_factory: sessionmaker[Session],
        timeout_seconds: int = 3600,
    ) -> None:
        self._factory = session_factory
        self._timeout = timedelta(seconds=timeout_seconds)
        self._stop = False

    def run(self, interval_seconds: int = 60) -> None:
        signal.signal(signal.SIGINT, self._handle_stop)
        signal.signal(signal.SIGTERM, self._handle_stop)

        logger.info(
            "StaleJobReaper started. timeout=%ss interval=%ss",
            self._timeout.total_seconds(),
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
        cutoff = utcnow() - self._timeout
        session = self._factory()
        try:
            stale_jobs = (
                session.query(Job)
                .filter(
                    Job.status == JobStatus.RUNNING,
                    Job.started_at < cutoff,
                )
                .all()
            )
            for job in stale_jobs:
                job.status = JobStatus.FAILED
                job.finished_at = utcnow()
                job.error_code = "JobTimeout"
                job.error_message = (
                    f"Job exceeded timeout of {self._timeout.total_seconds():.0f}s"
                )
                session.add(
                    JobEvent(
                        job_id=job.id,
                        event="job.timeout",
                        message=job.error_message,
                        fields={"timeout_seconds": self._timeout.total_seconds()},
                        level="error",
                    )
                )
                logger.warning(
                    "Marking stale job FAILED. job_id=%s operation=%s started_at=%s",
                    job.id,
                    job.operation,
                    job.started_at,
                )
            session.commit()
            return len(stale_jobs)
        except Exception:
            try:
                session.rollback()
            except Exception:
                pass
            raise
        finally:
            session.close()
