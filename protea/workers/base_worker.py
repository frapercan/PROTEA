# protea/workers/base_worker.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import UUID

from sqlalchemy.orm import Session, sessionmaker

from protea.core.contracts.operation import OperationResult
from protea.core.contracts.registry import OperationRegistry
from protea.infrastructure.orm.models.job import Job, JobEvent, JobStatus


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class WorkerConfig:
    worker_name: str


class BaseWorker:
    def __init__(
        self, session_factory: sessionmaker[Session], registry: OperationRegistry, config: WorkerConfig
    ) -> None:
        self._factory = session_factory
        self._registry = registry
        self._config = config

    def handle_job(self, job_id: UUID) -> None:
        # Claim + run with DB-backed state.
        session = self._factory()
        try:
            job = session.get(Job, job_id)
            if job is None:
                return

            if job.status != JobStatus.QUEUED:
                return

            job.status = JobStatus.RUNNING
            job.started_at = utcnow()
            self._emit(session, job_id, "job.started", None, {"worker": self._config.worker_name}, level="info")
            session.commit()
        finally:
            session.close()

        # Execute in a separate session
        session = self._factory()
        try:
            job = session.get(Job, job_id)
            if job is None:
                return

            op = self._registry.get(job.operation)

            def emit(
                event: str, message: Optional[str] = None,
                fields: Optional[Dict[str, Any]] = None, level: str = "info"
            ) -> None:
                self._emit(session, job_id, event, message, fields or {}, level=level)
                session.flush()

            try:
                result: OperationResult = op.execute(session, job.payload, emit=emit)

                if result.progress_current is not None:
                    job.progress_current = int(result.progress_current)
                if result.progress_total is not None:
                    job.progress_total = int(result.progress_total)

                job.status = JobStatus.SUCCEEDED
                job.finished_at = utcnow()
                self._emit(session, job_id, "job.succeeded", None, {"result": result.result}, level="info")
                session.commit()

            except Exception as e:
                job.status = JobStatus.FAILED
                job.finished_at = utcnow()
                job.error_code = e.__class__.__name__
                job.error_message = str(e)
                self._emit(session, job_id, "job.failed", str(e), {"error_code": job.error_code}, level="error")
                session.commit()
                raise
        finally:
            session.close()

    @staticmethod
    def _emit(
        session: Session,
        job_id: UUID,
        event: str,
        message: Optional[str],
        fields: Dict[str, Any],
        *,
        level: str,
    ) -> None:
        session.add(JobEvent(job_id=job_id, event=event, message=message, fields=fields, level=level))
