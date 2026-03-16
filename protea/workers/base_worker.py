# protea/workers/base_worker.py
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from sqlalchemy import func
from sqlalchemy import update as sa_update
from sqlalchemy.orm import Session, sessionmaker

from protea.core.contracts.operation import OperationResult, RetryLaterError
from protea.core.contracts.registry import OperationRegistry
from protea.core.utils import utcnow
from protea.infrastructure.orm.models.job import Job, JobEvent, JobStatus
from protea.infrastructure.queue.publisher import publish_job, publish_operation

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkerConfig:
    worker_name: str


class BaseWorker:
    """
    Executes queued jobs using a two-session pattern.

    Session 1 (claim): transitions the job from QUEUED → RUNNING and commits.
    Session 2 (execute): resolves the operation, runs it, and transitions to
    SUCCEEDED or FAILED. Every state change is recorded as a JobEvent row.

    This class is transport-agnostic: it receives a job_id and handles the
    rest. The caller (QueueConsumer) is responsible for acking/nacking.
    """

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        registry: OperationRegistry,
        config: WorkerConfig,
        *,
        amqp_url: str | None = None,
    ) -> None:
        self._factory = session_factory
        self._registry = registry
        self._config = config
        self._amqp_url = amqp_url

    def handle_job(self, job_id: UUID) -> None:
        """
        Claim and execute a single job identified by ``job_id``.

        Silently returns if the job does not exist or is not in QUEUED status.
        Re-raises any exception from the operation after recording FAILED status.
        """
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

            # If the parent was cancelled while this child was being claimed,
            # cancel ourselves and stop without executing.
            if job.parent_job_id is not None:
                parent = session.get(Job, job.parent_job_id)
                if parent is not None and parent.status == JobStatus.CANCELLED:
                    job.status = JobStatus.CANCELLED
                    job.finished_at = utcnow()
                    self._emit(session, job_id, "job.cancelled", None,
                               {"reason": "parent_cancelled"}, level="info")
                    session.commit()
                    return

            op = self._registry.get(job.operation)

            def emit(
                event: str, message: str | None = None,
                fields: dict[str, Any] | None = None, level: str = "info"
            ) -> None:
                # Dedicated short-lived session that commits immediately so
                # events are visible in real time, not just at job completion.
                f = fields or {}
                event_session = self._factory()
                try:
                    self._emit(event_session, job_id, event, message, f, level=level)
                    # Allow operations to report live progress via reserved fields.
                    if "_progress_current" in f or "_progress_total" in f:
                        j = event_session.get(Job, job_id)
                        if j is not None:
                            if "_progress_current" in f:
                                j.progress_current = int(f["_progress_current"])
                            if "_progress_total" in f:
                                j.progress_total = int(f["_progress_total"])
                    event_session.commit()
                finally:
                    event_session.close()

            try:
                # Inject runtime context into payload so operations can reference their own job.
                enhanced_payload = {**job.payload, "_job_id": str(job.id)}
                result: OperationResult = op.execute(session, enhanced_payload, emit=emit)

                if result.progress_current is not None:
                    job.progress_current = int(result.progress_current)
                if result.progress_total is not None:
                    job.progress_total = int(result.progress_total)

                if result.deferred:
                    # Coordinator job: children will mark it SUCCEEDED when done.
                    self._emit(session, job_id, "job.dispatched", None, {"result": result.result}, level="info")
                else:
                    job.status = JobStatus.SUCCEEDED
                    job.finished_at = utcnow()
                    self._emit(session, job_id, "job.succeeded", None, {"result": result.result}, level="info")

                session.commit()

                # Publish child jobs to RabbitMQ after commit so workers always find the DB row.
                if result.publish_after_commit and self._amqp_url:
                    for queue_name, child_job_id in result.publish_after_commit:
                        publish_job(self._amqp_url, queue_name, child_job_id)

                # Publish ephemeral operation messages (e.g. embedding batches).
                if result.publish_operations and self._amqp_url:
                    for queue_name, op_payload in result.publish_operations:
                        publish_operation(self._amqp_url, queue_name, op_payload)

            except RetryLaterError as e:
                # Resource busy — reset to QUEUED so the consumer can re-publish.
                job.status = JobStatus.QUEUED
                job.started_at = None
                self._emit(session, job_id, "job.retry_later", str(e),
                           {"delay_seconds": e.delay_seconds}, level="info")
                session.commit()
                raise  # consumer handles re-publish

            except Exception as e:
                job.status = JobStatus.FAILED
                job.finished_at = utcnow()
                job.error_code = e.__class__.__name__
                job.error_message = str(e)
                self._emit(session, job_id, "job.failed", str(e), {"error_code": job.error_code}, level="error")
                if job.parent_job_id is not None:
                    self._maybe_fail_parent(session, job.parent_job_id)
                try:
                    session.commit()
                except Exception as commit_exc:
                    # Execute session is corrupted (e.g. DB connection dropped during a
                    # long operation).  Fall back to a fresh session so the job is never
                    # left permanently stuck in RUNNING.
                    logger.error(
                        "Execute session commit failed; using fallback session. job_id=%s error=%s",
                        job_id, commit_exc,
                    )
                    self._force_fail_job(job_id, e)
                raise
        finally:
            session.close()

    def _force_fail_job(self, job_id: UUID, original_exc: Exception) -> None:
        """Mark a job FAILED using a fresh session.

        Called when the execute session is corrupted and cannot commit.
        Prevents jobs from being permanently stuck in RUNNING.
        """
        fallback = self._factory()
        try:
            fallback.execute(
                sa_update(Job)
                .where(Job.id == job_id, Job.status == JobStatus.RUNNING)
                .values(
                    status=JobStatus.FAILED,
                    finished_at=utcnow(),
                    error_code=original_exc.__class__.__name__,
                    error_message=str(original_exc)[:2000],
                )
            )
            fallback.commit()
            logger.info("Fallback session marked job FAILED. job_id=%s", job_id)
        except Exception as exc:
            logger.error(
                "Fallback session also failed; job may remain RUNNING. job_id=%s error=%s",
                job_id, exc,
            )
        finally:
            fallback.close()

    def _maybe_fail_parent(self, session: Session, parent_job_id: UUID) -> None:
        """Mark parent FAILED if all its children are in terminal states and none succeeded."""
        _TERMINAL = (JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED)
        non_terminal = (
            session.query(func.count(Job.id))
            .filter(Job.parent_job_id == parent_job_id, Job.status.not_in(_TERMINAL))
            .scalar()
        )
        if non_terminal and non_terminal > 0:
            return  # still children running/queued

        succeeded = (
            session.query(func.count(Job.id))
            .filter(Job.parent_job_id == parent_job_id, Job.status == JobStatus.SUCCEEDED)
            .scalar()
        )
        if succeeded and succeeded > 0:
            return  # at least one child succeeded — parent handled by _update_parent_progress

        # All children terminal, none succeeded → fail the parent
        session.execute(
            sa_update(Job)
            .where(Job.id == parent_job_id, Job.status == JobStatus.RUNNING)
            .values(
                status=JobStatus.FAILED,
                finished_at=utcnow(),
                error_code="AllChildrenFailed",
                error_message="All child jobs failed or were cancelled",
            )
        )
        self._emit(session, parent_job_id, "job.failed",
                   "All child jobs failed or were cancelled",
                   {"reason": "all_children_failed"}, level="error")

    @staticmethod
    def _emit(
        session: Session,
        job_id: UUID,
        event: str,
        message: str | None,
        fields: dict[str, Any],
        *,
        level: str,
    ) -> None:
        session.add(JobEvent(job_id=job_id, event=event, message=message, fields=fields, level=level))
