# protea/workers/base_worker.py
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import func
from sqlalchemy import update as sa_update
from sqlalchemy.engine import CursorResult
from sqlalchemy.orm import Session, sessionmaker

from protea.core.contracts.operation import OperationResult, RetryLaterError, make_safe_emit
from protea.core.contracts.registry import OperationRegistry
from protea.core.retry import RetryPolicy, is_retryable, with_retry
from protea.core.utils import utcnow
from protea.infrastructure.orm.models.job import Job, JobEvent, JobStatus
from protea.infrastructure.queue.publisher import publish_job, publish_operation

logger = logging.getLogger(__name__)


class WorkerShutdown(Exception):
    """Raised when a worker is asked to exit while a job is in flight.

    Used by the consumer SIGTERM handler to mark the in-flight job FAILED
    through ``BaseWorker._force_fail_job`` before the process exits, so
    deploy-keeper redeploys never leave jobs orphaned in RUNNING.
    """


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
        Transient infrastructure failures (Postgres deadlocks, brief
        connection resets) are retried up to 3 times with exponential
        backoff before the job is marked FAILED. Re-raises any exception
        from the operation after recording FAILED status.
        """
        if not self._claim_job(job_id):
            return
        try:
            with_retry(
                self._execute_with_session,
                job_id,
                policy=RetryPolicy(
                    max_attempts=3,
                    base_delay=1.0,
                    max_delay=10.0,
                    jitter_ratio=0.3,
                ),
            )
        except RetryLaterError:
            # Consumer re-publishes; job is already QUEUED.
            raise
        except Exception as exc:
            # Any other exception leaves the job in RUNNING unless we
            # close it out here. Two real-world paths hit this branch:
            #   - retryable infra error exhausted by ``with_retry`` (the
            #     execute session already rolled back, but no FAILED
            #     transition was committed).
            #   - non-retryable error raised BEFORE the operation ran
            #     (e.g. ``InFailedSqlTransaction`` on the ``session.get``
            #     inside ``_execute_with_session``), so the
            #     ``_on_operation_failure`` path that normally records
            #     FAILED never got a chance to fire.
            # ``_force_fail_job`` is idempotent (UPDATE ... WHERE
            # status=RUNNING) and uses a fresh session from the pool, so
            # invoking it unconditionally is safe even when the primary
            # session is aborted.
            self._force_fail_job(job_id, exc)
            raise

    #: Initial lease duration. The worker extends it via heartbeat while the
    #: job is running. Chosen to be safely larger than the heartbeat interval
    #: (PROTEA_JOB_HEARTBEAT_INTERVAL_SECONDS, default 30s) so a single missed
    #: heartbeat does not prematurely expire the lease.
    _LEASE_SECONDS: int = 120

    def _claim_job(self, job_id: UUID) -> bool:
        """Transition the job from QUEUED to RUNNING via a conditional UPDATE.

        Uses a single atomic UPDATE (WHERE id=:j AND status='queued') and
        checks the affected rowcount to detect race conditions. Returns True
        if the claim succeeded, False if the job is missing or already in a
        non-QUEUED state (e.g. a duplicate consumer picked it up first).

        Also sets ``started_at`` and the initial ``leased_until`` timestamp
        so the stale-job reaper can track liveness without relying on
        ``started_at`` alone.
        """
        session = self._factory()
        try:
            now = utcnow()
            result: CursorResult[Any] = session.execute(  # type: ignore[assignment]
                sa_update(Job)
                .where(Job.id == job_id, Job.status == JobStatus.QUEUED)
                .values(
                    status=JobStatus.RUNNING,
                    started_at=now,
                    leased_until=now + timedelta(seconds=self._LEASE_SECONDS),
                )
            )
            if result.rowcount == 0:
                # Job is missing or was already claimed by another consumer.
                session.rollback()
                return False

            self._emit(
                session,
                job_id,
                "job.started",
                None,
                {"worker": self._config.worker_name},
                level="info",
            )
            session.commit()
            return True
        finally:
            session.close()

    def extend_lease(self, job_id: UUID) -> None:
        """Extend the ``leased_until`` timestamp for a running job.

        Called periodically by the consumer heartbeat loop while a job
        is in progress. Uses a fresh session so the heartbeat never
        interferes with the execute session's transaction. No-op when the
        job is no longer in RUNNING state (e.g. already succeeded or was
        externally cancelled).
        """
        session = self._factory()
        try:
            now = utcnow()
            session.execute(
                sa_update(Job)
                .where(Job.id == job_id, Job.status == JobStatus.RUNNING)
                .values(leased_until=now + timedelta(seconds=self._LEASE_SECONDS))
            )
            session.commit()
        except Exception as exc:
            logger.warning(
                "Lease extension failed (non-fatal). job_id=%s error=%s", job_id, exc
            )
            try:
                session.rollback()
            except Exception:
                pass
        finally:
            session.close()

    def _execute_with_session(self, job_id: UUID) -> None:
        """Run the operation in a fresh session.

        Called by ``handle_job`` through ``with_retry`` so transient
        infrastructure failures (deadlocks, etc.) get a clean session
        on each attempt. Non-retryable exceptions propagate through to
        the FAILED-handling branch below; ``RetryLaterError`` is also
        propagated so the consumer can re-publish.
        """
        session = self._factory()
        try:
            job = session.get(Job, job_id)
            if job is None:
                return

            if self._cancel_if_parent_cancelled(session, job, job_id):
                return

            op = self._registry.get(job.operation)
            emit = make_safe_emit(self._build_emit(job_id))
            enhanced_payload = {**job.payload, "_job_id": str(job.id)}

            try:
                result: OperationResult = op.execute(session, enhanced_payload, emit=emit)
                self._on_operation_success(session, job, job_id, result)
            except RetryLaterError as e:
                self._on_retry_later(session, job, job_id, e)
                raise
            except Exception as e:
                if is_retryable(e):
                    # Let with_retry handle this; rollback so the next
                    # attempt sees a clean session state.
                    try:
                        session.rollback()
                    except Exception:
                        pass
                    raise
                self._on_operation_failure(session, job, job_id, e)
                raise
        finally:
            session.close()

    def _build_emit(self, job_id: UUID):
        """Build the raw emit closure that writes JobEvent rows.

        Returned callable opens a short-lived session per event so
        progress is visible in real time. Wrapped by ``make_safe_emit``
        before being handed to operations so emit failures never crash
        the job.
        """

        def raw_emit(
            event: str,
            message: str | None = None,
            fields: dict[str, Any] | None = None,
            level: str = "info",
        ) -> None:
            f = fields or {}
            event_session = self._factory()
            try:
                self._emit(event_session, job_id, event, message, f, level=level)
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

        return raw_emit

    def _cancel_if_parent_cancelled(
        self, session: Session, job: Job, job_id: UUID
    ) -> bool:
        if job.parent_job_id is None:
            return False
        parent = session.get(Job, job.parent_job_id)
        if parent is None or parent.status != JobStatus.CANCELLED:
            return False
        job.status = JobStatus.CANCELLED
        job.finished_at = utcnow()
        self._emit(
            session,
            job_id,
            "job.cancelled",
            None,
            {"reason": "parent_cancelled"},
            level="info",
        )
        session.commit()
        return True

    def _on_operation_success(
        self, session: Session, job: Job, job_id: UUID, result: OperationResult
    ) -> None:
        # Long-running operations (multi-hour exports, training jobs) can
        # outlive the underlying DB connection: pika reconnects, idle pools
        # recycle, and the original Job ORM instance becomes detached. A
        # plain attribute access then triggers a refresh on a dead session
        # and raises "Instance ... is not bound to a Session". Re-fetch via
        # ``session.get`` so we either get a bound instance back or surface
        # a clean None to handle gracefully. ``pool_pre_ping=True`` on the
        # engine already revives the connection on the SELECT below.
        job = self._rebind_job(session, job, job_id)
        if result.progress_current is not None:
            job.progress_current = int(result.progress_current)
        if result.progress_total is not None:
            job.progress_total = int(result.progress_total)

        if result.deferred:
            self._emit(
                session,
                job_id,
                "job.dispatched",
                None,
                {"result": result.result},
                level="info",
            )
        else:
            job.status = JobStatus.SUCCEEDED
            job.finished_at = utcnow()
            self._emit(
                session,
                job_id,
                "job.succeeded",
                None,
                {"result": result.result},
                level="info",
            )
        session.commit()

        if result.publish_after_commit and self._amqp_url:
            for queue_name, child_job_id in result.publish_after_commit:
                publish_job(self._amqp_url, queue_name, child_job_id)
        if result.publish_operations and self._amqp_url:
            for queue_name, op_payload in result.publish_operations:
                publish_operation(self._amqp_url, queue_name, op_payload)

    def _on_retry_later(
        self, session: Session, job: Job, job_id: UUID, exc: RetryLaterError
    ) -> None:
        retry_count = (
            session.query(func.count(JobEvent.id))
            .filter(JobEvent.job_id == job_id, JobEvent.event == "job.retry_later")
            .scalar()
            or 0
        )
        delay = min(exc.delay_seconds * (2**retry_count), 600)
        job.status = JobStatus.QUEUED
        job.started_at = None
        self._emit(
            session,
            job_id,
            "job.retry_later",
            str(exc),
            {"delay_seconds": delay, "retry_count": retry_count + 1},
            level="info",
        )
        session.commit()
        exc.delay_seconds = delay

    def _on_operation_failure(
        self, session: Session, job: Job, job_id: UUID, exc: Exception
    ) -> None:
        # Same rebind dance as _on_operation_success: an op that took hours
        # may have left the Job ORM instance detached from a recycled
        # connection. Pull a fresh copy before mutating it.
        job = self._rebind_job(session, job, job_id)
        job.status = JobStatus.FAILED
        job.finished_at = utcnow()
        job.error_code = exc.__class__.__name__
        job.error_message = str(exc)
        self._emit(
            session,
            job_id,
            "job.failed",
            str(exc),
            {"error_code": job.error_code},
            level="error",
        )
        if job.parent_job_id is not None:
            self._maybe_fail_parent(session, job.parent_job_id)
        try:
            session.commit()
        except Exception as commit_exc:
            logger.error(
                "Execute session commit failed; using fallback session. job_id=%s error=%s",
                job_id,
                commit_exc,
            )
            self._force_fail_job(job_id, exc)

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
                job_id,
                exc,
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
        self._emit(
            session,
            parent_job_id,
            "job.failed",
            "All child jobs failed or were cancelled",
            {"reason": "all_children_failed"},
            level="error",
        )

    @staticmethod
    def _rebind_job(session: Session, job: Job, job_id: UUID) -> Job:
        """Return a Job ORM instance guaranteed bound to ``session``.

        Long operations (multi-hour exports, retrains) may outlive the
        original DB connection. When that happens the cached ``job``
        instance is detached and any attribute read raises
        ``DetachedInstanceError``. ``session.get`` issues a fresh SELECT
        (revived by ``pool_pre_ping=True``) and returns a bound row. If
        the row has somehow disappeared we fall back to the original
        instance and merge it back into the session so callers can still
        record a terminal state.
        """
        fresh = session.get(Job, job_id)
        if fresh is not None:
            return fresh
        # Row genuinely missing (rare). Re-attach the in-memory copy so
        # subsequent attribute writes do not explode the session.
        try:
            return session.merge(job)
        except Exception:
            return job

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
        session.add(
            JobEvent(job_id=job_id, event=event, message=message, fields=fields, level=level)
        )
