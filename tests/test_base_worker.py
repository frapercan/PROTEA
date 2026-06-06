"""
Unit tests for BaseWorker and StaleJobReaper.
Uses a mocked session factory and a fake Operation — no real DB needed.
"""

from __future__ import annotations

import signal
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from protea.core.contracts.operation import OperationResult, RetryLaterError
from protea.core.contracts.registry import OperationRegistry
from protea.infrastructure.orm.models.job import Job, JobStatus
from protea.workers.base_worker import BaseWorker, WorkerConfig
from protea.workers.stale_job_reaper import StaleJobReaper, StaleJobReaperConfig

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_registry(op_name: str = "ping", result: OperationResult = None, raises=None):
    op = MagicMock()
    op.name = op_name
    if raises:
        op.execute.side_effect = raises
    else:
        op.execute.return_value = result or OperationResult(result={"ok": True})

    reg = OperationRegistry()
    reg.register(op)
    return reg, op


def _make_job(status=JobStatus.QUEUED, operation="ping", parent_job_id=None):
    job = MagicMock(spec=Job)
    job.id = uuid4()
    job.status = status
    job.operation = operation
    job.payload = {}
    job.parent_job_id = parent_job_id
    return job


def _make_factory(job):
    """Session factory that always returns a session holding `job`."""
    session = MagicMock()
    session.get.return_value = job
    session.__enter__ = lambda s: s
    session.__exit__ = MagicMock(return_value=False)
    factory = MagicMock(return_value=session)
    return factory, session


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestBaseWorkerHandleJob:
    def test_unknown_job_id_does_nothing(self):
        """If the job row doesn't exist, handle_job returns silently."""
        session = MagicMock()
        session.get.return_value = None
        factory = MagicMock(return_value=session)
        registry, _ = _make_registry()

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        worker.handle_job(uuid4())  # should not raise

    def test_non_queued_job_is_skipped(self):
        """A job already claimed by another consumer (rowcount=0) is not executed again.

        The new conditional-UPDATE _claim_job returns False when the DB
        reports rowcount=0 (job was not in QUEUED state when the UPDATE
        ran). We simulate that by returning a mock result with rowcount=0.
        """
        job = _make_job(status=JobStatus.RUNNING)
        session = MagicMock()
        claim_result = MagicMock()
        claim_result.rowcount = 0  # UPDATE found no QUEUED row
        session.execute.return_value = claim_result
        session.get.return_value = job
        factory = MagicMock(return_value=session)
        registry, op = _make_registry()

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        worker.handle_job(job.id)

        op.execute.assert_not_called()

    def test_successful_job_transitions_to_succeeded(self):
        job = _make_job()
        # factory returns same session and job for both claim and execute passes
        session = MagicMock()
        session.get.return_value = job
        factory = MagicMock(return_value=session)
        registry, op = _make_registry(result=OperationResult(result={"x": 1}))

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        worker.handle_job(job.id)

        assert job.status == JobStatus.SUCCEEDED
        assert job.finished_at is not None
        op.execute.assert_called_once()

    def test_failing_job_transitions_to_failed(self):
        job = _make_job()
        session = MagicMock()
        session.get.return_value = job
        factory = MagicMock(return_value=session)
        registry, op = _make_registry(raises=RuntimeError("boom"))

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        with pytest.raises(RuntimeError, match="boom"):
            worker.handle_job(job.id)

        assert job.status == JobStatus.FAILED
        assert job.error_code == "RuntimeError"
        assert "boom" in job.error_message

    def test_emit_writes_job_events(self):
        """Verify that session.add is called for each emit during execution."""
        job = _make_job()
        session = MagicMock()
        session.get.return_value = job
        factory = MagicMock(return_value=session)

        # Operation that calls emit once
        def _execute(sess, payload, *, emit):
            emit("custom.event", "hello", {"k": 1}, "info")
            return OperationResult()

        op = MagicMock()
        op.name = "ping"
        op.execute.side_effect = _execute
        registry = OperationRegistry()
        registry.register(op)

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        worker.handle_job(job.id)

        # session.add should have been called at least for: job.started, custom.event, job.succeeded
        assert session.add.call_count >= 3

    def test_progress_fields_are_set(self):
        job = _make_job()
        session = MagicMock()
        session.get.return_value = job
        factory = MagicMock(return_value=session)
        registry, _ = _make_registry(
            result=OperationResult(result={}, progress_current=5, progress_total=10)
        )

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        worker.handle_job(job.id)

        assert job.progress_current == 5
        assert job.progress_total == 10

    def test_retry_later_uses_adaptive_backoff(self):
        """RetryLaterError delay should increase based on previous retry count."""
        job = _make_job()
        session = MagicMock()
        session.get.return_value = job
        # Simulate 2 previous retries
        session.query.return_value.filter.return_value.scalar.return_value = 2
        factory = MagicMock(return_value=session)

        registry, _ = _make_registry(raises=RetryLaterError("GPU busy", delay_seconds=30))

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        with pytest.raises(RetryLaterError) as exc_info:
            worker.handle_job(job.id)

        # 30 * 2^2 = 120 seconds
        assert exc_info.value.delay_seconds == 120
        assert job.status == JobStatus.QUEUED

    def test_retry_backoff_capped_at_600(self):
        """Adaptive backoff should be capped at 600 seconds."""
        job = _make_job()
        session = MagicMock()
        session.get.return_value = job
        # Simulate 10 previous retries → 60 * 2^10 = 61440, capped to 600
        session.query.return_value.filter.return_value.scalar.return_value = 10
        factory = MagicMock(return_value=session)

        registry, _ = _make_registry(raises=RetryLaterError("GPU busy", delay_seconds=60))

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        with pytest.raises(RetryLaterError) as exc_info:
            worker.handle_job(job.id)

        assert exc_info.value.delay_seconds == 600

    def test_retryable_db_error_is_retried_then_succeeds(self):
        """Postgres deadlock during op.execute() retries until the op succeeds."""
        from sqlalchemy.exc import OperationalError

        job = _make_job()
        session = MagicMock()
        session.get.return_value = job
        factory = MagicMock(return_value=session)

        # First call raises retryable OperationalError; second succeeds.
        attempts = {"n": 0}

        def _execute(sess, payload, *, emit):
            attempts["n"] += 1
            if attempts["n"] == 1:
                orig = MagicMock()
                orig.pgcode = "40P01"  # deadlock_detected
                err = OperationalError("stmt", {}, orig)
                err.orig = orig
                raise err
            return OperationResult(result={"ok": True})

        op = MagicMock()
        op.name = "ping"
        op.execute.side_effect = _execute
        registry = OperationRegistry()
        registry.register(op)

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        with patch("protea.core.retry.time.sleep"):
            worker.handle_job(job.id)

        assert attempts["n"] == 2
        assert job.status == JobStatus.SUCCEEDED

    def test_retryable_db_error_max_attempts_then_fails(self):
        """If retryable error keeps recurring, after max_attempts the job is FAILED."""
        from sqlalchemy.exc import OperationalError

        job = _make_job()
        session = MagicMock()
        session.get.return_value = job
        factory = MagicMock(return_value=session)

        def _always_deadlock(sess, payload, *, emit):
            orig = MagicMock()
            orig.pgcode = "40P01"
            err = OperationalError("stmt", {}, orig)
            err.orig = orig
            raise err

        op = MagicMock()
        op.name = "ping"
        op.execute.side_effect = _always_deadlock
        registry = OperationRegistry()
        registry.register(op)

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        with patch("protea.core.retry.time.sleep"):
            with pytest.raises(OperationalError):
                worker.handle_job(job.id)

        # 3 attempts (max_attempts=3 in handle_job).
        assert op.execute.call_count == 3
        # Retry-exhausted path uses the fallback session via sa_update,
        # so the in-memory mock Job is not directly mutated. Verify the
        # fallback session.execute was invoked instead.
        # session was reused as the factory's only fixture, so an
        # update statement should have run on it.
        assert any(
            "UPDATE" in str(call.args[0]).upper() if call.args else False
            for call in session.execute.call_args_list
        )

    def test_non_retryable_error_does_not_retry(self):
        """Non-infrastructure errors propagate immediately, single attempt."""
        job = _make_job()
        session = MagicMock()
        session.get.return_value = job
        factory = MagicMock(return_value=session)
        registry, op = _make_registry(raises=ValueError("bad payload"))

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        with pytest.raises(ValueError, match="bad payload"):
            worker.handle_job(job.id)

        assert op.execute.call_count == 1
        assert job.status == JobStatus.FAILED


# ---------------------------------------------------------------------------
# StaleJobReaper
# ---------------------------------------------------------------------------


class TestStaleJobReaper:
    def test_reaps_stale_running_jobs(self):
        """Legacy rows (leased_until IS NULL) past the timeout with no recent
        events are marked FAILED with ``lease_expired`` (the post-F-OPS-JOBS.1
        successor of the old ``JobTimeout`` code) when no AMQP URL is wired
        and the reaper therefore cannot re-enqueue."""
        stale_job = MagicMock(spec=Job)
        stale_job.id = uuid4()
        stale_job.status = JobStatus.RUNNING
        stale_job.operation = "compute_embeddings"
        stale_job.started_at = datetime.now(UTC) - timedelta(hours=2)
        stale_job.leased_until = None
        stale_job.queue_name = "protea.jobs"

        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = [stale_job]
        # No recent events → scalar() returns None → job qualifies for reaping.
        session.query.return_value.filter.return_value.scalar.return_value = None
        factory = MagicMock(return_value=session)

        # No amqp_url → reaper has no re-enqueue path, falls straight to FAILED.
        reaper = StaleJobReaper(factory)
        count = reaper._reap()

        assert count == 1
        assert stale_job.status == JobStatus.FAILED
        assert stale_job.error_code == "lease_expired"
        session.add.assert_called_once()  # JobEvent
        session.commit.assert_called_once()

    def test_reaper_skips_legacy_job_with_recent_activity(self):
        """A legacy candidate job (leased_until IS NULL) with a JobEvent
        inside the stall window is left alone — the heartbeat-less path
        preserves the pre-F-OPS-JOBS.1 grace period."""
        live_job = MagicMock(spec=Job)
        live_job.id = uuid4()
        live_job.status = JobStatus.RUNNING
        live_job.operation = "compute_embeddings"
        live_job.started_at = datetime.now(UTC) - timedelta(hours=8)
        live_job.leased_until = None
        live_job.queue_name = "protea.jobs"

        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = [live_job]
        # Last event was 5 minutes ago — inside the 30-min stall window.
        session.query.return_value.filter.return_value.scalar.return_value = datetime.now(
            UTC
        ) - timedelta(minutes=5)
        factory = MagicMock(return_value=session)

        reaper = StaleJobReaper(
            factory,
            config=StaleJobReaperConfig(timeout_seconds=3600, stall_seconds=1800),
        )
        count = reaper._reap()

        assert count == 0
        assert live_job.status == JobStatus.RUNNING  # unchanged
        session.add.assert_not_called()
        session.commit.assert_called_once()

    def test_reaper_kills_job_with_stale_activity(self):
        """A candidate whose most recent event is older than stall_seconds is reaped."""
        stalled_job = MagicMock(spec=Job)
        stalled_job.id = uuid4()
        stalled_job.status = JobStatus.RUNNING
        stalled_job.operation = "compute_embeddings"
        stalled_job.started_at = datetime.now(UTC) - timedelta(hours=8)
        stalled_job.leased_until = None
        stalled_job.queue_name = "protea.jobs"

        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = [stalled_job]
        # First scalar() returns the (stale) last_event_ts; second returns 0
        # because no prior requeue attempt has been recorded.
        session.query.return_value.filter.return_value.scalar.side_effect = [
            datetime.now(UTC) - timedelta(hours=2),
            0,
        ]
        factory = MagicMock(return_value=session)

        reaper = StaleJobReaper(
            factory,
            config=StaleJobReaperConfig(timeout_seconds=3600, stall_seconds=1800),
        )
        count = reaper._reap()

        assert count == 1
        assert stalled_job.status == JobStatus.FAILED
        assert stalled_job.error_code == "lease_expired"

    def test_no_stale_jobs_returns_zero(self):
        """When no jobs are stale, reaper does nothing."""
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = []
        factory = MagicMock(return_value=session)

        reaper = StaleJobReaper(factory)
        count = reaper._reap()

        assert count == 0
        session.commit.assert_called_once()

    def test_reaper_handles_db_error_gracefully(self):
        """If the DB query fails, reaper raises but does not crash permanently."""
        session = MagicMock()
        session.query.side_effect = RuntimeError("DB connection lost")
        factory = MagicMock(return_value=session)

        reaper = StaleJobReaper(factory)
        with pytest.raises(RuntimeError, match="DB connection lost"):
            reaper._reap()
        session.rollback.assert_called_once()

    def test_reaper_rollback_also_fails(self):
        """If rollback itself raises, the exception from _reap still propagates."""
        session = MagicMock()
        session.query.side_effect = RuntimeError("DB gone")
        session.rollback.side_effect = RuntimeError("rollback failed too")
        factory = MagicMock(return_value=session)

        reaper = StaleJobReaper(factory)
        with pytest.raises(RuntimeError, match="DB gone"):
            reaper._reap()
        session.rollback.assert_called_once()
        session.close.assert_called_once()

    def test_run_registers_signal_handlers(self):
        """run() should register SIGINT and SIGTERM handlers."""
        factory = MagicMock()
        reaper = StaleJobReaper(factory)
        # Make _reap set _stop=True so the loop exits after one iteration
        reaper._stop = False
        call_count = [0]

        def fake_reap():
            call_count[0] += 1
            reaper._stop = True
            return 0

        reaper._reap = fake_reap

        with (
            patch("protea.workers.stale_job_reaper.signal.signal") as mock_signal,
            patch("protea.workers.stale_job_reaper.time.sleep"),
        ):
            reaper.run(interval_seconds=1)

        # Should register both SIGINT and SIGTERM
        calls = [c[0] for c in mock_signal.call_args_list]
        assert (signal.SIGINT, reaper._handle_stop) in calls
        assert (signal.SIGTERM, reaper._handle_stop) in calls

    def test_run_loops_and_stops_on_flag(self):
        """run() calls _reap repeatedly until _stop is set."""
        factory = MagicMock()
        reaper = StaleJobReaper(factory)
        reap_count = [0]

        def fake_reap():
            reap_count[0] += 1
            if reap_count[0] >= 3:
                reaper._stop = True
            return 0

        reaper._reap = fake_reap

        with (
            patch("protea.workers.stale_job_reaper.signal.signal"),
            patch("protea.workers.stale_job_reaper.time.sleep"),
        ):
            reaper.run(interval_seconds=1)

        assert reap_count[0] == 3

    def test_run_logs_reaped_count(self):
        """When _reap returns non-zero, run() logs it."""
        factory = MagicMock()
        reaper = StaleJobReaper(factory)

        def fake_reap():
            reaper._stop = True
            return 5

        reaper._reap = fake_reap

        with (
            patch("protea.workers.stale_job_reaper.signal.signal"),
            patch("protea.workers.stale_job_reaper.time.sleep"),
            patch("protea.workers.stale_job_reaper.logger") as mock_logger,
        ):
            reaper.run(interval_seconds=1)

        # Should have logged the reaped count
        info_calls = [str(c) for c in mock_logger.info.call_args_list]
        assert any("5" in c for c in info_calls)

    def test_run_catches_reap_exception(self):
        """If _reap raises, run() logs the error and continues."""
        factory = MagicMock()
        reaper = StaleJobReaper(factory)
        call_count = [0]

        def failing_reap():
            call_count[0] += 1
            if call_count[0] == 1:
                raise RuntimeError("transient DB error")
            reaper._stop = True
            return 0

        reaper._reap = failing_reap

        with (
            patch("protea.workers.stale_job_reaper.signal.signal"),
            patch("protea.workers.stale_job_reaper.time.sleep"),
            patch("protea.workers.stale_job_reaper.logger") as mock_logger,
        ):
            reaper.run(interval_seconds=1)

        # Should have logged the error but continued
        mock_logger.error.assert_called_once()
        assert call_count[0] == 2

    def test_handle_stop_sets_flag(self):
        """_handle_stop sets the _stop flag."""
        factory = MagicMock()
        reaper = StaleJobReaper(factory)
        assert reaper._stop is False
        reaper._handle_stop(signal.SIGINT, None)
        assert reaper._stop is True


# ---------------------------------------------------------------------------
# Feature engineering warmup
# ---------------------------------------------------------------------------


class TestTaxonomyWarmup:
    def test_warmup_calls_get_ncbi(self):
        from protea.core.feature_engineering import warmup_taxonomy_db

        with (
            patch("protea.core.feature_engineering._get_ncbi") as mock_get,
            patch("protea.core.feature_engineering._ETE3_AVAILABLE", True),
        ):
            warmup_taxonomy_db()
        mock_get.assert_called_once()

    def test_warmup_skips_when_ete3_unavailable(self):
        from protea.core.feature_engineering import warmup_taxonomy_db

        with (
            patch("protea.core.feature_engineering._ETE3_AVAILABLE", False),
            patch("protea.core.feature_engineering._get_ncbi") as mock_get,
        ):
            warmup_taxonomy_db()  # should not raise
        mock_get.assert_not_called()


# ---------------------------------------------------------------------------
# BaseWorker — extended coverage
# ---------------------------------------------------------------------------


class TestBaseWorkerParentCancelled:
    """Cover parent_job_id cancellation detection (lines 93-106)."""

    def test_cancelled_parent_cancels_child(self):
        """If parent is CANCELLED during claim, child should be CANCELLED too."""
        parent_id = uuid4()
        child_job = _make_job(parent_job_id=parent_id)
        parent_job = MagicMock(spec=Job)
        parent_job.id = parent_id
        parent_job.status = JobStatus.CANCELLED

        session = MagicMock()

        # session.get returns child_job by default, parent_job when queried by parent_id
        def get_side_effect(model, id_val):
            if id_val == parent_id:
                return parent_job
            return child_job

        session.get.side_effect = get_side_effect

        factory = MagicMock(return_value=session)
        registry, op = _make_registry()

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        worker.handle_job(child_job.id)

        assert child_job.status == JobStatus.CANCELLED
        assert child_job.finished_at is not None
        op.execute.assert_not_called()

    def test_active_parent_does_not_cancel_child(self):
        """If parent is still RUNNING, child should execute normally."""
        parent_id = uuid4()
        child_job = _make_job(parent_job_id=parent_id)
        parent_job = MagicMock(spec=Job)
        parent_job.id = parent_id
        parent_job.status = JobStatus.RUNNING

        session = MagicMock()

        def get_side_effect(model, id_val):
            if id_val == parent_id:
                return parent_job
            return child_job

        session.get.side_effect = get_side_effect

        factory = MagicMock(return_value=session)
        registry, op = _make_registry()

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        worker.handle_job(child_job.id)

        assert child_job.status == JobStatus.SUCCEEDED
        op.execute.assert_called_once()


class TestBaseWorkerUnknownOperation:
    """Cover unknown operation name — registry.get raises KeyError."""

    def test_unknown_operation_raises_key_error(self):
        """KeyError from registry.get propagates without being caught by inner handler."""
        job = _make_job(operation="nonexistent_op")
        session = MagicMock()
        session.get.return_value = job

        factory = MagicMock(return_value=session)
        # Real registry with no operations registered
        registry = OperationRegistry()

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        with pytest.raises(KeyError, match="nonexistent_op"):
            worker.handle_job(job.id)

        # Session should still be closed (finally block)
        session.close.assert_called()


class TestBaseWorkerTwoSessionPattern:
    """Verify the two-session pattern: claim session commits before execute session."""

    def test_two_sessions_are_created(self):
        job = _make_job()
        sessions = []

        def make_session():
            s = MagicMock()
            s.get.return_value = job
            sessions.append(s)
            return s

        factory = MagicMock(side_effect=make_session)
        registry, op = _make_registry()

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        worker.handle_job(job.id)

        # Two sessions: claim + execute
        assert len(sessions) >= 2
        # Both should have been committed
        sessions[0].commit.assert_called()
        sessions[1].commit.assert_called()
        # Both should have been closed
        sessions[0].close.assert_called_once()
        sessions[1].close.assert_called_once()

    def test_claim_session_issues_conditional_update(self):
        """First session issues a conditional UPDATE (WHERE status='queued').

        After F-OPS-JOBS.1a the claim path uses an atomic UPDATE with a WHERE
        clause instead of ORM read-then-write, so the in-memory ``job.status``
        field is NOT mutated to RUNNING by the claim session. The invariant is
        now: the claim session calls ``session.execute(sa_update(...))`` with
        status='running' and ``leased_until``, then commits if rowcount > 0.
        """
        from sqlalchemy import update as sa_update

        from protea.infrastructure.orm.models.job import Job

        job = _make_job()
        sessions = []

        def make_session():
            s = MagicMock()
            s.get.return_value = job
            # Default rowcount is a truthy MagicMock; claim succeeds
            sessions.append(s)
            return s

        factory = MagicMock(side_effect=make_session)
        registry, _ = _make_registry()

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        worker.handle_job(job.id)

        # Session 0 (claim) must have called execute with an UPDATE statement
        claim_session = sessions[0]
        claim_session.execute.assert_called_once()
        stmt = claim_session.execute.call_args.args[0]
        assert isinstance(stmt, type(sa_update(Job)))
        # Confirm the UPDATE targets QUEUED → RUNNING and sets leased_until
        compiled = stmt.compile(compile_kwargs={"literal_binds": False})
        params = compiled.params
        assert params.get("status_1") == JobStatus.RUNNING or "RUNNING" in str(params).upper()
        assert "leased_until" in compiled.params
        # Claim session must commit
        claim_session.commit.assert_called()


class TestBaseWorkerJobNotFoundOnExecute:
    """Cover line 88: job is None on the execute session."""

    def test_job_disappears_between_sessions(self):
        job = _make_job()
        call_count = [0]

        def make_session():
            s = MagicMock()
            call_count[0] += 1
            if call_count[0] == 1:
                # Claim session finds the job
                s.get.return_value = job
            else:
                # Execute session: job is gone
                s.get.return_value = None
            return s

        factory = MagicMock(side_effect=make_session)
        registry, op = _make_registry()

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        worker.handle_job(job.id)

        op.execute.assert_not_called()


class TestBaseWorkerProgressFromResult:
    """Cover progress update from OperationResult (lines 139-142)."""

    def test_progress_current_only(self):
        job = _make_job()
        session = MagicMock()
        session.get.return_value = job
        factory = MagicMock(return_value=session)
        registry, _ = _make_registry(result=OperationResult(result={}, progress_current=42))

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        worker.handle_job(job.id)

        assert job.progress_current == 42

    def test_no_progress_fields_leaves_job_unchanged(self):
        job = _make_job()
        job.progress_current = None
        job.progress_total = None
        session = MagicMock()
        session.get.return_value = job
        factory = MagicMock(return_value=session)
        registry, _ = _make_registry(result=OperationResult(result={}))

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        worker.handle_job(job.id)

        # progress_current/total should not be set if result has None
        # (succeeded is set, but progress fields are untouched)
        assert job.status == JobStatus.SUCCEEDED


class TestBaseWorkerDeferredResult:
    """Cover deferred result handling (lines 144-153)."""

    def test_deferred_result_does_not_set_succeeded(self):
        job = _make_job()
        session = MagicMock()
        session.get.return_value = job
        factory = MagicMock(return_value=session)
        registry, _ = _make_registry(
            result=OperationResult(result={"dispatched": True}, deferred=True)
        )

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        worker.handle_job(job.id)

        # Deferred: should NOT transition to SUCCEEDED.
        # Since _claim_job now uses a SQL UPDATE (not ORM mutation), the
        # in-memory mock job.status reflects whatever state the mock held before
        # the test ran (QUEUED). The key assertion is that SUCCEEDED was never
        # written to the job — not that the in-memory object became RUNNING.
        assert job.status != JobStatus.SUCCEEDED

    def test_deferred_result_clears_lease(self):
        """FIX-PREDICT-COORD-RELIABLE: a deferred coordinator clears
        ``leased_until`` so the consumer (which stops heartbeating after the
        deferred return) does not leave a stale claim-time lease that the
        reaper would requeue mid-flight."""
        job = _make_job()
        job.leased_until = datetime.now(UTC) + timedelta(seconds=120)
        session = MagicMock()
        session.get.return_value = job
        factory = MagicMock(return_value=session)
        registry, _ = _make_registry(
            result=OperationResult(result={"dispatched": True}, deferred=True)
        )

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        worker.handle_job(job.id)

        assert job.status != JobStatus.SUCCEEDED
        # Lease cleared → reaper falls back to event-based liveness, never the
        # 120s lease-expiry requeue path.
        assert job.leased_until is None


class TestBaseWorkerPublishAfterCommit:
    """Cover publish_after_commit and publish_operations (lines 169-176)."""

    def test_publish_after_commit_publishes_child_jobs(self):
        child_id = uuid4()
        job = _make_job()
        session = MagicMock()
        session.get.return_value = job
        factory = MagicMock(return_value=session)
        registry, _ = _make_registry(
            result=OperationResult(
                result={},
                publish_after_commit=[("protea.jobs", child_id)],
            )
        )

        worker = BaseWorker(
            factory,
            registry,
            WorkerConfig(worker_name="test"),
            amqp_url="amqp://localhost/",
        )

        with patch("protea.workers.base_worker.publish_job") as mock_pub:
            worker.handle_job(job.id)

        mock_pub.assert_called_once_with("amqp://localhost/", "protea.jobs", child_id)

    def test_publish_operations_publishes_ephemeral_messages(self):
        job = _make_job()
        session = MagicMock()
        session.get.return_value = job
        factory = MagicMock(return_value=session)
        registry, _ = _make_registry(
            result=OperationResult(
                result={},
                publish_operations=[
                    ("protea.embeddings.batch", {"batch_data": [1, 2]}),
                ],
            )
        )

        worker = BaseWorker(
            factory,
            registry,
            WorkerConfig(worker_name="test"),
            amqp_url="amqp://localhost/",
        )

        with patch("protea.workers.base_worker.publish_operation") as mock_pub:
            worker.handle_job(job.id)

        mock_pub.assert_called_once_with(
            "amqp://localhost/", "protea.embeddings.batch", {"batch_data": [1, 2]}
        )

    def test_no_amqp_url_skips_publish(self):
        """Without amqp_url, publish_after_commit is silently skipped."""
        child_id = uuid4()
        job = _make_job()
        session = MagicMock()
        session.get.return_value = job
        factory = MagicMock(return_value=session)
        registry, _ = _make_registry(
            result=OperationResult(
                result={},
                publish_after_commit=[("protea.jobs", child_id)],
            )
        )

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))

        with patch("protea.workers.base_worker.publish_job") as mock_pub:
            worker.handle_job(job.id)

        mock_pub.assert_not_called()


class TestBaseWorkerEmitProgress:
    """Cover emit callback writing _progress_current/_progress_total (lines 124-129)."""

    def test_emit_with_progress_fields_updates_job(self):
        job = _make_job()

        sessions = []

        def make_session():
            s = MagicMock()
            s.get.return_value = job
            sessions.append(s)
            return s

        factory = MagicMock(side_effect=make_session)

        def _execute(sess, payload, *, emit):
            emit("progress", "step done", {"_progress_current": 5, "_progress_total": 20}, "info")
            return OperationResult()

        op = MagicMock()
        op.name = "ping"
        op.execute.side_effect = _execute
        registry = OperationRegistry()
        registry.register(op)

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        worker.handle_job(job.id)

        # The emit session (3rd session: claim, execute, emit) should have updated progress
        # Find the session where progress was set
        assert job.progress_current == 5
        assert job.progress_total == 20


class TestBaseWorkerForceFailJob:
    """Cover _force_fail_job (lines 242-263)."""

    def test_force_fail_on_commit_failure(self):
        """When execute session commit fails, _force_fail_job is called."""
        job = _make_job()
        call_count = [0]

        def make_session():
            s = MagicMock()
            s.get.return_value = job
            call_count[0] += 1
            current = call_count[0]
            if current == 2:
                # Execute session: commit raises on second call (after failure recording)
                commit_count = [0]

                def commit_side():
                    commit_count[0] += 1
                    if commit_count[0] == 1:
                        raise RuntimeError("DB connection dropped")

                s.commit.side_effect = commit_side
            return s

        factory = MagicMock(side_effect=make_session)
        registry, _ = _make_registry(raises=ValueError("op failed"))

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))

        with pytest.raises(ValueError, match="op failed"):
            worker.handle_job(job.id)

        # The fallback session (3rd) should have been created
        assert call_count[0] >= 3

    def test_force_fail_direct_call(self):
        """Direct test of _force_fail_job method."""
        job_id = uuid4()
        session = MagicMock()
        factory = MagicMock(return_value=session)
        registry, _ = _make_registry()

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        worker._force_fail_job(job_id, ValueError("original"))

        session.execute.assert_called_once()
        session.commit.assert_called_once()
        session.close.assert_called_once()

    def test_force_fail_handles_fallback_failure(self):
        """If the fallback session also fails, it logs but doesn't crash."""
        job_id = uuid4()
        session = MagicMock()
        session.commit.side_effect = RuntimeError("still broken")
        factory = MagicMock(return_value=session)
        registry, _ = _make_registry()

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        # Should not raise
        worker._force_fail_job(job_id, ValueError("original"))

        session.close.assert_called_once()


class TestBaseWorkerForceFailOnNonRetryablePreOpException:
    """FIX-BASEWORKER-ROBUSTNESS — Bug #2.

    Regression: a non-retryable error raised inside ``_execute_with_session``
    BEFORE the operation gets a chance to run (e.g.
    ``psycopg.errors.InFailedSqlTransaction`` on the leading
    ``session.get(Job, job_id)``) used to bypass both fail paths. The
    outer except only invoked ``_force_fail_job`` when ``is_retryable``
    was True, so the row stayed RUNNING forever (orphan job).

    After the fix, ANY exception (except ``RetryLaterError``) is routed
    through ``_force_fail_job``, which uses a fresh session from the
    pool and writes FAILED idempotently via ``WHERE status=RUNNING``.
    """

    def test_non_retryable_db_error_in_pre_op_phase_marks_failed(self):
        """Simulate Bug #2: session.get raises in _execute_with_session
        with a non-retryable exception; outer except must still mark
        the job FAILED through the fallback session.

        Session call order:
            #1 claim    — returns a QUEUED job, commits cleanly.
            #2 execute  — session.get raises an aborted-transaction
                          equivalent BEFORE the operation runs.
            #3 fallback — opened by _force_fail_job; must commit the
                          UPDATE that flips status RUNNING -> FAILED.
        """
        from sqlalchemy import update as sa_update

        from protea.infrastructure.orm.models.job import Job, JobStatus

        class _AbortedTransaction(RuntimeError):
            """Stand-in for psycopg.errors.InFailedSqlTransaction.

            A plain RuntimeError subclass so ``is_retryable`` returns
            False (no pgcode, not a connection error).
            """

        job_id = uuid4()
        captured: list[MagicMock] = []

        def make_session():
            s = MagicMock()
            captured.append(s)
            current = len(captured)
            if current == 1:
                claim_job = MagicMock()
                claim_job.id = job_id
                claim_job.status = JobStatus.QUEUED
                claim_job.parent_job_id = None
                s.get.return_value = claim_job
            elif current == 2:
                s.get.side_effect = _AbortedTransaction("current transaction is aborted")
            return s

        factory = MagicMock(side_effect=make_session)
        registry, _ = _make_registry()  # op never runs in this test

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        with pytest.raises(_AbortedTransaction):
            worker.handle_job(job_id)

        # Three sessions: claim, execute, fallback.
        assert len(captured) == 3
        fallback = captured[2]
        # The fallback session must have issued an UPDATE Job ...
        # WHERE status=RUNNING with FAILED + the right error_code.
        fallback.execute.assert_called_once()
        stmt = fallback.execute.call_args.args[0]
        assert isinstance(stmt, type(sa_update(Job)))
        fallback.commit.assert_called_once()
        fallback.close.assert_called_once()


class TestBaseWorkerMaybeFailParent:
    """Cover _maybe_fail_parent (lines 267-302)."""

    def test_all_children_failed_marks_parent_failed(self):
        """When all children are terminal and none succeeded, parent fails."""
        parent_id = uuid4()
        job = _make_job(parent_job_id=parent_id)

        session = MagicMock()
        session.get.return_value = job
        # First query: non_terminal count = 0
        # Second query: succeeded count = 0
        query_results = [0, 0]
        call_count = [0]

        def scalar_side():
            idx = call_count[0]
            call_count[0] += 1
            return query_results[idx] if idx < len(query_results) else 0

        session.query.return_value.filter.return_value.scalar.side_effect = scalar_side
        factory = MagicMock(return_value=session)
        registry, _ = _make_registry(raises=RuntimeError("child failed"))

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        with pytest.raises(RuntimeError, match="child failed"):
            worker.handle_job(job.id)

        # session.execute should have been called for the sa_update on parent
        session.execute.assert_called()

    def test_children_still_running_does_not_fail_parent(self):
        """If some children are still running, parent is not failed.

        After FIX-BASEWORKER-ROBUSTNESS ``_force_fail_job`` runs
        unconditionally on every non-RetryLater exception. That fallback
        emits one sa_update against the in-flight job (idempotent via
        ``WHERE status=RUNNING``). After F-OPS-JOBS.1a the claim phase also
        issues an sa_update (conditional claim), so the session sees 2 UPDATE
        calls total: 1 for the claim + 1 for the fallback _force_fail_job.

        The behavioural assertion is: the parent UPDATE does NOT fire; the
        two observed UPDATEs are the claim and the fallback against the
        in-flight job.
        """
        from sqlalchemy import update as sa_update

        from protea.infrastructure.orm.models.job import Job

        parent_id = uuid4()
        job = _make_job(parent_job_id=parent_id)

        session = MagicMock()
        session.get.return_value = job
        # non_terminal count = 3 (children still running)
        session.query.return_value.filter.return_value.scalar.return_value = 3
        factory = MagicMock(return_value=session)
        registry, _ = _make_registry(raises=RuntimeError("child failed"))

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        with pytest.raises(RuntimeError, match="child failed"):
            worker.handle_job(job.id)

        # Exactly 2 execute calls: claim UPDATE (session 1) + fallback UPDATE
        # (_force_fail_job, session 3). No parent UPDATE because
        # _maybe_fail_parent short-circuits when children are still running.
        execute_calls = session.execute.call_args_list
        assert len(execute_calls) == 2

        # The fallback UPDATE (last call) targets the in-flight job, not parent.
        fallback_stmt = execute_calls[-1].args[0]
        assert isinstance(fallback_stmt, type(sa_update(Job)))
        compiled = fallback_stmt.compile(compile_kwargs={"literal_binds": False})
        params = compiled.params
        assert job.id in params.values()
        assert parent_id not in params.values()


class TestBaseWorkerRebindJob:
    """Cover ``_rebind_job`` (long-running op session-detach fix)."""

    def test_rebind_returns_fresh_instance_from_session_get(self):
        """The rebind helper prefers a freshly fetched Job over the cached one."""
        job_id = uuid4()
        stale = MagicMock(spec=Job)
        stale.id = job_id
        fresh = MagicMock(spec=Job)
        fresh.id = job_id
        session = MagicMock()
        session.get.return_value = fresh

        result = BaseWorker._rebind_job(session, stale, job_id)

        assert result is fresh
        session.get.assert_called_once_with(Job, job_id)

    def test_rebind_falls_back_to_merge_when_row_missing(self):
        """If ``session.get`` returns None we merge the in-memory copy back in."""
        job_id = uuid4()
        stale = MagicMock(spec=Job)
        stale.id = job_id
        merged = MagicMock(spec=Job)
        merged.id = job_id
        session = MagicMock()
        session.get.return_value = None
        session.merge.return_value = merged

        result = BaseWorker._rebind_job(session, stale, job_id)

        assert result is merged
        session.merge.assert_called_once_with(stale)

    def test_rebind_falls_back_to_original_when_merge_explodes(self):
        """If even ``merge`` fails we return the original so callers can still write."""
        job_id = uuid4()
        stale = MagicMock(spec=Job)
        stale.id = job_id
        session = MagicMock()
        session.get.return_value = None
        session.merge.side_effect = RuntimeError("session is wrecked")

        result = BaseWorker._rebind_job(session, stale, job_id)

        assert result is stale

    def test_success_path_rebinds_detached_job(self):
        """Regression: a detached Job at success time must be re-fetched, not used as-is.

        Simulates the LB.1 v226 production crash: the op took hours, the
        pool recycled, and the cached ``job`` instance is detached. The
        old code accessed ``job.progress_current = ...`` first and blew
        up with ``DetachedInstanceError``. After the fix, ``_rebind_job``
        replaces it with a freshly fetched row before any attribute
        write, so the final commit succeeds.

        We model the detachment with a ``Job``-shaped stand-in that
        raises ``DetachedInstanceError`` on any non-id attribute access
        (the SQLAlchemy real behaviour). ``session.get`` returns a
        fresh, healthy stand-in on the rebind. Without the fix the
        worker would touch the detached instance first and crash.
        """
        from sqlalchemy.orm.exc import DetachedInstanceError

        stale_id = uuid4()

        class _DetachedJob:
            id = stale_id
            # Required for claim_job and execute pre-rebind reads; we
            # have to expose status/operation/payload/parent_job_id
            # because the worker reads those during the claim and
            # pre-execute phases, well before the rebind happens.
            status = JobStatus.QUEUED
            operation = "ping"
            payload: dict = {}
            parent_job_id = None
            started_at = None
            progress_current = None
            progress_total = None

            def __setattr__(self, name, value):
                # The rebind happens BEFORE any write in
                # ``_on_operation_success``. So writes here would only
                # occur if the fix were missing. Make them blow up so
                # the test fails loudly in that case.
                if name in {
                    "status",
                    "finished_at",
                    "progress_current",
                    "progress_total",
                    "error_code",
                    "error_message",
                }:
                    raise DetachedInstanceError(f"write to detached Job.{name} not allowed")
                object.__setattr__(self, name, value)

        stale_job = _DetachedJob()
        fresh_job = MagicMock(spec=Job)
        fresh_job.id = stale_id

        call_count = [0]

        def make_session():
            s = MagicMock()
            call_count[0] += 1
            if call_count[0] == 1:
                # Claim session: returns the (still QUEUED) detached
                # job. Claim mutates status + started_at, which
                # ``_DetachedJob.__setattr__`` would refuse, so swap in
                # a plain mock here just for the claim phase.
                claim_job = MagicMock(spec=Job)
                claim_job.id = stale_id
                claim_job.status = JobStatus.QUEUED
                claim_job.parent_job_id = None
                s.get.return_value = claim_job
            else:
                # Execute session: first ``get`` returns the detached
                # instance (modelling SQLA returning a row whose
                # connection is dead); subsequent ``get`` (the rebind)
                # returns the fresh instance.
                get_results = [stale_job, fresh_job]
                get_idx = [0]

                def _get(model, _id):
                    i = get_idx[0]
                    get_idx[0] = min(i + 1, len(get_results) - 1)
                    return get_results[i]

                s.get.side_effect = _get
            return s

        factory = MagicMock(side_effect=make_session)
        registry, _ = _make_registry(
            result=OperationResult(result={"ok": True}, progress_current=3)
        )

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        # Must not raise: the rebind swaps stale_job for fresh_job
        # before any attribute write.
        worker.handle_job(stale_id)

        assert fresh_job.status == JobStatus.SUCCEEDED
        assert fresh_job.progress_current == 3

    def test_failure_path_rebinds_detached_job(self):
        """Same regression on the failure branch: re-fetch before writing FAILED."""
        from sqlalchemy.orm.exc import DetachedInstanceError

        stale_id = uuid4()

        class _DetachedJob:
            id = stale_id
            status = JobStatus.QUEUED
            operation = "ping"
            payload: dict = {}
            parent_job_id = None
            started_at = None

            def __setattr__(self, name, value):
                if name in {
                    "status",
                    "finished_at",
                    "error_code",
                    "error_message",
                }:
                    raise DetachedInstanceError(f"write to detached Job.{name} not allowed")
                object.__setattr__(self, name, value)

        stale_job = _DetachedJob()
        fresh_job = MagicMock(spec=Job)
        fresh_job.id = stale_id
        fresh_job.parent_job_id = None

        call_count = [0]

        def make_session():
            s = MagicMock()
            call_count[0] += 1
            if call_count[0] == 1:
                claim_job = MagicMock(spec=Job)
                claim_job.id = stale_id
                claim_job.status = JobStatus.QUEUED
                claim_job.parent_job_id = None
                s.get.return_value = claim_job
            else:
                get_results = [stale_job, fresh_job]
                get_idx = [0]

                def _get(model, _id):
                    i = get_idx[0]
                    get_idx[0] = min(i + 1, len(get_results) - 1)
                    return get_results[i]

                s.get.side_effect = _get
            return s

        factory = MagicMock(side_effect=make_session)
        registry, _ = _make_registry(raises=ValueError("op failed at the end"))

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        with pytest.raises(ValueError, match="op failed"):
            worker.handle_job(stale_id)

        assert fresh_job.status == JobStatus.FAILED
        assert fresh_job.error_code == "ValueError"


# ---------------------------------------------------------------------------
# F-OPS-JOBS.1a: conditional claim via rowcount + leased_until
# ---------------------------------------------------------------------------


class TestBaseWorkerConditionalClaim:
    """F-OPS-JOBS.1a: _claim_job uses a conditional UPDATE, not ORM read-then-write.

    The key invariant: if sa_update returns rowcount=0 (another consumer
    already claimed the job, or the job was cancelled/missing), _claim_job
    returns False without executing the job and without opening a second
    session for the execute phase.
    """

    def test_claim_succeeds_when_update_affects_one_row(self):
        """Normal path: rowcount=1 means the claim succeeded."""
        job = _make_job(status=JobStatus.QUEUED)
        session = MagicMock()
        # Simulate rowcount=1 from the conditional UPDATE
        result = MagicMock()
        result.rowcount = 1
        session.execute.return_value = result
        session.get.return_value = job  # execute session still reads job
        factory = MagicMock(return_value=session)
        registry, op = _make_registry()

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        claimed = worker._claim_job(job.id)

        assert claimed is True
        session.execute.assert_called_once()
        session.commit.assert_called_once()
        session.rollback.assert_not_called()

    def test_claim_fails_when_update_affects_zero_rows(self):
        """Duplicate consumer: rowcount=0 means the job was already claimed."""
        job_id = uuid4()
        session = MagicMock()
        result = MagicMock()
        result.rowcount = 0
        session.execute.return_value = result
        factory = MagicMock(return_value=session)
        registry, op = _make_registry()

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        claimed = worker._claim_job(job_id)

        assert claimed is False
        session.rollback.assert_called_once()
        session.commit.assert_not_called()

    def test_claim_sets_leased_until(self):
        """_claim_job sets leased_until in the UPDATE statement."""
        from sqlalchemy import update as sa_update

        from protea.infrastructure.orm.models.job import Job

        job_id = uuid4()
        session = MagicMock()
        result = MagicMock()
        result.rowcount = 1
        session.execute.return_value = result
        job = _make_job(status=JobStatus.QUEUED)
        session.get.return_value = job
        factory = MagicMock(return_value=session)
        registry, _ = _make_registry()

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        worker._claim_job(job_id)

        # Verify the UPDATE statement was issued
        stmt = session.execute.call_args.args[0]
        assert isinstance(stmt, type(sa_update(Job)))
        # The compiled params should include leased_until
        compiled = stmt.compile(compile_kwargs={"literal_binds": False})
        assert "leased_until" in compiled.params

    def test_handle_job_skips_execute_when_claim_returns_false(self):
        """If _claim_job returns False, handle_job exits without running the op."""
        job = _make_job(status=JobStatus.QUEUED)
        session = MagicMock()
        result = MagicMock()
        result.rowcount = 0  # claim fails
        session.execute.return_value = result
        factory = MagicMock(return_value=session)
        registry, op = _make_registry()

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        worker.handle_job(job.id)

        op.execute.assert_not_called()


class TestBaseWorkerExtendLease:
    """F-OPS-JOBS.1a: extend_lease renews leased_until on RUNNING jobs."""

    def test_extend_lease_updates_running_job(self):
        from sqlalchemy import update as sa_update

        from protea.infrastructure.orm.models.job import Job

        job_id = uuid4()
        session = MagicMock()
        factory = MagicMock(return_value=session)
        registry, _ = _make_registry()

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        worker.extend_lease(job_id)

        session.execute.assert_called_once()
        stmt = session.execute.call_args.args[0]
        assert isinstance(stmt, type(sa_update(Job)))
        compiled = stmt.compile(compile_kwargs={"literal_binds": False})
        assert "leased_until" in compiled.params
        session.commit.assert_called_once()
        session.close.assert_called_once()

    def test_extend_lease_is_nonfatal_on_db_error(self):
        """A DB error during heartbeat must not propagate; the job continues."""
        job_id = uuid4()
        session = MagicMock()
        session.execute.side_effect = RuntimeError("connection lost")
        factory = MagicMock(return_value=session)
        registry, _ = _make_registry()

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        worker.extend_lease(job_id)  # must not raise

        session.rollback.assert_called_once()
        session.close.assert_called_once()


# ---------------------------------------------------------------------------
# F-OPS-JOBS.1a: compute_dedup_key
# ---------------------------------------------------------------------------


class TestComputeDedupKey:
    """Verify dedup_key is deterministic, stable, and operation-sensitive."""

    def test_same_inputs_produce_same_key(self):
        from protea.services.jobs_service import compute_dedup_key

        k1 = compute_dedup_key("ping", {})
        k2 = compute_dedup_key("ping", {})
        assert k1 == k2

    def test_different_operations_produce_different_keys(self):
        from protea.services.jobs_service import compute_dedup_key

        k1 = compute_dedup_key("ping", {})
        k2 = compute_dedup_key("export_research_dataset", {})
        assert k1 != k2

    def test_different_payloads_produce_different_keys(self):
        from protea.services.jobs_service import compute_dedup_key

        k1 = compute_dedup_key("export", {"k": 5})
        k2 = compute_dedup_key("export", {"k": 10})
        assert k1 != k2

    def test_key_length_is_16(self):
        from protea.services.jobs_service import compute_dedup_key

        key = compute_dedup_key("ping", {"x": 1})
        assert len(key) == 16
        assert all(c in "0123456789abcdef" for c in key)

    def test_payload_key_order_is_irrelevant(self):
        """Canonical JSON sort_keys=True so key is stable regardless of dict insertion order."""
        from protea.services.jobs_service import compute_dedup_key

        k1 = compute_dedup_key("op", {"b": 2, "a": 1})
        k2 = compute_dedup_key("op", {"a": 1, "b": 2})
        assert k1 == k2


# ---------------------------------------------------------------------------
# F-OPS-JOBS.1: requeue_on_shutdown — SIGTERM mid-job re-queue path
# ---------------------------------------------------------------------------


class TestBaseWorkerRequeueOnShutdown:
    """``requeue_on_shutdown`` flips RUNNING jobs back to QUEUED so that a
    SIGTERM mid-job re-runs the work instead of marking it FAILED."""

    def test_requeue_flips_status_and_clears_lease(self):
        from sqlalchemy import update as sa_update

        from protea.infrastructure.orm.models.job import Job

        job_id = uuid4()
        result = MagicMock()
        result.rowcount = 1
        session = MagicMock()
        session.execute.return_value = result
        factory = MagicMock(return_value=session)
        registry, _ = _make_registry()

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        assert worker.requeue_on_shutdown(job_id) is True

        session.execute.assert_called_once()
        stmt = session.execute.call_args.args[0]
        assert isinstance(stmt, type(sa_update(Job)))
        compiled = stmt.compile(compile_kwargs={"literal_binds": False})
        # The UPDATE clears both timestamps and resets status to QUEUED so
        # the next consumer can re-claim the row through ``_claim_job``.
        assert "status" in compiled.params
        assert "started_at" in compiled.params
        assert "leased_until" in compiled.params
        session.commit.assert_called_once()

    def test_requeue_returns_false_when_no_row_updated(self):
        """Rowcount=0 means the job is no longer RUNNING (already
        succeeded / failed / cancelled). The watchdog then falls back to
        force_fail rather than silently doing nothing."""
        job_id = uuid4()
        result = MagicMock()
        result.rowcount = 0
        session = MagicMock()
        session.execute.return_value = result
        factory = MagicMock(return_value=session)
        registry, _ = _make_registry()

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        assert worker.requeue_on_shutdown(job_id) is False

        session.rollback.assert_called_once()
        session.commit.assert_not_called()

    def test_requeue_returns_false_on_db_error(self):
        """An UPDATE that raises must not propagate; the guard falls
        back to ``force_fail`` instead."""
        job_id = uuid4()
        session = MagicMock()
        session.execute.side_effect = RuntimeError("db down")
        factory = MagicMock(return_value=session)
        registry, _ = _make_registry()

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        assert worker.requeue_on_shutdown(job_id) is False
        session.rollback.assert_called_once()
        session.close.assert_called_once()


# ---------------------------------------------------------------------------
# F-OPS-JOBS.1: lease-expiry re-enqueue (StaleJobReaper)
# ---------------------------------------------------------------------------


class TestStaleJobReaperLeaseRequeue:
    """The reaper re-enqueues lease-expired jobs (up to ``max_lease_requeues``)
    instead of marking them FAILED on the first miss, so a transient worker
    crash does not lose accepted work."""

    def _stale_job(self, *, queue_name="protea.jobs", started_hours_ago=2):
        job = MagicMock(spec=Job)
        job.id = uuid4()
        job.status = JobStatus.RUNNING
        job.operation = "compute_embeddings"
        job.started_at = datetime.now(UTC) - timedelta(hours=started_hours_ago)
        # Lease expired one minute ago.
        job.leased_until = datetime.now(UTC) - timedelta(minutes=1)
        job.queue_name = queue_name
        return job

    def test_first_lease_expiry_requeues_via_publisher(self):
        """First reap on a lease-expired job re-publishes it instead of
        marking it FAILED."""
        stale = self._stale_job()
        session = MagicMock()
        # First call to all() is for RUNNING candidates in _reap
        # Second call to all() is for QUEUED candidates in _reap_orphaned_queued
        session.query.return_value.filter.return_value.all.side_effect = [[stale], []]
        # last_event_ts irrelevant when leased_until is present; count() = 0.
        session.query.return_value.filter.return_value.scalar.side_effect = [None, 0]
        factory = MagicMock(return_value=session)

        reaper = StaleJobReaper(
            factory,
            amqp_url="amqp://test/",
            config=StaleJobReaperConfig(
                timeout_seconds=3600,
                max_lease_requeues=3,
            ),
        )
        with patch("protea.workers.stale_job_reaper.publish_job") as mock_publish:
            count = reaper._reap()

        assert count == 1
        assert stale.status == JobStatus.QUEUED
        assert stale.started_at is None
        assert stale.leased_until is None
        mock_publish.assert_called_once_with("amqp://test/", "protea.jobs", stale.id)

    def test_lease_expiry_fails_after_max_requeues_exhausted(self):
        """Once ``max_lease_requeues`` requeues are recorded, the next
        lease miss flips the job to FAILED with ``error_code=lease_expired``."""
        stale = self._stale_job()
        session = MagicMock()
        # First call to all() is for RUNNING candidates in _reap
        # Second call to all() is for QUEUED candidates in _reap_orphaned_queued
        session.query.return_value.filter.return_value.all.side_effect = [[stale], []]
        # Three prior re-enqueues already happened — budget exhausted.
        session.query.return_value.filter.return_value.scalar.side_effect = [None, 3]
        factory = MagicMock(return_value=session)

        reaper = StaleJobReaper(
            factory,
            amqp_url="amqp://test/",
            config=StaleJobReaperConfig(
                timeout_seconds=3600,
                max_lease_requeues=3,
            ),
        )
        with patch("protea.workers.stale_job_reaper.publish_job") as mock_publish:
            count = reaper._reap()

        assert count == 1
        assert stale.status == JobStatus.FAILED
        assert stale.error_code == "lease_expired"
        mock_publish.assert_not_called()

    def test_no_amqp_url_skips_requeue_path(self):
        """When the reaper was constructed without an AMQP URL it falls
        back to the legacy "mark FAILED" behaviour even if requeue budget
        is available — there is nowhere to republish."""
        stale = self._stale_job()
        session = MagicMock()
        # First call to all() is for RUNNING candidates in _reap
        # Second call to all() is for QUEUED candidates in _reap_orphaned_queued
        session.query.return_value.filter.return_value.all.side_effect = [[stale], []]
        session.query.return_value.filter.return_value.scalar.side_effect = [None, 0]
        factory = MagicMock(return_value=session)

        reaper = StaleJobReaper(
            factory,
            amqp_url=None,
            config=StaleJobReaperConfig(
                timeout_seconds=3600,
                max_lease_requeues=3,
            ),
        )
        with patch("protea.workers.stale_job_reaper.publish_job") as mock_publish:
            count = reaper._reap()

        assert count == 1
        assert stale.status == JobStatus.FAILED
        assert stale.error_code == "lease_expired"
        mock_publish.assert_not_called()

    def test_publish_failure_leaves_row_queued_for_next_cycle(self):
        """A failed re-publish is logged but does not propagate; the row
        stays QUEUED (no leased_until) so the next reaper cycle skips it
        and a sibling consumer or manual re-dispatch recovers."""
        stale = self._stale_job()
        session = MagicMock()
        # First call to all() is for RUNNING candidates in _reap
        # Second call to all() is for QUEUED candidates in _reap_orphaned_queued
        session.query.return_value.filter.return_value.all.side_effect = [[stale], []]
        session.query.return_value.filter.return_value.scalar.side_effect = [None, 0]
        factory = MagicMock(return_value=session)

        reaper = StaleJobReaper(
            factory,
            amqp_url="amqp://test/",
            config=StaleJobReaperConfig(
                timeout_seconds=3600,
                max_lease_requeues=3,
            ),
        )
        with patch(
            "protea.workers.stale_job_reaper.publish_job",
            side_effect=RuntimeError("rabbit down"),
        ):
            count = reaper._reap()

        # We still consider the row "handled" this cycle (status flipped),
        # but it is left QUEUED so the next reaper cycle will not see it.
        assert count == 1
        assert stale.status == JobStatus.QUEUED
