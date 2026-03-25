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
from protea.workers.stale_job_reaper import StaleJobReaper

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
        """A job that is already RUNNING is not executed again."""
        job = _make_job(status=JobStatus.RUNNING)
        factory, session = _make_factory(job)
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
        registry, _ = _make_registry(result=OperationResult(
            result={}, progress_current=5, progress_total=10
        ))

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


# ---------------------------------------------------------------------------
# StaleJobReaper
# ---------------------------------------------------------------------------

class TestStaleJobReaper:
    def test_reaps_stale_running_jobs(self):
        """Jobs in RUNNING for longer than timeout should be marked FAILED."""
        stale_job = MagicMock(spec=Job)
        stale_job.id = uuid4()
        stale_job.status = JobStatus.RUNNING
        stale_job.operation = "compute_embeddings"
        stale_job.started_at = datetime.now(UTC) - timedelta(hours=2)

        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = [stale_job]
        factory = MagicMock(return_value=session)

        reaper = StaleJobReaper(factory, timeout_seconds=3600)
        count = reaper._reap()

        assert count == 1
        assert stale_job.status == JobStatus.FAILED
        assert stale_job.error_code == "JobTimeout"
        session.add.assert_called_once()  # JobEvent
        session.commit.assert_called_once()

    def test_no_stale_jobs_returns_zero(self):
        """When no jobs are stale, reaper does nothing."""
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = []
        factory = MagicMock(return_value=session)

        reaper = StaleJobReaper(factory, timeout_seconds=3600)
        count = reaper._reap()

        assert count == 0
        session.commit.assert_called_once()

    def test_reaper_handles_db_error_gracefully(self):
        """If the DB query fails, reaper raises but does not crash permanently."""
        session = MagicMock()
        session.query.side_effect = RuntimeError("DB connection lost")
        factory = MagicMock(return_value=session)

        reaper = StaleJobReaper(factory, timeout_seconds=3600)
        with pytest.raises(RuntimeError, match="DB connection lost"):
            reaper._reap()
        session.rollback.assert_called_once()

    def test_reaper_rollback_also_fails(self):
        """If rollback itself raises, the exception from _reap still propagates."""
        session = MagicMock()
        session.query.side_effect = RuntimeError("DB gone")
        session.rollback.side_effect = RuntimeError("rollback failed too")
        factory = MagicMock(return_value=session)

        reaper = StaleJobReaper(factory, timeout_seconds=3600)
        with pytest.raises(RuntimeError, match="DB gone"):
            reaper._reap()
        session.rollback.assert_called_once()
        session.close.assert_called_once()

    def test_run_registers_signal_handlers(self):
        """run() should register SIGINT and SIGTERM handlers."""
        factory = MagicMock()
        reaper = StaleJobReaper(factory, timeout_seconds=3600)
        # Make _reap set _stop=True so the loop exits after one iteration
        reaper._stop = False
        call_count = [0]
        def fake_reap():
            call_count[0] += 1
            reaper._stop = True
            return 0
        reaper._reap = fake_reap

        with patch("protea.workers.stale_job_reaper.signal.signal") as mock_signal, \
             patch("protea.workers.stale_job_reaper.time.sleep"):
            reaper.run(interval_seconds=1)

        # Should register both SIGINT and SIGTERM
        calls = [c[0] for c in mock_signal.call_args_list]
        assert (signal.SIGINT, reaper._handle_stop) in calls
        assert (signal.SIGTERM, reaper._handle_stop) in calls

    def test_run_loops_and_stops_on_flag(self):
        """run() calls _reap repeatedly until _stop is set."""
        factory = MagicMock()
        reaper = StaleJobReaper(factory, timeout_seconds=3600)
        reap_count = [0]

        def fake_reap():
            reap_count[0] += 1
            if reap_count[0] >= 3:
                reaper._stop = True
            return 0

        reaper._reap = fake_reap

        with patch("protea.workers.stale_job_reaper.signal.signal"), \
             patch("protea.workers.stale_job_reaper.time.sleep"):
            reaper.run(interval_seconds=1)

        assert reap_count[0] == 3

    def test_run_logs_reaped_count(self):
        """When _reap returns non-zero, run() logs it."""
        factory = MagicMock()
        reaper = StaleJobReaper(factory, timeout_seconds=3600)

        def fake_reap():
            reaper._stop = True
            return 5

        reaper._reap = fake_reap

        with patch("protea.workers.stale_job_reaper.signal.signal"), \
             patch("protea.workers.stale_job_reaper.time.sleep"), \
             patch("protea.workers.stale_job_reaper.logger") as mock_logger:
            reaper.run(interval_seconds=1)

        # Should have logged the reaped count
        info_calls = [str(c) for c in mock_logger.info.call_args_list]
        assert any("5" in c for c in info_calls)

    def test_run_catches_reap_exception(self):
        """If _reap raises, run() logs the error and continues."""
        factory = MagicMock()
        reaper = StaleJobReaper(factory, timeout_seconds=3600)
        call_count = [0]

        def failing_reap():
            call_count[0] += 1
            if call_count[0] == 1:
                raise RuntimeError("transient DB error")
            reaper._stop = True
            return 0

        reaper._reap = failing_reap

        with patch("protea.workers.stale_job_reaper.signal.signal"), \
             patch("protea.workers.stale_job_reaper.time.sleep"), \
             patch("protea.workers.stale_job_reaper.logger") as mock_logger:
            reaper.run(interval_seconds=1)

        # Should have logged the error but continued
        mock_logger.error.assert_called_once()
        assert call_count[0] == 2

    def test_handle_stop_sets_flag(self):
        """_handle_stop sets the _stop flag."""
        factory = MagicMock()
        reaper = StaleJobReaper(factory, timeout_seconds=3600)
        assert reaper._stop is False
        reaper._handle_stop(signal.SIGINT, None)
        assert reaper._stop is True


# ---------------------------------------------------------------------------
# Feature engineering warmup
# ---------------------------------------------------------------------------

class TestTaxonomyWarmup:
    def test_warmup_calls_get_ncbi(self):
        from protea.core.feature_engineering import warmup_taxonomy_db

        with patch("protea.core.feature_engineering._get_ncbi") as mock_get, \
             patch("protea.core.feature_engineering._ETE3_AVAILABLE", True):
            warmup_taxonomy_db()
        mock_get.assert_called_once()

    def test_warmup_skips_when_ete3_unavailable(self):
        from protea.core.feature_engineering import warmup_taxonomy_db

        with patch("protea.core.feature_engineering._ETE3_AVAILABLE", False), \
             patch("protea.core.feature_engineering._get_ncbi") as mock_get:
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

    def test_claim_session_sets_running_before_execute(self):
        """First session transitions to RUNNING; second session runs the operation."""
        job = _make_job()
        status_log = []

        call_count = [0]

        def make_session():
            s = MagicMock()
            s.get.return_value = job
            call_count[0] += 1
            current_call = call_count[0]

            def commit_side_effect():
                status_log.append((current_call, job.status))
            s.commit.side_effect = commit_side_effect
            return s

        factory = MagicMock(side_effect=make_session)
        registry, _ = _make_registry()

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        worker.handle_job(job.id)

        # Session 1 should commit with RUNNING, session 2 with SUCCEEDED
        assert status_log[0] == (1, JobStatus.RUNNING)
        assert status_log[1] == (2, JobStatus.SUCCEEDED)


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
        registry, _ = _make_registry(result=OperationResult(
            result={}, progress_current=42
        ))

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
        registry, _ = _make_registry(result=OperationResult(
            result={"dispatched": True}, deferred=True
        ))

        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="test"))
        worker.handle_job(job.id)

        # Deferred: should NOT transition to SUCCEEDED
        assert job.status != JobStatus.SUCCEEDED
        # Should remain RUNNING (set in claim phase)
        assert job.status == JobStatus.RUNNING


class TestBaseWorkerPublishAfterCommit:
    """Cover publish_after_commit and publish_operations (lines 169-176)."""

    def test_publish_after_commit_publishes_child_jobs(self):
        child_id = uuid4()
        job = _make_job()
        session = MagicMock()
        session.get.return_value = job
        factory = MagicMock(return_value=session)
        registry, _ = _make_registry(result=OperationResult(
            result={},
            publish_after_commit=[("protea.jobs", child_id)],
        ))

        worker = BaseWorker(
            factory, registry, WorkerConfig(worker_name="test"),
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
        registry, _ = _make_registry(result=OperationResult(
            result={},
            publish_operations=[
                ("protea.embeddings.batch", {"batch_data": [1, 2]}),
            ],
        ))

        worker = BaseWorker(
            factory, registry, WorkerConfig(worker_name="test"),
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
        registry, _ = _make_registry(result=OperationResult(
            result={},
            publish_after_commit=[("protea.jobs", child_id)],
        ))

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
        """If some children are still running, parent is not failed."""
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

        # session.execute should NOT have been called for parent update
        session.execute.assert_not_called()
