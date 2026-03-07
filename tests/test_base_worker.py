"""
Unit tests for BaseWorker.
Uses a mocked session factory and a fake Operation — no real DB needed.
"""
from __future__ import annotations

from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from protea.core.contracts.operation import OperationResult
from protea.core.contracts.registry import OperationRegistry
from protea.infrastructure.orm.models.job import Job, JobStatus
from protea.workers.base_worker import BaseWorker, WorkerConfig

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


def _make_job(status=JobStatus.QUEUED, operation="ping"):
    job = MagicMock(spec=Job)
    job.id = uuid4()
    job.status = status
    job.operation = operation
    job.payload = {}
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
