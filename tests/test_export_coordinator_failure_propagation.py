"""Failure-propagation tests for the export coordinator.

Regression cover for F-OPS-COORD-FAIL-PROPAGATE (incident 2026-05-26):

- 5 ``export_coordinator`` parents stayed ``RUNNING`` for 1h15m while
  every dispatched child raised ``ValueError("Unknown search backend:
  'torch'. Choose 'numpy' or 'faiss'.")``. The bug had two halves:

  1. The coordinator validated the payload schema but accepted any
     ``search_backend`` string, so the dispatch wave went out and only
     the workers found out the value was bad.
  2. The consumer recorded each ``child.failed`` event but never
     touched the parent's ``status``, so the row sat in ``RUNNING``
     until a human ran a manual SQL UPDATE.

This module pins the two fixes:

* ``TestPreflightFastFail`` — invalid ``search_backend`` makes
  ``_dispatch_minijobs`` raise before any message is published.
* ``TestAggregateFailure`` — ``report_child_failure`` aggregates
  failures on the parent ``meta`` and transitions to ``FAILED`` once
  the threshold is hit, surfacing the top three causes.
"""

from __future__ import annotations

import datetime
import uuid
from typing import Any
from unittest.mock import MagicMock

import pytest

from protea.core.contracts.parent_failure import ChildFailure, report_child_failure
from protea.core.operations.export_minijobs.export_coordinator import (
    ExportCoordinatorOperation,
)
from protea.infrastructure.orm.models.job import JobStatus

# ---------------------------------------------------------------------------
# Shared scaffolding
# ---------------------------------------------------------------------------

_TORCH_ERROR = (
    "Unknown search backend: 'torch'. Choose 'numpy' or 'faiss'."
)


def _make_payload(search_backend: str = "faiss") -> dict[str, Any]:
    return {
        "output_name": "bench-v1-K5-v226-lineage",
        "embedding_config_id": str(uuid.uuid4()),
        "annotation_set_id": str(uuid.uuid4()),
        "ontology_snapshot_id": str(uuid.uuid4()),
        "train_versions": [220, 221, 222, 223],
        "test_versions": [226],
        "k": 5,
        "search_backend": search_backend,
        "_job_id": str(uuid.uuid4()),
    }


def _capturing_emit() -> tuple[list[dict[str, Any]], Any]:
    """Return ``(events, emit)`` where ``emit`` appends each call to ``events``."""
    events: list[dict[str, Any]] = []

    def emit(event: str, message: str | None, fields: dict[str, Any], level: str) -> None:
        events.append(
            {"event": event, "message": message, "fields": fields, "level": level}
        )

    return events, emit


# ---------------------------------------------------------------------------
# Pre-flight fast-fail (goal 1)
# ---------------------------------------------------------------------------


class TestPreflightFastFail:
    """Coordinator must refuse the dispatch wave when payload is doomed."""

    @pytest.fixture(autouse=True)
    def _enable_minijobs(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PROTEA_EXPORT_MINIJOBS", "1")

    def test_unknown_search_backend_raises_before_dispatch(self) -> None:
        op = ExportCoordinatorOperation()
        events, emit = _capturing_emit()
        payload = _make_payload(search_backend="annoy")

        with pytest.raises(ValueError) as exc_info:
            op.execute(MagicMock(), payload, emit=emit)

        msg = str(exc_info.value)
        assert "first child failed with non-retryable error" in msg
        assert "search_backend='annoy'" in msg
        assert "numpy" in msg and "faiss" in msg and "torch" in msg

        # No dispatching event must have been emitted before the failure.
        emitted_names = [e["event"] for e in events]
        assert "export_coordinator.dispatching" not in emitted_names
        # And a structured failure event must have been emitted instead.
        assert "export_coordinator.failed" in emitted_names
        failed = next(e for e in events if e["event"] == "export_coordinator.failed")
        assert failed["level"] == "error"
        assert failed["fields"]["search_backend"] == "annoy"
        assert set(failed["fields"]["valid_search_backends"]) == {"numpy", "faiss", "torch"}

    @pytest.mark.parametrize("bad_backend", ["annoy", "", "FAISS", "Numpy"])
    def test_only_numpy_and_faiss_are_accepted(self, bad_backend: str) -> None:
        op = ExportCoordinatorOperation()
        _, emit = _capturing_emit()
        payload = _make_payload(search_backend=bad_backend)
        with pytest.raises(ValueError):
            op.execute(MagicMock(), payload, emit=emit)

    @pytest.mark.parametrize("good_backend", ["numpy", "faiss", "torch"])
    def test_valid_backends_dispatch_normally(self, good_backend: str) -> None:
        op = ExportCoordinatorOperation()
        events, emit = _capturing_emit()
        payload = _make_payload(search_backend=good_backend)

        result = op.execute(MagicMock(), payload, emit=emit)
        assert result.deferred is True
        assert result.progress_total == 5  # 4 train + 1 eval
        assert result.result["dispatched_total"] == 5
        emitted = {e["event"] for e in events}
        assert "export_coordinator.dispatching" in emitted
        assert "export_coordinator.failed" not in emitted


# ---------------------------------------------------------------------------
# Aggregate failure (goal 2)
# ---------------------------------------------------------------------------


class _FakeQuery:
    """Stand-in for ``Job`` row that mutates in-place when we ``update``."""

    def __init__(self, parent_job_id: uuid.UUID, dispatched_total: int) -> None:
        self.id = parent_job_id
        self.status = JobStatus.RUNNING
        self.progress_total = dispatched_total
        self.progress_current = 0
        self.meta: dict[str, Any] = {}
        self.error_code: str | None = None
        self.error_message: str | None = None
        self.finished_at: Any = None
        self.added_events: list[Any] = []


#: Marks a column whose value the server computes, so the fake has to stand in
#: for Postgres instead of reading a literal that is not there.
_SERVER_SIDE = object()


class _FakeSession:
    """Minimal stand-in for the slice of ``Session`` ``report_child_failure``
    actually touches.

    Covers ``get``, ``execute(update().where().values().returning(...))``,
    ``add``, ``flush``. Not a general ORM mock — only enough to validate
    the failure-aggregation transitions atomically.
    """

    def __init__(self, parent: _FakeQuery) -> None:
        self._parent = parent
        self.committed = False
        self.flush_calls = 0

    def get(self, model: type, job_id: uuid.UUID) -> _FakeQuery | None:
        if job_id == self._parent.id:
            return self._parent
        return None

    def execute(self, stmt: Any) -> Any:  # noqa: ANN401
        # We only support the single UPDATE...RETURNING(Job.id) call that
        # ``report_child_failure`` makes. Inspect the compiled SQL via the
        # element tree to extract the SET values.
        try:
            compiled = stmt.compile(compile_kwargs={"literal_binds": False})
        except Exception:
            compiled = None
        # Best-effort: the helper only cares about ``.fetchone()`` returning
        # a row when the WHERE matches. We satisfy that by mutating the
        # parent row + returning a row stub iff the parent is still RUNNING.
        result = MagicMock()
        if self._parent.status == JobStatus.RUNNING:
            # Mutate ``parent`` in place to mirror what the DB would do.
            params = getattr(stmt, "_values", None) or {}
            # ``Update._values`` maps Column -> BindParameter for a literal, or
            # -> a SQL expression for something the SERVER computes. finished_at
            # is now func.now(), which has no .value because the value does not
            # exist until Postgres evaluates it. Standing in for the database
            # means evaluating it here rather than reaching for an attribute a
            # SQL function was never going to have.
            for col, bind in dict(params).items():
                col_name = getattr(col, "key", None) or getattr(col, "name", None)
                literal = getattr(bind, "value", _SERVER_SIDE)
                if col_name == "status":
                    self._parent.status = literal
                elif col_name == "error_code":
                    self._parent.error_code = literal
                elif col_name == "error_message":
                    self._parent.error_message = literal
                elif col_name == "finished_at":
                    self._parent.finished_at = (
                        datetime.datetime.now(datetime.UTC)
                        if literal is _SERVER_SIDE
                        else literal
                    )
            result.fetchone = MagicMock(return_value=MagicMock(id=self._parent.id))
        else:
            result.fetchone = MagicMock(return_value=None)
        # Silence the unused-variable warning for compiled.
        _ = compiled
        return result

    def add(self, obj: Any) -> None:  # noqa: ANN401
        self._parent.added_events.append(obj)

    def flush(self) -> None:
        self.flush_calls += 1


def _run_n_failures(
    n: int,
    dispatched_total: int,
    *,
    failure_messages: list[str] | None = None,
) -> tuple[_FakeQuery, list[dict[str, Any]], list[bool]]:
    """Drive ``report_child_failure`` ``n`` times and return the parent + events."""
    parent_id = uuid.uuid4()
    parent = _FakeQuery(parent_id, dispatched_total)
    session = _FakeSession(parent)
    events, emit = _capturing_emit()
    msgs = failure_messages or [_TORCH_ERROR] * n
    closed_flags: list[bool] = []
    for idx in range(n):
        failure = ChildFailure(
            error_message=msgs[idx % len(msgs)],
            error_code="ValueError",
            dispatched_total=dispatched_total,
        )
        closed_flags.append(
            report_child_failure(session, parent_id, failure, emit)
        )
    return parent, events, closed_flags


class TestAggregateFailure:
    """``report_child_failure`` must transition the parent on threshold."""

    def test_all_five_children_failing_torch_marks_parent_failed(self) -> None:
        parent, events, closed_flags = _run_n_failures(
            n=5, dispatched_total=5
        )

        assert parent.status == JobStatus.FAILED
        assert parent.error_code == "ValueError"
        assert parent.error_message is not None
        assert "all 5 dispatched children failed" in parent.error_message
        assert _TORCH_ERROR in parent.error_message
        # Only the last call should have closed the parent.
        assert closed_flags == [False, False, False, False, True]

        # Aggregate emit must surface failed_children + total_children.
        failed_events = [e for e in events if e["event"] == "export_coordinator.failed"]
        assert len(failed_events) == 1
        f = failed_events[0]
        assert f["fields"]["failed_children"] == 5
        assert f["fields"]["total_children"] == 5
        assert f["fields"]["top_error_messages"] == [_TORCH_ERROR]

    def test_single_first_child_failure_marks_parent_failed(self) -> None:
        """Goal 1 (aggregate side): dispatched_total=1 + 1 failure flips immediately."""
        parent, events, closed_flags = _run_n_failures(n=1, dispatched_total=1)

        assert parent.status == JobStatus.FAILED
        assert closed_flags == [True]
        assert parent.error_message is not None
        assert "first child failed with non-retryable error" in parent.error_message
        assert _TORCH_ERROR in parent.error_message

    def test_top_three_unique_error_messages_are_captured(self) -> None:
        msgs = [
            _TORCH_ERROR,
            "ConnectionError: snapshot not hydrated",
            "TimeoutError: KNN search exceeded 600s",
            "OSError: temp file missing",
        ]
        # 4 distinct messages over 5 children (first repeats once).
        parent, events, _ = _run_n_failures(
            n=5,
            dispatched_total=5,
            failure_messages=[msgs[0], msgs[0], msgs[1], msgs[2], msgs[3]],
        )
        assert parent.status == JobStatus.FAILED
        top = parent.meta["top_error_messages"]
        assert len(top) == 3
        # Insertion order, duplicates dropped, capped at 3.
        assert top[0] == _TORCH_ERROR
        assert top[1] == msgs[1]
        assert top[2] == msgs[2]
        assert msgs[3] not in top

    def test_below_threshold_keeps_parent_running(self) -> None:
        # 4 failures out of 5 expected — default failure_ratio is 1.0
        # so the parent must stay RUNNING.
        parent, events, closed_flags = _run_n_failures(n=4, dispatched_total=5)
        assert parent.status == JobStatus.RUNNING
        assert parent.meta["failed_children_count"] == 4
        assert parent.meta["dispatched_total"] == 5
        assert all(flag is False for flag in closed_flags)

    def test_repeat_call_after_terminal_state_is_noop(self) -> None:
        parent, _, _ = _run_n_failures(n=5, dispatched_total=5)
        assert parent.status == JobStatus.FAILED

        session = _FakeSession(parent)
        events, emit = _capturing_emit()
        late = ChildFailure(
            error_message="late straggler",
            error_code="ValueError",
            dispatched_total=5,
        )
        closed = report_child_failure(session, parent.id, late, emit)
        assert closed is False
        # No additional events emitted.
        assert events == []
