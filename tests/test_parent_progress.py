"""Tests for protea.core.contracts.parent_progress (T0.7)."""

from __future__ import annotations

from unittest.mock import MagicMock
from uuid import uuid4

from protea.core.contracts.parent_progress import update_parent_progress


def _row(current: int, total: int) -> MagicMock:
    r = MagicMock()
    r.progress_current = current
    r.progress_total = total
    return r


class TestUpdateParentProgress:
    def test_silent_when_no_row_returned(self) -> None:
        session = MagicMock()
        session.execute.return_value.fetchone.return_value = None
        emit = MagicMock()

        update_parent_progress(
            session,
            uuid4(),
            emit,
            event_name="store_x.parent_succeeded",
        )

        # Only the increment statement was executed.
        assert session.execute.call_count == 1
        emit.assert_not_called()
        session.add.assert_not_called()

    def test_returns_when_not_last_batch(self) -> None:
        session = MagicMock()
        session.execute.return_value.fetchone.return_value = _row(2, 5)
        emit = MagicMock()

        update_parent_progress(
            session,
            uuid4(),
            emit,
            event_name="store_x.parent_succeeded",
        )

        assert session.execute.call_count == 1
        emit.assert_not_called()
        session.add.assert_not_called()

    def test_marks_succeeded_on_last_batch(self) -> None:
        parent_id = uuid4()
        session = MagicMock()
        # First execute (the increment) returns last-batch progress;
        # second execute (the SUCCEEDED transition) returns the closed row.
        increment = MagicMock()
        increment.fetchone.return_value = _row(5, 5)
        succeeded = MagicMock()
        succeeded.fetchone.return_value = MagicMock(id=parent_id)
        session.execute.side_effect = [increment, succeeded]
        emit = MagicMock()

        update_parent_progress(
            session,
            parent_id,
            emit,
            event_name="store_predictions.parent_succeeded",
        )

        assert session.execute.call_count == 2
        # JobEvent row added once.
        session.add.assert_called_once()
        # Custom event emitted with the operation-specific name.
        emit.assert_called_once()
        emit_call = emit.call_args
        assert emit_call.args[0] == "store_predictions.parent_succeeded"
        assert emit_call.args[2] == {"parent_job_id": str(parent_id)}

    def test_emit_skipped_when_succeeded_returns_no_row(self) -> None:
        # Race: another worker finished before us. Our SUCCEEDED update
        # returns no row because the parent is no longer RUNNING.
        session = MagicMock()
        increment = MagicMock()
        increment.fetchone.return_value = _row(5, 5)
        succeeded = MagicMock()
        succeeded.fetchone.return_value = None
        session.execute.side_effect = [increment, succeeded]
        emit = MagicMock()

        update_parent_progress(
            session,
            uuid4(),
            emit,
            event_name="store_x.parent_succeeded",
        )

        assert session.execute.call_count == 2
        session.add.assert_not_called()
        emit.assert_not_called()

    def test_event_name_is_passed_through(self) -> None:
        parent_id = uuid4()
        session = MagicMock()
        increment = MagicMock()
        increment.fetchone.return_value = _row(1, 1)
        succeeded = MagicMock()
        succeeded.fetchone.return_value = MagicMock(id=parent_id)
        session.execute.side_effect = [increment, succeeded]
        emit = MagicMock()

        update_parent_progress(
            session,
            parent_id,
            emit,
            event_name="custom.parent_succeeded",
        )

        emit.assert_called_once()
        assert emit.call_args.args[0] == "custom.parent_succeeded"
