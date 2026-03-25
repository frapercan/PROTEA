"""Unit tests for GenerateEvaluationSetOperation — DB mocked."""
from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch

import pytest

from protea.core.evaluation import EvaluationData
from protea.core.operations.generate_evaluation_set import (
    GenerateEvaluationSetOperation,
    GenerateEvaluationSetPayload,
)

# ---------------------------------------------------------------------------
# Payload validator
# ---------------------------------------------------------------------------

class TestGenerateEvaluationSetPayload:
    def test_valid_uuids(self):
        old = str(uuid.uuid4())
        new = str(uuid.uuid4())
        p = GenerateEvaluationSetPayload(old_annotation_set_id=old, new_annotation_set_id=new)
        assert p.old_annotation_set_id == old
        assert p.new_annotation_set_id == new

    def test_empty_old_raises(self):
        with pytest.raises(ValueError):
            GenerateEvaluationSetPayload(old_annotation_set_id="  ", new_annotation_set_id=str(uuid.uuid4()))

    def test_empty_new_raises(self):
        with pytest.raises(ValueError):
            GenerateEvaluationSetPayload(old_annotation_set_id=str(uuid.uuid4()), new_annotation_set_id="")

    def test_strips_whitespace(self):
        uid = str(uuid.uuid4())
        p = GenerateEvaluationSetPayload(
            old_annotation_set_id=f"  {uid}  ",
            new_annotation_set_id=uid,
        )
        assert p.old_annotation_set_id == uid


# ---------------------------------------------------------------------------
# Operation execute — mocked session
# ---------------------------------------------------------------------------

def _make_annotation_set(snapshot_id: uuid.UUID) -> MagicMock:
    s = MagicMock()
    s.ontology_snapshot_id = snapshot_id
    return s


def _make_eval_data() -> EvaluationData:
    return EvaluationData(
        nk={"P1": {"GO:0001"}},
        lk={"P2": {"GO:0002"}},
        pk={},
    )


class TestGenerateEvaluationSetExecute:
    def setup_method(self):
        self.op = GenerateEvaluationSetOperation()
        self.emit = MagicMock()

    def _payload(self, old_id=None, new_id=None):
        return {
            "old_annotation_set_id": str(old_id or uuid.uuid4()),
            "new_annotation_set_id": str(new_id or uuid.uuid4()),
        }

    def test_old_set_not_found_raises(self):
        session = MagicMock()
        session.get.return_value = None
        with pytest.raises(ValueError, match="not found"):
            self.op.execute(session, self._payload(), emit=self.emit)

    def test_new_set_not_found_raises(self):
        session = MagicMock()
        snap_id = uuid.uuid4()
        old_set = _make_annotation_set(snap_id)
        session.get.side_effect = [old_set, None]
        with pytest.raises(ValueError, match="not found"):
            self.op.execute(session, self._payload(), emit=self.emit)

    def test_different_snapshot_raises(self):
        session = MagicMock()
        old_set = _make_annotation_set(uuid.uuid4())
        new_set = _make_annotation_set(uuid.uuid4())  # different snapshot
        session.get.side_effect = [old_set, new_set]
        with pytest.raises(ValueError, match="same ontology snapshot"):
            self.op.execute(session, self._payload(), emit=self.emit)

    def test_successful_execution(self):
        session = MagicMock()
        snap_id = uuid.uuid4()
        old_set = _make_annotation_set(snap_id)
        new_set = _make_annotation_set(snap_id)
        session.get.side_effect = [old_set, new_set]

        eval_set = MagicMock()
        eval_set.id = uuid.uuid4()

        def add_side(obj):
            obj.id = eval_set.id

        session.add.side_effect = add_side
        session.flush = MagicMock()

        eval_data = _make_eval_data()

        with patch(
            "protea.core.operations.generate_evaluation_set.compute_evaluation_data",
            return_value=eval_data,
        ):
            result = self.op.execute(session, self._payload(), emit=self.emit)

        assert "evaluation_set_id" in result.result
        assert result.result["nk_proteins"] == 1
        assert result.result["lk_proteins"] == 1
        assert self.emit.call_count >= 3  # start, computing, done

    def test_emits_start_event(self):
        session = MagicMock()
        snap_id = uuid.uuid4()
        old_set = _make_annotation_set(snap_id)
        new_set = _make_annotation_set(snap_id)
        session.get.side_effect = [old_set, new_set]

        MagicMock()

        def add_side(obj):
            obj.id = uuid.uuid4()

        session.add.side_effect = add_side
        session.flush = MagicMock()

        with patch(
            "protea.core.operations.generate_evaluation_set.compute_evaluation_data",
            return_value=_make_eval_data(),
        ):
            self.op.execute(session, self._payload(), emit=self.emit)

        events = [call.args[0] for call in self.emit.call_args_list]
        assert "generate_evaluation_set.start" in events
        assert "generate_evaluation_set.done" in events
