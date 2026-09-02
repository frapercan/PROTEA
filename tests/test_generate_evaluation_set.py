"""Unit tests for GenerateEvaluationSetOperation — DB mocked."""

from __future__ import annotations

import uuid
from datetime import date
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
            GenerateEvaluationSetPayload(
                old_annotation_set_id="  ", new_annotation_set_id=str(uuid.uuid4())
            )

    def test_empty_new_raises(self):
        with pytest.raises(ValueError):
            GenerateEvaluationSetPayload(
                old_annotation_set_id=str(uuid.uuid4()), new_annotation_set_id=""
            )

    def test_strips_whitespace(self):
        uid = str(uuid.uuid4())
        p = GenerateEvaluationSetPayload(
            old_annotation_set_id=f"  {uid}  ",
            new_annotation_set_id=uid,
        )
        assert p.old_annotation_set_id == uid

    def test_window_role_defaults_none(self):
        p = GenerateEvaluationSetPayload(
            old_annotation_set_id=str(uuid.uuid4()),
            new_annotation_set_id=str(uuid.uuid4()),
        )
        assert p.window_role is None

    @pytest.mark.parametrize("role", ["valid", "test"])
    def test_window_role_accepts_vocab(self, role):
        p = GenerateEvaluationSetPayload(
            old_annotation_set_id=str(uuid.uuid4()),
            new_annotation_set_id=str(uuid.uuid4()),
            window_role=role,
        )
        assert p.window_role == role

    def test_window_role_rejects_unknown(self):
        with pytest.raises(ValueError):
            GenerateEvaluationSetPayload(
                old_annotation_set_id=str(uuid.uuid4()),
                new_annotation_set_id=str(uuid.uuid4()),
                window_role="train",
            )

    def test_native_overrides_default_none(self):
        p = GenerateEvaluationSetPayload(
            old_annotation_set_id=str(uuid.uuid4()),
            new_annotation_set_id=str(uuid.uuid4()),
        )
        assert p.old_native_snapshot_id is None
        assert p.new_native_snapshot_id is None

    def test_native_overrides_accept_uuid_strings(self):
        on = str(uuid.uuid4())
        nn = str(uuid.uuid4())
        p = GenerateEvaluationSetPayload(
            old_annotation_set_id=str(uuid.uuid4()),
            new_annotation_set_id=str(uuid.uuid4()),
            old_native_snapshot_id=f"  {on}  ",
            new_native_snapshot_id=nn,
        )
        assert p.old_native_snapshot_id == on  # stripped
        assert p.new_native_snapshot_id == nn

    def test_native_override_rejects_empty(self):
        with pytest.raises(ValueError):
            GenerateEvaluationSetPayload(
                old_annotation_set_id=str(uuid.uuid4()),
                new_annotation_set_id=str(uuid.uuid4()),
                old_native_snapshot_id="   ",
            )


# ---------------------------------------------------------------------------
# Operation execute — mocked session
# ---------------------------------------------------------------------------


def _make_annotation_set(snapshot_id: uuid.UUID) -> MagicMock:
    """A stand-in corpus, and it has to say when it was published.

    A MagicMock answers every attribute with another MagicMock, so a corpus
    whose publication date is left unspecified used to compare as "some date" to
    anything that asked. The holdout guard asks, and it refuses rather than
    guesses: a corpus with no readable date is a corpus that cannot be placed
    either side of the board's mark. Dated well before the mark here, since
    these tests are about reconciliation and not about the reserve.
    """
    s = MagicMock()
    s.source_published_at = date(2024, 4, 16)
    s.source_version = "220"
    s.ontology_snapshot_id = snapshot_id
    return s


def _make_eval_data() -> EvaluationData:
    return EvaluationData(
        nk={"P1": {"GO:0001"}},
        lk={"P2": {"GO:0002"}},
        pk={},
    )


@pytest.fixture(autouse=True)
def _mock_artifact_store(request):
    """Stub the artifact store for tests that exercise execute()."""
    if not request.cls or request.cls.__name__ != "TestGenerateEvaluationSetExecute":
        yield
        return
    with (
        patch(
            "protea.core.operations.generate_evaluation_set.get_artifact_store",
            return_value=MagicMock(),
        ),
        patch(
            "protea.core.operations.generate_evaluation_set.load_settings",
            return_value=MagicMock(),
        ),
    ):
        yield


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

    def test_different_snapshot_dispatches_reconciled(self):
        """Mismatched snapshots should invoke the reconciled compute path."""
        session = MagicMock()
        old_set = _make_annotation_set(uuid.uuid4())
        new_set = _make_annotation_set(uuid.uuid4())  # different snapshot
        session.get.side_effect = [old_set, new_set]
        # No pre-existing EvaluationSet — force the compute path.
        session.query.return_value.filter_by.return_value.one_or_none.return_value = None

        def add_side(obj):
            obj.id = uuid.uuid4()

        session.add.side_effect = add_side
        session.flush = MagicMock()

        with patch(
            "protea.core.operations.generate_evaluation_set.compute_evaluation_data_reconciled",
            return_value=_make_eval_data(),
        ) as mock_reconciled:
            with patch(
                "protea.core.operations.generate_evaluation_set.compute_evaluation_data",
            ) as mock_same:
                self.op.execute(session, self._payload(), emit=self.emit)

        assert mock_reconciled.called
        assert not mock_same.called
        # Pivot defaults to new_set.ontology_snapshot_id.
        kwargs_or_args = mock_reconciled.call_args
        assert kwargs_or_args.args[5] == new_set.ontology_snapshot_id

    def test_explicit_pivot_snapshot(self):
        """Explicit pivot with matching old+new snapshots still uses same-snapshot path."""
        session = MagicMock()
        snap_id = uuid.uuid4()
        old_set = _make_annotation_set(snap_id)
        new_set = _make_annotation_set(snap_id)
        pivot_snap = MagicMock()
        # session.get is called for old, new, and the pivot lookup.
        session.get.side_effect = [old_set, new_set, pivot_snap]
        # No pre-existing EvaluationSet — force the compute path.
        session.query.return_value.filter_by.return_value.one_or_none.return_value = None

        def add_side(obj):
            obj.id = uuid.uuid4()

        session.add.side_effect = add_side
        session.flush = MagicMock()

        pivot_id = str(snap_id)  # same as old/new → same_snapshot mode
        payload = self._payload()
        payload["pivot_ontology_snapshot_id"] = pivot_id

        with patch(
            "protea.core.operations.generate_evaluation_set.compute_evaluation_data",
            return_value=_make_eval_data(),
        ) as mock_same:
            self.op.execute(session, payload, emit=self.emit)

        assert mock_same.called

    def test_successful_execution(self):
        session = MagicMock()
        snap_id = uuid.uuid4()
        old_set = _make_annotation_set(snap_id)
        new_set = _make_annotation_set(snap_id)
        session.get.side_effect = [old_set, new_set]
        # No pre-existing EvaluationSet — force the compute path.
        session.query.return_value.filter_by.return_value.one_or_none.return_value = None

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

    def test_job_id_threaded_onto_eval_set(self):
        """R0.1: the worker-injected ``_job_id`` is stamped onto the new set.

        Without this the EvaluationSet is an orphan artifact (job_id=None),
        the exact provenance gap the reproducible-frame slice closes.
        """
        session = MagicMock()
        snap_id = uuid.uuid4()
        old_set = _make_annotation_set(snap_id)
        new_set = _make_annotation_set(snap_id)
        session.get.side_effect = [old_set, new_set]
        session.query.return_value.filter_by.return_value.one_or_none.return_value = None

        created: list = []

        def add_side(obj):
            obj.id = uuid.uuid4()
            created.append(obj)

        session.add.side_effect = add_side
        session.flush = MagicMock()

        job_id = uuid.uuid4()
        payload = self._payload()
        payload["_job_id"] = str(job_id)

        with patch(
            "protea.core.operations.generate_evaluation_set.compute_evaluation_data",
            return_value=_make_eval_data(),
        ):
            self.op.execute(session, payload, emit=self.emit)

        assert created, "no EvaluationSet was added"
        assert created[0].job_id == job_id

    def test_job_id_none_when_payload_omits_it(self):
        """A direct (non-job-backed) dispatch leaves job_id None, not a crash."""
        session = MagicMock()
        snap_id = uuid.uuid4()
        old_set = _make_annotation_set(snap_id)
        new_set = _make_annotation_set(snap_id)
        session.get.side_effect = [old_set, new_set]
        session.query.return_value.filter_by.return_value.one_or_none.return_value = None

        created: list = []

        def add_side(obj):
            obj.id = uuid.uuid4()
            created.append(obj)

        session.add.side_effect = add_side
        session.flush = MagicMock()

        with patch(
            "protea.core.operations.generate_evaluation_set.compute_evaluation_data",
            return_value=_make_eval_data(),
        ):
            self.op.execute(session, self._payload(), emit=self.emit)

        assert created and created[0].job_id is None

    def test_emits_start_event(self):
        session = MagicMock()
        snap_id = uuid.uuid4()
        old_set = _make_annotation_set(snap_id)
        new_set = _make_annotation_set(snap_id)
        session.get.side_effect = [old_set, new_set]
        # No pre-existing EvaluationSet — force the compute path.
        session.query.return_value.filter_by.return_value.one_or_none.return_value = None

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

    def test_window_role_passed_to_new_set(self):
        session = MagicMock()
        snap_id = uuid.uuid4()
        old_set = _make_annotation_set(snap_id)
        new_set = _make_annotation_set(snap_id)
        session.get.side_effect = [old_set, new_set]
        session.query.return_value.filter_by.return_value.one_or_none.return_value = None

        added: list = []
        session.add.side_effect = lambda obj: (setattr(obj, "id", uuid.uuid4()), added.append(obj))
        session.flush = MagicMock()

        payload = self._payload()
        payload["window_role"] = "valid"
        with patch(
            "protea.core.operations.generate_evaluation_set.compute_evaluation_data",
            return_value=_make_eval_data(),
        ):
            self.op.execute(session, payload, emit=self.emit)

        assert added, "expected an EvaluationSet to be added"
        assert added[0].window_role == "valid"

    def test_reuse_redesignates_window_role(self):
        session = MagicMock()
        snap_id = uuid.uuid4()
        old_set = _make_annotation_set(snap_id)
        new_set = _make_annotation_set(snap_id)
        session.get.side_effect = [old_set, new_set]

        existing = MagicMock()
        existing.id = uuid.uuid4()
        existing.stats = {"nk_proteins": 1}
        existing.groundtruth_uri = "file://gt.parquet"
        existing.window_role = None
        session.query.return_value.filter_by.return_value.one_or_none.return_value = existing
        session.flush = MagicMock()

        payload = self._payload()
        payload["window_role"] = "test"
        with patch(
            "protea.core.operations.generate_evaluation_set.compute_evaluation_data",
        ) as mock_compute:
            result = self.op.execute(session, payload, emit=self.emit)

        # Reuse path must not recompute the delta.
        assert not mock_compute.called
        assert existing.window_role == "test"
        assert result.result["evaluation_set_id"] == str(existing.id)
        events = [call.args[0] for call in self.emit.call_args_list]
        assert "generate_evaluation_set.window_role_set" in events

    def test_native_override_passed_to_reconciled(self):
        """Explicit native snapshots drive the reconcile DAG, decoupled from the set binding."""
        session = MagicMock()
        old_set = _make_annotation_set(uuid.uuid4())
        new_set = _make_annotation_set(uuid.uuid4())
        old_native = uuid.uuid4()
        new_native = uuid.uuid4()
        # get: old set, new set, then OntologySnapshot existence for each override.
        session.get.side_effect = [old_set, new_set, MagicMock(), MagicMock()]
        session.query.return_value.filter_by.return_value.one_or_none.return_value = None
        session.add.side_effect = lambda obj: setattr(obj, "id", uuid.uuid4())
        session.flush = MagicMock()

        payload = self._payload()
        payload["old_native_snapshot_id"] = str(old_native)
        payload["new_native_snapshot_id"] = str(new_native)

        with patch(
            "protea.core.operations.generate_evaluation_set.compute_evaluation_data_reconciled",
            return_value=_make_eval_data(),
        ) as mock_reconciled:
            with patch(
                "protea.core.operations.generate_evaluation_set.compute_evaluation_data",
            ) as mock_same:
                self.op.execute(session, payload, emit=self.emit)

        assert mock_reconciled.called
        assert not mock_same.called
        args = mock_reconciled.call_args.args
        assert args[3] == old_native  # old native DAG = override, NOT old_set's binding
        assert args[4] == new_native  # new native DAG = override

    def test_native_override_with_existing_pair_raises(self):
        """A native override must not silently reuse/overwrite the unique-per-pair set."""
        session = MagicMock()
        old_set = _make_annotation_set(uuid.uuid4())
        new_set = _make_annotation_set(uuid.uuid4())
        old_native = uuid.uuid4()
        session.get.side_effect = [old_set, new_set, MagicMock()]
        existing = MagicMock()
        existing.id = uuid.uuid4()
        existing.window_role = None
        session.query.return_value.filter_by.return_value.one_or_none.return_value = existing

        payload = self._payload()
        payload["old_native_snapshot_id"] = str(old_native)

        with patch(
            "protea.core.operations.generate_evaluation_set.compute_evaluation_data_reconciled",
        ) as mock_reconciled:
            with pytest.raises(ValueError, match="already exists"):
                self.op.execute(session, payload, emit=self.emit)
        assert not mock_reconciled.called
