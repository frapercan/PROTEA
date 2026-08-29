"""Unit tests for the ``export_research_dataset`` operation.

Focus: the long-running operation session-lifecycle fix. The bug
(LB.1 v226, 2026-05-12) was that a single session was held across a
3-4 hour compute window. The pool recycled the connection underneath
it, and the final ``Dataset`` insert + ``Job.status=SUCCEEDED`` commit
blew up with ``Instance ... is not bound to a Session``.

These tests verify the three-window structure of ``execute``:

* Window 1: uniqueness check on the passed session, then ``expire_all``.
* Window 2: a fresh ``session_scope`` for the dump/compute phase.
* Window 3: a fresh ``session_scope`` for the final ``Dataset`` insert.

The fresh session in window 3 is the load-bearing piece: even if the
worker's original session is dead by the time compute finishes, the
``Dataset`` row still lands.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from protea.core.contracts.operation import OperationResult
from protea.core.operations.export_research_dataset import (
    ExportResearchDatasetOperation,
    _DumpOutcome,
)


@pytest.fixture
def valid_payload() -> dict:
    return {
        "embedding_config_id": str(uuid.uuid4()),
        "ontology_snapshot_id": str(uuid.uuid4()),
        "train_versions": [220, 221, 222],
        "test_versions": [223],
        "annotation_source": "goa",
        "output_name": "test_export_v226",
        "k": 5,
        "search_backend": "faiss",
        "compute_alignments": False,
        "compute_taxonomy": False,
        "expand_votes_to_ancestors": False,
        "use_embedding_pca": False,
        "_job_id": str(uuid.uuid4()),
    }


def _stub_settings() -> MagicMock:
    settings = MagicMock()
    settings.storage_backend = "local"
    settings.db_url = "postgresql+psycopg://stub:stub@localhost:5432/stub"
    return settings


def _stub_manifest() -> dict:
    return {
        "schema_sha": "deadbeef",
        "n_train_rows": 100,
        "n_eval_rows": 20,
        "k": 5,
        "annotation_source": "goa",
        "train_snapshot_pairs": [[220, 221]],
        "eval_snapshot_pair": [222, 223],
        "producer_version": "0.8.0",
        "producer_git_sha": "abcd1234",
    }


class TestExecuteSessionWindows:
    """Verify ``execute`` runs compute + register against fresh sessions."""

    def test_uses_fresh_session_factory_after_compute(self, valid_payload, tmp_path):
        """The original session must not be reused for the final Dataset insert.

        Regression for the LB.1 v226 crash: after a multi-hour compute
        the supplied session may be detached. The fix opens a separate
        ``session_scope`` via ``build_session_factory`` for window 3,
        so the Dataset row insert lands on a freshly checked-out
        connection.
        """
        op = ExportResearchDatasetOperation()

        # The worker-supplied session: only window 1 may touch it.
        passed_session = MagicMock()
        passed_session.query.return_value.filter.return_value.first.return_value = None

        # Two fresh sessions: one for compute (window 2), one for
        # register (window 3).
        compute_session = MagicMock()
        register_session = MagicMock()

        # Stand-in factory: hand out compute_session first, then
        # register_session. ``session_scope`` will call commit/close
        # itself, so we don't have to model that here.
        fresh_factory = MagicMock(side_effect=[compute_session, register_session])

        dataset_id = uuid.uuid4()

        with (
            patch(
                "protea.core.operations.export_research_dataset.load_settings",
                return_value=_stub_settings(),
            ),
            patch(
                "protea.core.operations.export_research_dataset.get_artifact_store"
            ) as mock_store,
            patch(
                "protea.core.operations.export_research_dataset.build_session_factory",
                return_value=fresh_factory,
            ) as mock_build_factory,
            patch.object(
                op,
                "_dump_to_stage",
                return_value=OperationResult(result={"dump_ok": True}),
            ) as mock_dump,
            patch.object(
                op,
                "_upload_outputs",
                return_value=(
                    {
                        "train.parquet": "local://datasets/test/train.parquet",
                        "eval.parquet": "local://datasets/test/eval.parquet",
                        "manifest.json": "local://datasets/test/manifest.json",
                    },
                    _stub_manifest(),
                    "manifest-sha",
                ),
            ) as mock_upload,
            patch.object(
                op, "_register_dataset", return_value=dataset_id
            ) as mock_register,
        ):
            mock_store.return_value = MagicMock()
            emit = MagicMock()
            result = op.execute(passed_session, valid_payload, emit=emit)

        # Window 1: uniqueness check ran on the worker-supplied session.
        passed_session.query.assert_called_once()
        # And we expired any cached state so attribute reads do not
        # hold a stale identity map.
        passed_session.expire_all.assert_called_once()

        # Window 2: dump ran against the compute_session (NOT the passed
        # session). This is the load-bearing assertion: the long
        # compute phase must not pin the worker's session open.
        mock_dump.assert_called_once()
        assert mock_dump.call_args.args[0] is compute_session
        assert mock_dump.call_args.args[0] is not passed_session

        # Window 3: register ran against the register_session.
        mock_register.assert_called_once()
        assert mock_register.call_args.args[0] is register_session
        assert mock_register.call_args.args[0] is not passed_session
        assert mock_register.call_args.args[0] is not compute_session

        # The fresh session factory was built exactly once (windows 2+3
        # share it via the ``session_scope`` context manager).
        mock_build_factory.assert_called_once()

        # Result carries the dataset_id back to the worker.
        assert result.result["dataset_id"] == str(dataset_id)
        assert result.result["output_name"] == "test_export_v226"
        mock_upload.assert_called_once()

    def test_passed_session_never_touched_during_compute(self, valid_payload):
        """Compute window must not call any method on the original session.

        Old code shape: ``self._dump_to_stage(session, ...)``. New shape:
        ``self._dump_to_stage(compute_session, ...)``. The regression
        check is that after the uniqueness probe + expire, the passed
        session is otherwise untouched.
        """
        op = ExportResearchDatasetOperation()
        passed_session = MagicMock()
        passed_session.query.return_value.filter.return_value.first.return_value = None

        compute_session = MagicMock()
        register_session = MagicMock()
        fresh_factory = MagicMock(side_effect=[compute_session, register_session])

        with (
            patch(
                "protea.core.operations.export_research_dataset.load_settings",
                return_value=_stub_settings(),
            ),
            patch(
                "protea.core.operations.export_research_dataset.get_artifact_store"
            ),
            patch(
                "protea.core.operations.export_research_dataset.build_session_factory",
                return_value=fresh_factory,
            ),
            patch.object(
                op, "_dump_to_stage", return_value=OperationResult(result={})
            ),
            patch.object(
                op,
                "_upload_outputs",
                return_value=({"manifest.json": "x"}, _stub_manifest(), "sha"),
            ),
            patch.object(op, "_register_dataset", return_value=uuid.uuid4()),
        ):
            op.execute(passed_session, valid_payload, emit=MagicMock())

        # Only ``query`` and ``expire_all`` should have been called on
        # the worker-supplied session : nothing else.
        permitted = {"query", "expire_all"}
        actual_methods = {
            c[0] for c in passed_session.method_calls if c[0] not in {"__bool__"}
        }
        # method_calls includes the chained ``.query().filter().first()``
        # as separate entries; restrict to the top-level method names.
        top_level = {name.split(".")[0] for name in actual_methods}
        assert top_level <= permitted, (
            f"passed session was touched outside window 1: extra={top_level - permitted}"
        )

    def test_duplicate_name_rejected_before_compute(self, valid_payload):
        """If the name is already taken we abort BEFORE touching the dump."""
        op = ExportResearchDatasetOperation()
        passed_session = MagicMock()
        # Simulate an existing Dataset row.
        passed_session.query.return_value.filter.return_value.first.return_value = (
            uuid.uuid4(),
        )

        with (
            patch(
                "protea.core.operations.export_research_dataset.load_settings",
                return_value=_stub_settings(),
            ),
            patch(
                "protea.core.operations.export_research_dataset.get_artifact_store"
            ),
            patch(
                "protea.core.operations.export_research_dataset.build_session_factory"
            ) as mock_factory,
            patch.object(op, "_dump_to_stage") as mock_dump,
        ):
            with pytest.raises(ValueError, match="already exists"):
                op.execute(passed_session, valid_payload, emit=MagicMock())

        mock_dump.assert_not_called()
        # We never even built the fresh factory.
        mock_factory.assert_not_called()


class TestExecuteSurvivesDetachedPassedSession:
    """The crucial regression: a dead passed-session must not lose the Dataset row.

    This is the LB.1 v226 scenario, re-encoded: imagine the passed
    session is healthy enough for window 1 (a single uniqueness probe),
    but everything afterwards must come off the fresh factory. Even if
    we mutate the passed session to look broken after window 1, the
    Dataset insert must still succeed because window 3 uses a different
    session.
    """

    def test_dataset_inserts_even_when_passed_session_dies_after_window1(
        self, valid_payload
    ):
        op = ExportResearchDatasetOperation()
        passed_session = MagicMock()
        passed_session.query.return_value.filter.return_value.first.return_value = None

        # After the uniqueness check, the passed session becomes
        # completely poisoned. Any access raises. This mirrors the
        # production failure mode where the worker's session has had
        # its underlying connection recycled while compute was running.
        def explode(*_a, **_kw):
            raise RuntimeError("Instance is not bound to a Session")

        # Snapshot the methods we already need (query was used) : we
        # only want to poison *future* access.
        original_query = passed_session.query
        post_window1_invoked = {"flag": False}

        def maybe_explode_query(*a, **kw):
            if post_window1_invoked["flag"]:
                explode()
            return original_query(*a, **kw)

        passed_session.query = maybe_explode_query

        compute_session = MagicMock()
        register_session = MagicMock()
        fresh_factory = MagicMock(side_effect=[compute_session, register_session])

        # Capture the dataset_id the operation says it produced.
        observed_dataset_id = uuid.uuid4()

        def register_side_effect(session, *_a, **_kw):
            # Sanity: window 3 must be using a fresh session.
            assert session is register_session
            return observed_dataset_id

        with (
            patch(
                "protea.core.operations.export_research_dataset.load_settings",
                return_value=_stub_settings(),
            ),
            patch(
                "protea.core.operations.export_research_dataset.get_artifact_store"
            ),
            patch(
                "protea.core.operations.export_research_dataset.build_session_factory",
                return_value=fresh_factory,
            ),
            patch.object(
                op,
                "_dump_to_stage",
                side_effect=lambda *_a, **_kw: (
                    post_window1_invoked.update({"flag": True})
                    or OperationResult(result={"ok": True})
                ),
            ),
            patch.object(
                op,
                "_upload_outputs",
                return_value=({"manifest.json": "x"}, _stub_manifest(), "sha"),
            ),
            patch.object(op, "_register_dataset", side_effect=register_side_effect),
        ):
            result = op.execute(passed_session, valid_payload, emit=MagicMock())

        # The Dataset row was registered despite the passed session
        # being detached after window 1.
        assert result.result["dataset_id"] == str(observed_dataset_id)


class TestRegisterDataset:
    """Ensure ``_register_dataset`` still wires the manifest into the row."""

    def test_register_dataset_inserts_row_and_emits(self, valid_payload):
        op = ExportResearchDatasetOperation()
        session = MagicMock()
        flushed_dataset = {}

        def add_side_effect(obj):
            flushed_dataset["obj"] = obj

        def flush_side_effect():
            # Assign an id like SQLAlchemy would on flush.
            flushed_dataset["obj"].id = uuid.uuid4()

        session.add.side_effect = add_side_effect
        session.flush.side_effect = flush_side_effect

        from protea.core.operations.export_research_dataset import (
            ExportResearchDatasetPayload,
        )

        # Through the same door production uses. The fixture carries the
        # _job_id base_worker injects, and the contract forbids undeclared
        # keys, so validating the delivered dict raw is not what any
        # operation does.
        from protea.core.utils import contract_payload

        p = ExportResearchDatasetPayload.model_validate(contract_payload(valid_payload))
        outcome = _DumpOutcome(
            settings=_stub_settings(),
            key_prefix="datasets/test_export_v226/",
            uploaded={
                "train.parquet": "local://train",
                "eval.parquet": "local://eval",
                "manifest.json": "local://manifest",
            },
            manifest_data=_stub_manifest(),
            manifest_sha="manifest-sha",
        )

        emit = MagicMock()
        dataset_id = op._register_dataset(session, p, outcome, uuid.UUID(valid_payload["_job_id"]), emit)

        assert dataset_id == flushed_dataset["obj"].id
        # Manifest fields propagated to the ORM row.
        assert flushed_dataset["obj"].manifest_sha == "manifest-sha"
        assert flushed_dataset["obj"].n_train_rows == 100
        # Emit fired the registered event.
        events = [c.args[0] for c in emit.call_args_list]
        assert "export_research_dataset.registered" in events


def test_resolve_project_root_returns_path():
    from protea.core.operations.export_research_dataset import _resolve_project_root

    root = _resolve_project_root()
    assert isinstance(root, Path)
    assert root.exists()
