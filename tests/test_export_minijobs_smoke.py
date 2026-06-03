"""Smoke tests for the export minijob pipeline (F-EXPORT-MINIJOB.1 + .2 + .3).

F-EXPORT-MINIJOB.1 acceptance:
- All four operations import without error.
- All four operations are registered in the OperationRegistry returned by
  build_operation_registry().
- Each handler can be invoked with minimal/stub inputs.

F-EXPORT-MINIJOB.2 acceptance:
- export_coordinator.execute() with PROTEA_EXPORT_MINIJOBS=1 dispatches
  len(train_versions) + len(test_versions) KNN minijob messages to
  protea.training.knn-batch.
- The coordinator returns deferred=True (stays RUNNING).
- With PROTEA_EXPORT_MINIJOBS=0 the coordinator delegates to the legacy
  monolithic path.
- Integration: 2-train + 1-test versions dispatch exactly 3 publish_operations.
"""

from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# F-EXPORT-MINIJOB.1 — import smoke
# ---------------------------------------------------------------------------


class TestExportMinijobImports:
    def test_all_four_operations_import(self) -> None:
        from protea.core.operations.export_minijobs import (
            ExportCoordinatorOperation,
            ExportFeaturesBatchOperation,
            ExportKnnBatchOperation,
            ExportWriteOperation,
        )

        assert ExportCoordinatorOperation.name == "export_coordinator"
        assert ExportKnnBatchOperation.name == "export_knn_batch"
        assert ExportFeaturesBatchOperation.name == "export_features_batch"
        assert ExportWriteOperation.name == "export_write"

    def test_all_four_registered_in_catalog(self) -> None:
        from protea.core.operation_catalog import build_operation_registry

        registry = build_operation_registry()
        for name in (
            "export_coordinator",
            "export_knn_batch",
            "export_features_batch",
            "export_write",
        ):
            op = registry.get(name)
            assert op is not None, f"Operation {name!r} not in registry"
            assert op.name == name


# ---------------------------------------------------------------------------
# F-EXPORT-MINIJOB.1 — stub handler invocations
# ---------------------------------------------------------------------------


def _noop_emit(event: str, msg: Any, fields: Any, level: str) -> None:
    pass


def _stub_session() -> MagicMock:
    return MagicMock()


class TestExportKnnBatchSmoke:
    """Smoke test: operation instantiates; summarize_payload works without DB."""

    def test_name_and_description(self) -> None:
        from protea.core.operations.export_minijobs._export_knn_batch import (
            ExportKnnBatchOperation,
        )

        op = ExportKnnBatchOperation()
        assert op.name == "export_knn_batch"
        assert "KNN" in op.description or "knn" in op.description.lower()

    def test_summarize_payload(self) -> None:
        from protea.core.operations.export_minijobs._export_knn_batch import (
            ExportKnnBatchOperation,
        )

        op = ExportKnnBatchOperation()
        summary = op.summarize_payload({"pair_id": "train-220", "k": 5})
        assert "train-220" in summary
        assert "k=5" in summary


class TestExportFeaturesBatchSmoke:
    """Smoke test: null URI path returns empty shard without DB access."""

    def test_null_uri_returns_empty_shard(self) -> None:
        from protea.core.operations.export_minijobs._export_features_batch import (
            ExportFeaturesBatchOperation,
        )

        op = ExportFeaturesBatchOperation()
        payload = {
            "coordinator_job_id": str(uuid.uuid4()),
            "pair_id": "train-220",
            "temp_knn_uri": None,
            "embedding_config_id": str(uuid.uuid4()),
            "annotation_set_id": str(uuid.uuid4()),
            "ontology_snapshot_id": str(uuid.uuid4()),
        }
        result = op.execute(_stub_session(), payload, emit=_noop_emit)
        assert result.result["pair_id"] == "train-220"
        assert result.result["n_rows"] == 0

    def test_summarize_payload(self) -> None:
        from protea.core.operations.export_minijobs._export_features_batch import (
            ExportFeaturesBatchOperation,
        )

        op = ExportFeaturesBatchOperation()
        summary = op.summarize_payload({"pair_id": "eval-230"})
        assert "eval-230" in summary


class TestExportWriteSmoke:
    """Smoke: payload validation + summarize behaviour without DB."""

    def test_summarize_payload(self) -> None:
        from protea.core.operations.export_minijobs._export_write import (
            ExportWriteOperation,
        )

        op = ExportWriteOperation()
        summary = op.summarize_payload(
            {"output_name": "bench-v1-K5-v226-lineage", "pair_id": "train-220"}
        )
        assert "bench-v1-K5-v226-lineage" in summary
        assert "train-220" in summary

    def test_payload_rejects_missing_required_fields(self) -> None:
        import pytest
        from pydantic import ValidationError

        from protea.core.operations.export_minijobs._export_write import (
            ExportWritePayload,
        )

        with pytest.raises(ValidationError):
            ExportWritePayload.model_validate({"output_name": "x"})


# ---------------------------------------------------------------------------
# F-EXPORT-MINIJOB.2 — coordinator dispatch
# ---------------------------------------------------------------------------


class TestExportCoordinatorMinijobsOff:
    """When PROTEA_EXPORT_MINIJOBS=0 the coordinator delegates to the legacy op."""

    def test_delegates_to_legacy_when_gate_off(self, monkeypatch) -> None:
        from protea.core.operations.export_minijobs.export_coordinator import (
            ExportCoordinatorOperation,
        )

        monkeypatch.delenv("PROTEA_EXPORT_MINIJOBS", raising=False)

        legacy_result = MagicMock()
        legacy_result.result = {"legacy": True}

        with patch(
            "protea.core.operations.export_minijobs.export_coordinator"
            ".ExportCoordinatorOperation._delegate_legacy",
            return_value=legacy_result,
        ) as mock_delegate:
            op = ExportCoordinatorOperation()
            payload: dict[str, Any] = {
                "output_name": "test-export",
                "embedding_config_id": str(uuid.uuid4()),
                "annotation_set_id": str(uuid.uuid4()),
                "ontology_snapshot_id": str(uuid.uuid4()),
                "train_versions": [220, 221],
                "test_versions": [222],
                "_job_id": str(uuid.uuid4()),
            }
            result = op.execute(_stub_session(), payload, emit=_noop_emit)
            mock_delegate.assert_called_once()
            assert result.result["legacy"] is True


class TestExportCoordinatorMinijobsOn:
    """When PROTEA_EXPORT_MINIJOBS=1 the coordinator partitions and dispatches."""

    def _make_payload(
        self,
        train_versions: list[int],
        test_versions: list[int],
        *,
        output_name: str = "test-export",
    ) -> dict[str, Any]:
        return {
            "output_name": output_name,
            "embedding_config_id": str(uuid.uuid4()),
            "annotation_set_id": str(uuid.uuid4()),
            "ontology_snapshot_id": str(uuid.uuid4()),
            "train_versions": train_versions,
            "test_versions": test_versions,
            "k": 5,
            "search_backend": "faiss",
            "_job_id": str(uuid.uuid4()),
        }

    def test_two_train_one_test_dispatches_three_minijobs(self, monkeypatch) -> None:
        """2 train versions + 1 test version -> 3 KNN minijob messages."""
        from protea.core.operations.export_minijobs.export_coordinator import (
            ExportCoordinatorOperation,
        )

        monkeypatch.setenv("PROTEA_EXPORT_MINIJOBS", "1")

        op = ExportCoordinatorOperation()
        payload = self._make_payload(
            train_versions=[220, 221],
            test_versions=[222],
        )
        result = op.execute(_stub_session(), payload, emit=_noop_emit)

        assert result.deferred is True, "coordinator must stay RUNNING (deferred=True)"
        assert result.progress_total == 3, "3 minijobs expected (2 train + 1 test)"
        assert result.progress_current == 0
        assert len(result.publish_operations) == 3

    def test_all_published_to_knn_batch_queue(self, monkeypatch) -> None:
        from protea.core.operations.export_minijobs.export_coordinator import (
            _KNN_BATCH_QUEUE,
            ExportCoordinatorOperation,
        )

        monkeypatch.setenv("PROTEA_EXPORT_MINIJOBS", "1")

        op = ExportCoordinatorOperation()
        payload = self._make_payload(train_versions=[220, 221], test_versions=[222])
        result = op.execute(_stub_session(), payload, emit=_noop_emit)

        for queue_name, _ in result.publish_operations:
            assert queue_name == _KNN_BATCH_QUEUE

    def test_each_minijob_is_self_contained(self, monkeypatch) -> None:
        """Each minijob payload must carry coordinator_job_id + pair_id + embedding config."""
        from protea.core.operations.export_minijobs.export_coordinator import (
            ExportCoordinatorOperation,
        )

        monkeypatch.setenv("PROTEA_EXPORT_MINIJOBS", "1")

        cfg_id = str(uuid.uuid4())
        ann_id = str(uuid.uuid4())
        job_id = str(uuid.uuid4())
        op = ExportCoordinatorOperation()
        payload = {
            "output_name": "bench-v1-K5-v226",
            "embedding_config_id": cfg_id,
            "annotation_set_id": ann_id,
            "ontology_snapshot_id": str(uuid.uuid4()),
            "train_versions": [220, 221],
            "test_versions": [222],
            "k": 5,
            "search_backend": "faiss",
            "_job_id": job_id,
        }
        result = op.execute(_stub_session(), payload, emit=_noop_emit)

        for _, msg in result.publish_operations:
            inner = msg["payload"]
            assert inner["coordinator_job_id"] == job_id
            assert inner["embedding_config_id"] == cfg_id
            assert inner["annotation_set_id"] == ann_id
            assert "pair_id" in inner
            assert inner["k"] == 5

    def test_train_and_eval_pair_ids_distinguished(self, monkeypatch) -> None:
        """Train pairs carry ``train-N`` pair_id, eval pairs carry ``eval-N``."""
        from protea.core.operations.export_minijobs.export_coordinator import (
            ExportCoordinatorOperation,
        )

        monkeypatch.setenv("PROTEA_EXPORT_MINIJOBS", "1")

        op = ExportCoordinatorOperation()
        payload = self._make_payload(train_versions=[220, 221], test_versions=[222])
        result = op.execute(_stub_session(), payload, emit=_noop_emit)

        pair_ids = [msg["payload"]["pair_id"] for _, msg in result.publish_operations]
        train_pairs = [p for p in pair_ids if p.startswith("train-")]
        eval_pairs = [p for p in pair_ids if p.startswith("eval-")]

        assert len(train_pairs) == 2
        assert len(eval_pairs) == 1
        assert "train-220" in train_pairs
        assert "train-221" in train_pairs
        assert "eval-222" in eval_pairs

    def test_result_carries_metadata(self, monkeypatch) -> None:
        from protea.core.operations.export_minijobs.export_coordinator import (
            ExportCoordinatorOperation,
        )

        monkeypatch.setenv("PROTEA_EXPORT_MINIJOBS", "1")

        op = ExportCoordinatorOperation()
        payload = self._make_payload(train_versions=[220, 221], test_versions=[222])
        result = op.execute(_stub_session(), payload, emit=_noop_emit)

        assert result.result["output_name"] == "test-export"
        assert result.result["n_minijobs"] == 3
        assert result.result["train_versions"] == [220, 221]
        assert result.result["test_versions"] == [222]

    def test_summarize_payload(self) -> None:
        from protea.core.operations.export_minijobs.export_coordinator import (
            ExportCoordinatorOperation,
        )

        op = ExportCoordinatorOperation()
        summary = op.summarize_payload(
            {
                "output_name": "bench-v1-K5-v226",
                "train_versions": [220, 221, 222],
                "test_versions": [223],
                "k": 5,
            }
        )
        assert "bench-v1-K5-v226" in summary
        assert "k=5" in summary
