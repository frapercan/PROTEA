"""Tests for the export-minijob pipeline (F-EXPORT-MINIJOB.1/.2/.3).

Coverage:
  - All four operations register without import errors (smoke test, .1)
  - ExportCoordinatorOperation fan-out: 2-pair payload produces the
    expected number of KNN minijob messages (.2)
  - ExportCoordinatorOperation falls back to monolithic path when
    PROTEA_EXPORT_MINIJOBS is not set (.2)
  - _build_snapshot_pairs produces correct (pair_id, type, q, r) tuples
  - ExportKnnBatchOperation.execute publishes a follow-on features message
    and writes a temp artefact (.3)
  - ExportFeaturesBatchOperation.execute reads KNN artefact and writes a
    feature shard (.3)
  - Payload validation edge cases
"""

from __future__ import annotations

import os
import uuid
from io import BytesIO
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from protea.core.contracts.operation import OperationResult
from protea.core.operation_catalog import build_operation_registry
from protea.core.operations.export_minijob import (
    ExportCoordinatorOperation,
    ExportFeaturesBatchOperation,
    ExportKnnBatchOperation,
)
from protea.core.operations.export_minijob._payloads import (
    ExportCoordinatorPayload,
    ExportKnnBatchPayload,
    PairSpec,
    build_knn_batch_payload,
)
from protea.core.operations.export_minijob.export_coordinator import _build_snapshot_pairs


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_coordinator_payload(
    *,
    train_versions: list[int] | None = None,
    test_versions: list[int] | None = None,
    output_name: str = "test_dataset",
    embedding_config_id: str | None = None,
    annotation_set_id: str | None = None,
    ontology_snapshot_id: str | None = None,
) -> dict[str, Any]:
    # Use sentinel to distinguish caller-supplied empty list from "not provided"
    return {
        "embedding_config_id": embedding_config_id if embedding_config_id is not None else str(uuid.uuid4()),
        "ontology_snapshot_id": ontology_snapshot_id if ontology_snapshot_id is not None else str(uuid.uuid4()),
        "annotation_set_id": annotation_set_id if annotation_set_id is not None else str(uuid.uuid4()),
        "train_versions": train_versions if train_versions is not None else [220, 221, 222],
        "test_versions": test_versions if test_versions is not None else [223],
        "output_name": output_name,
        "k": 5,
        "search_backend": "faiss",
        "_job_id": str(uuid.uuid4()),
    }


def _noop_emit(event: str, scope: Any, fields: dict, level: str) -> None:
    pass


# ---------------------------------------------------------------------------
# Smoke tests (.1): registry import
# ---------------------------------------------------------------------------


class TestRegistrySmoke:
    def test_all_minijob_ops_registered(self) -> None:
        registry = build_operation_registry()
        assert "export_coordinator" in registry._ops
        assert "export_knn_batch" in registry._ops
        assert "export_features_batch" in registry._ops

    def test_legacy_op_still_registered(self) -> None:
        registry = build_operation_registry()
        assert "export_research_dataset" in registry._ops

    def test_ops_have_required_attributes(self) -> None:
        for op in (
            ExportCoordinatorOperation(),
            ExportKnnBatchOperation(),
            ExportFeaturesBatchOperation(),
        ):
            assert isinstance(op.name, str) and op.name
            assert isinstance(op.description, str) and op.description


# ---------------------------------------------------------------------------
# Coordinator fan-out (.2): _build_snapshot_pairs
# ---------------------------------------------------------------------------


class TestBuildSnapshotPairs:
    def test_two_train_one_eval_pair(self) -> None:
        p = ExportCoordinatorPayload.model_validate(
            _make_coordinator_payload(train_versions=[220, 221], test_versions=[222])
        )
        pairs = _build_snapshot_pairs(p)
        # 1 train pair + 1 eval pair
        assert len(pairs) == 2
        types = [t for _, t, _, _ in pairs]
        assert "train" in types
        assert "eval" in types

    def test_three_train_versions_yields_two_train_pairs(self) -> None:
        p = ExportCoordinatorPayload.model_validate(
            _make_coordinator_payload(train_versions=[220, 221, 222], test_versions=[223])
        )
        pairs = _build_snapshot_pairs(p)
        assert len(pairs) == 3
        train_pairs = [(q, r) for _, t, q, r in pairs if t == "train"]
        eval_pairs = [(q, r) for _, t, q, r in pairs if t == "eval"]
        assert len(train_pairs) == 2
        assert len(eval_pairs) == 1

    def test_eval_pair_uses_last_train_as_ref(self) -> None:
        p = ExportCoordinatorPayload.model_validate(
            _make_coordinator_payload(train_versions=[220, 221, 222], test_versions=[223])
        )
        pairs = _build_snapshot_pairs(p)
        eval_pairs = [(q, r) for _, t, q, r in pairs if t == "eval"]
        assert len(eval_pairs) == 1
        q_ver, r_ver = eval_pairs[0]
        assert q_ver == 223
        assert r_ver == 222

    def test_pair_ids_are_stable_slugs(self) -> None:
        p = ExportCoordinatorPayload.model_validate(
            _make_coordinator_payload(train_versions=[220, 221], test_versions=[222])
        )
        pairs = _build_snapshot_pairs(p)
        ids = [pid for pid, _, _, _ in pairs]
        # Each pair_id must be a non-empty string with no path separators
        for pid in ids:
            assert isinstance(pid, str) and pid
            assert "/" not in pid

    def test_pair_ids_unique(self) -> None:
        p = ExportCoordinatorPayload.model_validate(
            _make_coordinator_payload(train_versions=[220, 221, 222], test_versions=[223])
        )
        pairs = _build_snapshot_pairs(p)
        ids = [pid for pid, _, _, _ in pairs]
        assert len(ids) == len(set(ids))


# ---------------------------------------------------------------------------
# Coordinator operation (.2): execute with gate on/off
# ---------------------------------------------------------------------------


class TestExportCoordinatorExecute:
    def test_gate_off_delegates_to_monolithic(self) -> None:
        """When PROTEA_EXPORT_MINIJOBS is unset, delegate to legacy operation."""
        op = ExportCoordinatorOperation()
        session = MagicMock()
        payload = _make_coordinator_payload()
        emitted: list[str] = []

        def emit(event: str, *a: Any, **kw: Any) -> None:
            emitted.append(event)

        mock_result = OperationResult(result={"mock": True})
        with (
            patch.dict(os.environ, {}, clear=False),
            patch(
                "protea.core.operations.export_research_dataset.ExportResearchDatasetOperation.execute",
                return_value=mock_result,
            ),
        ):
            os.environ.pop("PROTEA_EXPORT_MINIJOBS", None)
            result = op.execute(session, payload, emit=emit)

        assert result.result == {"mock": True}
        assert any("minijob_gate_off" in e for e in emitted)

    def test_gate_on_fans_out_knn_batch_messages(self) -> None:
        """With gate on, coordinator produces N publish_operations to knn-batch queue."""
        op = ExportCoordinatorOperation()
        session = MagicMock()
        payload = _make_coordinator_payload(
            train_versions=[220, 221, 222], test_versions=[223]
        )

        with patch.dict(os.environ, {"PROTEA_EXPORT_MINIJOBS": "1"}):
            result = op.execute(session, payload, emit=_noop_emit)

        # 2 train pairs + 1 eval pair = 3 total
        assert not result.deferred or result.deferred  # deferred must be set
        assert result.deferred is True
        assert len(result.publish_operations) == 3
        queues = {q for q, _ in result.publish_operations}
        assert queues == {"protea.training.knn-batch"}

    def test_gate_on_two_pair_payload(self) -> None:
        """Minimal 2-train-version payload yields exactly 2 KNN messages."""
        op = ExportCoordinatorOperation()
        session = MagicMock()
        payload = _make_coordinator_payload(
            train_versions=[220, 221], test_versions=[222]
        )

        with patch.dict(os.environ, {"PROTEA_EXPORT_MINIJOBS": "1"}):
            result = op.execute(session, payload, emit=_noop_emit)

        assert len(result.publish_operations) == 2
        # Each message must have coordinator_job_id and total_expected_pairs=2
        for _, msg in result.publish_operations:
            assert "coordinator_job_id" in msg
            assert msg["total_expected_pairs"] == 2

    def test_result_contains_coordinator_job_id(self) -> None:
        op = ExportCoordinatorOperation()
        session = MagicMock()
        job_id = str(uuid.uuid4())
        payload = _make_coordinator_payload()
        payload["_job_id"] = job_id

        with patch.dict(os.environ, {"PROTEA_EXPORT_MINIJOBS": "1"}):
            result = op.execute(session, payload, emit=_noop_emit)

        assert result.result["coordinator_job_id"] == job_id

    def test_progress_total_matches_pair_count(self) -> None:
        op = ExportCoordinatorOperation()
        session = MagicMock()
        payload = _make_coordinator_payload(
            train_versions=[220, 221, 222, 223], test_versions=[224]
        )

        with patch.dict(os.environ, {"PROTEA_EXPORT_MINIJOBS": "1"}):
            result = op.execute(session, payload, emit=_noop_emit)

        # 3 train pairs + 1 eval pair = 4
        assert result.progress_total == 4


# ---------------------------------------------------------------------------
# KNN batch operation (.3): execute with mocked components
# ---------------------------------------------------------------------------


class TestExportKnnBatchExecute:
    def _make_knn_payload(self, **kw: Any) -> dict[str, Any]:
        defaults: dict[str, Any] = {
            "coordinator_job_id": str(uuid.uuid4()),
            "pair_id": "train_220_221",
            "pair_type": "train",
            "query_version": 221,
            "ref_version": 220,
            "embedding_config_id": str(uuid.uuid4()),
            "annotation_set_id": str(uuid.uuid4()),
            "ontology_snapshot_id": str(uuid.uuid4()),
            "output_name": "test_dataset",
            "k": 3,
            "search_backend": "numpy",
            "total_expected_pairs": 2,
        }
        defaults.update(kw)
        return defaults

    def test_publishes_follow_on_features_message(self) -> None:
        """execute() must publish exactly one features message to the right queue."""
        op = ExportKnnBatchOperation()
        session = MagicMock()
        payload = self._make_knn_payload()

        # Synthetic embeddings: 5 ref, 3 query, dim 4
        ref_emb = np.random.rand(5, 4).astype(np.float32)
        ref_acc = [f"REF{i}" for i in range(5)]
        query_emb = np.random.rand(3, 4).astype(np.float32)
        query_acc = ["Q1", "Q2", "Q3"]
        mock_uri = "file:///tmp/fake_knn.npz"

        with (
            patch(
                "protea.core.operations.export_minijob.export_knn_batch._load_ref_pool",
                return_value=(ref_emb, ref_acc),
            ),
            patch(
                "protea.core.operations.export_minijob.export_knn_batch._load_query_embeddings",
                return_value=(query_emb, query_acc),
            ),
            patch(
                "protea.core.operations.export_minijob.export_knn_batch._run_knn",
                return_value=(
                    np.zeros((3, 3), dtype=np.int32),
                    np.zeros((3, 3), dtype=np.float32),
                ),
            ),
            patch(
                "protea.core.operations.export_minijob.export_knn_batch._write_knn_artefact",
                return_value=mock_uri,
            ),
        ):
            result = op.execute(session, payload, emit=_noop_emit)

        assert len(result.publish_operations) == 1
        queue, msg = result.publish_operations[0]
        assert queue == "protea.training.features"
        assert msg["knn_temp_uri"] == mock_uri
        assert msg["pair_id"] == payload["pair_id"]
        assert msg["coordinator_job_id"] == payload["coordinator_job_id"]

    def test_result_contains_expected_fields(self) -> None:
        op = ExportKnnBatchOperation()
        session = MagicMock()
        payload = self._make_knn_payload()

        ref_emb = np.random.rand(5, 4).astype(np.float32)
        query_emb = np.random.rand(2, 4).astype(np.float32)

        with (
            patch(
                "protea.core.operations.export_minijob.export_knn_batch._load_ref_pool",
                return_value=(ref_emb, [f"R{i}" for i in range(5)]),
            ),
            patch(
                "protea.core.operations.export_minijob.export_knn_batch._load_query_embeddings",
                return_value=(query_emb, ["Q1", "Q2"]),
            ),
            patch(
                "protea.core.operations.export_minijob.export_knn_batch._run_knn",
                return_value=(
                    np.zeros((2, 3), dtype=np.int32),
                    np.zeros((2, 3), dtype=np.float32),
                ),
            ),
            patch(
                "protea.core.operations.export_minijob.export_knn_batch._write_knn_artefact",
                return_value="file:///tmp/x.npz",
            ),
        ):
            result = op.execute(session, payload, emit=_noop_emit)

        assert result.result["pair_id"] == payload["pair_id"]
        assert result.result["pair_type"] == payload["pair_type"]
        assert "knn_temp_uri" in result.result
        assert result.result["n_queries"] == 2


# ---------------------------------------------------------------------------
# Features batch operation (.3): execute with mocked components
# ---------------------------------------------------------------------------


class TestExportFeaturesBatchExecute:
    def _make_features_payload(self, **kw: Any) -> dict[str, Any]:
        defaults: dict[str, Any] = {
            "coordinator_job_id": str(uuid.uuid4()),
            "pair_id": "train_220_221",
            "pair_type": "train",
            "query_version": 221,
            "ref_version": 220,
            "embedding_config_id": str(uuid.uuid4()),
            "annotation_set_id": str(uuid.uuid4()),
            "ontology_snapshot_id": str(uuid.uuid4()),
            "output_name": "test_dataset",
            "knn_temp_uri": "file:///tmp/fake_knn.npz",
            "k": 3,
            "total_expected_pairs": 2,
        }
        defaults.update(kw)
        return defaults

    def _make_fake_knn_artefact(self, n_queries: int = 3, k: int = 3, n_ref: int = 5) -> dict[str, Any]:
        return {
            "indices": np.zeros((n_queries, k), dtype=np.int32),
            "distances": np.ones((n_queries, k), dtype=np.float32),
            "query_accessions": [f"Q{i}" for i in range(n_queries)],
            "ref_accessions": [f"R{i}" for i in range(n_ref)],
        }

    def test_increments_parent_progress_and_returns_shard_uri(self) -> None:
        """execute() must call update_parent_progress and return shard_uri in result."""
        op = ExportFeaturesBatchOperation()
        session = MagicMock()
        payload = self._make_features_payload()
        knn_data = self._make_fake_knn_artefact()
        fake_records = [{"col_a": 1, "col_b": 2.0}]

        with (
            patch(
                "protea.core.operations.export_minijob.export_features_batch._read_knn_artefact",
                return_value=knn_data,
            ),
            patch(
                "protea.core.operations.export_minijob.export_features_batch._run_feature_pipeline",
                return_value=fake_records,
            ),
            patch(
                "protea.core.operations.export_minijob.export_features_batch._write_feature_shard",
                return_value="file:///tmp/shard.parquet",
            ),
            patch(
                "protea.core.operations.export_minijob.export_features_batch.update_parent_progress"
            ) as mock_progress,
        ):
            result = op.execute(session, payload, emit=_noop_emit)

        mock_progress.assert_called_once()
        assert result.result["shard_uri"] == "file:///tmp/shard.parquet"
        assert result.result["pair_id"] == payload["pair_id"]
        assert result.result["n_rows"] == 1

    def test_result_pair_type_propagated(self) -> None:
        op = ExportFeaturesBatchOperation()
        session = MagicMock()
        payload = self._make_features_payload(pair_type="eval")

        with (
            patch(
                "protea.core.operations.export_minijob.export_features_batch._read_knn_artefact",
                return_value=self._make_fake_knn_artefact(),
            ),
            patch(
                "protea.core.operations.export_minijob.export_features_batch._run_feature_pipeline",
                return_value=[],
            ),
            patch(
                "protea.core.operations.export_minijob.export_features_batch._write_feature_shard",
                return_value="s3://bucket/shard.parquet",
            ),
            patch(
                "protea.core.operations.export_minijob.export_features_batch.update_parent_progress"
            ),
        ):
            result = op.execute(session, payload, emit=_noop_emit)

        assert result.result["pair_type"] == "eval"


# ---------------------------------------------------------------------------
# Payload validation
# ---------------------------------------------------------------------------


class TestPayloadValidation:
    def test_coordinator_rejects_single_train_version(self) -> None:
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="at least 2"):
            ExportCoordinatorPayload.model_validate(
                _make_coordinator_payload(train_versions=[220])
            )

    def test_coordinator_rejects_empty_test_versions(self) -> None:
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="at least 1"):
            ExportCoordinatorPayload.model_validate(
                _make_coordinator_payload(test_versions=[])
            )

    def test_coordinator_rejects_empty_output_name(self) -> None:
        from pydantic import ValidationError

        payload = _make_coordinator_payload()
        payload["output_name"] = "   "
        with pytest.raises(ValidationError):
            ExportCoordinatorPayload.model_validate(payload)

    def test_knn_batch_payload_defaults(self) -> None:
        p = ExportKnnBatchPayload.model_validate(
            {
                "coordinator_job_id": str(uuid.uuid4()),
                "pair_id": "train_220_221",
                "pair_type": "train",
                "query_version": 221,
                "ref_version": 220,
                "embedding_config_id": str(uuid.uuid4()),
                "annotation_set_id": str(uuid.uuid4()),
                "ontology_snapshot_id": str(uuid.uuid4()),
                "output_name": "ds",
            }
        )
        assert p.k == 5
        assert p.search_backend == "faiss"
        assert p.total_expected_pairs == 1

    def test_build_knn_batch_payload_round_trips(self) -> None:
        """build_knn_batch_payload output must parse back as ExportKnnBatchPayload."""
        coord_p = ExportCoordinatorPayload.model_validate(
            _make_coordinator_payload(train_versions=[220, 221], test_versions=[222])
        )
        spec = PairSpec(
            coordinator_job_id=str(uuid.uuid4()),
            pair_id="train_220_221",
            pair_type="train",
            query_version=221,
            ref_version=220,
            total=2,
        )
        raw = build_knn_batch_payload(spec, coord_p)
        parsed = ExportKnnBatchPayload.model_validate(raw)
        assert parsed.pair_id == "train_220_221"
        assert parsed.total_expected_pairs == 2


# ---------------------------------------------------------------------------
# Summarize payload
# ---------------------------------------------------------------------------


class TestSummarizePayload:
    def test_coordinator_summary_includes_output_name(self) -> None:
        op = ExportCoordinatorOperation()
        s = op.summarize_payload({"output_name": "my_ds", "k": 5})
        assert "my_ds" in s

    def test_knn_batch_summary_includes_pair_id(self) -> None:
        op = ExportKnnBatchOperation()
        s = op.summarize_payload({"pair_id": "train_220_221"})
        assert "train_220_221" in s

    def test_features_batch_summary_includes_pair_id(self) -> None:
        op = ExportFeaturesBatchOperation()
        s = op.summarize_payload({"pair_id": "eval_222_223"})
        assert "eval_222_223" in s
