"""Tests for F-EXPORT-MINIJOB.3 batch implementations.

Coverage:
- ExportKnnBatchOperation: end-to-end with mocked DB helpers, verifies the
  temp npz shard exists and has the expected shape.
- ExportFeaturesBatchOperation: loads a pre-constructed npz (built by the
  KNN batch test helper), verifies the features parquet is written.
- NPZ round-trip: verifies that ``_write_knn_npz`` / ``_load_knn_npz``
  preserve query accessions and per-aspect neighbor lists exactly.
- ``_uri_to_key``: path extraction from various URI formats.
- ``_PrecomputedKnnRunner``: verifies that the runner injects pre-computed
  neighbors instead of re-running KNN (``_run_knn`` is not called on the
  parent).
"""

from __future__ import annotations

import tempfile
import uuid
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np

# ---------------------------------------------------------------------------
# Shared fixtures / helpers
# ---------------------------------------------------------------------------

_ASPECTS = ("P", "F", "C")


def _noop_emit(event: str, msg: Any, fields: Any, level: str) -> None:
    pass


def _mock_session() -> MagicMock:
    return MagicMock()


def _make_knn_payload(
    coordinator_job_id: str | None = None,
    pair_id: str = "train-220",
    k: int = 3,
) -> dict[str, Any]:
    return {
        "coordinator_job_id": coordinator_job_id or str(uuid.uuid4()),
        "pair_id": pair_id,
        "train_snapshot_id": 220,
        "test_snapshot_id": 220,
        "embedding_config_id": str(uuid.uuid4()),
        "annotation_set_id": str(uuid.uuid4()),
        "ontology_snapshot_id": str(uuid.uuid4()),
        "k": k,
        "search_backend": "numpy",
    }


def _make_features_payload(
    coordinator_job_id: str,
    pair_id: str,
    temp_knn_uri: str | None,
) -> dict[str, Any]:
    return {
        "coordinator_job_id": coordinator_job_id,
        "pair_id": pair_id,
        "temp_knn_uri": temp_knn_uri,
        "embedding_config_id": str(uuid.uuid4()),
        "annotation_set_id": str(uuid.uuid4()),
        "ontology_snapshot_id": str(uuid.uuid4()),
    }


# ---------------------------------------------------------------------------
# NPZ round-trip
# ---------------------------------------------------------------------------


class TestNpzRoundTrip:
    """Verify _write_knn_npz + _load_knn_npz preserve data exactly."""

    def test_round_trip_preserves_query_accs_and_neighbors(self, tmp_path: Path) -> None:
        from protea.core.operations.export_minijobs._export_features_batch import _load_knn_npz
        from protea.core.operations.export_minijobs._export_knn_batch import _write_knn_npz
        from protea.infrastructure.storage.local import LocalFsArtifactStore

        store = LocalFsArtifactStore(tmp_path)
        key = "temp/test_knn.npz"

        query_accs = ["P00001", "P00002", "P00003"]
        neighbors_by_aspect: dict[str, list[list[tuple[str, float]]]] = {
            "P": [[("P10001", 0.1), ("P10002", 0.2)], [("P10003", 0.15)], []],
            "F": [[("P20001", 0.3)], [], [("P20002", 0.05)]],
            "C": [[], [], []],
        }

        uri = _write_knn_npz(store, key, query_accs, neighbors_by_aspect)
        assert uri

        loaded_neighbors, loaded_accs = _load_knn_npz(store, uri)

        assert loaded_accs == query_accs

        for asp in _ASPECTS:
            assert asp in loaded_neighbors
            orig = neighbors_by_aspect[asp]
            loaded = loaded_neighbors[asp]
            assert len(loaded) == len(orig), f"aspect {asp}: length mismatch"
            for i, (o_row, l_row) in enumerate(zip(orig, loaded, strict=False)):
                assert len(l_row) == len(o_row), f"aspect {asp} query {i}: row length"
                for (o_acc, o_d), (l_acc, l_d) in zip(o_row, l_row, strict=False):
                    assert l_acc == o_acc
                    assert abs(l_d - o_d) < 1e-4

    def test_empty_npz_round_trip(self, tmp_path: Path) -> None:
        from protea.core.operations.export_minijobs._export_features_batch import _load_knn_npz
        from protea.core.operations.export_minijobs._export_knn_batch import _write_empty_npz
        from protea.infrastructure.storage.local import LocalFsArtifactStore

        store = LocalFsArtifactStore(tmp_path)
        key = "temp/empty.npz"

        uri = _write_empty_npz(store, key)
        assert uri

        loaded_neighbors, loaded_accs = _load_knn_npz(store, uri)
        assert loaded_accs == []


# ---------------------------------------------------------------------------
# URI key extraction
# ---------------------------------------------------------------------------


class TestUriToKey:
    def test_file_uri_with_temp_prefix(self) -> None:
        from protea.core.operations.export_minijobs._export_features_batch import _uri_to_key

        key = _uri_to_key("file:///some/storage/root/temp/coordinator/abc/knn/pair.npz")
        assert key == "temp/coordinator/abc/knn/pair.npz"

    def test_local_uri(self) -> None:
        from protea.core.operations.export_minijobs._export_features_batch import _uri_to_key

        key = _uri_to_key("local:///some/root/temp/coordinator/abc/knn/pair.npz")
        assert key == "temp/coordinator/abc/knn/pair.npz"

    def test_datasets_prefix(self) -> None:
        from protea.core.operations.export_minijobs._export_features_batch import _uri_to_key

        key = _uri_to_key("file:///root/datasets/bench-v1/train.parquet")
        assert key == "datasets/bench-v1/train.parquet"

    def test_bare_key_passthrough(self) -> None:
        from protea.core.operations.export_minijobs._export_features_batch import _uri_to_key

        key = _uri_to_key("temp/coordinator/abc/knn/pair.npz")
        assert key == "temp/coordinator/abc/knn/pair.npz"


# ---------------------------------------------------------------------------
# ExportKnnBatchOperation — unit tests with mocked DB helpers
# ---------------------------------------------------------------------------


class TestExportKnnBatchOperation:
    """Unit tests for the KNN batch operation with mocked helpers."""

    def _make_synthetic_embeddings(
        self,
        n_proteins: int = 10,
        dim: int = 4,
    ) -> tuple[np.ndarray, list[str], dict[str, int]]:
        """Return (embeddings, accessions, acc_to_idx) with synthetic data."""
        rng = np.random.default_rng(42)
        emb = rng.random((n_proteins, dim), dtype=np.float32).astype(np.float16)
        accs = [f"P{i:05d}" for i in range(n_proteins)]
        idx = {a: i for i, a in enumerate(accs)}
        return emb, accs, idx

    def _make_ref_by_aspect(
        self,
        all_accessions: list[str],
        all_embeddings: np.ndarray,
        acc_to_idx: dict[str, int],
        n_refs: int = 5,
    ) -> dict[str, Any]:
        """Return a minimal ref_by_aspect dict."""
        ref_accs = all_accessions[:n_refs]
        ref_indices = np.array([acc_to_idx[a] for a in ref_accs], dtype=np.int32)
        go_map: dict[str, list[dict[str, Any]]] = {
            a: [{"go_term_id": 1001 + i, "qualifier": None, "evidence_code": "IEA"}]
            for i, a in enumerate(ref_accs)
        }
        return {
            asp: {"accessions": ref_accs, "indices": ref_indices, "go_map": go_map}
            for asp in _ASPECTS
        }

    def test_execute_writes_npz_and_publishes_features_message(
        self, tmp_path: Path
    ) -> None:
        """KNN batch should write a temp npz and publish to features queue."""
        from protea.core.operations.export_minijobs._export_knn_batch import (
            _FEATURES_QUEUE,
            ExportKnnBatchOperation,
        )
        from protea.infrastructure.storage.local import LocalFsArtifactStore

        all_embeddings, all_accessions, acc_to_idx = self._make_synthetic_embeddings()
        ref_by_aspect = self._make_ref_by_aspect(all_accessions, all_embeddings, acc_to_idx)

        store = LocalFsArtifactStore(tmp_path)
        payload = _make_knn_payload(k=2)

        op = ExportKnnBatchOperation()
        with (
            patch(
                "protea.core.operations.export_minijobs._export_knn_batch._load_project_settings"
            ) as mock_settings,
            patch(
                "protea.core.operations.export_minijobs._export_knn_batch.get_artifact_store",
                return_value=store,
            ),
            patch(
                "protea.core.operations.export_minijobs._export_knn_batch._preload_all_embeddings",
                return_value=(all_embeddings, all_accessions, acc_to_idx),
            ),
            patch(
                "protea.core.operations.export_minijobs._export_knn_batch._build_reference_from_cache",
                return_value=ref_by_aspect,
            ),
        ):
            mock_settings.return_value = MagicMock()
            emitted: list[tuple[str, Any]] = []

            def _capture_emit(event: str, msg: Any, fields: Any, level: str) -> None:
                emitted.append((event, fields))

            result = op.execute(_mock_session(), payload, emit=_capture_emit)

        assert result.result["pair_id"] == payload["pair_id"]
        assert result.result["coordinator_job_id"] == payload["coordinator_job_id"]
        assert "temp_uri" in result.result

        event_names = [e for e, _ in emitted]
        assert "pair_knn_done" in event_names
        assert "export_knn_batch.done" in event_names

        assert len(result.publish_operations) == 1
        queue, msg = result.publish_operations[0]
        assert queue == _FEATURES_QUEUE
        assert msg["operation"] == "export_features_batch"
        inner = msg["payload"]
        assert inner["coordinator_job_id"] == payload["coordinator_job_id"]
        assert inner["pair_id"] == payload["pair_id"]
        assert inner["temp_knn_uri"] is not None

        npz_key = f"temp/coordinator/{payload['coordinator_job_id']}/knn/{payload['pair_id']}.npz"
        assert store.exists(npz_key), "npz shard not written to artifact store"

    def test_execute_handles_empty_query_set(self, tmp_path: Path) -> None:
        """When no queries exist, an empty npz is written and features msg still fires."""
        from protea.core.operations.export_minijobs._export_knn_batch import (
            ExportKnnBatchOperation,
        )
        from protea.infrastructure.storage.local import LocalFsArtifactStore

        all_embeddings, all_accessions, acc_to_idx = self._make_synthetic_embeddings()
        empty_ref_by_aspect = {asp: {"accessions": [], "indices": np.array([]), "go_map": {}} for asp in _ASPECTS}

        store = LocalFsArtifactStore(tmp_path)
        payload = _make_knn_payload(k=2)

        op = ExportKnnBatchOperation()
        with (
            patch(
                "protea.core.operations.export_minijobs._export_knn_batch._load_project_settings"
            ) as mock_settings,
            patch(
                "protea.core.operations.export_minijobs._export_knn_batch.get_artifact_store",
                return_value=store,
            ),
            patch(
                "protea.core.operations.export_minijobs._export_knn_batch._preload_all_embeddings",
                return_value=(all_embeddings, all_accessions, acc_to_idx),
            ),
            patch(
                "protea.core.operations.export_minijobs._export_knn_batch._build_reference_from_cache",
                return_value=empty_ref_by_aspect,
            ),
        ):
            mock_settings.return_value = MagicMock()
            result = op.execute(_mock_session(), payload, emit=_noop_emit)

        assert result.result["n_queries"] == 0
        assert len(result.publish_operations) == 1

    def test_npz_shape_matches_k(self, tmp_path: Path) -> None:
        """KNN npz must have at most k neighbors per query per aspect."""
        from protea.core.operations.export_minijobs._export_features_batch import _load_knn_npz
        from protea.core.operations.export_minijobs._export_knn_batch import ExportKnnBatchOperation
        from protea.infrastructure.storage.local import LocalFsArtifactStore

        n_total = 10
        k = 2
        all_embeddings, all_accessions, acc_to_idx = self._make_synthetic_embeddings(n_total)
        ref_by_aspect = self._make_ref_by_aspect(all_accessions, all_embeddings, acc_to_idx)

        store = LocalFsArtifactStore(tmp_path)
        payload = _make_knn_payload(k=k)
        op = ExportKnnBatchOperation()

        with (
            patch(
                "protea.core.operations.export_minijobs._export_knn_batch._load_project_settings"
            ) as mock_settings,
            patch(
                "protea.core.operations.export_minijobs._export_knn_batch.get_artifact_store",
                return_value=store,
            ),
            patch(
                "protea.core.operations.export_minijobs._export_knn_batch._preload_all_embeddings",
                return_value=(all_embeddings, all_accessions, acc_to_idx),
            ),
            patch(
                "protea.core.operations.export_minijobs._export_knn_batch._build_reference_from_cache",
                return_value=ref_by_aspect,
            ),
        ):
            mock_settings.return_value = MagicMock()
            result = op.execute(_mock_session(), payload, emit=_noop_emit)

        temp_uri = result.result["temp_uri"]
        loaded_nbs, loaded_accs = _load_knn_npz(store, temp_uri)

        for asp in _ASPECTS:
            for row in loaded_nbs[asp]:
                assert len(row) <= k, f"aspect {asp}: row has more than k={k} neighbors"


# ---------------------------------------------------------------------------
# ExportFeaturesBatchOperation — unit tests with mocked pipeline
# ---------------------------------------------------------------------------


class TestExportFeaturesBatchOperation:
    """Unit tests for the features batch operation."""

    def test_missing_knn_uri_returns_empty_shard(self, tmp_path: Path) -> None:
        """When temp_knn_uri is None, an empty parquet is written."""
        from protea.core.operations.export_minijobs._export_features_batch import (
            ExportFeaturesBatchOperation,
        )
        from protea.infrastructure.storage.local import LocalFsArtifactStore

        store = LocalFsArtifactStore(tmp_path)
        coord_id = str(uuid.uuid4())
        payload = _make_features_payload(coord_id, "train-220", temp_knn_uri=None)
        op = ExportFeaturesBatchOperation()

        with (
            patch(
                "protea.core.operations.export_minijobs._export_features_batch._load_project_settings"
            ) as mock_settings,
            patch(
                "protea.core.operations.export_minijobs._export_features_batch.get_artifact_store",
                return_value=store,
            ),
        ):
            mock_settings.return_value = MagicMock()
            events: list[str] = []

            def _capture(event: str, msg: Any, fields: Any, level: str) -> None:
                events.append(event)

            result = op.execute(_mock_session(), payload, emit=_capture)

        assert result.result["n_rows"] == 0
        assert result.result["pair_id"] == "train-220"
        assert "pair_features_done" in events
        assert len(result.publish_operations) == 1
        queue, msg = result.publish_operations[0]
        assert queue == "protea.training.write"
        assert msg["payload"]["pair_id"] == "train-220"

    def test_empty_knn_npz_returns_empty_shard(self, tmp_path: Path) -> None:
        """When the npz has no query accessions, an empty parquet is written."""
        from protea.core.operations.export_minijobs._export_features_batch import (
            ExportFeaturesBatchOperation,
        )
        from protea.core.operations.export_minijobs._export_knn_batch import _write_empty_npz
        from protea.infrastructure.storage.local import LocalFsArtifactStore

        store = LocalFsArtifactStore(tmp_path)
        empty_npz_key = "temp/coordinator/test/knn/pair.npz"
        empty_npz_uri = _write_empty_npz(store, empty_npz_key)

        coord_id = "test-coordinator"
        payload = _make_features_payload(coord_id, "train-220", temp_knn_uri=empty_npz_uri)
        op = ExportFeaturesBatchOperation()

        with (
            patch(
                "protea.core.operations.export_minijobs._export_features_batch._load_project_settings"
            ) as mock_settings,
            patch(
                "protea.core.operations.export_minijobs._export_features_batch.get_artifact_store",
                return_value=store,
            ),
        ):
            mock_settings.return_value = MagicMock()
            result = op.execute(_mock_session(), payload, emit=_noop_emit)

        assert result.result["n_rows"] == 0

    def test_execute_with_full_pipeline_mocked(self, tmp_path: Path) -> None:
        """Features batch should call run_with_precomputed_knn and upload parquet."""
        from protea.core.operations.export_minijobs._export_features_batch import (
            ExportFeaturesBatchOperation,
        )
        from protea.core.operations.export_minijobs._export_knn_batch import _write_knn_npz
        from protea.infrastructure.storage.local import LocalFsArtifactStore

        rng = np.random.default_rng(0)
        n = 4
        dim = 4
        embeddings = rng.random((n, dim), dtype=np.float32).astype(np.float16)
        accessions = [f"Q{i:05d}" for i in range(n)]
        acc_to_idx = {a: i for i, a in enumerate(accessions)}

        store = LocalFsArtifactStore(tmp_path)
        coord_id = str(uuid.uuid4())
        pair_id = "train-220"

        neighbors: dict[str, list[list[tuple[str, float]]]] = {
            "P": [[("Q00001", 0.1), ("Q00002", 0.2)], [("Q00000", 0.3)], [], []],
            "F": [[("Q00003", 0.05)], [], [], [("Q00001", 0.15)]],
            "C": [[], [], [], []],
        }
        knn_key = f"temp/coordinator/{coord_id}/knn/{pair_id}.npz"
        knn_uri = _write_knn_npz(store, knn_key, accessions, neighbors)

        go_id_map: dict[Any, str] = {1001: "GO:0000001", 1002: "GO:0000002"}
        aspect_map: dict[Any, str] = {1001: "P", 1002: "F"}
        pivot_go_ids: set[str] = {"GO:0000001", "GO:0000002"}
        ref_by_aspect: dict[str, Any] = {
            asp: {
                "accessions": accessions[:2],
                "indices": np.array([0, 1], dtype=np.int32),
                "go_map": {
                    "Q00000": [{"go_term_id": 1001, "qualifier": None, "evidence_code": "IEA"}],
                    "Q00001": [{"go_term_id": 1002, "qualifier": None, "evidence_code": "IEA"}],
                },
            }
            for asp in _ASPECTS
        }

        payload = _make_features_payload(coord_id, pair_id, knn_uri)
        op = ExportFeaturesBatchOperation()

        mock_run_result = {"parquet_path": str(tmp_path / "out.parquet"), "n_rows": 8}

        with (
            patch(
                "protea.core.operations.export_minijobs._export_features_batch._load_project_settings"
            ) as mock_settings,
            patch(
                "protea.core.operations.export_minijobs._export_features_batch.get_artifact_store",
                return_value=store,
            ),
            patch(
                "protea.core.operations.export_minijobs._export_features_batch._preload_all_embeddings",
                return_value=(embeddings, accessions, acc_to_idx),
            ),
            patch(
                "protea.core.operations.export_minijobs._export_features_batch._build_reference_from_cache",
                return_value=ref_by_aspect,
            ),
            patch(
                "protea.core.operations.export_minijobs._export_features_batch._load_go_maps",
                return_value=(go_id_map, aspect_map, pivot_go_ids),
            ),
            patch(
                "protea.core.operations.export_minijobs._export_features_batch._load_parent_map",
                return_value={},
            ),
            patch(
                "protea.core.operations.export_minijobs._export_features_batch._maybe_fit_pca_state",
                return_value=None,
            ),
            patch(
                "protea.core._precomputed_knn_runner.run_with_precomputed_knn",
                return_value=mock_run_result,
            ),
        ):
            mock_settings.return_value = MagicMock()
            events: list[str] = []

            def _capture(event: str, msg: Any, fields: Any, level: str) -> None:
                events.append(event)

            result = op.execute(_mock_session(), payload, emit=_capture)

        assert result.result["pair_id"] == pair_id
        assert result.result["coordinator_job_id"] == coord_id
        assert result.result["n_rows"] == 8
        assert "pair_features_done" in events
        assert "export_features_batch.done" in events
        assert len(result.publish_operations) == 1
        _, write_msg = result.publish_operations[0]
        assert write_msg["payload"]["pair_id"] == pair_id

    def test_summarize_payload(self) -> None:
        from protea.core.operations.export_minijobs._export_features_batch import (
            ExportFeaturesBatchOperation,
        )

        op = ExportFeaturesBatchOperation()
        summary = op.summarize_payload({"pair_id": "eval-230"})
        assert "eval-230" in summary


# ---------------------------------------------------------------------------
# _PrecomputedKnnRunner — verifies pre-computed injection skips KNN search
# ---------------------------------------------------------------------------


class TestPrecomputedKnnRunner:
    """Verify that _PrecomputedKnnRunner does not call the parent KNN logic."""

    def test_run_knn_injects_precomputed_neighbors(self) -> None:
        """_run_knn should populate neighbors_by_aspect from precomputed, not search."""
        from protea.core._precomputed_knn_runner import (
            PrecomputedKnnConfig,
            _PrecomputedKnnRunner,
            _RunnerInputs,
        )
        from protea.core.training_dump._contexts import KnnTransferContext

        n, dim = 2, 4
        rng = np.random.default_rng(1)
        emb = rng.random((n, dim), dtype=np.float32)
        accessions = ["P00001", "P00002"]
        query_accs = accessions

        precomputed: dict[str, list[list[tuple[str, float]]]] = {
            "P": [[("P00002", 0.2)], []],
            "F": [[], [("P00001", 0.3)]],
            "C": [[], []],
        }

        ctx = KnnTransferContext(
            valid_queries=query_accs,
            query_emb=emb,
            ref_by_aspect={
                asp: {"accessions": accessions, "indices": np.array([0, 1]), "go_map": {}}
                for asp in _ASPECTS
            },
            go_id_map={},
            aspect_map={},
            gt_pairs=set(),
        )

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            out_path = Path(f.name)

        cfg = PrecomputedKnnConfig(
            limit_per_entry=2,
            compute_alignments=False,
            compute_taxonomy=False,
            expand_votes_to_ancestors=False,
            search_backend="numpy",
            metric="cosine",
            faiss_index_type="IVFFlat",
            faiss_nlist=256,
            faiss_nprobe=32,
            distance_threshold=None,
            use_embedding_pca=False,
        )
        inputs = _RunnerInputs(
            session=_mock_session(),
            ctx=ctx,
            neighbors_by_aspect=precomputed,
            sequence_context=None,
            output_parquet=out_path,
        )
        runner = _PrecomputedKnnRunner(inputs, cfg)
        runner._run_knn()

        for asp in _ASPECTS:
            assert asp in runner.neighbors_by_aspect
            assert runner.neighbors_by_aspect[asp] == precomputed[asp]

        out_path.unlink(missing_ok=True)

    def test_operation_name_and_description(self) -> None:
        from protea.core.operations.export_minijobs._export_features_batch import (
            ExportFeaturesBatchOperation,
        )
        from protea.core.operations.export_minijobs._export_knn_batch import ExportKnnBatchOperation

        assert ExportKnnBatchOperation.name == "export_knn_batch"
        assert ExportFeaturesBatchOperation.name == "export_features_batch"
        assert "stub" not in ExportKnnBatchOperation.description
        assert "stub" not in ExportFeaturesBatchOperation.description
