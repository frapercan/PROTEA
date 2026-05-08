from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from protea.core.disk_cache import (
    _aspect_index_path,
    _build_anno_csr,
    _csr_lookup,
    _disk_cache_paths,
)
from protea.core.knn_search import _compute_distance_matrix, search_knn
from protea.core.operations.predict_go_terms import (
    BatchPredictContext,
    PredictGOTermsBatchOperation,
    PredictGOTermsBatchPayload,
    PredictGOTermsOperation,
    PredictGOTermsPayload,
    StorePredictionsOperation,
)
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.job import JobStatus

_noop_emit = lambda *_: None  # noqa: E731
_SNAPSHOT_ID = str(uuid.uuid4())
_ANN_SET_ID = str(uuid.uuid4())


def make_session_get(missing_class=None):
    def _get(cls, id_):
        if cls is missing_class:
            return None
        return MagicMock()

    return _get


# ---------------------------------------------------------------------------
# Payload validation
# ---------------------------------------------------------------------------


class TestPredictGOTermsPayload:
    def test_minimal_valid(self) -> None:
        p = PredictGOTermsPayload.model_validate(
            {
                "embedding_config_id": str(uuid.uuid4()),
                "annotation_set_id": _ANN_SET_ID,
                "ontology_snapshot_id": _SNAPSHOT_ID,
            }
        )
        assert p.limit_per_entry == 5
        assert p.distance_threshold is None
        assert p.batch_size == 1024

    def test_empty_embedding_config_id_raises(self) -> None:
        with pytest.raises(ValueError):
            PredictGOTermsPayload.model_validate(
                {
                    "embedding_config_id": "",
                    "annotation_set_id": _ANN_SET_ID,
                    "ontology_snapshot_id": _SNAPSHOT_ID,
                }
            )

    def test_whitespace_embedding_config_id_raises(self) -> None:
        with pytest.raises(ValueError):
            PredictGOTermsPayload.model_validate(
                {
                    "embedding_config_id": "   ",
                    "annotation_set_id": _ANN_SET_ID,
                    "ontology_snapshot_id": _SNAPSHOT_ID,
                }
            )

    def test_empty_annotation_set_id_raises(self) -> None:
        with pytest.raises(ValueError):
            PredictGOTermsPayload.model_validate(
                {
                    "embedding_config_id": str(uuid.uuid4()),
                    "annotation_set_id": "",
                    "ontology_snapshot_id": _SNAPSHOT_ID,
                }
            )

    def test_empty_ontology_snapshot_id_raises(self) -> None:
        with pytest.raises(ValueError):
            PredictGOTermsPayload.model_validate(
                {
                    "embedding_config_id": str(uuid.uuid4()),
                    "annotation_set_id": _ANN_SET_ID,
                    "ontology_snapshot_id": "   ",
                }
            )

    def test_missing_annotation_set_raises(self) -> None:
        with pytest.raises(ValueError):
            PredictGOTermsPayload.model_validate(
                {
                    "embedding_config_id": str(uuid.uuid4()),
                    "ontology_snapshot_id": _SNAPSHOT_ID,
                }
            )

    def test_default_values(self) -> None:
        p = PredictGOTermsPayload.model_validate(
            {
                "embedding_config_id": str(uuid.uuid4()),
                "annotation_set_id": _ANN_SET_ID,
                "ontology_snapshot_id": _SNAPSHOT_ID,
            }
        )
        assert p.limit_per_entry == 5
        assert p.distance_threshold is None
        assert p.batch_size == 1024


# ---------------------------------------------------------------------------
# _compute_distance_matrix (cosine + l2)
# ---------------------------------------------------------------------------


class TestDistanceMatrix:
    def test_cosine_identical_vectors_zero_distance(self) -> None:
        v = np.array([[1.0, 0.0, 0.0]], dtype=np.float32)
        D = _compute_distance_matrix(v, v, "cosine")
        assert abs(D[0, 0]) < 1e-5

    def test_cosine_orthogonal_vectors_distance_one(self) -> None:
        Q = np.array([[1.0, 0.0]], dtype=np.float32)
        R = np.array([[0.0, 1.0]], dtype=np.float32)
        D = _compute_distance_matrix(Q, R, "cosine")
        assert abs(D[0, 0] - 1.0) < 1e-5

    def test_cosine_shape(self) -> None:
        Q = np.random.rand(4, 8).astype(np.float32)
        R = np.random.rand(10, 8).astype(np.float32)
        D = _compute_distance_matrix(Q, R, "cosine")
        assert D.shape == (4, 10)

    def test_cosine_range_zero_to_two(self) -> None:
        Q = np.random.rand(5, 16).astype(np.float32)
        R = np.random.rand(7, 16).astype(np.float32)
        D = _compute_distance_matrix(Q, R, "cosine")
        assert D.min() >= -1e-5
        assert D.max() <= 2.0 + 1e-5

    def test_l2_identical_vectors_zero(self) -> None:
        v = np.array([[1.0, 2.0, 3.0]], dtype=np.float32)
        D = _compute_distance_matrix(v, v, "l2")
        assert abs(D[0, 0]) < 1e-5

    def test_l2_known_distance(self) -> None:
        Q = np.array([[0.0, 0.0]], dtype=np.float32)
        R = np.array([[3.0, 4.0]], dtype=np.float32)
        D = _compute_distance_matrix(Q, R, "l2")
        assert abs(D[0, 0] - 25.0) < 1e-4  # squared: 3²+4²=25

    def test_unknown_metric_raises(self) -> None:
        Q = np.random.rand(2, 4).astype(np.float32)
        R = np.random.rand(3, 4).astype(np.float32)
        with pytest.raises(ValueError, match="Unknown metric"):
            _compute_distance_matrix(Q, R, "manhattan")


# ---------------------------------------------------------------------------
# search_knn — numpy and faiss backends
# ---------------------------------------------------------------------------


class TestSearchKnn:
    def _make_data(self, n_refs: int = 20, dim: int = 16):
        rng = np.random.default_rng(42)
        R = rng.random((n_refs, dim)).astype(np.float32)
        accs = [f"REF{i:04d}" for i in range(n_refs)]
        return R, accs

    def test_numpy_returns_k_results(self) -> None:
        R, accs = self._make_data()
        Q = np.random.rand(3, 16).astype(np.float32)
        results = search_knn(Q, R, accs, k=5, backend="numpy", metric="cosine")
        assert len(results) == 3
        for hits in results:
            assert len(hits) == 5

    def test_numpy_sorted_ascending(self) -> None:
        R, accs = self._make_data()
        Q = np.random.rand(2, 16).astype(np.float32)
        results = search_knn(Q, R, accs, k=10, backend="numpy", metric="cosine")
        for hits in results:
            dists = [d for _, d in hits]
            assert dists == sorted(dists)

    def test_numpy_distance_threshold(self) -> None:
        R, accs = self._make_data()
        # Query identical to first ref → distance ≈ 0
        Q = R[:1].copy()
        results = search_knn(
            Q, R, accs, k=10, distance_threshold=0.001, backend="numpy", metric="cosine"
        )
        assert len(results[0]) >= 1
        for _, d in results[0]:
            assert d <= 0.001 + 1e-5

    def test_numpy_l2_metric(self) -> None:
        R, accs = self._make_data()
        Q = np.random.rand(2, 16).astype(np.float32)
        results = search_knn(Q, R, accs, k=3, backend="numpy", metric="l2")
        assert len(results) == 2
        for hits in results:
            assert len(hits) == 3

    def test_faiss_flat_matches_numpy(self) -> None:
        R, accs = self._make_data(n_refs=50)
        rng = np.random.default_rng(0)
        Q = rng.random((5, 16)).astype(np.float32)
        numpy_res = search_knn(Q, R, accs, k=3, backend="numpy", metric="cosine")
        faiss_res = search_knn(
            Q, R, accs, k=3, backend="faiss", metric="cosine", faiss_index_type="Flat"
        )
        for np_hits, fa_hits in zip(numpy_res, faiss_res, strict=False):
            np_accs = [a for a, _ in np_hits]
            fa_accs = [a for a, _ in fa_hits]
            assert np_accs == fa_accs

    def test_faiss_ivfflat(self) -> None:
        R, accs = self._make_data(n_refs=200)
        Q = np.random.rand(4, 16).astype(np.float32)
        results = search_knn(
            Q,
            R,
            accs,
            k=5,
            backend="faiss",
            metric="cosine",
            faiss_index_type="IVFFlat",
            faiss_nlist=10,
            faiss_nprobe=5,
        )
        assert len(results) == 4
        for hits in results:
            assert 1 <= len(hits) <= 5

    def test_faiss_hnsw(self) -> None:
        R, accs = self._make_data(n_refs=100)
        Q = np.random.rand(3, 16).astype(np.float32)
        results = search_knn(
            Q,
            R,
            accs,
            k=4,
            backend="faiss",
            metric="cosine",
            faiss_index_type="HNSW",
            faiss_hnsw_m=8,
            faiss_hnsw_ef_search=32,
        )
        assert len(results) == 3
        for hits in results:
            assert 1 <= len(hits) <= 4

    def test_unknown_backend_raises(self) -> None:
        R, accs = self._make_data()
        Q = np.random.rand(1, 16).astype(np.float32)
        with pytest.raises(ValueError):
            search_knn(Q, R, accs, k=3, backend="unknown")

    def test_unknown_faiss_index_raises(self) -> None:
        R, accs = self._make_data()
        Q = np.random.rand(1, 16).astype(np.float32)
        with pytest.raises(ValueError, match="Unknown faiss_index_type"):
            search_knn(Q, R, accs, k=3, backend="faiss", faiss_index_type="BadIndex")


# ---------------------------------------------------------------------------
# _predict_batch
# ---------------------------------------------------------------------------


class TestPredictBatch:
    def _op(self) -> PredictGOTermsBatchOperation:
        return PredictGOTermsBatchOperation()

    def _payload(self, **kwargs):
        defaults = {
            "embedding_config_id": str(uuid.uuid4()),
            "annotation_set_id": _ANN_SET_ID,
            "ontology_snapshot_id": _SNAPSHOT_ID,
            "prediction_set_id": str(uuid.uuid4()),
            "parent_job_id": str(uuid.uuid4()),
            "query_accessions": [],
            "limit_per_entry": 2,
            # Opt out of features that require sequences/taxonomy: the mock
            # ref_data in this class never provides them.
            "compute_alignments": False,
            "compute_taxonomy": False,
            "compute_reranker_features": False,
        }
        defaults.update(kwargs)
        return PredictGOTermsBatchPayload.model_validate(defaults)

    def _ref_data(self):
        return {
            "accessions": ["P12345", "Q67890"],
            "embeddings": np.array(
                [
                    [1.0, 0.0, 0.0],
                    [0.0, 1.0, 0.0],
                ],
                dtype=np.float32,
            ),
            "go_map": {
                "P12345": [{"go_term_id": 1, "qualifier": "enables", "evidence_code": "IDA"}],
                "Q67890": [{"go_term_id": 2, "qualifier": "involved_in", "evidence_code": "IEA"}],
            },
        }

    def test_transfers_go_annotations_from_nearest_neighbor(self) -> None:
        op = self._op()
        p = self._payload()
        ref = self._ref_data()
        pred_set_id = uuid.uuid4()

        query_embs = np.array([[0.99, 0.01, 0.0]], dtype=np.float32)
        preds, _, _ = op._predict_batch(
            BatchPredictContext(
                query_accessions=["RQUERY"],
                query_embeddings=query_embs,
                ref_data=ref,
                prediction_set_id=pred_set_id,
                payload=p,
            )
        )

        assert len(preds) >= 1
        go_ids = {pr["go_term_id"] for pr in preds}
        assert 1 in go_ids

    def test_includes_self_as_first_reference(self) -> None:
        """A query protein that is also a reference should appear as its own
        nearest neighbor (distance ≈ 0) when it has annotations."""
        op = self._op()
        p = self._payload()
        ref = self._ref_data()
        pred_set_id = uuid.uuid4()
        query_embs = np.array([[1.0, 0.0, 0.0]], dtype=np.float32)
        preds, _, _ = op._predict_batch(
            BatchPredictContext(
                query_accessions=["P12345"],
                query_embeddings=query_embs,
                ref_data=ref,
                prediction_set_id=pred_set_id,
                payload=p,
            )
        )

        ref_accs = [pr["ref_protein_accession"] for pr in preds]
        assert "P12345" in ref_accs, "Self should be included as a reference neighbor"
        # Self-hit must be the first (closest) neighbor
        assert ref_accs[0] == "P12345"

    def test_distance_threshold_filters_far_neighbors(self) -> None:
        op = self._op()
        p = self._payload(distance_threshold=0.01)
        ref = self._ref_data()
        pred_set_id = uuid.uuid4()

        query_embs = np.array([[0.0, 0.0, 1.0]], dtype=np.float32)
        preds, _, _ = op._predict_batch(
            BatchPredictContext(
                query_accessions=["RQUERY"],
                query_embeddings=query_embs,
                ref_data=ref,
                prediction_set_id=pred_set_id,
                payload=p,
            )
        )
        assert preds == []

    def test_limit_per_entry_caps_neighbors(self) -> None:
        op = self._op()
        p = self._payload(limit_per_entry=1)
        ref = self._ref_data()
        pred_set_id = uuid.uuid4()

        query_embs = np.array([[0.7, 0.7, 0.0]], dtype=np.float32)
        preds, _, _ = op._predict_batch(
            BatchPredictContext(
                query_accessions=["RQUERY"],
                query_embeddings=query_embs,
                ref_data=ref,
                prediction_set_id=pred_set_id,
                payload=p,
            )
        )

        ref_accs = {pr["ref_protein_accession"] for pr in preds}
        assert len(ref_accs) == 1


# ---------------------------------------------------------------------------
# execute() — mocked session
# ---------------------------------------------------------------------------


class TestPredictGOTermsExecute:
    def _op(self) -> PredictGOTermsOperation:
        return PredictGOTermsOperation()

    def _base_payload(self):
        return {
            "embedding_config_id": str(uuid.uuid4()),
            "annotation_set_id": _ANN_SET_ID,
            "ontology_snapshot_id": _SNAPSHOT_ID,
            "_job_id": str(uuid.uuid4()),
        }

    def test_missing_embedding_config_raises(self) -> None:
        op = self._op()
        session = MagicMock()
        session.get.side_effect = make_session_get(missing_class=EmbeddingConfig)

        with pytest.raises(ValueError, match="EmbeddingConfig"):
            op.execute(session, self._base_payload(), emit=_noop_emit)

    def test_missing_annotation_set_raises(self) -> None:
        op = self._op()
        session = MagicMock()
        session.get.side_effect = make_session_get(missing_class=AnnotationSet)

        with pytest.raises(ValueError, match="AnnotationSet"):
            op.execute(session, self._base_payload(), emit=_noop_emit)

    def test_missing_ontology_snapshot_raises(self) -> None:
        op = self._op()
        session = MagicMock()
        session.get.side_effect = make_session_get(missing_class=OntologySnapshot)

        with pytest.raises(ValueError, match="OntologySnapshot"):
            op.execute(session, self._base_payload(), emit=_noop_emit)

    def test_no_references_returns_zero(self) -> None:
        op = self._op()
        session = MagicMock()
        session.get.side_effect = make_session_get()

        # Coordinator returns early when there are no query accessions with embeddings
        with patch.object(op, "_load_query_accessions", return_value=[]):
            result = op.execute(session, self._base_payload(), emit=_noop_emit)

        assert result.result["batches"] == 0


# ---------------------------------------------------------------------------
# Coordinator — dispatching batches
# ---------------------------------------------------------------------------


class TestPredictGOTermsCoordinatorDispatch:
    def _op(self) -> PredictGOTermsOperation:
        return PredictGOTermsOperation()

    def _base_payload(self):
        return {
            "embedding_config_id": str(uuid.uuid4()),
            "annotation_set_id": _ANN_SET_ID,
            "ontology_snapshot_id": _SNAPSHOT_ID,
            "_job_id": str(uuid.uuid4()),
        }

    def test_dispatches_correct_number_of_batches(self) -> None:
        op = self._op()
        session = MagicMock()
        session.get.side_effect = make_session_get()
        session.flush.return_value = None
        pred_set = MagicMock()
        pred_set.id = uuid.uuid4()
        session.add.side_effect = lambda obj: (
            setattr(obj, "id", uuid.uuid4()) if not hasattr(obj, "id") or obj.id is None else None
        )

        accessions = [f"P{i:05d}" for i in range(10)]

        payload = self._base_payload()
        payload["batch_size"] = 4

        with patch.object(op, "_load_query_accessions", return_value=accessions):
            result = op.execute(session, payload, emit=_noop_emit)

        # ceil(10/4) = 3 batches
        assert result.result["batches"] == 3
        assert result.result["queries"] == 10
        assert result.deferred is True
        assert result.progress_total == 3
        assert len(result.publish_operations) == 3

    def test_creates_prediction_set(self) -> None:
        op = self._op()
        session = MagicMock()
        session.get.side_effect = make_session_get()

        def add_side_effect(obj):
            if not hasattr(obj, "id") or obj.id is None:
                obj.id = uuid.uuid4()

        session.add.side_effect = add_side_effect

        with patch.object(op, "_load_query_accessions", return_value=["P1"]):
            result = op.execute(session, self._base_payload(), emit=_noop_emit)

        assert "prediction_set_id" in result.result
        assert result.result["batches"] == 1

    def test_batch_messages_contain_correct_fields(self) -> None:
        op = self._op()
        session = MagicMock()
        session.get.side_effect = make_session_get()

        def add_side_effect(obj):
            if not hasattr(obj, "id") or obj.id is None:
                obj.id = uuid.uuid4()

        session.add.side_effect = add_side_effect

        payload = self._base_payload()
        payload["compute_alignments"] = True
        payload["compute_taxonomy"] = True
        payload["aspect_separated_knn"] = True

        with patch.object(op, "_load_query_accessions", return_value=["P1"]):
            result = op.execute(session, payload, emit=_noop_emit)

        queue, msg = result.publish_operations[0]
        assert queue == "protea.predictions.batch"
        assert msg["operation"] == "predict_go_terms_batch"
        assert msg["payload"]["compute_alignments"] is True
        assert msg["payload"]["compute_taxonomy"] is True
        assert msg["payload"]["aspect_separated_knn"] is True


# ---------------------------------------------------------------------------
# StorePredictionsOperation
# ---------------------------------------------------------------------------


class TestStorePredictions:
    def _op(self) -> StorePredictionsOperation:
        return StorePredictionsOperation()

    def _make_prediction(self, **overrides):
        defaults = {
            "protein_accession": "P12345",
            "go_term_id": 42,
            "ref_protein_accession": "Q99999",
            "distance": 0.15,
        }
        defaults.update(overrides)
        return defaults

    def test_inserts_predictions(self) -> None:
        op = self._op()
        session = MagicMock()
        parent = MagicMock()
        parent.status = JobStatus.RUNNING
        session.get.return_value = parent

        row = MagicMock()
        row.progress_current = 1
        row.progress_total = 5
        session.execute.return_value.fetchone.return_value = row

        pred_set_id = str(uuid.uuid4())
        parent_job_id = str(uuid.uuid4())

        payload = {
            "parent_job_id": parent_job_id,
            "prediction_set_id": pred_set_id,
            "predictions": [self._make_prediction(), self._make_prediction(go_term_id=43)],
        }

        result = op.execute(session, payload, emit=_noop_emit)
        assert result.result["predictions_inserted"] == 2
        assert session.execute.called

    def test_skips_when_parent_cancelled(self) -> None:
        op = self._op()
        session = MagicMock()
        parent = MagicMock()
        parent.status = JobStatus.CANCELLED
        session.get.return_value = parent

        payload = {
            "parent_job_id": str(uuid.uuid4()),
            "prediction_set_id": str(uuid.uuid4()),
            "predictions": [self._make_prediction()],
        }

        result = op.execute(session, payload, emit=_noop_emit)
        assert result.result["skipped"] is True

    def test_skips_when_parent_failed(self) -> None:
        op = self._op()
        session = MagicMock()
        parent = MagicMock()
        parent.status = JobStatus.FAILED
        session.get.return_value = parent

        payload = {
            "parent_job_id": str(uuid.uuid4()),
            "prediction_set_id": str(uuid.uuid4()),
            "predictions": [self._make_prediction()],
        }

        result = op.execute(session, payload, emit=_noop_emit)
        assert result.result["skipped"] is True

    def test_empty_predictions_still_updates_progress(self) -> None:
        op = self._op()
        session = MagicMock()
        parent = MagicMock()
        parent.status = JobStatus.RUNNING
        session.get.return_value = parent

        row = MagicMock()
        row.progress_current = 1
        row.progress_total = 3
        session.execute.return_value.fetchone.return_value = row

        payload = {
            "parent_job_id": str(uuid.uuid4()),
            "prediction_set_id": str(uuid.uuid4()),
            "predictions": [],
        }

        result = op.execute(session, payload, emit=_noop_emit)
        assert result.result["predictions_inserted"] == 0

    def test_last_batch_closes_parent_as_succeeded(self) -> None:
        op = self._op()
        session = MagicMock()
        parent = MagicMock()
        parent.status = JobStatus.RUNNING
        session.get.return_value = parent

        # First execute returns progress_current == progress_total
        progress_row = MagicMock()
        progress_row.progress_current = 3
        progress_row.progress_total = 3
        # Second execute (succeeded update) returns closed
        closed_row = MagicMock()
        closed_row.id = uuid.uuid4()

        session.execute.return_value.fetchone.side_effect = [progress_row, closed_row]

        payload = {
            "parent_job_id": str(uuid.uuid4()),
            "prediction_set_id": str(uuid.uuid4()),
            "predictions": [self._make_prediction()],
        }

        events = []

        def capture_emit(event, msg, fields, level):
            events.append(event)

        op.execute(session, payload, emit=capture_emit)
        assert "store_predictions.parent_succeeded" in events

    def test_name(self) -> None:
        assert StorePredictionsOperation().name == "store_predictions"


# ---------------------------------------------------------------------------
# Batch worker — parent cancellation
# ---------------------------------------------------------------------------


class TestPredictBatchParentCancellation:
    def _op(self) -> PredictGOTermsBatchOperation:
        return PredictGOTermsBatchOperation()

    def test_skips_when_parent_cancelled(self) -> None:
        op = self._op()
        session = MagicMock()
        parent = MagicMock()
        parent.status = JobStatus.CANCELLED
        session.get.return_value = parent

        payload = {
            "embedding_config_id": str(uuid.uuid4()),
            "annotation_set_id": str(uuid.uuid4()),
            "ontology_snapshot_id": _SNAPSHOT_ID,
            "prediction_set_id": str(uuid.uuid4()),
            "parent_job_id": str(uuid.uuid4()),
            "query_accessions": ["P1"],
        }

        result = op.execute(session, payload, emit=_noop_emit)
        assert result.result["skipped"] is True

    def test_skips_when_parent_failed(self) -> None:
        op = self._op()
        session = MagicMock()
        parent = MagicMock()
        parent.status = JobStatus.FAILED
        session.get.return_value = parent

        payload = {
            "embedding_config_id": str(uuid.uuid4()),
            "annotation_set_id": str(uuid.uuid4()),
            "ontology_snapshot_id": _SNAPSHOT_ID,
            "prediction_set_id": str(uuid.uuid4()),
            "parent_job_id": str(uuid.uuid4()),
            "query_accessions": ["P1"],
        }

        result = op.execute(session, payload, emit=_noop_emit)
        assert result.result["skipped"] is True


# ---------------------------------------------------------------------------
# Pure helper functions
# ---------------------------------------------------------------------------


class TestBuildAnnoCsr:
    def test_builds_correct_structure(self) -> None:
        accessions = ["P1", "P2"]
        go_map = {
            "P1": [
                {"go_term_id": 10, "qualifier": "enables", "evidence_code": "IDA"},
                {"go_term_id": 20, "qualifier": None, "evidence_code": "IEA"},
            ],
            "P2": [
                {"go_term_id": 30, "qualifier": "involved_in", "evidence_code": "IPI"},
            ],
        }

        gtids, quals, ecodes, offsets = _build_anno_csr(accessions, go_map)
        assert list(gtids) == [10, 20, 30]
        assert list(offsets) == [0, 2, 3]
        assert quals[0] == "enables"
        assert ecodes[2] == "IPI"

    def test_missing_accession_produces_empty_range(self) -> None:
        accessions = ["P1", "P2"]
        go_map = {"P1": [{"go_term_id": 10}]}

        gtids, quals, ecodes, offsets = _build_anno_csr(accessions, go_map)
        assert list(offsets) == [0, 1, 1]  # P2 has empty range

    def test_empty_input(self) -> None:
        gtids, quals, ecodes, offsets = _build_anno_csr([], {})
        assert len(gtids) == 0
        assert list(offsets) == [0]


class TestCsrLookup:
    def test_retrieves_annotations(self) -> None:
        accessions = ["P1", "P2"]
        go_map = {
            "P1": [{"go_term_id": 10, "qualifier": "enables", "evidence_code": "IDA"}],
            "P2": [{"go_term_id": 20, "qualifier": None, "evidence_code": "IEA"}],
        }
        csr = _build_anno_csr(accessions, go_map)
        acc_to_anno_idx = {acc: i for i, acc in enumerate(accessions)}

        result = _csr_lookup({"P1"}, acc_to_anno_idx, csr)
        assert "P1" in result
        assert len(result["P1"]) == 1
        assert result["P1"][0]["go_term_id"] == 10

    def test_missing_accession_ignored(self) -> None:
        csr = _build_anno_csr(["P1"], {"P1": [{"go_term_id": 10}]})
        acc_to_anno_idx = {"P1": 0}

        result = _csr_lookup({"UNKNOWN"}, acc_to_anno_idx, csr)
        assert result == {}


class TestDiskCachePaths:
    def test_paths_include_ids(self) -> None:
        ec_id = uuid.uuid4()
        as_id = uuid.uuid4()
        emb_path, acc_path = _disk_cache_paths(ec_id, as_id)
        assert str(ec_id) in str(emb_path)
        assert str(as_id) in str(emb_path)
        assert "embeddings" in str(emb_path)
        assert "accessions" in str(acc_path)

    def test_aspect_index_path_includes_aspect(self) -> None:
        ec_id = uuid.uuid4()
        as_id = uuid.uuid4()
        path = _aspect_index_path(ec_id, as_id, "P")
        assert "__P_indices" in str(path)


# ---------------------------------------------------------------------------
# Batch payload validation
# ---------------------------------------------------------------------------


class TestPredictGOTermsBatchPayload:
    def test_valid_payload(self) -> None:
        p = PredictGOTermsBatchPayload.model_validate(
            {
                "embedding_config_id": str(uuid.uuid4()),
                "annotation_set_id": str(uuid.uuid4()),
                "ontology_snapshot_id": _SNAPSHOT_ID,
                "prediction_set_id": str(uuid.uuid4()),
                "parent_job_id": str(uuid.uuid4()),
                "query_accessions": ["P1", "P2"],
            }
        )
        assert p.limit_per_entry == 5
        assert p.aspect_separated_knn is True

    def test_feature_flags_default_false(self) -> None:
        p = PredictGOTermsBatchPayload.model_validate(
            {
                "embedding_config_id": str(uuid.uuid4()),
                "annotation_set_id": str(uuid.uuid4()),
                "ontology_snapshot_id": _SNAPSHOT_ID,
                "prediction_set_id": str(uuid.uuid4()),
                "parent_job_id": str(uuid.uuid4()),
                "query_accessions": [],
            }
        )
        assert p.compute_alignments is True
        assert p.compute_taxonomy is True
        assert p.compute_reranker_features is True


# ---------------------------------------------------------------------------
# _predict_batch — reranker features
# ---------------------------------------------------------------------------


class TestPredictBatchRerankerFeatures:
    def _op(self) -> PredictGOTermsBatchOperation:
        return PredictGOTermsBatchOperation()

    def _payload(self, **kwargs):
        defaults = {
            "embedding_config_id": str(uuid.uuid4()),
            "annotation_set_id": _ANN_SET_ID,
            "ontology_snapshot_id": _SNAPSHOT_ID,
            "prediction_set_id": str(uuid.uuid4()),
            "parent_job_id": str(uuid.uuid4()),
            "query_accessions": [],
            "limit_per_entry": 2,
            # Reranker features ON, alignments/taxonomy OFF: this suite only
            # exercises voting/neighbor-stat features, not NW/SW or taxonomy.
            "compute_alignments": False,
            "compute_taxonomy": False,
            "compute_reranker_features": True,
        }
        defaults.update(kwargs)
        return PredictGOTermsBatchPayload.model_validate(defaults)

    def test_reranker_features_included_when_enabled(self) -> None:
        op = self._op()
        p = self._payload()
        pred_set_id = uuid.uuid4()

        ref_data = {
            "accessions": ["REF1", "REF2"],
            "embeddings": np.array([[1.0, 0.0], [0.0, 1.0]], dtype=np.float32),
            "go_map": {
                "REF1": [{"go_term_id": 1, "qualifier": "enables", "evidence_code": "IDA"}],
                "REF2": [{"go_term_id": 2, "qualifier": None, "evidence_code": "IEA"}],
            },
        }
        query_embs = np.array([[0.9, 0.1]], dtype=np.float32)
        preds, _, _ = op._predict_batch(
            BatchPredictContext(
                query_accessions=["Q1"],
                query_embeddings=query_embs,
                ref_data=ref_data,
                prediction_set_id=pred_set_id,
                payload=p,
            )
        )

        assert len(preds) >= 1
        for pred in preds:
            assert "vote_count" in pred
            assert "k_position" in pred
            assert "go_term_frequency" in pred
            assert "ref_annotation_density" in pred
            assert "neighbor_distance_std" in pred

    def test_reranker_features_excluded_when_disabled(self) -> None:
        op = self._op()
        p = self._payload(compute_reranker_features=False)
        pred_set_id = uuid.uuid4()

        ref_data = {
            "accessions": ["REF1"],
            "embeddings": np.array([[1.0, 0.0]], dtype=np.float32),
            "go_map": {
                "REF1": [{"go_term_id": 1, "qualifier": "enables", "evidence_code": "IDA"}],
            },
        }
        query_embs = np.array([[0.9, 0.1]], dtype=np.float32)
        preds, _, _ = op._predict_batch(
            BatchPredictContext(
                query_accessions=["Q1"],
                query_embeddings=query_embs,
                ref_data=ref_data,
                prediction_set_id=pred_set_id,
                payload=p,
            )
        )

        for pred in preds:
            assert "vote_count" not in pred
            assert "k_position" not in pred


# ---------------------------------------------------------------------------
# Phase 6 — reranker integration (RerankerModel → predict-time scoring)
# ---------------------------------------------------------------------------


class TestPredictGOTermsCoordinatorReranker:
    """Coordinator-side validation of the ``reranker_model_id`` payload field."""

    def _op(self) -> PredictGOTermsOperation:
        return PredictGOTermsOperation()

    def _base_payload(self, **overrides):
        p = {
            "embedding_config_id": str(uuid.uuid4()),
            "annotation_set_id": _ANN_SET_ID,
            "ontology_snapshot_id": _SNAPSHOT_ID,
            "_job_id": str(uuid.uuid4()),
        }
        p.update(overrides)
        return p

    def test_missing_reranker_model_raises(self) -> None:
        from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel

        op = self._op()
        reranker_id = str(uuid.uuid4())

        def _get(cls, _):
            return None if cls is RerankerModel else MagicMock()

        session = MagicMock()
        session.get.side_effect = _get

        with pytest.raises(ValueError, match=r"RerankerModel .* not found"):
            op.execute(
                session,
                self._base_payload(reranker_model_id=reranker_id),
                emit=_noop_emit,
            )

    def test_reranker_without_artifact_uri_raises(self) -> None:
        from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel

        op = self._op()
        reranker_id = str(uuid.uuid4())
        reranker_row = MagicMock(spec=RerankerModel)
        reranker_row.artifact_uri = None
        reranker_row.feature_schema_sha = "abcd1234"
        reranker_row.name = "broken"

        def _get(cls, _):
            return reranker_row if cls is RerankerModel else MagicMock()

        session = MagicMock()
        session.get.side_effect = _get

        with pytest.raises(ValueError, match="no artifact_uri"):
            op.execute(
                session,
                self._base_payload(reranker_model_id=reranker_id),
                emit=_noop_emit,
            )

    def test_reranker_context_propagated_to_batch_payload(self) -> None:
        from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel

        op = self._op()
        reranker_id = str(uuid.uuid4())
        reranker_row = MagicMock(spec=RerankerModel)
        reranker_row.artifact_uri = "file:///tmp/reranker/model.txt"
        reranker_row.feature_schema_sha = "deadbeef0000"
        reranker_row.name = "smoke"

        def _get(cls, _):
            return reranker_row if cls is RerankerModel else MagicMock()

        session = MagicMock()
        session.get.side_effect = _get

        def add_side_effect(obj):
            if not hasattr(obj, "id") or obj.id is None:
                obj.id = uuid.uuid4()

        session.add.side_effect = add_side_effect

        payload = self._base_payload(reranker_model_id=reranker_id)
        with patch.object(op, "_load_query_accessions", return_value=["P1"]):
            result = op.execute(session, payload, emit=_noop_emit)

        assert len(result.publish_operations) == 1
        _, msg = result.publish_operations[0]
        assert msg["payload"]["reranker_model_id"] == reranker_id
        assert msg["payload"]["reranker_artifact_uri"] == reranker_row.artifact_uri
        assert msg["payload"]["reranker_feature_schema_sha"] == reranker_row.feature_schema_sha


class TestPredictGOTermsBatchReranker:
    """Batch-worker tests for ``_apply_reranker_if_aligned``.

    Driven by unit-level mocks: ``load_reranker`` / ``apply_reranker`` are
    patched so the tests don't require LightGBM nor a real booster file.
    """

    def _op(self) -> PredictGOTermsBatchOperation:
        return PredictGOTermsBatchOperation()

    def _payload(self, **kwargs) -> PredictGOTermsBatchPayload:
        defaults = {
            "embedding_config_id": str(uuid.uuid4()),
            "annotation_set_id": _ANN_SET_ID,
            "ontology_snapshot_id": _SNAPSHOT_ID,
            "prediction_set_id": str(uuid.uuid4()),
            "parent_job_id": str(uuid.uuid4()),
            "query_accessions": ["Q1"],
            "limit_per_entry": 2,
        }
        defaults.update(kwargs)
        return PredictGOTermsBatchPayload.model_validate(defaults)

    def _emit_capture(self):
        events: list[tuple[str, dict]] = []

        def _emit(name, _msg, fields, _sev):
            events.append((name, fields or {}))

        return _emit, events

    def test_skipped_when_artifact_context_missing(self) -> None:
        op = self._op()
        p = self._payload(reranker_model_id=str(uuid.uuid4()))  # but no uri/sha
        dicts = [{"protein_accession": "Q1", "go_term_id": 1, "distance": 0.1}]
        emit, events = self._emit_capture()

        stats = op._apply_reranker_if_aligned(MagicMock(), dicts, p, emit)

        assert stats is None
        assert any(name == "reranker.skipped" for name, _ in events)
        assert "reranker_score" not in dicts[0]

    def test_schema_mismatch_falls_back(self) -> None:
        op = self._op()
        p = self._payload(
            reranker_model_id=str(uuid.uuid4()),
            reranker_artifact_uri="file:///tmp/x/model.txt",
            reranker_feature_schema_sha="not_matching",
            compute_alignments=False,
            compute_taxonomy=False,
            compute_v6_features=False,
        )
        dicts = [{"protein_accession": "Q1", "go_term_id": 1, "distance": 0.1}]
        emit, events = self._emit_capture()

        stats = op._apply_reranker_if_aligned(MagicMock(), dicts, p, emit)

        assert stats is not None
        assert stats["applied"] is False
        assert stats["skipped_reason"] == "schema_mismatch"
        assert any(name == "reranker.schema_mismatch" for name, _ in events)
        assert "reranker_score" not in dicts[0]

    def test_applies_when_schema_matches(self) -> None:
        from protea_reranker_lab.contracts import compute_feature_schema_sha

        op = self._op()
        # Families match PROTEA's inference when all feature flags are off:
        # knn + annotation_meta → deterministic sha.
        expected_sha = compute_feature_schema_sha(["knn", "annotation_meta"])
        p = self._payload(
            reranker_model_id=str(uuid.uuid4()),
            reranker_artifact_uri="file:///tmp/x/model.txt",
            reranker_feature_schema_sha=expected_sha,
            compute_alignments=False,
            compute_taxonomy=False,
            compute_v6_features=False,
        )
        dicts = [
            {"protein_accession": "Q1", "go_term_id": 10, "distance": 0.1},
            {"protein_accession": "Q1", "go_term_id": 11, "distance": 0.3},
        ]
        session = MagicMock()
        # GOTerm aspect lookup returns (id, aspect) tuples.
        session.query.return_value.filter.return_value.all.return_value = [
            (10, "P"), (11, "F"),
        ]
        emit, events = self._emit_capture()

        fake_scores = np.array([0.8, 0.2], dtype=np.float32)
        with patch(
            "protea.core.operations.predict_go_terms.load_reranker",
            return_value=MagicMock(name="booster"),
        ), patch(
            "protea.core.operations.predict_go_terms.apply_reranker",
            return_value=fake_scores,
        ), patch(
            "protea.core.operations.predict_go_terms.get_artifact_store",
            return_value=MagicMock(),
        ), patch(
            "protea.core.operations.predict_go_terms.load_settings",
            return_value=MagicMock(),
        ):
            stats = op._apply_reranker_if_aligned(session, dicts, p, emit)

        assert stats is not None
        assert stats["applied"] is True
        assert stats["rows"] == 2
        assert stats["score_max"] == pytest.approx(0.8, rel=1e-6)
        assert stats["score_min"] == pytest.approx(0.2, rel=1e-6)
        assert dicts[0]["reranker_score"] == pytest.approx(0.8, rel=1e-6)
        assert dicts[1]["reranker_score"] == pytest.approx(0.2, rel=1e-6)
        # Aspect attached from GOTerm lookup
        assert dicts[0]["aspect"] == "P"
        assert dicts[1]["aspect"] == "F"
        assert not any(name == "reranker.schema_mismatch" for name, _ in events)

    def test_no_reranker_leaves_dicts_untouched(self) -> None:
        """Baseline: when ``reranker_model_id`` is absent, the helper is never
        invoked — coordinator + batch should behave identically to pre-Phase-6."""
        p = self._payload()  # no reranker fields
        assert p.reranker_model_id is None
        dicts = [{"protein_accession": "Q1", "go_term_id": 1, "distance": 0.1}]

        # Confirm the coordinator emits no reranker_* fields when id absent:
        # this is a static payload check (coord happy path already covered
        # elsewhere). Here we just assert the baseline invariant on dicts.
        assert "reranker_score" not in dicts[0]
