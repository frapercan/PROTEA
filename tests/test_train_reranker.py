"""Unit tests for protea.core.operations.train_reranker.

Covers payload validation, the TrainRerankerOperation helper methods,
and the _compute_comparison_metrics logic.  Heavy DB / model training
is mocked — no real infrastructure required.
"""
from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from protea.core.operations.train_reranker import (
    TrainRerankerOperation,
    TrainRerankerPayload,
)


_noop_emit = lambda *a, **kw: None


# ---------------------------------------------------------------------------
# Payload validation
# ---------------------------------------------------------------------------

class TestTrainRerankerPayload:
    def _valid_kwargs(self, **overrides) -> dict[str, Any]:
        defaults = {
            "name": "test-model",
            "old_annotation_set_id": str(uuid.uuid4()),
            "new_annotation_set_id": str(uuid.uuid4()),
            "embedding_config_id": str(uuid.uuid4()),
            "ontology_snapshot_id": str(uuid.uuid4()),
        }
        defaults.update(overrides)
        return defaults

    def test_valid_payload(self):
        p = TrainRerankerPayload(**self._valid_kwargs())
        assert p.name == "test-model"
        assert p.category == "nk"
        assert p.limit_per_entry == 5

    def test_empty_name_raises(self):
        with pytest.raises(ValueError):
            TrainRerankerPayload(**self._valid_kwargs(name=""))

    def test_whitespace_name_raises(self):
        with pytest.raises(ValueError):
            TrainRerankerPayload(**self._valid_kwargs(name="   "))

    def test_empty_old_annotation_set_id_raises(self):
        with pytest.raises(ValueError):
            TrainRerankerPayload(**self._valid_kwargs(old_annotation_set_id=""))

    def test_empty_new_annotation_set_id_raises(self):
        with pytest.raises(ValueError):
            TrainRerankerPayload(**self._valid_kwargs(new_annotation_set_id=""))

    def test_empty_embedding_config_id_raises(self):
        with pytest.raises(ValueError):
            TrainRerankerPayload(**self._valid_kwargs(embedding_config_id=""))

    def test_empty_ontology_snapshot_id_raises(self):
        with pytest.raises(ValueError):
            TrainRerankerPayload(**self._valid_kwargs(ontology_snapshot_id=""))

    def test_invalid_category_raises(self):
        with pytest.raises(ValueError):
            TrainRerankerPayload(**self._valid_kwargs(category="invalid"))

    def test_valid_categories(self):
        for cat in ("nk", "lk", "pk"):
            p = TrainRerankerPayload(**self._valid_kwargs(category=cat))
            assert p.category == cat

    def test_custom_knn_params(self):
        p = TrainRerankerPayload(**self._valid_kwargs(
            limit_per_entry=10,
            distance_threshold=0.5,
            search_backend="faiss",
            metric="euclidean",
        ))
        assert p.limit_per_entry == 10
        assert p.distance_threshold == 0.5
        assert p.search_backend == "faiss"

    def test_custom_lightgbm_params(self):
        p = TrainRerankerPayload(**self._valid_kwargs(
            num_boost_round=500,
            early_stopping_rounds=25,
            val_fraction=0.1,
            neg_pos_ratio=3.0,
        ))
        assert p.num_boost_round == 500
        assert p.early_stopping_rounds == 25
        assert p.val_fraction == 0.1
        assert p.neg_pos_ratio == 3.0

    def test_feature_flags_default_false(self):
        p = TrainRerankerPayload(**self._valid_kwargs())
        assert p.compute_alignments is False
        assert p.compute_taxonomy is False

    def test_aspect_filter(self):
        p = TrainRerankerPayload(**self._valid_kwargs(aspect="bpo"))
        assert p.aspect == "bpo"

    def test_name_is_stripped(self):
        p = TrainRerankerPayload(**self._valid_kwargs(name="  my model  "))
        assert p.name == "my model"

    def test_limit_per_entry_must_be_positive(self):
        with pytest.raises(ValueError):
            TrainRerankerPayload(**self._valid_kwargs(limit_per_entry=0))

        with pytest.raises(ValueError):
            TrainRerankerPayload(**self._valid_kwargs(limit_per_entry=-1))


# ---------------------------------------------------------------------------
# _validate
# ---------------------------------------------------------------------------

class TestValidate:
    def _make_op(self):
        return TrainRerankerOperation()

    def _make_payload(self, **kw):
        defaults = {
            "name": "test",
            "old_annotation_set_id": str(uuid.uuid4()),
            "new_annotation_set_id": str(uuid.uuid4()),
            "embedding_config_id": str(uuid.uuid4()),
            "ontology_snapshot_id": str(uuid.uuid4()),
        }
        defaults.update(kw)
        return TrainRerankerPayload(**defaults)

    def test_old_annotation_set_not_found(self):
        op = self._make_op()
        session = MagicMock()
        session.get.return_value = None
        p = self._make_payload()

        with pytest.raises(ValueError, match="AnnotationSet"):
            op._validate(
                session, p,
                uuid.UUID(p.old_annotation_set_id),
                uuid.UUID(p.new_annotation_set_id),
                uuid.UUID(p.embedding_config_id),
                uuid.UUID(p.ontology_snapshot_id),
            )

    def test_new_annotation_set_not_found(self):
        op = self._make_op()
        session = MagicMock()
        # First call (old) returns something, second (new) returns None
        session.get.side_effect = [MagicMock(), None]
        p = self._make_payload()

        with pytest.raises(ValueError, match="AnnotationSet"):
            op._validate(
                session, p,
                uuid.UUID(p.old_annotation_set_id),
                uuid.UUID(p.new_annotation_set_id),
                uuid.UUID(p.embedding_config_id),
                uuid.UUID(p.ontology_snapshot_id),
            )

    def test_embedding_config_not_found(self):
        op = self._make_op()
        session = MagicMock()
        # old and new found, embedding config not found
        session.get.side_effect = [MagicMock(), MagicMock(), None]
        p = self._make_payload()

        with pytest.raises(ValueError, match="EmbeddingConfig"):
            op._validate(
                session, p,
                uuid.UUID(p.old_annotation_set_id),
                uuid.UUID(p.new_annotation_set_id),
                uuid.UUID(p.embedding_config_id),
                uuid.UUID(p.ontology_snapshot_id),
            )

    def test_duplicate_name_raises(self):
        op = self._make_op()
        session = MagicMock()
        session.get.return_value = MagicMock()  # all lookups succeed
        session.query.return_value.filter.return_value.first.return_value = MagicMock()  # name exists
        p = self._make_payload()

        with pytest.raises(ValueError, match="already exists"):
            op._validate(
                session, p,
                uuid.UUID(p.old_annotation_set_id),
                uuid.UUID(p.new_annotation_set_id),
                uuid.UUID(p.embedding_config_id),
                uuid.UUID(p.ontology_snapshot_id),
            )

    def test_valid_passes(self):
        op = self._make_op()
        session = MagicMock()
        session.get.return_value = MagicMock()  # all lookups succeed
        session.query.return_value.filter.return_value.first.return_value = None  # no duplicate name
        p = self._make_payload()

        # Should not raise
        op._validate(
            session, p,
            uuid.UUID(p.old_annotation_set_id),
            uuid.UUID(p.new_annotation_set_id),
            uuid.UUID(p.embedding_config_id),
            uuid.UUID(p.ontology_snapshot_id),
        )


# ---------------------------------------------------------------------------
# _load_query_embeddings
# ---------------------------------------------------------------------------

class TestLoadQueryEmbeddings:
    def test_returns_empty_when_no_matches(self):
        op = TrainRerankerOperation()
        session = MagicMock()
        session.query.return_value.join.return_value.filter.return_value.all.return_value = []

        emb, valid = op._load_query_embeddings(session, ["P1", "P2"], uuid.uuid4())
        assert len(valid) == 0
        assert emb.shape == (0,)

    def test_returns_embeddings_for_found(self):
        op = TrainRerankerOperation()
        session = MagicMock()
        session.query.return_value.join.return_value.filter.return_value.all.return_value = [
            ("P1", [0.1, 0.2, 0.3]),
            ("P2", [0.4, 0.5, 0.6]),
        ]

        emb, valid = op._load_query_embeddings(session, ["P1", "P2"], uuid.uuid4())
        assert valid == ["P1", "P2"]
        assert emb.shape == (2, 3)
        np.testing.assert_allclose(emb[0], [0.1, 0.2, 0.3], atol=1e-6)


# ---------------------------------------------------------------------------
# _load_sequences
# ---------------------------------------------------------------------------

class TestLoadSequences:
    def test_returns_dict(self):
        op = TrainRerankerOperation()
        session = MagicMock()
        session.query.return_value.join.return_value.filter.return_value.all.return_value = [
            ("P1", "MKVLWAGS"),
            ("P2", "ACDEF"),
        ]

        result = op._load_sequences(session, {"P1", "P2"})
        assert result == {"P1": "MKVLWAGS", "P2": "ACDEF"}

    def test_empty_accessions(self):
        op = TrainRerankerOperation()
        session = MagicMock()
        result = op._load_sequences(session, set())
        assert result == {}


# ---------------------------------------------------------------------------
# _load_taxonomy_ids
# ---------------------------------------------------------------------------

class TestLoadTaxonomyIds:
    def test_returns_dict(self):
        op = TrainRerankerOperation()
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = [
            ("P1", 9606),
            ("P2", 10090),
        ]

        result = op._load_taxonomy_ids(session, {"P1", "P2"})
        assert result == {"P1": 9606, "P2": 10090}

    def test_none_taxonomy_id(self):
        op = TrainRerankerOperation()
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = [
            ("P1", None),
        ]

        result = op._load_taxonomy_ids(session, {"P1"})
        assert result == {"P1": None}


# ---------------------------------------------------------------------------
# _compute_comparison_metrics
# ---------------------------------------------------------------------------

class TestComputeComparisonMetrics:
    def test_returns_expected_keys(self):
        op = TrainRerankerOperation()

        # Create a minimal DataFrame
        df = pd.DataFrame([
            {"protein_accession": "P1", "go_id": "GO:0001", "distance": 0.1, "label": 1},
            {"protein_accession": "P1", "go_id": "GO:0002", "distance": 0.9, "label": 0},
        ])

        # Mock train result
        train_result = MagicMock()
        train_result.model = MagicMock()

        # Mock evaluation data
        eval_data = MagicMock()
        eval_data.nk = {"P1": {"GO:0001"}}

        with patch(
            "protea.core.operations.train_reranker.reranker_predict",
            return_value=np.array([0.9, 0.1]),
        ), patch(
            "protea.core.operations.train_reranker.compute_cafa_metrics",
        ) as mock_cafa:
            mock_metrics = MagicMock()
            mock_metrics.fmax = 0.5
            mock_metrics.auc_pr = 0.4
            mock_metrics.threshold_at_fmax = 0.3
            mock_metrics.n_ground_truth_proteins = 1
            mock_cafa.return_value = mock_metrics

            result = op._compute_comparison_metrics(df, train_result, eval_data, "nk")

        expected_keys = {
            "baseline_fmax", "baseline_auc_pr", "baseline_threshold",
            "reranker_fmax", "reranker_auc_pr", "reranker_threshold",
            "fmax_improvement", "auc_pr_improvement", "n_ground_truth_proteins",
        }
        assert set(result.keys()) == expected_keys

    def test_fmax_improvement_computed(self):
        op = TrainRerankerOperation()
        df = pd.DataFrame([
            {"protein_accession": "P1", "go_id": "GO:0001", "distance": 0.1, "label": 1},
        ])

        train_result = MagicMock()

        call_count = [0]
        def fake_cafa(*args, **kwargs):
            call_count[0] += 1
            m = MagicMock()
            if call_count[0] == 1:
                m.fmax = 0.4  # baseline
                m.auc_pr = 0.3
            else:
                m.fmax = 0.6  # reranker
                m.auc_pr = 0.5
            m.threshold_at_fmax = 0.3
            m.n_ground_truth_proteins = 1
            return m

        with patch(
            "protea.core.operations.train_reranker.reranker_predict",
            return_value=np.array([0.9]),
        ), patch(
            "protea.core.operations.train_reranker.compute_cafa_metrics",
            side_effect=fake_cafa,
        ):
            result = op._compute_comparison_metrics(df, train_result, MagicMock(), "nk")

        assert result["baseline_fmax"] == 0.4
        assert result["reranker_fmax"] == 0.6
        assert result["fmax_improvement"] == 0.2


# ---------------------------------------------------------------------------
# _load_go_maps
# ---------------------------------------------------------------------------

class TestLoadGoMaps:
    def test_returns_id_and_aspect_maps(self):
        op = TrainRerankerOperation()
        session = MagicMock()
        session.execute.return_value.fetchall.return_value = [
            (1, "GO:0001", "P"),
            (2, "GO:0002", "F"),
            (3, "GO:0003", None),
        ]

        id_map, aspect_map = op._load_go_maps(session, uuid.uuid4())
        assert id_map == {1: "GO:0001", 2: "GO:0002", 3: "GO:0003"}
        assert aspect_map == {1: "P", 2: "F"}
        assert 3 not in aspect_map  # None aspect excluded


# ---------------------------------------------------------------------------
# Full execute flow (heavily mocked)
# ---------------------------------------------------------------------------

class TestExecuteFlow:
    def test_no_ground_truth_raises(self):
        op = TrainRerankerOperation()
        session = MagicMock()
        session.get.return_value = MagicMock()
        session.query.return_value.filter.return_value.first.return_value = None

        payload = {
            "name": "test",
            "old_annotation_set_id": str(uuid.uuid4()),
            "new_annotation_set_id": str(uuid.uuid4()),
            "embedding_config_id": str(uuid.uuid4()),
            "ontology_snapshot_id": str(uuid.uuid4()),
        }

        with patch.object(op, "_validate"), \
             patch(
                 "protea.core.operations.train_reranker.compute_evaluation_data",
             ) as mock_eval:
            eval_data = MagicMock()
            eval_data.nk = {}  # empty ground truth
            eval_data.stats.return_value = {}
            mock_eval.return_value = eval_data

            with pytest.raises(ValueError, match="No ground truth"):
                op.execute(session, payload, emit=_noop_emit)

    def test_no_embeddings_raises(self):
        op = TrainRerankerOperation()
        session = MagicMock()
        session.get.return_value = MagicMock()
        session.query.return_value.filter.return_value.first.return_value = None

        payload = {
            "name": "test",
            "old_annotation_set_id": str(uuid.uuid4()),
            "new_annotation_set_id": str(uuid.uuid4()),
            "embedding_config_id": str(uuid.uuid4()),
            "ontology_snapshot_id": str(uuid.uuid4()),
        }

        with patch.object(op, "_validate"), \
             patch(
                 "protea.core.operations.train_reranker.compute_evaluation_data",
             ) as mock_eval, \
             patch.object(op, "_load_go_maps", return_value=({}, {})), \
             patch.object(op, "_load_reference_per_aspect", return_value={
                 "P": {"accessions": [], "embeddings": np.empty((0,)), "go_map": {}},
                 "F": {"accessions": [], "embeddings": np.empty((0,)), "go_map": {}},
                 "C": {"accessions": [], "embeddings": np.empty((0,)), "go_map": {}},
             }), \
             patch.object(op, "_load_query_embeddings", return_value=(np.empty((0,)), [])):

            eval_data = MagicMock()
            eval_data.nk = {"P1": {"GO:0001"}}
            eval_data.stats.return_value = {"nk": 1}
            mock_eval.return_value = eval_data

            with pytest.raises(ValueError, match="No delta proteins have embeddings"):
                op.execute(session, payload, emit=_noop_emit)


# ---------------------------------------------------------------------------
# Operation name
# ---------------------------------------------------------------------------

class TestOperationName:
    def test_name(self):
        assert TrainRerankerOperation().name == "train_reranker"
