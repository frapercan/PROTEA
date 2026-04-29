"""Unit tests for protea.core.operations.train_reranker.

Covers payload validation and the TrainRerankerOperation helper methods
that remain after the LightGBM training path moved to
``protea-reranker-lab`` — heavy DB / model training is no longer tested
here; the helpers exist only so ``TrainRerankerAutoOperation`` can dump
frozen datasets for the lab.
"""

from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import MagicMock

import numpy as np
import pytest

from protea.core.operations.train_reranker import (
    TrainRerankerOperation,
    TrainRerankerPayload,
)

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
        p = TrainRerankerPayload(
            **self._valid_kwargs(
                limit_per_entry=10,
                distance_threshold=0.5,
                search_backend="faiss",
                metric="euclidean",
            )
        )
        assert p.limit_per_entry == 10
        assert p.distance_threshold == 0.5
        assert p.search_backend == "faiss"

    def test_custom_lightgbm_params(self):
        p = TrainRerankerPayload(
            **self._valid_kwargs(
                num_boost_round=500,
                early_stopping_rounds=25,
                val_fraction=0.1,
                neg_pos_ratio=3.0,
            )
        )
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
                session,
                p,
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
                session,
                p,
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
                session,
                p,
                uuid.UUID(p.old_annotation_set_id),
                uuid.UUID(p.new_annotation_set_id),
                uuid.UUID(p.embedding_config_id),
                uuid.UUID(p.ontology_snapshot_id),
            )

    def test_duplicate_name_raises(self):
        op = self._make_op()
        session = MagicMock()
        session.get.return_value = MagicMock()  # all lookups succeed
        session.query.return_value.filter.return_value.first.return_value = (
            MagicMock()
        )  # name exists
        p = self._make_payload()

        with pytest.raises(ValueError, match="already exists"):
            op._validate(
                session,
                p,
                uuid.UUID(p.old_annotation_set_id),
                uuid.UUID(p.new_annotation_set_id),
                uuid.UUID(p.embedding_config_id),
                uuid.UUID(p.ontology_snapshot_id),
            )

    def test_valid_passes(self):
        op = self._make_op()
        session = MagicMock()
        session.get.return_value = MagicMock()  # all lookups succeed
        session.query.return_value.filter.return_value.first.return_value = (
            None  # no duplicate name
        )
        p = self._make_payload()

        # Should not raise
        op._validate(
            session,
            p,
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
        # SequenceEmbedding.embedding is a pgvector ``halfvec`` column
        # (migrated 2026-04-11); values come back as HalfVector objects
        # that expose ``.to_list()``. Mimic that with a tiny shim so the
        # loader's unwrap call succeeds without touching a real DB.
        class _HV:
            def __init__(self, values: list[float]) -> None:
                self._values = values

            def to_list(self) -> list[float]:
                return self._values

        op = TrainRerankerOperation()
        session = MagicMock()
        session.query.return_value.join.return_value.filter.return_value.all.return_value = [
            ("P1", _HV([0.1, 0.2, 0.3])),
            ("P2", _HV([0.4, 0.5, 0.6])),
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
# Operation name
# ---------------------------------------------------------------------------


class TestOperationName:
    def test_name(self):
        assert TrainRerankerOperation().name == "train_reranker"
