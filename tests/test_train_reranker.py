"""Unit tests for protea.core.operations.train_reranker.

Covers ``TrainRerankerPayload`` (still imported by the lab via the
``protea_reranker_lab.contracts`` mirror) and the few module-level
helpers (``_load_sequences``, ``_load_taxonomy_ids``) that
``TrainRerankerAutoOperation`` keeps using to drive the dataset-export
pipeline. Heavy DB / model training is no longer tested here — LightGBM
training lives in ``protea-reranker-lab``.
"""

from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import MagicMock

import numpy as np
import pytest

from protea.core.operations.train_reranker import (
    TrainRerankerPayload,
    _load_sequences,
    _load_taxonomy_ids,
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
# _load_sequences (used by TrainRerankerAutoOperation in dump_only mode)
# ---------------------------------------------------------------------------


class TestLoadSequences:
    def test_returns_dict(self):
        session = MagicMock()
        session.query.return_value.join.return_value.filter.return_value.all.return_value = [
            ("P1", "MKVLWAGS"),
            ("P2", "ACDEF"),
        ]

        result = _load_sequences(session, {"P1", "P2"})
        assert result == {"P1": "MKVLWAGS", "P2": "ACDEF"}

    def test_empty_accessions(self):
        session = MagicMock()
        result = _load_sequences(session, set())
        assert result == {}


# ---------------------------------------------------------------------------
# _load_taxonomy_ids (used by TrainRerankerAutoOperation in dump_only mode)
# ---------------------------------------------------------------------------


class TestLoadTaxonomyIds:
    def test_returns_dict(self):
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = [
            ("P1", 9606),
            ("P2", 10090),
        ]

        result = _load_taxonomy_ids(session, {"P1", "P2"})
        assert result == {"P1": 9606, "P2": 10090}

    def test_none_taxonomy_id(self):
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = [
            ("P1", None),
        ]

        result = _load_taxonomy_ids(session, {"P1"})
        assert result == {"P1": None}


# Import np to keep the test module's structure consistent with other test
# files even though the explicit numpy assertions used by the previous
# TrainReranker test surface have been removed.
_ = np
