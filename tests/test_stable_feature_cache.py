"""Unit tests for the NFR-ARCH "export decouple" stable-feature cache.

Covers the two phase-1 levers:

* :func:`compute_stable_cache_key` -- same stable config -> same key; swapping
  the classifier config keeps the SAME key (the whole point: a classifier A/B
  reuses the cached stable table); changing any stable axis -> a different key.
* :func:`apply_classifier_to_frame` -- the parquet-level classifier post-pass
  fills the VARIABLE columns and leaves stable columns untouched, reusing the
  predict-path producer so the values match the inline applier.

Pure-python: no DB, no live stack. The classifier producer's DB-bound loaders
are mocked.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

from protea.core.classifier_producer import ClassifierPrediction
from protea.core.training_dump import _classifier_postpass as cpp
from protea.core.training_dump._classifier_postpass import ClassifierStampSpec
from protea.core.training_dump._payload import TrainRerankerAutoPayload
from protea.core.training_dump._stable_feature_cache import (
    VARIABLE_FEATURE_COLUMNS,
    compute_stable_cache_key,
    stable_cache_prefix,
)


def _payload(**overrides: object) -> TrainRerankerAutoPayload:
    base: dict[str, object] = {
        "name": "decouple-test",
        "embedding_config_id": "11111111-1111-1111-1111-111111111111",
        "ontology_snapshot_id": "22222222-2222-2222-2222-222222222222",
        "train_versions": [160, 165, 170],
        "test_versions": [175],
        "limit_per_entry": 5,
    }
    base.update(overrides)
    return TrainRerankerAutoPayload.model_validate(base)


class TestStableCacheKey:
    def test_same_config_same_key(self) -> None:
        assert compute_stable_cache_key(_payload()) == compute_stable_cache_key(_payload())

    def test_swapping_classifier_keeps_same_key(self) -> None:
        """The decouple's reason for being: classifier config is NOT in the key.

        Two exports differing only in the classifier family (and the cache /
        variant knobs) must share the stable key so the second reuses the first
        export's cached stable table.
        """
        a = _payload(compute_classifier=False)
        b = _payload(
            compute_classifier=True,
            classifier_variants=[{"label": "ema", "seed_dir": "/tmp/ema"}],
        )
        assert compute_stable_cache_key(a) == compute_stable_cache_key(b)

    def test_stable_feature_cache_flag_not_in_key(self) -> None:
        a = _payload(stable_feature_cache=False)
        b = _payload(stable_feature_cache=True)
        assert compute_stable_cache_key(a) == compute_stable_cache_key(b)

    def test_changing_embedding_config_changes_key(self) -> None:
        a = _payload()
        b = _payload(embedding_config_id="33333333-3333-3333-3333-333333333333")
        assert compute_stable_cache_key(a) != compute_stable_cache_key(b)

    def test_changing_k_changes_key(self) -> None:
        assert compute_stable_cache_key(_payload(limit_per_entry=5)) != compute_stable_cache_key(
            _payload(limit_per_entry=10)
        )

    def test_changing_versions_changes_key(self) -> None:
        a = _payload(train_versions=[160, 165, 170])
        b = _payload(train_versions=[160, 165, 175])
        assert compute_stable_cache_key(a) != compute_stable_cache_key(b)

    def test_changing_a_stable_flag_changes_key(self) -> None:
        a = _payload(compute_self_prior=False)
        b = _payload(compute_self_prior=True)
        assert compute_stable_cache_key(a) != compute_stable_cache_key(b)

    def test_prefix_is_under_stable_features(self) -> None:
        key = compute_stable_cache_key(_payload())
        assert stable_cache_prefix(key) == f"stable_features/{key}/"
        assert len(key) == 16


def _stable_frame() -> pd.DataFrame:
    """A consolidated export frame with classifier columns zero-filled.

    ``go_term_id`` stores the GO id STRING, mirroring
    ``parquet_export._load_train_shards`` (which renames ``go_id`` ->
    ``go_term_id`` without int conversion).
    """
    return pd.DataFrame(
        {
            "protein_accession": ["Q1", "Q1", "Q2"],
            "go_term_id": ["GO:0000099", "GO:0000010", "GO:0000099"],
            "knn_present": [1.0, 1.0, 1.0],
            "self_prior_score": [0.0, 1.0, 0.0],
            "classifier_score": [0.0, 0.0, 0.0],
            "classifier_present": [0.0, 0.0, 0.0],
        }
    )


def _run_postpass(
    frame: pd.DataFrame,
    preds: list[ClassifierPrediction],
    spec: ClassifierStampSpec | None = None,
) -> None:
    clf = MagicMock(predict=MagicMock(return_value=preds))
    with (
        patch(
            "protea.core.classifier_producer.load_concat_features",
            return_value=(_features(frame), list(dict.fromkeys(frame["protein_accession"]))),
        ),
        patch("protea.core.classifier_producer.get_classifier", return_value=clf),
        patch(
            "protea.core.classifier_producer.resolve_go_term_ids",
            return_value={pr.go_id: 1 for pr in preds},
        ),
    ):
        cpp.apply_classifier_to_frame(MagicMock(), frame, MagicMock(), spec)


def _features(frame: pd.DataFrame):
    import numpy as np

    n = len(set(frame["protein_accession"]))
    return np.zeros((n, 8320), dtype=np.float32)


class TestClassifierPostPass:
    def test_stamps_matching_candidates_only(self) -> None:
        frame = _stable_frame()
        _run_postpass(frame, [ClassifierPrediction("Q1", "GO:0000099", 0.9)])
        # (Q1, GO:0000099) -> stamped; the other two rows stay zero.
        assert frame.loc[0, "classifier_score"] == 0.9
        assert frame.loc[0, "classifier_present"] == 1.0
        assert frame.loc[1, "classifier_score"] == 0.0
        assert frame.loc[2, "classifier_score"] == 0.0
        # No rows added / dropped.
        assert len(frame) == 3

    def test_leaves_stable_columns_untouched(self) -> None:
        frame = _stable_frame()
        before = frame[["knn_present", "self_prior_score"]].copy()
        _run_postpass(frame, [ClassifierPrediction("Q1", "GO:0000099", 0.9)])
        pd.testing.assert_frame_equal(frame[["knn_present", "self_prior_score"]], before)

    def test_emits_into_custom_variant_columns(self) -> None:
        """Phase-2 seam: the post-pass can target ``__<label>`` columns."""
        frame = _stable_frame()
        _run_postpass(
            frame,
            [ClassifierPrediction("Q1", "GO:0000099", 0.7)],
            ClassifierStampSpec(
                score_column="classifier_score__ema",
                present_column="classifier_present__ema",
            ),
        )
        assert frame.loc[0, "classifier_score__ema"] == 0.7
        assert frame.loc[0, "classifier_present__ema"] == 1.0
        # Canonical columns stay at their zero-fill default.
        assert frame.loc[0, "classifier_score"] == 0.0

    def test_empty_frame_is_a_noop(self) -> None:
        frame = pd.DataFrame()
        cpp.apply_classifier_to_frame(MagicMock(), frame, MagicMock())
        assert frame.empty


def test_variable_columns_constant_matches_classifier_family() -> None:
    """Guard the local literal against the feature-registry classifier family."""
    from protea.core.features._bindings import _CLASSIFIER_FEATURES

    assert VARIABLE_FEATURE_COLUMNS == _CLASSIFIER_FEATURES
