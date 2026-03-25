"""Unit tests for the LightGBM re-ranker core module."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from protea.core.reranker import (
    ALL_FEATURES,
    CATEGORICAL_FEATURES,
    LABEL_COLUMN,
    NUMERIC_FEATURES,
    TrainResult,
    load_training_tsv,
    model_from_string,
    model_to_string,
    predict,
    prepare_dataset,
    train,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_training_df(n: int = 200, positive_rate: float = 0.3, seed: int = 42) -> pd.DataFrame:
    """Generate a synthetic training DataFrame with realistic feature distributions."""
    rng = np.random.RandomState(seed)

    labels = (rng.random(n) < positive_rate).astype(int)

    data: dict[str, list] = {
        "protein_accession": [f"P{i:05d}" for i in range(n)],
        "go_id": [f"GO:{rng.randint(1, 99999):07d}" for _ in range(n)],
        "aspect": rng.choice(["F", "P", "C"], n).tolist(),
        "label": labels.tolist(),
    }

    # Numeric features — positives get slightly better values
    for col in NUMERIC_FEATURES:
        if col == "distance":
            data[col] = (rng.random(n) * 0.5 + (1 - labels) * 0.3).tolist()
        elif "identity" in col or "similarity" in col:
            data[col] = (rng.random(n) * 0.5 + labels * 0.3).tolist()
        elif "gaps" in col:
            data[col] = (rng.random(n) * 0.1).tolist()
        elif "score" in col:
            data[col] = (rng.random(n) * 500 + labels * 200).tolist()
        elif "length" in col or "alignment_length" in col:
            data[col] = (rng.randint(100, 1000, n)).tolist()
        elif col == "vote_count":
            data[col] = (rng.randint(1, 10, n) + labels * 2).tolist()
        elif col == "k_position":
            data[col] = (rng.randint(1, 5, n)).tolist()
        elif col == "go_term_frequency":
            data[col] = (rng.randint(1, 100, n)).tolist()
        elif col == "ref_annotation_density":
            data[col] = (rng.randint(1, 50, n)).tolist()
        elif col == "neighbor_distance_std":
            data[col] = (rng.random(n) * 0.1).tolist()
        else:
            data[col] = (rng.random(n) * 10).tolist()

    # Categorical features
    data["qualifier"] = rng.choice(["enables", "involved_in", "located_in", ""], n).tolist()
    data["evidence_code"] = rng.choice(["IDA", "IEA", "ISS", "EXP", ""], n).tolist()
    data["taxonomic_relation"] = rng.choice(["self", "sibling", "ancestor", ""], n).tolist()

    return pd.DataFrame(data)


# ---------------------------------------------------------------------------
# prepare_dataset
# ---------------------------------------------------------------------------


class TestPrepareDataset:
    def test_returns_correct_shapes(self):
        df = _make_training_df(50)
        X, y = prepare_dataset(df)
        assert X.shape == (50, len(ALL_FEATURES))
        assert y.shape == (50,)

    def test_categorical_columns_are_category_dtype(self):
        df = _make_training_df(20)
        X, _ = prepare_dataset(df)
        for col in CATEGORICAL_FEATURES:
            assert X[col].dtype.name == "category"

    def test_label_is_int(self):
        df = _make_training_df(20)
        _, y = prepare_dataset(df)
        assert y.dtype == int

    def test_only_feature_columns_in_X(self):
        df = _make_training_df(20)
        X, _ = prepare_dataset(df)
        assert list(X.columns) == ALL_FEATURES
        assert "protein_accession" not in X.columns
        assert "go_id" not in X.columns

    def test_empty_strings_become_na_for_categoricals(self):
        df = _make_training_df(20)
        df.loc[0, "qualifier"] = ""
        X, _ = prepare_dataset(df)
        assert pd.isna(X.loc[0, "qualifier"])


# ---------------------------------------------------------------------------
# train
# ---------------------------------------------------------------------------


class TestTrain:
    def test_returns_train_result(self):
        df = _make_training_df(200)
        result = train(df, num_boost_round=10, early_stopping_rounds=5)
        assert isinstance(result, TrainResult)
        assert result.model is not None
        assert "val_auc" in result.metrics
        assert "val_f1" in result.metrics
        assert "best_iteration" in result.metrics
        assert len(result.feature_importance) > 0

    def test_metrics_are_reasonable(self):
        df = _make_training_df(500, positive_rate=0.3)
        result = train(df, num_boost_round=50, early_stopping_rounds=10)
        assert 0.0 <= result.metrics["val_auc"] <= 1.0
        assert 0.0 <= result.metrics["val_precision"] <= 1.0
        assert 0.0 <= result.metrics["val_recall"] <= 1.0
        assert result.metrics["train_samples"] > 0
        assert result.metrics["val_samples"] > 0

    def test_custom_params(self):
        df = _make_training_df(200)
        result = train(
            df,
            params={"num_leaves": 15, "learning_rate": 0.1},
            num_boost_round=10,
            early_stopping_rounds=5,
        )
        assert result.model is not None

    def test_feature_importance_keys_are_features(self):
        df = _make_training_df(200)
        result = train(df, num_boost_round=10, early_stopping_rounds=5)
        for key in result.feature_importance:
            assert key in ALL_FEATURES

    def test_positive_rate_in_metrics(self):
        df = _make_training_df(200, positive_rate=0.4)
        result = train(df, num_boost_round=10, early_stopping_rounds=5)
        assert 0.2 < result.metrics["positive_rate"] < 0.6  # approximate


# ---------------------------------------------------------------------------
# predict
# ---------------------------------------------------------------------------


class TestPredict:
    def test_returns_probabilities(self):
        df = _make_training_df(200)
        result = train(df, num_boost_round=10, early_stopping_rounds=5)
        scores = predict(result.model, df)
        assert len(scores) == 200
        assert all(0.0 <= s <= 1.0 for s in scores)

    def test_scores_without_label_column(self):
        df = _make_training_df(200)
        result = train(df, num_boost_round=10, early_stopping_rounds=5)
        df_no_label = df.drop(columns=[LABEL_COLUMN])
        scores = predict(result.model, df_no_label)
        assert len(scores) == 200

    def test_higher_scores_for_positive_examples(self):
        """On average, positive examples should get higher scores."""
        df = _make_training_df(1000, positive_rate=0.3)
        result = train(df, num_boost_round=50, early_stopping_rounds=10)
        scores = predict(result.model, df)
        pos_mean = np.mean(scores[df["label"] == 1])
        neg_mean = np.mean(scores[df["label"] == 0])
        assert pos_mean > neg_mean


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


class TestSerialization:
    def test_roundtrip(self):
        df = _make_training_df(200)
        result = train(df, num_boost_round=10, early_stopping_rounds=5)
        model_str = model_to_string(result.model)
        assert isinstance(model_str, str)
        assert len(model_str) > 100

        restored = model_from_string(model_str)
        original_scores = predict(result.model, df)
        restored_scores = predict(restored, df)
        np.testing.assert_array_almost_equal(original_scores, restored_scores)


# ---------------------------------------------------------------------------
# load_training_tsv
# ---------------------------------------------------------------------------


class TestLoadTrainingTSV:
    def test_parses_tsv_string(self):
        tsv = "distance\tvote_count\tlabel\n0.1\t3\t1\n0.5\t1\t0\n"
        df = load_training_tsv(tsv)
        assert len(df) == 2
        assert df["distance"].dtype == float
        assert np.issubdtype(df["vote_count"].dtype, np.number)
        assert df["label"].dtype == int

    def test_parses_tsv_bytes(self):
        tsv = b"distance\tvote_count\tlabel\n0.1\t3\t1\n"
        df = load_training_tsv(tsv)
        assert len(df) == 1

    def test_missing_values_become_nan(self):
        tsv = "distance\tvote_count\tlabel\n\t\t0\n"
        df = load_training_tsv(tsv)
        assert pd.isna(df.loc[0, "distance"])
        assert pd.isna(df.loc[0, "vote_count"])
        assert df.loc[0, "label"] == 0

    def test_handles_missing_columns_gracefully(self):
        tsv = "distance\tlabel\n0.1\t1\n"
        df = load_training_tsv(tsv)
        assert "distance" in df.columns
        assert "vote_count" not in df.columns


# ---------------------------------------------------------------------------
# Feature constants
# ---------------------------------------------------------------------------


class TestFeatureConstants:
    def test_no_duplicate_features(self):
        assert len(ALL_FEATURES) == len(set(ALL_FEATURES))

    def test_all_features_is_union(self):
        assert ALL_FEATURES == NUMERIC_FEATURES + CATEGORICAL_FEATURES

    def test_numeric_and_categorical_disjoint(self):
        assert set(NUMERIC_FEATURES) & set(CATEGORICAL_FEATURES) == set()
