"""Unit tests for the re-ranker inference module.

Training was moved to ``protea-reranker-lab``; the tests below cover
only the helpers that remain in PROTEA: feature-column constants,
``prepare_dataset``, ``predict``, and model serialization round-trips.
"""

from __future__ import annotations

import lightgbm as lgb
import numpy as np
import pandas as pd

from protea.core.reranker import (
    ALL_FEATURES,
    CATEGORICAL_FEATURES,
    LABEL_COLUMN,
    NUMERIC_FEATURES,
    model_from_string,
    predict,
    prepare_dataset,
)


def _make_training_df(n: int = 200, positive_rate: float = 0.3, seed: int = 42) -> pd.DataFrame:
    """Generate a synthetic DataFrame with realistic feature distributions."""
    rng = np.random.RandomState(seed)

    labels = (rng.random(n) < positive_rate).astype(int)

    data: dict[str, list] = {
        "protein_accession": [f"P{i:05d}" for i in range(n)],
        "go_id": [f"GO:{rng.randint(1, 99999):07d}" for _ in range(n)],
        "aspect": rng.choice(["F", "P", "C"], n).tolist(),
        "label": labels.tolist(),
    }

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

    data["qualifier"] = rng.choice(["enables", "involved_in", "located_in", ""], n).tolist()
    data["evidence_code"] = rng.choice(["IDA", "IEA", "ISS", "EXP", ""], n).tolist()
    data["taxonomic_relation"] = rng.choice(["self", "sibling", "ancestor", ""], n).tolist()

    return pd.DataFrame(data)


def _fit_minimal_booster(df: pd.DataFrame, num_boost_round: int = 10) -> lgb.Booster:
    """Train a minimal LightGBM booster inline — replaces the removed
    ``protea.core.reranker.train`` helper, so tests still have a real
    booster to exercise ``predict`` / ``model_from_string`` against."""
    X, y = prepare_dataset(df)
    cat_cols = [c for c in CATEGORICAL_FEATURES if c in X.columns]
    dataset = lgb.Dataset(X, label=y, categorical_feature=cat_cols, free_raw_data=False)
    return lgb.train(
        {
            "objective": "binary",
            "metric": "binary_logloss",
            "verbose": -1,
            "seed": 42,
        },
        dataset,
        num_boost_round=num_boost_round,
    )


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
# predict
# ---------------------------------------------------------------------------


class TestPredict:
    def test_returns_probabilities(self):
        df = _make_training_df(200)
        model = _fit_minimal_booster(df)
        scores = predict(model, df)
        assert len(scores) == 200
        assert all(0.0 <= s <= 1.0 for s in scores)

    def test_scores_without_label_column(self):
        df = _make_training_df(200)
        model = _fit_minimal_booster(df)
        df_no_label = df.drop(columns=[LABEL_COLUMN])
        scores = predict(model, df_no_label)
        assert len(scores) == 200


# ---------------------------------------------------------------------------
# Serialization round-trip — scoring router loads stored boosters via
# model_from_string, so the roundtrip must yield identical scores.
# ---------------------------------------------------------------------------


class TestSerialization:
    def test_roundtrip(self):
        df = _make_training_df(200)
        model = _fit_minimal_booster(df)
        model_str = model.model_to_string()
        restored = model_from_string(model_str)
        np.testing.assert_array_almost_equal(predict(model, df), predict(restored, df))


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
