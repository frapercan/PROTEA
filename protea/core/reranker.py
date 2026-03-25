"""LightGBM re-ranker for GO term predictions.

Trains a binary classifier on labeled prediction data (from temporal holdout)
and produces calibrated probability scores that replace or supplement the
original distance-based ranking.

Feature columns are the numeric signals stored in ``GOPrediction``.  Categorical
features (``qualifier``, ``evidence_code``, ``taxonomic_relation``) are
label-encoded.  Missing values are left as NaN — LightGBM handles them natively.
"""

from __future__ import annotations

import io
import json
from dataclasses import dataclass
from typing import Any

import lightgbm as lgb
import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Feature definitions
# ---------------------------------------------------------------------------

NUMERIC_FEATURES: list[str] = [
    "distance",
    # NW alignment
    "identity_nw",
    "similarity_nw",
    "alignment_score_nw",
    "gaps_pct_nw",
    "alignment_length_nw",
    # SW alignment
    "identity_sw",
    "similarity_sw",
    "alignment_score_sw",
    "gaps_pct_sw",
    "alignment_length_sw",
    # Lengths
    "length_query",
    "length_ref",
    # Taxonomy
    "taxonomic_distance",
    "taxonomic_common_ancestors",
    # Re-ranker features
    "vote_count",
    "k_position",
    "go_term_frequency",
    "ref_annotation_density",
    "neighbor_distance_std",
]

CATEGORICAL_FEATURES: list[str] = [
    "qualifier",
    "evidence_code",
    "taxonomic_relation",
]

ALL_FEATURES: list[str] = NUMERIC_FEATURES + CATEGORICAL_FEATURES

LABEL_COLUMN = "label"


# ---------------------------------------------------------------------------
# Data preparation
# ---------------------------------------------------------------------------


def prepare_dataset(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """Extract feature matrix and label vector from a training DataFrame.

    Categorical columns are converted to pandas ``category`` dtype so that
    LightGBM can handle them directly (no manual encoding needed).

    Returns (X, y) where X has only the feature columns and y is the binary label.
    """
    X = df[ALL_FEATURES].copy()
    for col in NUMERIC_FEATURES:
        if col in X.columns:
            X[col] = pd.to_numeric(X[col], errors="coerce")
    for col in CATEGORICAL_FEATURES:
        if col in X.columns:
            X[col] = X[col].replace("", pd.NA).astype("category")
    y = df[LABEL_COLUMN].astype(int)
    return X, y


# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------

_DEFAULT_PARAMS: dict[str, Any] = {
    "objective": "binary",
    "metric": ["binary_logloss", "auc"],
    "boosting_type": "gbdt",
    "num_leaves": 31,
    "learning_rate": 0.01,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq": 5,
    "verbose": -1,
    "seed": 42,
}


@dataclass
class TrainResult:
    """Result of training a re-ranker model."""

    model: lgb.Booster
    metrics: dict[str, Any]
    feature_importance: dict[str, int]


def train(
    df: pd.DataFrame,
    *,
    params: dict[str, Any] | None = None,
    num_boost_round: int = 1000,
    early_stopping_rounds: int = 50,
    val_fraction: float = 0.2,
    neg_pos_ratio: float | None = None,
    sample_weight: np.ndarray | None = None,
) -> TrainResult:
    """Train a LightGBM binary classifier on labeled prediction data.

    Parameters
    ----------
    df:
        DataFrame with feature columns + ``label`` column (0/1).
    params:
        LightGBM parameters.  Merged on top of ``_DEFAULT_PARAMS``.
    num_boost_round:
        Maximum number of boosting rounds.
    early_stopping_rounds:
        Stop if validation metric doesn't improve for this many rounds.
    val_fraction:
        Fraction of data to hold out for early stopping validation.
    neg_pos_ratio:
        If set, subsample negatives so that the ratio of negatives to
        positives is at most this value (e.g. 1.0 for 1:1, 10.0 for 10:1).
        Applied independently to train and val splits.  When ``None``
        (default), all negatives are kept.
    sample_weight:
        Per-sample weights (e.g. Information Accretion of each GO term).
        Must have the same length as ``df``.  When provided, the weights
        are passed to LightGBM so that high-weight samples contribute
        more to the loss.

    Returns
    -------
    TrainResult with the trained Booster, validation metrics, and feature importance.
    """
    X, y = prepare_dataset(df)

    merged_params = {**_DEFAULT_PARAMS, **(params or {})}

    # Stratified train/val split
    rng = np.random.RandomState(merged_params.get("seed", 42))
    pos_idx = np.where(y == 1)[0]
    neg_idx = np.where(y == 0)[0]
    rng.shuffle(pos_idx)
    rng.shuffle(neg_idx)

    n_pos_val = max(1, int(len(pos_idx) * val_fraction))
    n_neg_val = max(1, int(len(neg_idx) * val_fraction))

    val_pos = pos_idx[:n_pos_val]
    val_neg = neg_idx[:n_neg_val]
    train_pos = pos_idx[n_pos_val:]
    train_neg = neg_idx[n_neg_val:]

    # Subsample negatives if requested
    if neg_pos_ratio is not None:
        max_train_neg = max(1, int(len(train_pos) * neg_pos_ratio))
        if len(train_neg) > max_train_neg:
            train_neg = train_neg[:max_train_neg]
        max_val_neg = max(1, int(len(val_pos) * neg_pos_ratio))
        if len(val_neg) > max_val_neg:
            val_neg = val_neg[:max_val_neg]

    val_idx = np.concatenate([val_pos, val_neg])
    train_idx = np.concatenate([train_pos, train_neg])

    cat_cols = [c for c in CATEGORICAL_FEATURES if c in X.columns]

    train_w = sample_weight[train_idx] if sample_weight is not None else None
    val_w = sample_weight[val_idx] if sample_weight is not None else None

    train_ds = lgb.Dataset(
        X.iloc[train_idx],
        label=y.iloc[train_idx],
        weight=train_w,
        categorical_feature=cat_cols,
        free_raw_data=False,
    )
    val_ds = lgb.Dataset(
        X.iloc[val_idx],
        label=y.iloc[val_idx],
        weight=val_w,
        categorical_feature=cat_cols,
        reference=train_ds,
        free_raw_data=False,
    )

    callbacks = [
        lgb.early_stopping(early_stopping_rounds, verbose=False),
        lgb.log_evaluation(period=0),
    ]

    booster = lgb.train(
        merged_params,
        train_ds,
        num_boost_round=num_boost_round,
        valid_sets=[val_ds],
        valid_names=["val"],
        callbacks=callbacks,
    )

    # Collect validation metrics
    val_preds = booster.predict(X.iloc[val_idx])
    val_labels = y.iloc[val_idx].values

    tp = np.sum((val_preds >= 0.5) & (val_labels == 1))
    fp = np.sum((val_preds >= 0.5) & (val_labels == 0))
    fn = np.sum((val_preds < 0.5) & (val_labels == 1))
    precision = float(tp / (tp + fp)) if (tp + fp) > 0 else 0.0
    recall = float(tp / (tp + fn)) if (tp + fn) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

    metrics = {
        "best_iteration": booster.best_iteration,
        "val_auc": float(booster.best_score.get("val", {}).get("auc", 0.0)),
        "val_logloss": float(booster.best_score.get("val", {}).get("binary_logloss", 0.0)),
        "val_precision": round(precision, 4),
        "val_recall": round(recall, 4),
        "val_f1": round(f1, 4),
        "train_samples": len(train_idx),
        "val_samples": len(val_idx),
        "positive_rate": round(float(y.mean()), 4),
    }

    importance = dict(
        zip(booster.feature_name(), booster.feature_importance(importance_type="gain").tolist())
    )

    return TrainResult(model=booster, metrics=metrics, feature_importance=importance)


# ---------------------------------------------------------------------------
# Inference
# ---------------------------------------------------------------------------


def predict(model: lgb.Booster, df: pd.DataFrame) -> np.ndarray:
    """Score predictions using a trained re-ranker.

    Returns an array of probabilities (0–1) where higher = more likely correct.
    """
    if LABEL_COLUMN in df.columns:
        X, _ = prepare_dataset(df)
    else:
        X = df[ALL_FEATURES].copy()
        for col in NUMERIC_FEATURES:
            if col in X.columns:
                X[col] = pd.to_numeric(X[col], errors="coerce")
        for col in CATEGORICAL_FEATURES:
            if col in X.columns:
                X[col] = X[col].replace("", pd.NA).astype("category")

    return model.predict(X)


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


def model_to_string(model: lgb.Booster) -> str:
    """Serialize a trained model to a string for DB storage."""
    return model.model_to_string()


def model_from_string(model_str: str) -> lgb.Booster:
    """Deserialize a model from its string representation."""
    return lgb.Booster(model_str=model_str)


def load_training_tsv(tsv_content: str | bytes) -> pd.DataFrame:
    """Parse a training data TSV (as produced by the training-data.tsv endpoint)."""
    if isinstance(tsv_content, bytes):
        tsv_content = tsv_content.decode("utf-8")
    df = pd.read_csv(io.StringIO(tsv_content), sep="\t", dtype=str)
    # Convert numeric columns
    for col in NUMERIC_FEATURES:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if LABEL_COLUMN in df.columns:
        df[LABEL_COLUMN] = pd.to_numeric(df[LABEL_COLUMN], errors="coerce").fillna(0).astype(int)
    return df
