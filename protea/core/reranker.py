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
from collections.abc import Callable
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
    # Consensus features (per candidate term, computed over voting neighbors)
    "neighbor_vote_fraction",
    "neighbor_min_distance",
    "neighbor_mean_distance",
    # Anc2Vec semantic-coherence features (GO release 2020-10-06 pretrained)
    "anc2vec_neighbor_cos",
    "anc2vec_neighbor_maxcos",
    "anc2vec_has_emb",
    # Query-side Anc2Vec (PK-killer): candidate vs query's pre-cutoff annotations
    "anc2vec_query_known_cos",
    "anc2vec_query_known_maxcos",
    "anc2vec_query_known_count",
    # Taxonomic consensus across voting neighbors (requires compute_taxonomy=True)
    "tax_voters_same_frac",
    "tax_voters_close_frac",
    "tax_voters_mean_common_ancestors",
    # Sequence-embedding PCA — 16-dim query projection onto the top principal
    # components of the reference embedding pool (use_embedding_pca flag).
    # NaN when the flag is disabled: LightGBM treats them as missing.
    "emb_pca_query_0",
    "emb_pca_query_1",
    "emb_pca_query_2",
    "emb_pca_query_3",
    "emb_pca_query_4",
    "emb_pca_query_5",
    "emb_pca_query_6",
    "emb_pca_query_7",
    "emb_pca_query_8",
    "emb_pca_query_9",
    "emb_pca_query_10",
    "emb_pca_query_11",
    "emb_pca_query_12",
    "emb_pca_query_13",
    "emb_pca_query_14",
    "emb_pca_query_15",
]

EMBEDDING_PCA_DIM = 16

CATEGORICAL_FEATURES: list[str] = [
    "qualifier",
    "evidence_code",
    "taxonomic_relation",
    "aspect",
]

ALL_FEATURES: list[str] = NUMERIC_FEATURES + CATEGORICAL_FEATURES


def fit_embedding_pca(
    embeddings: np.ndarray,
    n_components: int = EMBEDDING_PCA_DIM,
    *,
    max_fit_samples: int = 50_000,
    seed: int = 42,
) -> tuple[np.ndarray, np.ndarray]:
    """Fit PCA via truncated SVD on a (possibly subsampled) embedding matrix.

    Returns ``(mean, components)`` with ``mean`` shape ``(D,)`` and
    ``components`` shape ``(n_components, D)`` — both float32.  Designed to
    be called once per ``EmbeddingConfig`` pool; subsequent projections are
    a single matmul.
    """
    if embeddings.size == 0:
        raise ValueError("embeddings matrix is empty")
    n = embeddings.shape[0]
    rng = np.random.default_rng(seed)
    if n > max_fit_samples:
        idx = rng.choice(n, size=max_fit_samples, replace=False)
        x = embeddings[idx].astype(np.float32, copy=False)
    else:
        x = embeddings.astype(np.float32, copy=False)
    mean = x.mean(axis=0)
    xc = x - mean
    _, _, vh = np.linalg.svd(xc, full_matrices=False)
    k = min(n_components, vh.shape[0])
    components = vh[:k].astype(np.float32)
    return mean.astype(np.float32), components

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
    heartbeat: Callable[[int], None] | None = None,
    heartbeat_period: int = 50,
    objective: str = "binary",
    group_ids: np.ndarray | None = None,
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

    if objective == "lambdarank":
        if group_ids is None:
            raise ValueError("group_ids is required when objective='lambdarank'")
        if len(group_ids) != len(df):
            raise ValueError(
                f"group_ids length ({len(group_ids)}) must match df length ({len(df)})"
            )
        merged_params = {
            **merged_params,
            "objective": "lambdarank",
            "metric": ["ndcg", "map"],
            "ndcg_eval_at": [5, 10, 20],
            "map_eval_at": [10],
            "label_gain": [0, 1],
        }

    rng = np.random.RandomState(merged_params.get("seed", 42))
    cat_cols = [c for c in CATEGORICAL_FEATURES if c in X.columns]

    if objective == "lambdarank":
        # Group-level split: keep all rows of a protein together so LightGBM
        # can compute listwise gradients over a valid ranking, and so val
        # groups are never seen during training.
        order = np.argsort(group_ids, kind="stable")
        X_sorted = X.iloc[order].reset_index(drop=True)
        y_sorted = y.iloc[order].reset_index(drop=True)
        gids_sorted = np.asarray(group_ids)[order]
        sw_sorted = sample_weight[order] if sample_weight is not None else None

        unique_groups, first_idx = np.unique(gids_sorted, return_index=True)
        order_by_first = np.argsort(first_idx)
        unique_groups = unique_groups[order_by_first]
        first_idx = first_idx[order_by_first]
        sizes = np.diff(np.append(first_idx, len(gids_sorted)))

        perm = np.arange(len(unique_groups))
        rng.shuffle(perm)
        n_val_groups = max(1, int(len(unique_groups) * val_fraction))
        val_groups = set(unique_groups[perm[:n_val_groups]].tolist())

        train_rows: list[int] = []
        val_rows: list[int] = []
        train_sizes: list[int] = []
        val_sizes: list[int] = []
        for g, start, size in zip(unique_groups, first_idx, sizes, strict=False):
            stop = start + size
            if g in val_groups:
                val_rows.extend(range(start, stop))
                val_sizes.append(int(size))
            else:
                train_rows.extend(range(start, stop))
                train_sizes.append(int(size))

        train_idx = np.asarray(train_rows, dtype=np.int64)
        val_idx = np.asarray(val_rows, dtype=np.int64)

        train_w = sw_sorted[train_idx] if sw_sorted is not None else None
        val_w = sw_sorted[val_idx] if sw_sorted is not None else None

        train_ds = lgb.Dataset(
            X_sorted.iloc[train_idx],
            label=y_sorted.iloc[train_idx],
            weight=train_w,
            group=train_sizes,
            categorical_feature=cat_cols,
            free_raw_data=False,
        )
        val_ds = lgb.Dataset(
            X_sorted.iloc[val_idx],
            label=y_sorted.iloc[val_idx],
            weight=val_w,
            group=val_sizes,
            categorical_feature=cat_cols,
            reference=train_ds,
            free_raw_data=False,
        )
        # Rebind X/y so that downstream metric computation uses the sorted view
        X, y = X_sorted, y_sorted
    else:
        # Stratified row-level train/val split (binary classification).
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

        if neg_pos_ratio is not None:
            max_train_neg = max(1, int(len(train_pos) * neg_pos_ratio))
            if len(train_neg) > max_train_neg:
                train_neg = train_neg[:max_train_neg]
            max_val_neg = max(1, int(len(val_pos) * neg_pos_ratio))
            if len(val_neg) > max_val_neg:
                val_neg = val_neg[:max_val_neg]

        val_idx = np.concatenate([val_pos, val_neg])
        train_idx = np.concatenate([train_pos, train_neg])

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

    callbacks: list[Any] = [
        lgb.early_stopping(early_stopping_rounds, verbose=False),
        lgb.log_evaluation(period=0),
    ]
    if heartbeat is not None and heartbeat_period > 0:
        def _heartbeat_cb(env: Any) -> None:
            it = env.iteration + 1
            if it % heartbeat_period == 0 or it == env.end_iteration:
                heartbeat(it)
        _heartbeat_cb.order = 20  # after early_stopping
        _heartbeat_cb.before_iteration = False
        callbacks.append(_heartbeat_cb)

    booster = lgb.train(
        merged_params,
        train_ds,
        num_boost_round=num_boost_round,
        valid_sets=[val_ds],
        valid_names=["val"],
        callbacks=callbacks,  # type: ignore[arg-type]
    )

    # Collect validation metrics
    val_preds = np.asarray(booster.predict(X.iloc[val_idx]))
    val_labels = y.iloc[val_idx].values

    best = booster.best_score.get("val", {}) or {}
    metrics: dict[str, Any] = {
        "best_iteration": booster.best_iteration,
        "objective": objective,
        "train_samples": int(len(train_idx)),
        "val_samples": int(len(val_idx)),
        "positive_rate": round(float(y.mean()), 4),
    }

    if objective == "lambdarank":
        for k_at in (5, 10, 20):
            key = f"ndcg@{k_at}"
            if key in best:
                metrics[f"val_{key}"] = round(float(best[key]), 4)
        if "map@10" in best:
            metrics["val_map@10"] = round(float(best["map@10"]), 4)
    else:
        tp = np.sum((val_preds >= 0.5) & (val_labels == 1))
        fp = np.sum((val_preds >= 0.5) & (val_labels == 0))
        fn = np.sum((val_preds < 0.5) & (val_labels == 1))
        precision = float(tp / (tp + fp)) if (tp + fp) > 0 else 0.0
        recall = float(tp / (tp + fn)) if (tp + fn) > 0 else 0.0
        f1 = (
            2 * precision * recall / (precision + recall)
            if (precision + recall) > 0
            else 0.0
        )
        metrics.update(
            {
                "val_auc": float(best.get("auc", 0.0)),
                "val_logloss": float(best.get("binary_logloss", 0.0)),
                "val_precision": round(precision, 4),
                "val_recall": round(recall, 4),
                "val_f1": round(f1, 4),
            }
        )

    importance = dict(
        zip(
            booster.feature_name(),
            booster.feature_importance(importance_type="gain").tolist(),
            strict=False,
        )
    )

    return TrainResult(model=booster, metrics=metrics, feature_importance=importance)


# ---------------------------------------------------------------------------
# Inference
# ---------------------------------------------------------------------------


def predict(model: lgb.Booster, df: pd.DataFrame) -> np.ndarray:
    """Score predictions using a trained re-ranker.

    Returns an array of scores in [0, 1] where higher = more likely correct.
    For lambdarank boosters, raw scores are unbounded reals; we apply a
    sigmoid to calibrate them into the [0, 1] range expected by the
    downstream CAFA evaluator (which sweeps thresholds from 0 to 1).
    Binary boosters already emit probabilities, so we leave them alone.
    """
    if LABEL_COLUMN in df.columns:
        X, _ = prepare_dataset(df)
    else:
        # Align to the model's actual feature set. Older boosters were trained
        # before consensus features existed; newer ones expect them but the
        # columns aren't persisted in GOPrediction, so fill missing as NaN
        # (LightGBM routes NaN down the "missing" branch natively).
        model_features = list(model.feature_name())
        aligned = df.copy()
        for col in model_features:
            if col not in aligned.columns:
                aligned[col] = pd.NA
        X = aligned[model_features].copy()
        for col in model_features:
            if col in NUMERIC_FEATURES:
                X[col] = pd.to_numeric(X[col], errors="coerce")
            elif col in CATEGORICAL_FEATURES:
                X[col] = X[col].replace("", pd.NA).astype("category")

    raw = np.asarray(model.predict(X))
    if raw.size == 0:
        return raw
    # Binary classification always returns probabilities in [0, 1]; any
    # score outside that range must come from a ranking objective.
    if float(raw.min()) < 0.0 or float(raw.max()) > 1.0:
        return 1.0 / (1.0 + np.exp(-raw))
    return raw


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
