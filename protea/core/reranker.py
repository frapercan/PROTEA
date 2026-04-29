"""LightGBM re-ranker — inference helpers for GO term predictions.

Training has been moved to ``protea-reranker-lab``. This module now only
provides the feature-column schema, a ``prepare_dataset`` helper shared
with the lab (so inference-time input matches training-time input), and
the ``predict`` / ``model_from_string`` calls used by the scoring router
when scoring a prediction set with a registered booster.

Feature columns are the numeric signals stored in ``GOPrediction``.
Categorical features (``qualifier``, ``evidence_code``,
``taxonomic_relation``) are label-encoded. Missing values are left as
NaN — LightGBM handles them natively.
"""

from __future__ import annotations

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
# Training has been removed.
#
# LightGBM training lives in ``protea-reranker-lab``. PROTEA only keeps
# the inference path below (``predict`` + ``model_from_string``) so that
# registered boosters can score prediction sets at request time.
# ---------------------------------------------------------------------------


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


def model_from_string(model_str: str) -> lgb.Booster:
    """Deserialize a model from its string representation."""
    return lgb.Booster(model_str=model_str)
