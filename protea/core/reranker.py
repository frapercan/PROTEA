"""LightGBM re-ranker — inference helpers for GO term predictions.

Training has been moved to ``protea-reranker-lab``. This module now only
provides the feature-column schema, a ``prepare_dataset`` helper shared
with the lab (so inference-time input matches training-time input), the
``predict`` / ``model_from_string`` calls used by the scoring router,
and the ArtifactStore-backed booster loader (``load_reranker`` /
``apply_reranker``) used by ``predict_go_terms_batch``.

Feature columns are the numeric signals stored in ``GOPrediction``.
Categorical features (``qualifier``, ``evidence_code``,
``taxonomic_relation``) are label-encoded. Missing values are left as
NaN — LightGBM handles them natively.

Schema-sha validation is load-bearing: if the live feature set differs
from what the booster was trained on we refuse to rerank (caller falls
back to KNN distance ordering) rather than silently scoring rows with
missing columns filled as NaN.
"""

from __future__ import annotations

import logging
import threading
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd

# T1.5 of master plan v3: the feature schema is owned by protea-contracts.
# Re-export here so existing call sites that import from
# ``protea.core.reranker`` keep working; new code should import from
# ``protea_contracts`` directly.
from protea_contracts import (
    ALL_FEATURES,
    CATEGORICAL_FEATURES,
    EMBEDDING_PCA_DIM,
    LABEL_COLUMN,
    NUMERIC_FEATURES,
)

from protea.infrastructure.storage import ArtifactStore, LocalFsArtifactStore

logger = logging.getLogger(__name__)


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


# ---------------------------------------------------------------------------
# Data preparation
# ---------------------------------------------------------------------------


def prepare_dataset(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """Extract feature matrix and label vector from a training DataFrame.

    Categorical columns are label-encoded to int64 codes (missing → -1)
    via :func:`pandas.factorize` — this mirrors
    ``protea-reranker-lab.reranker.encode_categoricals`` so a booster
    trained either inline here or in the lab can be scored by the same
    ``predict`` helper without LightGBM's "categorical_feature do not
    match" error firing on cross-instance imports.

    Returns ``(X, y)`` where X has only the feature columns and y is
    the binary label.
    """
    X = df[ALL_FEATURES].copy()
    for col in NUMERIC_FEATURES:
        if col in X.columns:
            X[col] = pd.to_numeric(X[col], errors="coerce")
    for col in CATEGORICAL_FEATURES:
        if col in X.columns:
            s = X[col].replace("", pd.NA)
            s = s.astype("object").where(s.notna(), None)
            codes, _ = pd.factorize(s, use_na_sentinel=True)
            X[col] = codes
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


def predict(
    model: lgb.Booster,
    df: pd.DataFrame,
    *,
    categorical_codes: dict[str, list[str]] | None = None,
) -> np.ndarray:
    """Score predictions using a trained re-ranker.

    Returns an array of scores in [0, 1] where higher = more likely correct.
    For lambdarank boosters, raw scores are unbounded reals; we apply a
    sigmoid to calibrate them into the [0, 1] range expected by the
    downstream CAFA evaluator (which sweeps thresholds from 0 to 1).
    Binary boosters already emit probabilities, so we leave them alone.

    ``categorical_codes`` is the per-column ordered string vocabulary the lab
    used at training time (``{column: [val0, val1, ...]}``). When provided,
    each cat column is encoded against this fixed vocabulary so the codes
    match training; when omitted, falls back to ``pd.factorize`` over the
    inference batch (correct only if the batch happens to contain the same
    set of values in the same order — usually wrong for small or
    aspect-filtered batches).
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
                s = X[col].astype("object").where(X[col].notna(), None)
                if categorical_codes and col in categorical_codes:
                    # Encode against the lab's training vocabulary. Values
                    # not seen at training (rare evidence codes etc.) fall to
                    # -1 (missing), matching how the lab handled NaN.
                    mapping = {v: i for i, v in enumerate(categorical_codes[col])}
                    # Bind ``mapping`` at lambda-definition time so the
                    # closure does not see a later iteration's value (B023).
                    X[col] = s.map(lambda v, m=mapping: m.get(v, -1)).astype("int64")
                else:
                    # No code map — fall back to the (broken-for-small-batch)
                    # legacy path. Logged as a warning by callers.
                    codes, _ = pd.factorize(s, use_na_sentinel=True)
                    X[col] = codes

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


# ---------------------------------------------------------------------------
# ArtifactStore-backed loader (used by predict_go_terms_batch)
# ---------------------------------------------------------------------------

_BOOSTER_CACHE: dict[str, lgb.Booster] = {}
_CACHE_LOCK = threading.Lock()


def _default_cache_dir() -> Path:
    """Directory where booster blobs are cached between jobs.

    Mirrors the existing ``storage/`` layout so the reaper / ops tooling
    only needs to know one root.
    """
    return Path(__file__).resolve().parents[2] / "storage" / "reranker_cache"


def _cache_path(cache_dir: Path, feature_schema_sha: str) -> Path:
    safe = "".join(ch for ch in feature_schema_sha if ch.isalnum())[:32] or "booster"
    return cache_dir / f"{safe}.txt"


def _uri_to_key(artifact_uri: str, store: ArtifactStore) -> str:
    """Best-effort URI → store-key translation.

    ``LocalFsArtifactStore`` supports absolute ``file://`` URIs but also
    accepts plain keys relative to its root. ``MinioArtifactStore``
    expects ``s3://bucket/key``. This helper extracts a reasonable key
    from either form without depending on the concrete class.
    """
    if artifact_uri.startswith("s3://"):
        rest = artifact_uri[len("s3://"):]
        _, _, key = rest.partition("/")
        return key
    if artifact_uri.startswith("file://") and isinstance(store, LocalFsArtifactStore):
        local_path = Path(artifact_uri[len("file://"):])
        root = Path(store.root).resolve()
        try:
            return str(local_path.resolve().relative_to(root))
        except ValueError:
            return str(local_path)
    return artifact_uri


def load_reranker(
    artifact_uri: str,
    *,
    feature_schema_sha: str,
    store: ArtifactStore,
    cache_dir: Path | None = None,
) -> lgb.Booster:
    """Fetch (once) and load a LightGBM booster by URI.

    The first call materialises the booster blob under
    ``cache_dir/<feature_schema_sha>.txt``; subsequent calls reuse the
    on-disk file *and* an in-process booster cache keyed by the URI.

    ``store`` is used only when the cached file does not exist —
    ``artifact_uri`` is expected to resolve to a store key but the store
    implementation chooses whether to parse it (``LocalFsArtifactStore``
    ignores the URI and resolves keys from its root; MinIO derives the
    key from the ``s3://bucket/key`` URI).

    The on-disk cache is namespaced by ``feature_schema_sha`` because each
    sha represents a stable column layout; multiple boosters that share a
    sha (e.g. all per-cell v10 boosters) need to disambiguate by URI to
    avoid the in-process cache returning the first-loaded booster for
    every cell — which silently uses the wrong model and produces
    identical TSVs across LK/PK at scoring time.
    """
    with _CACHE_LOCK:
        cached = _BOOSTER_CACHE.get(artifact_uri)
        if cached is not None:
            return cached

    cache_dir = cache_dir or _default_cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    # Disambiguate the on-disk cache file by URI hash so two boosters with
    # the same schema_sha don't overwrite each other's blobs. usedforsecurity=False
    # because this is a cache key tag, not a security primitive (MD5 collision
    # resistance is irrelevant here).
    import hashlib
    uri_tag = hashlib.md5(artifact_uri.encode(), usedforsecurity=False).hexdigest()[:8]
    path = cache_dir / f"{feature_schema_sha}_{uri_tag}.txt"

    if not path.exists():
        key = _uri_to_key(artifact_uri, store)
        blob = store.get(key)
        path.write_bytes(blob)
        logger.info("cached reranker booster at %s (%d bytes)", path, len(blob))

    booster = lgb.Booster(model_file=str(path))
    with _CACHE_LOCK:
        _BOOSTER_CACHE[artifact_uri] = booster
    return booster


def apply_reranker(
    df: pd.DataFrame,
    booster: lgb.Booster,
    *,
    feature_cols: list[str] | None = None,
) -> np.ndarray:
    """Score ``df`` with ``booster`` and return an aligned array.

    If ``feature_cols`` is None we use the booster's own
    ``feature_name()``. Missing columns are filled with ``pd.NA`` so
    LightGBM routes them through its native missing-value branch rather
    than crashing on KeyError.
    """
    cols = feature_cols or list(booster.feature_name())
    aligned = df.copy()
    for col in cols:
        if col not in aligned.columns:
            aligned[col] = np.nan
    X = aligned[cols].copy()
    # LightGBM rejects ``object``-dtype columns; coerce everything that
    # isn't a pandas categorical into numeric so missing values land as
    # NaN (routed through the native missing-value branch).
    for col in cols:
        if not isinstance(X[col].dtype, pd.CategoricalDtype):
            X[col] = pd.to_numeric(X[col], errors="coerce")
    raw = np.asarray(booster.predict(X))
    if raw.size == 0:
        return raw
    # Ranking objectives emit unbounded reals — calibrate into [0, 1] so
    # downstream thresholding remains uniform with binary boosters.
    if float(raw.min()) < 0.0 or float(raw.max()) > 1.0:
        return 1.0 / (1.0 + np.exp(-raw))
    return raw


def infer_active_feature_families(
    *,
    compute_alignments: bool,
    compute_taxonomy: bool,
    compute_v6_features: bool,
) -> list[str]:
    """Map the predict-time feature flags onto lab feature families.

    The PROTEA predict pipeline always materialises KNN features and
    annotation-meta columns (qualifier/evidence_code/aspect); the
    optional flags enable alignment, taxonomy-pair, taxonomy-voters,
    GO-context, anc2vec, emb-pca and length families. Keep this in sync
    with ``protea_reranker_lab.contracts.FEATURE_FAMILIES``.
    """
    families: list[str] = ["knn", "annotation_meta"]
    if compute_alignments:
        families.append("alignment_nw")
        families.append("length")
    if compute_taxonomy:
        families.append("taxonomy_pair")
    if compute_v6_features:
        families.extend(
            ["anc2vec_neighbor", "anc2vec_query", "emb_pca", "taxonomy_voters", "go_context"]
        )
    # Sorted for stable sha computation.
    return sorted(set(families))
