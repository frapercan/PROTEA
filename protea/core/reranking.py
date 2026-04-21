"""Inference-side helpers for applying a trained lab re-ranker to PROTEA
predictions.

Responsibilities kept deliberately small:

* :func:`load_reranker` — download the booster once from the
  :class:`~protea.infrastructure.storage.ArtifactStore`, cache it on
  disk (keyed by ``feature_schema_sha``), and return a loaded
  ``lgb.Booster``.
* :func:`apply_reranker` — score a DataFrame with the booster and return
  the scores aligned to the input rows.
* :func:`infer_active_feature_families` — mirror of the predict-time
  flag set so the batch worker can compute a live
  ``feature_schema_sha`` and compare it against the value stored on the
  ``RerankerModel``.

Schema-sha validation is load-bearing: if the live feature set differs
from what the booster was trained on we refuse to rerank (caller falls
back to KNN distance ordering) rather than silently scoring rows with
missing columns filled as NaN.
"""

from __future__ import annotations

import logging
import threading
from pathlib import Path
from typing import TYPE_CHECKING

from protea.infrastructure.storage import ArtifactStore, LocalFsArtifactStore

if TYPE_CHECKING:  # pragma: no cover
    import lightgbm as lgb
    import numpy as np
    import pandas as pd

logger = logging.getLogger(__name__)

_BOOSTER_CACHE: dict[str, "lgb.Booster"] = {}
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


def load_reranker(
    artifact_uri: str,
    *,
    feature_schema_sha: str,
    store: ArtifactStore,
    cache_dir: Path | None = None,
) -> "lgb.Booster":
    """Fetch (once) and load a LightGBM booster by URI.

    The first call materialises the booster blob under
    ``cache_dir/<feature_schema_sha>.txt``; subsequent calls reuse the
    on-disk file *and* an in-process booster cache keyed by the sha.

    ``store`` is used only when the cached file does not exist —
    ``artifact_uri`` is expected to resolve to a store key but the store
    implementation chooses whether to parse it (``LocalFsArtifactStore``
    ignores the URI and resolves keys from its root; MinIO derives the
    key from the ``s3://bucket/key`` URI).
    """
    import lightgbm as lgb

    with _CACHE_LOCK:
        cached = _BOOSTER_CACHE.get(feature_schema_sha)
        if cached is not None:
            return cached

    cache_dir = cache_dir or _default_cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = _cache_path(cache_dir, feature_schema_sha)

    if not path.exists():
        key = _uri_to_key(artifact_uri, store)
        blob = store.get(key)
        path.write_bytes(blob)
        logger.info("cached reranker booster at %s (%d bytes)", path, len(blob))

    booster = lgb.Booster(model_file=str(path))
    with _CACHE_LOCK:
        _BOOSTER_CACHE[feature_schema_sha] = booster
    return booster


def _uri_to_key(artifact_uri: str, store: ArtifactStore) -> str:
    """Best-effort URI → store-key translation.

    ``LocalFsArtifactStore`` supports absolute ``file://`` URIs but also
    accepts plain keys relative to its root. ``MinioArtifactStore``
    expects ``s3://bucket/key``. This helper extracts a reasonable key
    from either form without depending on the concrete class.
    """
    if artifact_uri.startswith("s3://"):
        # s3://bucket/key/part
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


def apply_reranker(
    df: "pd.DataFrame",
    booster: "lgb.Booster",
    *,
    feature_cols: list[str] | None = None,
) -> "np.ndarray":
    """Score ``df`` with ``booster`` and return an aligned array.

    If ``feature_cols`` is None we use the booster's own
    ``feature_name()``. Missing columns are filled with ``pd.NA`` so
    LightGBM routes them through its native missing-value branch rather
    than crashing on KeyError.
    """
    import numpy as np
    import pandas as pd

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
        families.extend(["anc2vec_neighbor", "anc2vec_query", "emb_pca", "taxonomy_voters", "go_context"])
    # Sorted for stable sha computation.
    return sorted(set(families))
