"""LightGBM re-ranker — thin shim over ``protea_method.reranker``.

The pure helpers (feature-column constants, ``prepare_dataset``,
``predict``, ``apply_reranker``, ``model_from_string``,
``fit_embedding_pca``, ``infer_active_feature_families``) live in
the standalone ``protea-method`` library (F2C extraction,
2026-05-07). This module re-exports them and keeps the
``ArtifactStore``-bound loader (``load_reranker`` and friends) local
because they touch PROTEA-specific filesystem layout and the
artifact-store abstraction.

Existing call sites that import from ``protea.core.reranker`` keep
working without changes; new code should pull pure helpers directly
from ``protea_method.reranker``.
"""

from __future__ import annotations

import hashlib
import logging
import threading
from pathlib import Path

import lightgbm as lgb

# Re-exports: pure helpers + feature-column constants.
from protea_method.reranker import (
    ALL_FEATURES,
    CATEGORICAL_FEATURES,
    EMBEDDING_PCA_DIM,
    LABEL_COLUMN,
    NUMERIC_FEATURES,
    apply_reranker,
    fit_embedding_pca,
    infer_active_feature_families,
    load_from_bytes,
    model_from_string,
    predict,
    prepare_dataset,
)

from protea.infrastructure.storage import ArtifactStore, LocalFsArtifactStore

logger = logging.getLogger(__name__)


_BOOSTER_CACHE: dict[str, lgb.Booster] = {}
_CACHE_LOCK = threading.Lock()


def _default_cache_dir() -> Path:
    """Directory where booster blobs are cached between jobs.

    Mirrors the existing ``storage/`` layout so the reaper / ops
    tooling only needs to know one root.
    """
    return Path(__file__).resolve().parents[2] / "storage" / "reranker_cache"


def _cache_path(cache_dir: Path, feature_schema_sha: str) -> Path:
    safe = "".join(ch for ch in feature_schema_sha if ch.isalnum())[:32] or "booster"
    return cache_dir / f"{safe}.txt"


def _uri_to_key(artifact_uri: str, store: ArtifactStore) -> str:
    """Best-effort URI to store-key translation.

    ``LocalFsArtifactStore`` supports absolute ``file://`` URIs but
    also accepts plain keys relative to its root. ``MinioArtifactStore``
    expects ``s3://bucket/key``. This helper extracts a reasonable
    key from either form without depending on the concrete class.
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
    ``cache_dir/<feature_schema_sha>_<uri_tag>.txt``; subsequent
    calls reuse the on-disk file *and* an in-process booster cache
    keyed by the URI.

    ``store`` is used only when the cached file does not exist;
    ``artifact_uri`` is expected to resolve to a store key but the
    store implementation chooses whether to parse it
    (``LocalFsArtifactStore`` ignores the URI and resolves keys from
    its root; MinIO derives the key from the ``s3://bucket/key``
    URI).

    The on-disk cache is namespaced by ``feature_schema_sha``
    because each sha represents a stable column layout; multiple
    boosters that share a sha need to disambiguate by URI to avoid
    the in-process cache returning the first-loaded booster for
    every cell.
    """
    with _CACHE_LOCK:
        cached = _BOOSTER_CACHE.get(artifact_uri)
        if cached is not None:
            return cached

    cache_dir = cache_dir or _default_cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    # MD5 used as a cache-key tag, not a security primitive
    # (collision resistance is irrelevant for the disambiguation
    # purpose). usedforsecurity=False silences the bandit hint.
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


__all__ = [
    "ALL_FEATURES",
    "CATEGORICAL_FEATURES",
    "EMBEDDING_PCA_DIM",
    "LABEL_COLUMN",
    "NUMERIC_FEATURES",
    "apply_reranker",
    "fit_embedding_pca",
    "infer_active_feature_families",
    "load_from_bytes",
    "load_reranker",
    "model_from_string",
    "predict",
    "prepare_dataset",
]
