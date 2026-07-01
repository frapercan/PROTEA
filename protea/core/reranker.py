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
from typing import Any

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
    load_from_bytes,
    model_from_string,
    predict,
    prepare_dataset,
)
from protea_method.reranker import (
    infer_active_feature_families as _lib_infer_active_feature_families,
)

from protea.core.domain.aspect import reranker_aspect_code
from protea.infrastructure.storage import ArtifactStore, LocalFsArtifactStore

logger = logging.getLogger(__name__)


class RerankerFeatureDtypeError(Exception):
    """A booster-expected feature arrived as an un-encodable object column.

    Raised by :func:`prepare_reranker_frame` when, after deriving the ad-hoc
    reranker features, a column the booster lists in ``feature_name()`` is
    still ``object`` dtype and is NOT one of the governed categorical columns
    the scorer label-encodes. Without this guard the failure surfaces deep in
    LightGBM as the cryptic ``pandas dtypes must be int, float or bool`` error
    (the original ``aspect_code: object`` incident); raising here names the
    offending column so an ungoverned-feature gap is loud, not silent.
    """


def prepare_reranker_frame(booster: lgb.Booster, df: Any) -> Any:
    """Return ``df`` augmented with ad-hoc reranker features + object guard.

    Two responsibilities, both keyed off the booster's own
    ``feature_name()`` list:

    * **Derive ``aspect_code``** when the booster expects it. The offline
      lab bakes ``aspect_code`` (``mfo -> 0`` / ``bpo -> 1`` / ``cco -> 2``)
      as a numeric feature, but it is NOT part of the governed
      ``protea_contracts`` feature schema, so the generic
      :func:`predict` / :func:`apply_reranker` helpers never build it.
      Serve/eval prediction rows carry the single-char aspect (``P`` /
      ``F`` / ``C``); we map it through
      :func:`protea.core.domain.aspect.reranker_aspect_code` to the SAME
      integer the booster split on. Absent when the booster does not list
      it (backward compatible).

    * **Guard ungoverned object columns.** After the derivation, any
      booster-expected column still sitting as ``object`` dtype (and not a
      governed categorical the scorer label-encodes) raises
      :class:`RerankerFeatureDtypeError` naming the column, instead of
      letting LightGBM emit its opaque bad-dtype error at predict time.

    A shallow copy is returned so callers passing a DataFrame slice (e.g.
    the per-aspect eval path) are never mutated in place.
    """
    import numpy as np
    import pandas as pd

    feats = list(booster.feature_name())
    out = df.copy()

    if "aspect_code" in feats and "aspect" in out.columns:
        codes = out["aspect"].map(reranker_aspect_code)
        if codes.isna().any():
            out["aspect_code"] = pd.to_numeric(codes, errors="coerce").astype(np.float64)
        else:
            out["aspect_code"] = codes.astype(np.int64)

    _guard_ungoverned_object_columns(feats, out)
    return out


def _guard_ungoverned_object_columns(feats: list[str], df: Any) -> None:
    """Raise if a booster-expected feature is an un-encodable non-numeric column.

    Columns in ``NUMERIC_FEATURES`` are numeric-coerced by the scorer and
    columns in ``CATEGORICAL_FEATURES`` are label-encoded, so both are safe
    to leave as-is here. Any OTHER expected column that reaches predict as a
    non-numeric dtype (an ``object`` / string column LightGBM cannot ingest
    and the scorer will not coerce) is exactly the failure mode this guard
    exists to surface loudly. The check is on dtype-family (not literal
    ``== object``) so it also catches pandas' inferred ``str`` extension
    dtype.
    """
    import pandas as pd

    governed = set(NUMERIC_FEATURES) | set(CATEGORICAL_FEATURES)
    bad = [
        col
        for col in feats
        if col not in governed
        and col in df.columns
        and not pd.api.types.is_numeric_dtype(df[col].dtype)
        and not pd.api.types.is_bool_dtype(df[col].dtype)
    ]
    if bad:
        raise RerankerFeatureDtypeError(
            "reranker feature column(s) reached scoring as object dtype and "
            "are not governed numeric/categorical features: "
            f"{sorted(bad)}. This is an ungoverned ad-hoc feature the serve "
            "path does not know how to encode; add an explicit producer in "
            "prepare_reranker_frame (see aspect_code) rather than coercing "
            "silently."
        )


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
        rest = artifact_uri[len("s3://") :]
        _, _, key = rest.partition("/")
        return key
    if artifact_uri.startswith("file://") and isinstance(store, LocalFsArtifactStore):
        local_path = Path(artifact_uri[len("file://") :])
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


def infer_active_feature_families(
    *,
    compute_alignments: bool,
    compute_taxonomy: bool,
    compute_v6_features: bool,
    compute_lineage_features: bool = False,
) -> list[str]:
    """Map predict-time feature flags onto lab feature families.

    Thin PROTEA-side extension of
    :func:`protea_method.reranker.infer_active_feature_families`: it
    delegates to the library helper for the base families
    (``knn`` / ``annotation_meta`` / ``alignment_nw`` / ``length`` /
    ``taxonomy_pair`` / the v6 bundle) and layers the GO-DAG ``lineage``
    family on top when ``compute_lineage_features`` is set.

    The lineage family is governed here (not in the library) because it
    is opt-in at serve time via the ``compute_lineage_features`` payload
    flag and must round-trip through
    :func:`protea_contracts.compute_feature_schema_sha` so a booster
    trained with lineage gets a matching live schema sha.

    Invariant (load-bearing for the FARM-EXP.5 schema-sha guard): with
    ``compute_lineage_features=False`` (the default) the returned family
    list is byte-identical to the library output for every
    align/tax/v6 combination, so the eight existing schema shas
    (registered trio ``7fcecf26aa0a`` etc.) are unchanged. Setting the
    flag appends exactly the ``lineage`` family, yielding a new sha
    (e.g. align+tax+no-v6+lineage -> ``0810bef8fd4d``).
    """
    families = _lib_infer_active_feature_families(
        compute_alignments=compute_alignments,
        compute_taxonomy=compute_taxonomy,
        compute_v6_features=compute_v6_features,
    )
    if compute_lineage_features:
        families = sorted({*families, "lineage"})
    return families


__all__ = [
    "ALL_FEATURES",
    "CATEGORICAL_FEATURES",
    "EMBEDDING_PCA_DIM",
    "LABEL_COLUMN",
    "NUMERIC_FEATURES",
    "RerankerFeatureDtypeError",
    "apply_reranker",
    "fit_embedding_pca",
    "infer_active_feature_families",
    "load_from_bytes",
    "load_reranker",
    "model_from_string",
    "predict",
    "prepare_dataset",
    "prepare_reranker_frame",
]
