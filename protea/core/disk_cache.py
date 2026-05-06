"""On-disk caches for the predict_go_terms pipeline.

The reference embedding pool for an ``(EmbeddingConfig, AnnotationSet)``
pair is large (millions of rows × ~1280 dims × 2 bytes = several GB).
Re-fetching it from PostgreSQL on every batch worker is the main
bottleneck, so the pool is materialised once into ``data/ref_cache/``
and read back via numpy mmap on subsequent runs. Annotations are stored
in a CSR-style layout (``offsets`` + flat ``go_term_ids`` /
``qualifiers`` / ``evidence_codes`` arrays) so per-protein lookups stay
O(1) instead of dictionary-of-list-of-dict.

Files (under ``_DISK_CACHE_DIR``):

* ``{cfg}__{ann}_embeddings.npy``    — float16 reference embeddings
* ``{cfg}__{ann}_accessions.npy``    — aligned accession list
* ``{cfg}__{ann}__{aspect}_indices.npy`` — int32 indices into the unified pool
* ``{cfg}__{ann}__{aspect}_anno_*.npy``  — CSR annotation arrays

Invalidation: ``AnnotationSet`` rows are immutable once loaded, so the
cache stays valid for as long as the files exist. Delete the files
manually to force a reload after a model change.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any

import numpy as np

_DISK_CACHE_DIR = Path(os.environ.get("PROTEA_REF_CACHE_DIR", "data/ref_cache"))


def _disk_cache_paths(
    embedding_config_id: uuid.UUID,
    annotation_set_id: uuid.UUID,
) -> tuple[Path, Path]:
    """Return (embeddings_path, accessions_path) for the unified reference cache."""
    key = f"{embedding_config_id}__{annotation_set_id}"
    return (
        _DISK_CACHE_DIR / f"{key}_embeddings.npy",
        _DISK_CACHE_DIR / f"{key}_accessions.npy",
    )


def _aspect_index_path(
    embedding_config_id: uuid.UUID,
    annotation_set_id: uuid.UUID,
    aspect: str,
) -> Path:
    """Return the path for the per-aspect index array (int32 indices into the unified cache)."""
    key = f"{embedding_config_id}__{annotation_set_id}"
    return _DISK_CACHE_DIR / f"{key}__{aspect}_indices.npy"


def _anno_disk_cache_paths(
    embedding_config_id: uuid.UUID,
    annotation_set_id: uuid.UUID,
    aspect: str,
) -> tuple[Path, Path, Path, Path]:
    """Return (gtids, quals, ecodes, offsets) paths for the annotation CSR cache."""
    key = f"{embedding_config_id}__{annotation_set_id}__{aspect}"
    base = _DISK_CACHE_DIR
    return (
        base / f"{key}_anno_gtids.npy",
        base / f"{key}_anno_quals.npy",
        base / f"{key}_anno_ecodes.npy",
        base / f"{key}_anno_offsets.npy",
    )


def _build_anno_csr(
    accessions: list[str],
    go_map: dict[str, list[dict[str, Any]]],
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Build a CSR-style annotation structure for the given accession list.

    Returns (go_term_ids, qualifiers, evidence_codes, offsets) where
    annotations for accessions[i] are at indices offsets[i]:offsets[i+1].
    """
    all_gtids: list[int] = []
    all_quals: list[Any] = []
    all_ecodes: list[Any] = []
    offsets: list[int] = [0]
    for acc in accessions:
        for ann in go_map.get(acc, []):
            all_gtids.append(ann["go_term_id"])
            all_quals.append(ann.get("qualifier"))
            all_ecodes.append(ann.get("evidence_code"))
        offsets.append(len(all_gtids))
    return (
        np.array(all_gtids, dtype=np.int32),
        np.array(all_quals, dtype=object),
        np.array(all_ecodes, dtype=object),
        np.array(offsets, dtype=np.int32),
    )


def _load_anno_csr_from_disk(
    embedding_config_id: uuid.UUID,
    annotation_set_id: uuid.UUID,
    aspect: str,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray] | None:
    """Load annotation CSR arrays from disk. Returns None on miss or error."""
    gtids_p, quals_p, ecodes_p, offsets_p = _anno_disk_cache_paths(
        embedding_config_id, annotation_set_id, aspect
    )
    if not all(p.exists() for p in (gtids_p, quals_p, ecodes_p, offsets_p)):
        return None
    try:
        return (
            np.load(gtids_p),
            np.load(quals_p, allow_pickle=True),
            np.load(ecodes_p, allow_pickle=True),
            np.load(offsets_p),
        )
    except Exception:
        return None


def _save_anno_csr_to_disk(
    embedding_config_id: uuid.UUID,
    annotation_set_id: uuid.UUID,
    aspect: str,
    gtids: np.ndarray,
    quals: np.ndarray,
    ecodes: np.ndarray,
    offsets: np.ndarray,
) -> None:
    gtids_p, quals_p, ecodes_p, offsets_p = _anno_disk_cache_paths(
        embedding_config_id, annotation_set_id, aspect
    )
    gtids_p.parent.mkdir(parents=True, exist_ok=True)
    np.save(gtids_p, gtids)
    np.save(quals_p, quals)
    np.save(ecodes_p, ecodes)
    np.save(offsets_p, offsets)


def _csr_lookup(
    query_accessions: set[str],
    accessions: list[str],
    acc_to_anno_idx: dict[str, int],
    gtids: np.ndarray,
    quals: np.ndarray,
    ecodes: np.ndarray,
    offsets: np.ndarray,
) -> dict[str, list[dict[str, Any]]]:
    """Return a go_map for query_accessions using the preloaded CSR annotation cache."""
    go_map: dict[str, list[dict[str, Any]]] = {}
    for acc in query_accessions:
        idx = acc_to_anno_idx.get(acc)
        if idx is None:
            continue
        start, end = int(offsets[idx]), int(offsets[idx + 1])
        if start >= end:
            continue
        go_map[acc] = [
            {
                "go_term_id": int(gtids[j]),
                "qualifier": quals[j] if quals[j] is not None else None,
                "evidence_code": ecodes[j] if ecodes[j] is not None else None,
            }
            for j in range(start, end)
        ]
    return go_map


def _derive_reference_views(
    accessions: list[str],
    embeddings_f16: np.ndarray,
) -> dict[str, Any]:
    """Build the per-process reference view dict used by the KNN path.

    Stores one f32 copy of the embeddings plus an L2-normalised f32 copy
    for cosine similarity. These are the inputs the search path consumes —
    keeping them precomputed avoids an ``astype(np.float32)`` and an
    ``np.linalg.norm`` on every batch (together ~9 s per batch at 555k × 1280).

    The original f16 array is also kept on the result dict for any consumer
    that still expects it (e.g. size reporting), but the hot path reads
    ``embeddings_f32`` and ``embeddings_f32_cos`` directly.
    """
    if not accessions or embeddings_f16.size == 0:
        return {
            "accessions": accessions,
            "embeddings": embeddings_f16,
            "embeddings_f32": np.empty((0,), dtype=np.float32),
            "embeddings_f32_cos": np.empty((0,), dtype=np.float32),
        }
    emb_f32 = embeddings_f16.astype(np.float32)
    norms = np.linalg.norm(emb_f32, axis=1, keepdims=True)
    emb_f32_cos = emb_f32 / (norms + 1e-9)
    return {
        "accessions": accessions,
        "embeddings": embeddings_f16,
        "embeddings_f32": emb_f32,
        "embeddings_f32_cos": emb_f32_cos,
    }


def _load_from_disk_cache(
    embedding_config_id: uuid.UUID,
    annotation_set_id: uuid.UUID,
) -> dict[str, Any] | None:
    emb_path, acc_path = _disk_cache_paths(embedding_config_id, annotation_set_id)
    if not emb_path.exists() or not acc_path.exists():
        return None
    try:
        embeddings = np.load(emb_path)
        accessions = list(np.load(acc_path))
        return {"accessions": accessions, "embeddings": embeddings}
    except Exception:
        return None


def _save_to_disk_cache(
    embedding_config_id: uuid.UUID,
    annotation_set_id: uuid.UUID,
    accessions: list[str],
    embeddings: np.ndarray,
) -> None:
    emb_path, acc_path = _disk_cache_paths(embedding_config_id, annotation_set_id)
    emb_path.parent.mkdir(parents=True, exist_ok=True)
    np.save(emb_path, embeddings)
    np.save(acc_path, np.array(accessions))


__all__ = [
    "_DISK_CACHE_DIR",
    "_anno_disk_cache_paths",
    "_aspect_index_path",
    "_build_anno_csr",
    "_csr_lookup",
    "_derive_reference_views",
    "_disk_cache_paths",
    "_load_anno_csr_from_disk",
    "_load_from_disk_cache",
    "_save_anno_csr_to_disk",
    "_save_to_disk_cache",
]
