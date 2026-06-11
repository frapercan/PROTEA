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
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any, NamedTuple

import numpy as np

_DISK_CACHE_DIR = Path(os.environ.get("PROTEA_REF_CACHE_DIR", "data/ref_cache"))


class AnnoCsr(NamedTuple):
    """CSR-style annotation cache for one ``(EmbeddingConfig, AnnotationSet, aspect)``.

    Bundles the four parallel arrays produced by :func:`_build_anno_csr`
    and consumed by :func:`_csr_lookup` / :func:`_save_anno_csr_to_disk`.
    NamedTuple so existing 4-tuple unpacking patterns
    (``gtids, quals, ecodes, offsets = csr``) keep working alongside
    the named accessors (``csr.gtids``).

    Layout: annotations for accession ``i`` live at index range
    ``offsets[i]:offsets[i + 1]`` in the three flat arrays.
    """

    gtids: np.ndarray
    quals: np.ndarray
    ecodes: np.ndarray
    offsets: np.ndarray


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
) -> AnnoCsr:
    """Build a CSR-style annotation structure for the given accession list.

    Returns an :class:`AnnoCsr` whose flat arrays satisfy the layout
    invariant: annotations for ``accessions[i]`` live at indices
    ``offsets[i]:offsets[i + 1]``.
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
    return AnnoCsr(
        gtids=np.array(all_gtids, dtype=np.int32),
        quals=np.array(all_quals, dtype=object),
        ecodes=np.array(all_ecodes, dtype=object),
        offsets=np.array(offsets, dtype=np.int32),
    )


def _load_anno_csr_from_disk(
    embedding_config_id: uuid.UUID,
    annotation_set_id: uuid.UUID,
    aspect: str,
) -> AnnoCsr | None:
    """Load annotation CSR arrays from disk. Returns None on miss or error."""
    gtids_p, quals_p, ecodes_p, offsets_p = _anno_disk_cache_paths(
        embedding_config_id, annotation_set_id, aspect
    )
    if not all(p.exists() for p in (gtids_p, quals_p, ecodes_p, offsets_p)):
        return None
    try:
        return AnnoCsr(
            gtids=np.load(gtids_p),
            quals=np.load(quals_p, allow_pickle=True),
            ecodes=np.load(ecodes_p, allow_pickle=True),
            offsets=np.load(offsets_p),
        )
    except Exception:
        return None


def _save_anno_csr_to_disk(
    embedding_config_id: uuid.UUID,
    annotation_set_id: uuid.UUID,
    aspect: str,
    csr: AnnoCsr,
) -> None:
    gtids_p, quals_p, ecodes_p, offsets_p = _anno_disk_cache_paths(
        embedding_config_id, annotation_set_id, aspect
    )
    gtids_p.parent.mkdir(parents=True, exist_ok=True)
    np.save(gtids_p, csr.gtids)
    np.save(quals_p, csr.quals)
    np.save(ecodes_p, csr.ecodes)
    np.save(offsets_p, csr.offsets)


def _csr_lookup(
    query_accessions: set[str],
    acc_to_anno_idx: dict[str, int],
    csr: AnnoCsr,
) -> dict[str, list[dict[str, Any]]]:
    """Return a go_map for query_accessions using the preloaded CSR annotation cache."""
    gtids, quals, ecodes, offsets = csr
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
    *,
    need_cos: bool = True,
    need_plain: bool = True,
) -> dict[str, Any]:
    """Build the per-process reference view dict used by the KNN path.

    Stores precomputed f32 copies of the embeddings so the hot path avoids an
    ``astype(np.float32)`` and an ``np.linalg.norm`` on every batch (together
    ~9 s per batch at 555k × 1280). The KNN search reads exactly one of them
    per run: ``embeddings_f32_cos`` (L2-normalised) for cosine, otherwise the
    raw ``embeddings_f32``. ``need_cos`` / ``need_plain`` let callers skip the
    copy this run will not read, load-bearing for high-dim PLMs where each
    copy is tens of GB (ESM2-3B at 2560 dims). The unused slot is set to
    ``None``; downstream readers already select by metric and the PCA pool
    builder guards with ``is not None``. Both default to ``True`` so existing
    callers keep the old behaviour.

    The original f16 array is also kept on the result dict for any consumer
    that still expects it (e.g. size reporting), but the hot path reads the
    f32 views directly.
    """
    if not accessions or embeddings_f16.size == 0:
        return {
            "accessions": accessions,
            "embeddings": embeddings_f16,
            "embeddings_f32": np.empty((0,), dtype=np.float32),
            "embeddings_f32_cos": np.empty((0,), dtype=np.float32),
        }
    emb_f32 = embeddings_f16.astype(np.float32)
    emb_f32_cos = None
    if need_cos:
        norms = np.linalg.norm(emb_f32, axis=1, keepdims=True)
        emb_f32_cos = emb_f32 / (norms + 1e-9)
    return {
        "accessions": accessions,
        "embeddings": embeddings_f16,
        "embeddings_f32": emb_f32 if need_plain else None,
        "embeddings_f32_cos": emb_f32_cos,
    }


def _is_cache_fresh(
    embedding_config_id: uuid.UUID,
    annotation_set_id: uuid.UUID,
    freshness_seconds: int,
) -> bool:
    """Return True when both cache files exist and their oldest mtime is within
    ``freshness_seconds`` of wall-clock now.

    When ``freshness_seconds == 0`` the freshness check is disabled and the
    function always returns False (COUNT(*) is always issued).  Callers use
    this to skip the expensive COUNT(*)-over-JOIN validation when the disk
    cache is known to be recent.
    """
    if freshness_seconds <= 0:
        return False
    emb_path, acc_path = _disk_cache_paths(embedding_config_id, annotation_set_id)
    if not emb_path.exists() or not acc_path.exists():
        return False
    oldest_mtime = min(emb_path.stat().st_mtime, acc_path.stat().st_mtime)
    return (time.time() - oldest_mtime) < freshness_seconds


def _load_from_disk_cache(
    embedding_config_id: uuid.UUID,
    annotation_set_id: uuid.UUID,
    expected_count: int | None = None,
) -> dict[str, Any] | None:
    """Load the cached reference pool, optionally validating its row count.

    When ``expected_count`` is supplied (e.g. a fresh ``COUNT(*)`` from
    the DB), the cached row count must match exactly or the cache is
    treated as stale and ``None`` is returned. This lets callers detect
    drift after a reference re-ingest without needing to delete the
    cache files by hand.
    """
    emb_path, acc_path = _disk_cache_paths(embedding_config_id, annotation_set_id)
    if not emb_path.exists() or not acc_path.exists():
        return None
    try:
        embeddings = np.load(emb_path)
        accessions = list(np.load(acc_path))
    except Exception:
        return None
    if expected_count is not None and len(accessions) != expected_count:
        return None
    return {"accessions": accessions, "embeddings": embeddings}


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


def _emit_cache_hit(
    emit: Callable[[str, dict[str, Any]], None] | None,
    emb_path: Path,
    cached: dict[str, Any],
    *,
    fresh: bool = False,
    freshness_seconds: int = 0,
) -> None:
    """Emit a ``refpool.cache_hit`` or ``refpool.cache_hit_fresh`` audit event."""
    if emit is None:
        return
    if fresh:
        emit(
            "refpool.cache_hit_fresh",
            {
                "path": str(emb_path),
                "rows": len(cached["accessions"]),
                "bytes": int(cached["embeddings"].nbytes),
                "freshness_seconds": freshness_seconds,
            },
        )
    else:
        emit(
            "refpool.cache_hit",
            {
                "path": str(emb_path),
                "rows": len(cached["accessions"]),
                "bytes": int(cached["embeddings"].nbytes),
            },
        )


def _load_and_write_cache(
    embedding_config_id: uuid.UUID,
    annotation_set_id: uuid.UUID,
    db_loader: Callable[[], tuple[list[str], np.ndarray]],
    emit: Callable[[str, dict[str, Any]], None] | None,
    emb_path: Path,
) -> tuple[list[str], np.ndarray]:
    """Invoke ``db_loader``, persist to disk, emit ``cache_write`` audit event."""
    if emit is not None and emb_path.exists():
        emit("refpool.cache_stale", {"path": str(emb_path)})
    accessions, embeddings = db_loader()
    _save_to_disk_cache(embedding_config_id, annotation_set_id, accessions, embeddings)
    if emit is not None:
        size_bytes = emb_path.stat().st_size if emb_path.exists() else int(embeddings.nbytes)
        emit(
            "refpool.cache_write",
            {"path": str(emb_path), "rows": len(accessions), "bytes": int(size_bytes)},
        )
    return accessions, embeddings


def _load_reference_pool_cached(
    embedding_config_id: uuid.UUID,
    annotation_set_id: uuid.UUID,
    db_loader: Callable[[], tuple[list[str], np.ndarray]],
    *,
    expected_count: int | None = None,
    emit: Callable[[str, dict[str, Any]], None] | None = None,
    freshness_seconds: int = 0,
) -> tuple[list[str], np.ndarray]:
    """Return ``(accessions, embeddings)`` for the reference pool with disk-cache.

    ``db_loader`` is invoked only on miss or when the cached row count diverges
    from ``expected_count``.  When ``freshness_seconds > 0`` and the cache
    files are newer than that window, the COUNT(*) validation is skipped
    entirely; a ``refpool.cache_hit_fresh`` audit event is emitted instead of
    ``refpool.cache_hit``.  ``emit`` receives ``refpool.cache_hit`` /
    ``refpool.cache_hit_fresh`` / ``refpool.cache_stale`` /
    ``refpool.cache_write`` events; pass ``None`` to stay quiet.
    """
    emb_path, _ = _disk_cache_paths(embedding_config_id, annotation_set_id)
    if freshness_seconds > 0 and _is_cache_fresh(
        embedding_config_id, annotation_set_id, freshness_seconds
    ):
        cached = _load_from_disk_cache(embedding_config_id, annotation_set_id)
        if cached is not None:
            _emit_cache_hit(emit, emb_path, cached, fresh=True, freshness_seconds=freshness_seconds)
            return cached["accessions"], cached["embeddings"]
    cached = _load_from_disk_cache(
        embedding_config_id, annotation_set_id, expected_count=expected_count
    )
    if cached is not None:
        _emit_cache_hit(emit, emb_path, cached)
        return cached["accessions"], cached["embeddings"]
    return _load_and_write_cache(embedding_config_id, annotation_set_id, db_loader, emit, emb_path)


__all__ = [
    "_DISK_CACHE_DIR",
    "_anno_disk_cache_paths",
    "_aspect_index_path",
    "_build_anno_csr",
    "_csr_lookup",
    "_derive_reference_views",
    "_disk_cache_paths",
    "_is_cache_fresh",
    "_load_anno_csr_from_disk",
    "_load_from_disk_cache",
    "_load_reference_pool_cached",
    "_save_anno_csr_to_disk",
    "_save_to_disk_cache",
]
