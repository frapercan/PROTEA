"""The per-aspect indices into the unified pool, and the views cut from them.

Aspect-separated retrieval searches each aspect's donors: the members of the
unified pool with a positive row in that aspect. For each aspect the build
writes an index array into the unified pool, the fingerprint of the pool it was
built against and the annotation arrays it addresses, all under the pool's cache
key (``protea.core.disk_cache``), and a later run reads them back instead of
scanning the annotation set again.

AN ASPECT BUILT IN THIS PROCESS IS SERVED FROM MEMORY. When the tuning switch
``reference_pool_disk_write`` is off, a one-off pool is built in the worker and
nothing is written, so the arrays a run just built are what its views are cut
from (:class:`BuiltAspect`). With the switch on they are written as before and
served from the same arrays, which are the bytes the files hold.

Its own module because ``_batch_op_reference`` is at its size budget, the reason
``_reference_aspect`` and ``_reference_binary`` came out of it too.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, NamedTuple

import numpy as np

from protea.core.disk_cache import (
    AnnoCsr,
    _aspect_index_path,
    _aspect_index_pool_path,
    _load_anno_csr_from_disk,
    pool_fingerprint,
)
from protea.core.domain.aspect import ASPECT_CODES as _ASPECTS
from protea.core.operations.predict_go_terms._reference_aspect import _ASPECT_SLICE

if TYPE_CHECKING:
    from protea.core.operations.predict_go_terms._batch_op_reference import _PoolRequest

__all__ = [
    "BuiltAspect",
    "_assemble_aspect_view",
    "_check_index_addresses_pool",
    "_find_missing_aspects",
    "_recorded_pool",
]


class BuiltAspect(NamedTuple):
    """One aspect as this process built it: its index array, its annotations, its pool.

    ``pool`` is the fingerprint of the unified pool ``indices`` address, the value
    the build writes beside the index on disk.
    """

    indices: np.ndarray
    csr: AnnoCsr
    pool: str


def _find_missing_aspects(request: _PoolRequest, pool: str | None = None) -> list[str]:
    """Return aspects that cannot be served from disk for THIS pool.

    Takes the request so the discriminator travels with the ids: a permissive
    cache must not answer for a restricted pool. Takes the pool fingerprint so
    an index built against a different pool counts as missing and is rebuilt,
    rather than reaching the view and raising there. The reference pool grows
    whenever proteins are ingested, and a stale cache should cost one query,
    not a manual deletion before anything can run.

    ``pool`` of None skips that comparison, for callers with no pool in hand.
    """
    ecid, asid, d = request.embedding_config_id, request.annotation_set_id, request.discriminator

    def usable(asp: str) -> bool:
        if not _aspect_index_path(ecid, asid, asp, d).exists():
            return False
        recorded = _recorded_pool(ecid, asid, asp, d)
        if recorded is None or (pool is not None and recorded != pool):
            return False
        return _load_anno_csr_from_disk(ecid, asid, asp, d) is not None

    return [asp for asp in _ASPECTS if not usable(asp)]


def _recorded_pool(ecid: uuid.UUID, asid: uuid.UUID, aspect: str, discriminator: str) -> str | None:
    """The fingerprint an aspect index was built against, or None if absent.

    Absent means the index predates the fingerprint, so it cannot be told
    apart from one built against a different pool. Rebuilt, not trusted.
    """
    path = _aspect_index_pool_path(ecid, asid, aspect, discriminator)
    return path.read_text(encoding="utf-8").strip() if path.exists() else None


def _aspect_cache(
    aspect: str, request: _PoolRequest, built: BuiltAspect | None
) -> tuple[np.ndarray, AnnoCsr | None, str | None]:
    """The aspect's index, annotations and pool: as built in this process, else from disk."""
    if built is not None:
        return built.indices, built.csr, built.pool
    ecid, asid, d = request.embedding_config_id, request.annotation_set_id, request.discriminator
    return (
        np.load(_aspect_index_path(ecid, asid, aspect, d)),
        _load_anno_csr_from_disk(ecid, asid, aspect, d),
        _recorded_pool(ecid, asid, aspect, d),
    )


def _assemble_aspect_view(
    aspect: str,
    unified: dict[str, Any],
    request: _PoolRequest,
    *,
    need_cos: bool = True,
    need_plain: bool = True,
    built: BuiltAspect | None = None,
) -> tuple[dict[str, Any], int]:
    """Slice the unified embeddings by the aspect's index array and attach the
    CSR annotation cache. Returns ``(view, n_refs)``.

    ``built`` is the aspect as this process built it, served without reading the
    disk; None reads what an earlier build wrote.

    Each fancy-index slice is a full copy, so for high-dim PLMs a per-aspect
    f32 view is tens of GB. Only the copies this run reads are materialised
    (``need_cos`` → the cosine view, ``need_plain`` → the raw view); the f16
    slice is never read downstream and is left as an empty placeholder.
    """
    indices, anno_csr, pool = _aspect_cache(aspect, request, built)
    _check_index_addresses_pool(indices, unified["accessions"], aspect, pool)
    aspect_accessions = [unified["accessions"][i] for i in indices]
    unified_f32 = unified.get("embeddings_f32")
    unified_cos = unified.get("embeddings_f32_cos")
    # The f32 banks are NOT cut here. A fancy-index slice is a full copy, and
    # the three aspects together are 1,777,762 rows: 10.92 GB at 1536 dims.
    # Only ONE of them is read at a time when the aspect searches run in
    # series, so the view carries the pool and the index array and
    # :func:`aspect_bank` gathers the rows when a search asks for them.
    view: dict[str, Any] = {
        "accessions": aspect_accessions,
        "embeddings": np.empty((0,), dtype=np.float16),
        "embeddings_f32": None,
        "embeddings_f32_cos": None,
        _ASPECT_SLICE: (
            indices,
            unified_f32 if need_plain else None,
            unified_cos if need_cos else None,
        ),
    }
    if anno_csr is not None:
        gtids, quals, ecodes, offsets = anno_csr
        view.update(
            {
                "anno_gtids": gtids,
                "anno_quals": quals,
                "anno_ecodes": ecodes,
                "anno_offsets": offsets,
                "acc_to_anno_idx": {acc: i for i, acc in enumerate(aspect_accessions)},
            }
        )
    return view, len(indices)


def _check_index_addresses_pool(
    indices: np.ndarray, accessions: list[str], aspect: str, built_against: str | None
) -> None:
    """Fail unless this index was built against exactly this pool.

    Compares the pool's fingerprint, not its length. Length only catches the
    case where the two pools happen to differ in size, and an index built
    under a donor policy and served permissively does not: it addresses a
    prefix of the larger pool, raises nothing, and scores plausibly on a
    fraction of the references. That is the mode that reaches a thesis.

    ``built_against`` is None for an index written before the fingerprint
    existed. Those are unverifiable rather than known-good, so they are
    refused and rebuilt; a cache is cheap and a silent misattribution is not.
    """
    actual = pool_fingerprint(accessions)
    if built_against == actual:
        return
    raise ValueError(
        f"aspect index for {aspect} was not built against this pool "
        f"(index carries {built_against or 'no fingerprint'}, pool is {actual}, "
        f"{len(accessions)} accessions, max index "
        f"{int(indices.max()) if indices.size else -1}); refusing to use it."
    )
