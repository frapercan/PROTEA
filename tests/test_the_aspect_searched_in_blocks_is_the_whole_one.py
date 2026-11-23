"""Searching an aspect block by block returns the neighbours one whole search returns.

With every evidence code but ND, aspect P of photo 211 is 1,383,730 donors, and
cutting it out of the pool to search it was a 13.2 GiB copy at ESM-2 3B's 2,560
dimensions. The block search copies one block at a time and merges the blocks'
nearest by distance and then by position. That is only worth having if it is ONLY
a memory layout: a donor that changes, or two tied donors that swap, is a stored
row that changes, and exploration 1 (2026-11-05) measured what tie order costs.
So every case here compares against the whole search to the last bit, with ties
built in on purpose and block edges that split tied groups.

The search's float64 kernel reads the bank in blocks of its own (50,000 rows), and
the block search cuts at multiples of that, so every product is computed in the call
a whole search makes. The small cases shrink the kernel's block to 4 rows, which
makes these pools span many of its blocks, and one case runs with the real one.
"""

from __future__ import annotations

from functools import partial

import numpy as np
import pytest
from protea_method._self_by_sequence import Queries

from protea.core.knn_search import search_knn
from protea.core.operations.predict_go_terms._aspect_helpers import _AspectKnnPreSearch
from protea.core.operations.predict_go_terms._reference_aspect import (
    _ASPECT_SLICE,
    UncutAspect,
    search_aspect_in_blocks,
    uncut_aspect,
)


@pytest.fixture
def kernel_block_of_four(monkeypatch):
    """Small pools only cross the kernel's block edges when the block is small."""
    from protea_method import _order_invariance

    monkeypatch.setattr(_order_invariance, "_REF_BLOCK", 4)


def _normalised(a: np.ndarray) -> np.ndarray:
    return (a / np.linalg.norm(a, axis=1, keepdims=True)).astype(np.float32)


def _pool_with_twins(rows: int = 211, dims: int = 9, seed: int = 20261119) -> np.ndarray:
    """A pool where many rows are exact copies, so equal distances are common.

    Twins are the rule in the real bank, not a corner: a sequence shared by several
    accessions gives the same vector, and 91 % of queries have neighbours at
    distance about 0.
    """
    rng = np.random.default_rng(seed)
    base = rng.standard_normal((rows // 3, dims)).astype(np.float32)
    picks = rng.integers(0, len(base), rows)
    return _normalised(base[picks])


def _whole(queries, indices, pool, accs, k, threshold=None):
    return search_knn(
        queries,
        pool[indices],
        accs,
        k,
        distance_threshold=threshold,
        backend="numpy",
        metric="cosine",
        pre_normalized=True,
    )


def _blocks(queries, indices, pool, accs, k, block_rows, threshold=None):
    return search_aspect_in_blocks(
        queries,
        UncutAspect(indices, pool, accs),
        k=k,
        block_rows=block_rows,
        distance_threshold=threshold,
        search=partial(search_knn, backend="numpy", metric="cosine", pre_normalized=True),
    )


@pytest.mark.parametrize("block_rows", [1, 2, 3, 7, 50, 139, 140, 141, 10_000])
@pytest.mark.parametrize("k", [1, 5, 12, 400])
def test_every_block_size_gives_the_whole_search(
    block_rows: int, k: int, kernel_block_of_four
) -> None:
    pool = _pool_with_twins()
    rng = np.random.default_rng(7)
    indices = np.sort(rng.choice(len(pool), size=140, replace=False)).astype(np.int64)
    accs = [f"P{int(i):05d}" for i in indices]
    # Queries copied from donors (distance 0, tied with their twins) and fresh ones.
    queries = np.vstack([pool[indices[:6]], pool[:4], _normalised(rng.standard_normal((5, 9)))])
    whole = _whole(queries, indices, pool, accs, k)
    blocks = _blocks(queries, indices, pool, accs, k, block_rows)
    assert blocks == whole


def test_ties_at_the_kth_place_split_across_blocks_keep_the_whole_order(
    kernel_block_of_four,
) -> None:
    """Twelve copies of one row: the k-th place falls inside the tied group."""
    rng = np.random.default_rng(3)
    twin = _normalised(rng.standard_normal((1, 6)))
    other = _normalised(rng.standard_normal((30, 6)))
    pool = np.vstack([other[:10], np.repeat(twin, 12, axis=0), other[10:]])
    indices = np.arange(len(pool), dtype=np.int64)
    accs = [f"A{i:03d}" for i in indices]
    queries = np.vstack([twin, other[:2]])
    for block_rows in (1, 4, 5, 11, 13):
        for k in (3, 7, 12, 15):
            assert _blocks(queries, indices, pool, accs, k, block_rows) == _whole(
                queries, indices, pool, accs, k
            )
    # The tie is broken by position, which is the accession order of the aspect.
    hit = _blocks(queries, indices, pool, accs, 5, 4)[0]
    assert [a for a, _ in hit] == [f"A{i:03d}" for i in range(10, 15)]


def test_the_threshold_is_applied_after_the_merge_as_the_search_does(kernel_block_of_four) -> None:
    pool = _pool_with_twins(seed=11)
    indices = np.arange(0, len(pool), 2, dtype=np.int64)
    accs = [f"T{int(i):04d}" for i in indices]
    queries = pool[indices[:8]]
    for threshold in (0.0, 0.2, 0.9, 2.0):
        assert _blocks(queries, indices, pool, accs, 9, 6, threshold) == _whole(
            queries, indices, pool, accs, 9, threshold
        )


def test_a_block_that_is_not_a_multiple_of_the_kernels_is_rounded_up(kernel_block_of_four) -> None:
    """One row a block would be a matrix-vector product, which moved a distance by
    one float32 step near zero against the whole search; rounded up, it cannot."""
    rng = np.random.default_rng(3)
    twin = _normalised(rng.standard_normal((1, 6)))
    other = _normalised(rng.standard_normal((30, 6)))
    pool = np.vstack([other[:10], np.repeat(twin, 12, axis=0), other[10:]])
    indices = np.arange(len(pool), dtype=np.int64)
    accs = [f"A{i:03d}" for i in indices]
    queries = np.vstack([twin, other[:2]])
    for block_rows in (1, 3, 5, 6):
        got = _blocks(queries, indices, pool, accs, 12, block_rows)
        assert got == _whole(queries, indices, pool, accs, 12)


def test_with_the_kernels_real_block_three_blocks_give_the_whole_search() -> None:
    from protea_method import _order_invariance

    assert _order_invariance._REF_BLOCK == 50_000
    rng = np.random.default_rng(1119)
    base = rng.standard_normal((40_000, 4)).astype(np.float32)
    pool = _normalised(base[rng.integers(0, len(base), 120_000)])
    indices = np.arange(0, 120_000, dtype=np.int64)
    accs = [f"R{i:06d}" for i in indices]
    queries = np.vstack([pool[:5], pool[60_000:60_003], _normalised(rng.standard_normal((3, 4)))])
    assert _blocks(queries, indices, pool, accs, 7, 50_000) == _whole(
        queries, indices, pool, accs, 7
    )


def test_an_aspect_smaller_than_k_returns_all_its_donors(kernel_block_of_four) -> None:
    pool = _pool_with_twins(rows=20, seed=5)
    indices = np.array([1, 4, 9], dtype=np.int64)
    accs = ["X1", "X4", "X9"]
    queries = pool[:3]
    assert _blocks(queries, indices, pool, accs, 5, 2) == _whole(queries, indices, pool, accs, 5)
    assert all(len(row) == 3 for row in _blocks(queries, indices, pool, accs, 5, 2))


class TestTheOperationPicksTheBlockSearchOnlyWhereItApplies:
    @staticmethod
    def _payload(**over):
        class P:
            limit_per_entry = 5
            distance_threshold = None
            search_backend = "numpy"
            metric = "cosine"
            faiss_index_type = "Flat"
            faiss_nlist = 100
            faiss_nprobe = 10
            faiss_hnsw_m = 32
            faiss_hnsw_ef_search = 64
            exclude_self_neighbour = False

        for key, value in over.items():
            setattr(P, key, value)
        return P

    @staticmethod
    def _views(pool, indices, accs):
        lazy = {
            "accessions": accs,
            "embeddings_f32": None,
            "embeddings_f32_cos": None,
            _ASPECT_SLICE: (indices, None, pool),
        }
        eager = {"accessions": accs, "embeddings_f32": None, "embeddings_f32_cos": pool[indices]}
        return lazy, eager

    @pytest.mark.parametrize("block_rows", ["3", "0", "131072"])
    def test_lazy_and_eager_views_give_the_same_neighbours(
        self, monkeypatch, block_rows, kernel_block_of_four
    ) -> None:
        from protea.config.tuning import get_tuning

        pool = _pool_with_twins(seed=19)
        indices = np.sort(np.random.default_rng(2).choice(len(pool), 90, replace=False))
        accs = [f"Q{int(i):04d}" for i in indices]
        queries = Queries([f"q{i}" for i in range(7)], pool[indices[:7]].copy())
        lazy, eager = self._views(pool, indices.astype(np.int64), accs)
        monkeypatch.setenv("PROTEA_TUNING__operation__aspect_search_block_rows", block_rows)
        get_tuning.cache_clear()
        try:
            p = self._payload()
            got = _AspectKnnPreSearch._knn_one_aspect("P", queries, {"P": lazy}, p, True)
            ref = _AspectKnnPreSearch._knn_one_aspect("P", queries, {"P": eager}, p, True)
        finally:
            monkeypatch.delenv("PROTEA_TUNING__operation__aspect_search_block_rows")
            get_tuning.cache_clear()
        assert got == ref

    def test_an_eager_view_is_not_searched_in_blocks(self) -> None:
        pool = _pool_with_twins(rows=12, seed=23)
        indices = np.arange(6, dtype=np.int64)
        lazy, eager = self._views(pool, indices, [f"E{i}" for i in indices])
        assert uncut_aspect(eager, use_cos=True) is None
        got = uncut_aspect(lazy, use_cos=True)
        assert got is not None and got[0] is indices and got[1] is pool
        # No raw pool in the view: nothing to search the raw metric with.
        assert uncut_aspect(lazy, use_cos=False) is None
