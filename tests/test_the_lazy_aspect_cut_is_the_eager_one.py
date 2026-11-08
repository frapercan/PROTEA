"""Deferring the aspect cut changes when memory is spent, never what is in it.

The three aspect banks are 10.92 GB of float32 on the production pool and the
default ``aspect_knn_workers`` of 3 keeps all three alive at once. Only one is
read at a time when the searches run in series, so the view carries the index
array and the pool, and the cut happens when a search asks.

That is a memory layout, and the test that matters is that it is ONLY a memory
layout: the gathered rows have to be bit-identical to the eager slice, because
a bank that differs in its last bits reorders neighbours at equal distance, and
exploration 1 (2026-11-05) measured what that costs: 110,950 of 1,146,988 rows.
"""

from __future__ import annotations

import numpy as np

from protea.core.operations.predict_go_terms._reference_aspect import (
    _ASPECT_SLICE,
    aspect_bank,
)


def _pool(rows: int = 97, dims: int = 11) -> np.ndarray:
    rng = np.random.default_rng(20261105)
    return rng.standard_normal((rows, dims), dtype=np.float32)


def test_the_gathered_bank_is_bit_identical_to_the_eager_slice() -> None:
    pool = _pool()
    indices = np.array([3, 0, 96, 42, 42, 1], dtype=np.int64)
    view = {
        "embeddings_f32": None,
        "embeddings_f32_cos": None,
        _ASPECT_SLICE: (indices, pool, pool * 2.0),
    }
    for use_cos, expected in ((False, pool[indices]), (True, (pool * 2.0)[indices])):
        got = aspect_bank(view, use_cos=use_cos)
        assert got.dtype == expected.dtype
        assert got.shape == expected.shape
        assert np.array_equal(got, expected)
        assert got.tobytes() == expected.tobytes()


def test_a_view_that_was_cut_eagerly_is_returned_as_it_stands() -> None:
    """So a caller or a test holding a plain dict does not have to know this exists."""
    already = _pool(4, 3)
    assert aspect_bank({"embeddings_f32": already}, use_cos=False) is already
    assert aspect_bank({"embeddings_f32_cos": already}, use_cos=True) is already


def test_a_dtype_nobody_asked_for_is_absent_rather_than_guessed() -> None:
    pool = _pool(5, 3)
    view = {
        "embeddings_f32": None,
        "embeddings_f32_cos": None,
        _ASPECT_SLICE: (np.array([0, 2], dtype=np.int64), None, pool),
    }
    assert aspect_bank(view, use_cos=False) is None
    assert aspect_bank(view, use_cos=True) is not None


def test_a_view_with_no_source_and_no_bank_is_none() -> None:
    assert aspect_bank({"embeddings_f32": None}, use_cos=False) is None


class TestNormalisingInPlaceIsTheSameNumbers:
    """The cosine bank is the raw buffer divided in place when nobody reads the raw one.

    ``np.divide(a, b, out=a)`` is the same ufunc over the same operands as
    ``a / b``, so this is an allocation that does not happen rather than an
    arithmetic that changes. It is worth a test anyway: the whole exploration
    this was built for turns on whether a bank repeats to the bit.
    """

    @staticmethod
    def _bank(rows: int = 83, dims: int = 13) -> np.ndarray:
        rng = np.random.default_rng(1122)
        return rng.standard_normal((rows, dims)).astype(np.float16)

    def test_the_cosine_bank_is_the_same_whichever_route_built_it(self) -> None:
        from protea.core.disk_cache import _dense_f32_views

        bank = self._bank()
        _, kept = _dense_f32_views(bank, need_cos=True, need_plain=True)
        raw, in_place = _dense_f32_views(bank, need_cos=True, need_plain=False)
        assert raw is None
        assert in_place.dtype == kept.dtype
        assert in_place.tobytes() == kept.tobytes()

    def test_the_raw_bank_survives_when_something_reads_it(self) -> None:
        from protea.core.disk_cache import _dense_f32_views

        bank = self._bank()
        raw, cos = _dense_f32_views(bank, need_cos=True, need_plain=True)
        assert raw is not None and cos is not None
        assert raw.tobytes() == bank.astype(np.float32).tobytes()

    def test_no_cosine_asked_for_means_no_cosine_built(self) -> None:
        from protea.core.disk_cache import _dense_f32_views

        raw, cos = _dense_f32_views(self._bank(), need_cos=False, need_plain=True)
        assert cos is None and raw is not None
