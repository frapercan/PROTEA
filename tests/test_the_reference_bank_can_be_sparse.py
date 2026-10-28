"""A k-WTA bank is the wrong SHAPE for a dense copy, not merely a large one.

Measured over the whole store 2026-10-28: the ``learned-code`` bank is 684.048
rows of 8192 dimensions with exactly 64 non-zeros each. Dense float32 is
22,41 GB, and ``_derive_reference_views`` built the f32 copy unconditionally
and only dropped it at the return, so it held f16 plus two f32 copies at once:
**11,21 + 22,41 + 22,41 = 56,0 GB** on a machine with 30 GiB. The same rows as
a CSR are 0,35 GB, a factor of 64.

So the arm could not run at all, and the representation this campaign exists to
measure sat in the store with no number against it.

WHAT THIS DOES NOT CHANGE. ``protea_method.knn_search`` is untouched: its numpy
backend takes a ``csr_array`` as it stands, verified against the dense bank on
this venv with the same neighbour order and a maximum distance difference of
0,000e+00. A ``sparse`` backend also already exists there, written for k-WTA
codes, and is deliberately NOT used here: ``search_backend`` is a method
identity field, so choosing it would make the learned-code arm differ from the
ankh-base arm in two declared fields instead of one, and the comparison would
no longer have an axis.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pytest
from scipy.sparse import csr_array, issparse

from protea.core.disk_cache import _derive_reference_views, _l2_normalise_rows_sparse
from protea.core.operations.predict_go_terms._batch_op_reference import _bank_bytes
from protea.infrastructure.orm.models.embedding.embedding_config import SPARSE_CODE_BACKENDS

DIM = 2048
ACTIVE = 4


def _codes(n_rows: int, *, zero_rows: int = 0, seed: int = 1) -> np.ndarray:
    """``n_rows`` k-WTA rows, the last ``zero_rows`` of them all zero."""
    rng = np.random.default_rng(seed)
    out = np.zeros((n_rows, DIM), dtype=np.float32)
    for i in range(n_rows - zero_rows):
        out[i, rng.choice(DIM, ACTIVE, replace=False)] = rng.standard_normal(ACTIVE)
    return out


def _accs(n: int) -> list[str]:
    return [f"P{i:05d}" for i in range(n)]


class TestNothingIsDensified:
    def test_the_views_of_a_sparse_bank_are_sparse(self):
        views = _derive_reference_views(_accs(6), csr_array(_codes(6)))
        assert issparse(views["embeddings_f32_cos"])
        assert issparse(views["embeddings_f32"])
        assert issparse(views["embeddings"])

    def test_the_unread_copy_is_still_skipped(self):
        """``need_plain`` / ``need_cos`` keep meaning what they meant.

        They are the dense path's defence against tens of GB per copy, and a
        sparse bank that ignored them would reintroduce the second copy it
        exists to avoid.
        """
        views = _derive_reference_views(
            _accs(6), csr_array(_codes(6)), need_cos=True, need_plain=False
        )
        assert views["embeddings_f32"] is None
        assert issparse(views["embeddings_f32_cos"])

    def test_a_csr_costs_a_fraction_of_the_dense_bank_it_replaces(self):
        """The whole reason for the branch, at toy scale.

        2048 dims with 4 non-zeros is a factor near 100; the real bank is 8192
        with 64, measured at 64.
        """
        dense = _codes(50)
        assert _bank_bytes(csr_array(dense)) * 10 < _bank_bytes(dense)


class TestTheCosineViewAgreesWithTheDenseOne:
    def test_it_agrees_to_float32_round_off_on_the_same_values(self):
        """5,96e-08 measured. This is the equivalence the branch promises.

        Fed the SAME numbers, the sparse normalisation and the dense one are
        one calculation. Asserting bit-equality would be asserting something
        about summation order that neither path promises.
        """
        dense16 = _codes(6).astype(np.float16)
        cos_dense = _derive_reference_views(_accs(6), dense16)["embeddings_f32_cos"]
        cos_sparse = _derive_reference_views(
            _accs(6), csr_array(dense16.astype(np.float32))
        )["embeddings_f32_cos"].toarray()
        assert np.abs(cos_dense - cos_sparse).max() < 1e-6

    def test_the_remaining_gap_is_f16_AND_NOT_THE_NORMALISATION(self):
        """Measured 1,980e-04, and it is the dense path losing precision.

        Named here because it is the number a reader will find if they compare
        the two paths on float32 inputs and conclude the sparse one is wrong.
        It is the other way round: the dense bank is quantised to float16 and
        the sparse one is not, because scipy.sparse has no float16 at all. In
        production both read the same ``halfvec`` column, so the values agree
        and only the container's dtype differs.
        """
        dense32 = _codes(6)
        cos_from_f16 = _derive_reference_views(
            _accs(6), dense32.astype(np.float16)
        )["embeddings_f32_cos"]
        cos_from_f32 = _derive_reference_views(_accs(6), csr_array(dense32))[
            "embeddings_f32_cos"
        ].toarray()
        gap = np.abs(cos_from_f16 - cos_from_f32).max()
        assert gap > 1e-5, "f16 quantisation should be visible at this magnitude"

    def test_a_zero_row_stays_a_zero_vector_and_never_a_nan_one(self):
        """The epsilon is 1e-9 in both paths, and that is load-bearing.

        A bank can carry a zero row, and a NaN there poisons every distance
        rather than one of them.
        """
        bank = csr_array(_codes(6, zero_rows=2))
        cos = _derive_reference_views(_accs(6), bank)["embeddings_f32_cos"].toarray()
        assert np.isfinite(cos).all()
        assert np.linalg.norm(cos[-1]) == 0.0

    def test_the_rows_that_are_not_zero_come_out_unit_length(self):
        cos = _derive_reference_views(_accs(6), csr_array(_codes(6)))[
            "embeddings_f32_cos"
        ].toarray()
        assert np.allclose(np.linalg.norm(cos, axis=1), 1.0, atol=1e-6)

    def test_an_empty_row_does_not_take_its_neighbours_norm(self):
        """Why ``np.repeat`` over ``diff(indptr)`` and not ``add.reduceat``.

        ``reduceat`` on two equal offsets returns the element AT that offset
        instead of the empty sum, so the row with no non-zeros would be scaled
        by whatever the next row's first value happens to be. Pinned with the
        zero row in the MIDDLE, where that aliasing is reachable.
        """
        dense = _codes(5)
        dense[2] = 0.0
        cos = _l2_normalise_rows_sparse(csr_array(dense)).toarray()
        assert np.linalg.norm(cos[2]) == 0.0
        assert np.allclose(np.linalg.norm(cos[[0, 1, 3, 4]], axis=1), 1.0, atol=1e-6)


class TestEmptinessIsReadOffTheShape:
    def test_a_shaped_bank_with_no_stored_values_keeps_its_accessions(self):
        """``.size`` is NNZ on a sparse array, not rows times columns.

        The dense function's guard is ``embeddings.size == 0``. Reaching it
        with a sparse bank whose rows are all zero would return dense empty
        arrays and drop the accession list, which is the bank's identity: the
        search names its hits by position in that list.
        """
        bank = csr_array(np.zeros((4, DIM), dtype=np.float32))
        assert bank.size == 0
        views = _derive_reference_views(_accs(4), bank)
        assert views["accessions"] == _accs(4)
        assert views["embeddings_f32_cos"].shape == (4, DIM)

    def test_a_bank_with_no_rows_at_all_is_still_empty(self):
        views = _derive_reference_views([], csr_array((0, DIM), dtype=np.float32))
        assert views["accessions"] == []
        assert views["embeddings_f32_cos"].shape[0] == 0


class TestTheSearchAcceptsTheSparseView:
    """The end-to-end point: the library needs no change.

    If this fails, the branch above is an optimisation of something nobody can
    search, which is what the learned-code arm already was.
    """

    @staticmethod
    def _hits(bank: Any, queries: np.ndarray, accs: list[str]) -> list[list[tuple[str, float]]]:
        from protea_method.knn_search import search_knn

        return search_knn(
            queries, bank, accs, 3, backend="numpy", metric="cosine", pre_normalized=True
        )

    def test_the_csr_view_returns_the_same_neighbours_as_the_dense_view(self):
        dense = _codes(80, seed=7)
        accs = _accs(80)
        q_dense = _codes(5, seed=11)

        cos_dense = _derive_reference_views(accs, dense.astype(np.float16))[
            "embeddings_f32_cos"
        ]
        cos_sparse = _derive_reference_views(
            accs, csr_array(dense.astype(np.float16).astype(np.float32))
        )["embeddings_f32_cos"]

        queries = q_dense / (np.linalg.norm(q_dense, axis=1, keepdims=True) + 1e-9)
        from_dense = self._hits(cos_dense, queries, accs)
        from_sparse = self._hits(cos_sparse, queries, accs)

        assert [[a for a, _ in row] for row in from_dense] == [
            [a for a, _ in row] for row in from_sparse
        ]
        worst = max(
            abs(d1 - d2)
            for r1, r2 in zip(from_dense, from_sparse, strict=True)
            for (_, d1), (_, d2) in zip(r1, r2, strict=True)
        )
        assert worst < 1e-6

    def test_one_hit_list_per_query_still(self):
        """``search_knn._check_alignment`` raises on a short return, and a
        misaligned one would file every later query's donors under the wrong
        protein rather than failing."""
        dense = _codes(40, seed=3)
        accs = _accs(40)
        bank = _derive_reference_views(accs, csr_array(dense))["embeddings_f32_cos"]
        queries = _codes(6, seed=5)
        queries /= np.linalg.norm(queries, axis=1, keepdims=True) + 1e-9
        assert len(self._hits(bank, queries, accs)) == 6


class TestWhichBanksTakeTheSparseBranch:
    def test_the_learned_code_producer_is_the_one_named(self):
        assert "learned-code" in SPARSE_CODE_BACKENDS

    def test_a_dense_plm_backend_is_not(self):
        """A wide dense bank is expensive and still dense. ESM2-3B at 2560
        dimensions is the case the dense path's ``need_cos`` / ``need_plain``
        exist for, and it must not be rerouted here."""
        for backend in ("ankh", "esm", "esm3c", "t5", "protst"):
            assert backend not in SPARSE_CODE_BACKENDS

    def test_the_ingested_method_backend_is_not_either(self):
        """It stores no vectors at all, so there is no bank to shape."""
        assert "ingested-method" not in SPARSE_CODE_BACKENDS


def test_bank_bytes_does_not_raise_on_a_csr():
    """``.nbytes`` is a dense attribute; the size line of
    ``load_references_done`` read it unconditionally."""
    assert _bank_bytes(csr_array(np.eye(4, dtype=np.float32))) > 0
    assert _bank_bytes(np.zeros((4, 8), dtype=np.float16)) == 64


def test_the_dense_path_is_untouched():
    """Every existing bank must come out exactly as it did.

    The branch is additive: a dense array never reaches the sparse helpers, and
    the keys, dtypes and values of a dense call are the ones they were.
    """
    dense16 = _codes(6).astype(np.float16)
    views = _derive_reference_views(_accs(6), dense16)
    assert not issparse(views["embeddings_f32_cos"])
    assert views["embeddings_f32"].dtype == np.float32
    assert views["embeddings"].dtype == np.float16
    expected = dense16.astype(np.float32)
    expected = expected / (np.linalg.norm(expected, axis=1, keepdims=True) + 1e-9)
    assert np.array_equal(views["embeddings_f32_cos"], expected)


@pytest.mark.parametrize("zero_rows", [0, 1, 3])
def test_the_normaliser_holds_for_any_number_of_zero_rows(zero_rows: int):
    cos = _l2_normalise_rows_sparse(csr_array(_codes(6, zero_rows=zero_rows))).toarray()
    assert np.isfinite(cos).all()
    assert int((np.linalg.norm(cos, axis=1) == 0.0).sum()) == zero_rows
