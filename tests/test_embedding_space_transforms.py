"""The search-space transforms: each one against its formula, in place, chunk by chunk.

The statistics a transform reads are computed here on the rows in memory, so
every value can be checked against the definition: centring subtracts the mean,
standardising divides each centred dimension by its deviation, whitening
multiplies by the inverse square root of the Ledoit-Wolf covariance, and the
two removals take out the leading principal directions. The chunked in-place
walk gives the same rows as one pass over the whole array and never holds a
copy of it.
"""

from __future__ import annotations

import tracemalloc

import numpy as np
import pytest

from protea.core.embedding_space.statistics import ledoit_wolf_shrinkage
from protea.core.embedding_space.transforms import (
    TRANSFORMS,
    SpaceParameters,
    apply_in_place,
    fixed_count,
    transformed_copy,
    whitening_gains,
)


def _rows(n: int = 500, d: int = 8, seed: int = 2) -> np.ndarray:
    rng = np.random.default_rng(seed)
    mixing = rng.normal(size=(d, d))
    return rng.normal(size=(n, d)) @ mixing + rng.normal(size=d)


def _parameters(
    rows: np.ndarray, removed: int = 2, shrinkage: float | None = None
) -> SpaceParameters:
    centred = rows - rows.mean(axis=0)
    covariance = centred.T @ centred / len(rows)
    values, vectors = np.linalg.eigh(covariance)
    norms = np.einsum("ij,ij->i", centred, centred)
    delta = ledoit_wolf_shrinkage(covariance, float(norms @ norms), len(rows))
    return SpaceParameters(
        mean=rows.mean(axis=0),
        variances=np.diag(covariance).copy(),
        eigenvalues=values[::-1].copy(),
        eigenvectors=vectors[:, ::-1].copy(),
        shrinkage=delta if shrinkage is None else shrinkage,
        removed_directions=removed,
    )


class TestEachTransformIsItsFormula:
    def test_center(self) -> None:
        rows = _rows()
        parameters = _parameters(rows)
        assert np.allclose(transformed_copy(rows, "center", parameters), rows - rows.mean(axis=0))

    def test_standardize(self) -> None:
        rows = _rows()
        parameters = _parameters(rows)
        expected = (rows - rows.mean(axis=0)) / rows.std(axis=0)
        assert np.allclose(transformed_copy(rows, "standardize", parameters), expected)

    def test_whiten_is_the_inverse_square_root_of_the_shrunk_covariance(self) -> None:
        rows = _rows()
        parameters = _parameters(rows)
        centred = rows - rows.mean(axis=0)
        covariance = centred.T @ centred / len(rows)
        m = np.trace(covariance) / 8
        shrunk = (1 - parameters.shrinkage) * covariance + parameters.shrinkage * m * np.eye(8)
        values, vectors = np.linalg.eigh(shrunk)
        inverse_root = vectors @ np.diag(values**-0.5) @ vectors.T
        whitened = transformed_copy(rows, "whiten", parameters)
        assert np.allclose(whitened, centred @ inverse_root)
        assert np.allclose(inverse_root @ shrunk @ inverse_root, np.eye(8))

    def test_whitening_without_shrinkage_gives_unit_covariance(self) -> None:
        rows = _rows()
        whitened = transformed_copy(rows, "whiten", _parameters(rows, shrinkage=0.0))
        assert np.allclose(whitened.T @ whitened / len(rows), np.eye(8), atol=1e-10)

    @pytest.mark.parametrize(
        ("transform", "count"),
        [("remove_dominant_directions", 3), ("remove_dominant_directions_fixed_count", 0)],
    )
    def test_the_removals_take_out_the_leading_directions(self, transform: str, count: int) -> None:
        rows = _rows()
        parameters = _parameters(rows, removed=3)
        out = transformed_copy(rows, transform, parameters)
        leading = parameters.eigenvectors[:, :count]
        centred = rows - rows.mean(axis=0)
        assert np.allclose(out, centred - centred @ leading @ leading.T)
        assert np.allclose(out @ leading, 0.0)

    def test_the_fixed_count_follows_the_dimension(self) -> None:
        rows = _rows(n=900, d=230)
        parameters = _parameters(rows, removed=5)
        out = transformed_copy(rows, "remove_dominant_directions_fixed_count", parameters)
        assert parameters.fixed_count == fixed_count(230) == 2
        assert np.allclose(out @ parameters.eigenvectors[:, :2], 0.0)
        assert not np.allclose(out @ parameters.eigenvectors[:, 2:5], 0.0)


class TestInPlaceAndInChunks:
    @pytest.mark.parametrize("transform", TRANSFORMS)
    @pytest.mark.parametrize("chunk_rows", [1, 7, 10_000])
    def test_chunks_give_the_rows_one_pass_gives(self, transform: str, chunk_rows: int) -> None:
        rows = _rows()
        parameters = _parameters(rows)
        whole = transformed_copy(rows, transform, parameters)
        pool = rows.astype(np.float32)
        apply_in_place(pool, transform, parameters, chunk_rows=chunk_rows)
        assert pool.dtype == np.float32
        assert np.allclose(pool, whole.astype(np.float32), rtol=1e-6, atol=1e-6)

    def test_the_array_is_transformed_where_it_is(self) -> None:
        rows = _rows()
        pool = rows.astype(np.float32)
        before = pool.__array_interface__["data"][0]
        apply_in_place(pool, "whiten", _parameters(rows), chunk_rows=64)
        assert pool.__array_interface__["data"][0] == before
        assert not np.allclose(pool, rows.astype(np.float32))

    def test_a_query_transformed_alone_equals_its_row_in_the_pool(self) -> None:
        rows = _rows(n=3000, d=16)
        parameters = _parameters(rows, removed=4)
        for transform in TRANSFORMS:
            pool = rows.astype(np.float32)
            apply_in_place(pool, transform, parameters)
            query = rows[1234:1235].astype(np.float32)
            apply_in_place(query, transform, parameters)
            assert np.allclose(query[0], pool[1234], rtol=0, atol=1e-6), transform

    def test_no_copy_of_the_pool_is_made(self) -> None:
        rows = _rows(n=20_000, d=64).astype(np.float32)
        parameters = _parameters(rows.astype(np.float64))
        tracemalloc.start()
        try:
            apply_in_place(rows, "whiten", parameters, chunk_rows=500)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        # The pool is 5.1 MB; a chunk of 500 rows in float64 is 0.26 MB.
        assert peak < rows.nbytes / 4


class TestRefusals:
    def test_an_unknown_transform(self) -> None:
        rows = _rows()
        for name in ("none", "centre", "zca", "all-but-the-top", ""):
            with pytest.raises(ValueError, match="unknown transform"):
                apply_in_place(rows.copy(), name, _parameters(rows))

    def test_half_precision_rows(self) -> None:
        rows = _rows()
        with pytest.raises(ValueError, match="float32 or float64"):
            apply_in_place(rows.astype(np.float16), "center", _parameters(rows))

    def test_rows_of_another_width(self) -> None:
        rows = _rows()
        with pytest.raises(ValueError, match="rows of 8 values"):
            apply_in_place(np.zeros((3, 9)), "center", _parameters(rows))

    def test_a_dimension_without_variance_cannot_be_standardised(self) -> None:
        rows = _rows()
        rows[:, 3] = 1.5
        with pytest.raises(ValueError, match=r"dimensions \[3\] have no variance"):
            apply_in_place(rows.copy(), "standardize", _parameters(rows))

    def test_a_singular_covariance_cannot_be_whitened_without_shrinkage(self) -> None:
        rows = _rows()
        rows[:, 0] = rows[:, 1]
        parameters = _parameters(rows, shrinkage=0.0)
        with pytest.raises(ValueError, match="singular"):
            whitening_gains(
                SpaceParameters(
                    mean=parameters.mean,
                    variances=parameters.variances,
                    eigenvalues=np.r_[parameters.eigenvalues[:-1], 0.0],
                    eigenvectors=parameters.eigenvectors,
                    shrinkage=0.0,
                    removed_directions=2,
                )
            )

    def test_inconsistent_parameters(self) -> None:
        good = _parameters(_rows())
        with pytest.raises(ValueError, match="in \\[0, 1\\]"):
            SpaceParameters(good.mean, good.variances, good.eigenvalues, good.eigenvectors, 1.5, 2)
        with pytest.raises(ValueError, match="removed directions"):
            SpaceParameters(good.mean, good.variances, good.eigenvalues, good.eigenvectors, 0.1, 8)
        with pytest.raises(ValueError, match="8 x 8"):
            SpaceParameters(good.mean, good.variances, good.eigenvalues, np.eye(7), 0.1, 2)
