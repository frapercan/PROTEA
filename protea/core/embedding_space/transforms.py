"""The search-space transforms of a bank's vectors, applied in place and in chunks.

Each transform is a fixed affine map of a vector, read from the label-free
statistics of the configuration's space at a cut
(:mod:`protea.core.embedding_space.statistics`): the mean ``mu``, the
per-dimension variances, the eigendecomposition ``S = U diag(lambda) U^T`` of
the covariance, Ledoit and Wolf's shrinkage ``delta*`` and the number of
removed directions ``D*`` chosen by the scree elbow. Every transform centres:

- ``center``: ``x - mu``;
- ``standardize``: ``(x_j - mu_j) / sigma_j``, dimension by dimension;
- ``whiten``: ``(x - mu) W`` with ``W = U diag((1 - delta*) lambda + delta* m)^(-1/2) U^T``
  and ``m = tr(S) / d``, the inverse square root of the Ledoit-Wolf covariance;
- ``remove_dominant_directions``: ``(x - mu) - U_D U_D^T (x - mu)`` with ``D = D*``,
  the literature's all-but-the-top with the elbow's count (Mu and Viswanath 2018);
- ``remove_dominant_directions_fixed_count``: the same with ``D = floor(d / 100)``,
  the fixed count of Bouras and Edwards, a literal variant that is never adopted.

The cosine search normalises the transformed vectors afterwards; that step is
the caller's, not a transform's.

A pool of the widest model is about 15.7 GB in float32, so a transform never
makes a second copy of it: :func:`apply_in_place` walks the rows in chunks, each
computed in float64 and written back in the array's own precision. The extra
memory is a few chunks, whatever the number of rows.

The module imports nothing beyond numpy, so the search path and the statistics
read one implementation.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import numpy as np

__all__ = [
    "CHUNK_ROWS",
    "FIXED_COUNT_DIVISOR",
    "TRANSFORMS",
    "SpaceParameters",
    "apply_in_place",
    "fixed_count",
    "transformed_copy",
    "whitening_gains",
]

#: The transform values, in the order the contracts list them.
TRANSFORMS: tuple[str, ...] = (
    "center",
    "standardize",
    "whiten",
    "remove_dominant_directions",
    "remove_dominant_directions_fixed_count",
)

#: Bouras and Edwards' fixed count removes one leading direction per hundred dimensions.
FIXED_COUNT_DIVISOR = 100

#: Rows transformed at a time: 4,096 rows of 2,560 dimensions in float64 are 84 MB.
CHUNK_ROWS = 4096


def fixed_count(dimension: int) -> int:
    """The literal variant's number of removed directions, ``floor(d / 100)``."""
    if dimension < 0:
        raise ValueError(f"a space has no negative dimension ({dimension})")
    return dimension // FIXED_COUNT_DIVISOR


@dataclass(frozen=True)
class SpaceParameters:
    """What the transforms read from the statistics of one space.

    ``eigenvalues`` are in descending order and column ``i`` of ``eigenvectors``
    belongs to ``eigenvalues[i]``. ``variances`` are the diagonal of the
    covariance, divided by ``n`` as Ledoit and Wolf's covariance is.
    """

    mean: np.ndarray
    variances: np.ndarray
    eigenvalues: np.ndarray
    eigenvectors: np.ndarray
    shrinkage: float
    removed_directions: int

    def __post_init__(self) -> None:
        d = self.dimension
        if d == 0 or self.mean.shape != (d,):
            raise ValueError("the mean must be a non-empty vector")
        if self.variances.shape != (d,) or self.eigenvalues.shape != (d,):
            raise ValueError(f"the variances and the eigenvalues must hold {d} values")
        if self.eigenvectors.shape != (d, d):
            raise ValueError(f"the eigenvectors must be a {d} x {d} matrix")
        if not 0.0 <= self.shrinkage <= 1.0:
            raise ValueError(f"a shrinkage intensity lies in [0, 1], not {self.shrinkage}")
        if not 0 <= self.removed_directions < d:
            raise ValueError(f"{self.removed_directions} removed directions out of {d}")

    @property
    def dimension(self) -> int:
        """The space's dimension ``d``."""
        return int(self.mean.shape[0])

    @property
    def mean_eigenvalue(self) -> float:
        """``m = tr(S) / d``, the target of Ledoit and Wolf's shrinkage."""
        return float(self.variances.mean())

    @property
    def fixed_count(self) -> int:
        """``floor(d / 100)``, the literal variant's count."""
        return fixed_count(self.dimension)


def whitening_gains(parameters: SpaceParameters) -> np.ndarray:
    """``((1 - delta*) lambda + delta* m)^(-1/2)`` for every direction, refused when not finite."""
    delta = parameters.shrinkage
    shrunk = (1.0 - delta) * parameters.eigenvalues + delta * parameters.mean_eigenvalue
    if not np.all(np.isfinite(shrunk)) or np.any(shrunk <= 0.0):
        raise ValueError(
            "the shrunk covariance is singular: whitening would divide by zero "
            f"(shrinkage {delta}, smallest shrunk eigenvalue {float(np.min(shrunk))})"
        )
    gains: np.ndarray = shrunk**-0.5
    return gains


def _projection_step(basis: np.ndarray) -> Callable[[np.ndarray], np.ndarray]:
    """Remove the span of ``basis``'s columns from centred rows."""

    def step(centred: np.ndarray) -> np.ndarray:
        if basis.shape[1] == 0:
            return centred
        out: np.ndarray = centred - (centred @ basis) @ basis.T
        return out

    return step


def _step(transform: str, parameters: SpaceParameters) -> Callable[[np.ndarray], np.ndarray]:
    """The map one transform applies to centred float64 rows, built once per call."""
    if transform == "center":
        return lambda centred: centred
    if transform == "standardize":
        sigma = np.sqrt(parameters.variances)
        if np.any(sigma <= 0.0):
            zero = np.flatnonzero(sigma <= 0.0)
            raise ValueError(f"dimensions {zero[:5].tolist()} have no variance to divide by")
        return lambda centred: centred / sigma
    if transform == "whiten":
        vectors = parameters.eigenvectors
        matrix = (vectors * whitening_gains(parameters)) @ vectors.T
        return lambda centred: centred @ matrix
    if transform == "remove_dominant_directions":
        return _projection_step(parameters.eigenvectors[:, : parameters.removed_directions])
    if transform == "remove_dominant_directions_fixed_count":
        count = parameters.fixed_count
        return _projection_step(parameters.eigenvectors[:, :count])
    raise ValueError(f"unknown transform {transform!r}; expected one of {TRANSFORMS}")


def apply_in_place(
    vectors: np.ndarray,
    transform: str,
    parameters: SpaceParameters,
    chunk_rows: int = CHUNK_ROWS,
) -> None:
    """Transform the rows of ``vectors`` in place, ``chunk_rows`` at a time.

    Each chunk is centred and mapped in float64 and written back in the array's
    own precision (float32 or float64), so a row transforms to the same values
    whatever chunk it falls in and the array is never copied whole.
    """
    if transform not in TRANSFORMS:
        raise ValueError(f"unknown transform {transform!r}; expected one of {TRANSFORMS}")
    if vectors.dtype not in (np.float32, np.float64):
        raise ValueError(f"transform float32 or float64 rows, not {vectors.dtype}")
    if vectors.ndim != 2 or vectors.shape[1] != parameters.dimension:
        raise ValueError(
            f"rows of {parameters.dimension} values expected, got an array of {vectors.shape}"
        )
    if chunk_rows < 1:
        raise ValueError("a chunk holds at least one row")
    step = _step(transform, parameters)
    for start in range(0, vectors.shape[0], chunk_rows):
        chunk = vectors[start : start + chunk_rows]
        chunk[...] = step(chunk.astype(np.float64) - parameters.mean)


def transformed_copy(
    vectors: np.ndarray, transform: str, parameters: SpaceParameters
) -> np.ndarray:
    """A float64 copy of ``vectors`` under ``transform``; the input is left as it was."""
    out = np.array(vectors, dtype=np.float64, copy=True)
    apply_in_place(out, transform, parameters)
    return out
