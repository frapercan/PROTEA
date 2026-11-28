"""Label-free statistics of a configuration's embedding space at a cut.

The population is a photo's distinct member sequences with a vector under the
configuration, the same for every breadth level of the donor pool. Its vectors
are read from the bank's own shards on this node (ADR-D62: ``md5`` and a
fixed-size list of halves per row), each shard checked against the sha256 its
manifest recorded before a byte of it is used, in two streaming passes in
float64:

1. the count, the sum (so the mean), the histogram of the stored values'
   float16 exponents (so ``nu16``) and the vectors of a seeded sample;
2. the cross-products of the centred rows (so the covariance ``S``, divided by
   ``n`` as Ledoit and Wolf's is, and its diagonal, the per-dimension
   variances), their sum (the corrected two-pass algorithm's correction) and
   ``sum_k ||x_k - mu||^4``, which with ``S`` gives Ledoit and Wolf's ``b2bar``
   without a third pass, since ``sum_k (x_k - mu)^T S (x_k - mu) = n ||S||_F^2``.

From these:

- ``delta*``, Ledoit and Wolf's (2004) shrinkage intensity towards ``m I``,
  ``m = tr(S) / d``, as scikit-learn computes it;
- the eigendecomposition of ``S``, eigenvalues descending, each eigenvector's
  largest component positive;
- ``D*``, the directions the elbow removes: the profile-likelihood scree elbow
  of Zhu and Ghodsi (2006) over the eigenvalues, with the broken-stick count and
  the participation ratio ``(sum lambda)^2 / sum lambda^2`` as cross-checks
  recorded beside it;
- ``nu16``, the float16 rounding variance of the stored values, the mean over
  the stored values of ``ulp16(x)^2 / 12``, where ``ulp16(x)`` is the spacing
  of float16 numbers in the binade of ``x`` (``2^-24`` below the normal range);
- ``n_below_shrinkage``, the eigenvalues below ``delta* m``, and
  ``n_below_storage``, those below ``nu16``: the counts the rule for a second
  whitening value reads (it is evaluated over every configuration, not here);
- the fixed count ``floor(d / 100)`` and its share of the spectrum.

The label-free diagnostics are read before and after each transform of
:mod:`protea.core.embedding_space.transforms` on a seeded sample of the
population: the mean cosine over every pair of the sample, each dimension's
contribution to it, the skewness of the 10-occurrence (how often a vector is
among the ten nearest of the others, a measure of hubness) and the share of the
variance along the leading direction.

Everything is written to one ``.npz`` (the arrays a transform reads, the elbow's
curve, the diagnostics' contributions and the JSON document of every recorded
value), whose bytes do not depend on when it was written.
"""

from __future__ import annotations

import io
import json
import zipfile
from collections import deque
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import scipy.linalg

from protea.core._artifact_identity import sha256_of_bytes
from protea.core.embedding_space.transforms import (
    TRANSFORMS,
    SpaceParameters,
    fixed_count,
    transformed_copy,
)

__all__ = [
    "BATCH_ROWS",
    "FORMAT_VERSION",
    "NEIGHBOURS",
    "SAMPLE_SEED",
    "SAMPLE_SIZE",
    "VARIANTS",
    "BankShard",
    "Progress",
    "SpaceStatistics",
    "SpaceStatisticsError",
    "broken_stick_count",
    "compute_space_statistics",
    "diagnostic_sample",
    "exponent_histogram",
    "float16_rounding_variance",
    "ledoit_wolf_shrinkage",
    "pair_diagnostics",
    "participation_ratio",
    "population_array",
    "population_rows",
    "profile_likelihood_elbow",
    "read_statistics_artifact",
    "space_diagnostics",
    "spectrum_share",
    "spectrum_summary",
    "statistics_artifact",
]

#: The artifact's layout; a reader refuses another.
FORMAT_VERSION = 1
#: Rows decoded from a shard at a time, and centred together in float64.
BATCH_ROWS = 4096
#: The seeded sample the diagnostics read, and its seed.
SAMPLE_SIZE = 10_000
SAMPLE_SEED = 0
#: The ``k`` of the k-occurrence whose skewness measures hubness.
NEIGHBOURS = 10
#: The diagnostics' spaces: as stored, then under each transform.
VARIANTS: tuple[str, ...] = ("raw", *TRANSFORMS)

#: float16 has 10 explicit mantissa bits and an exponent field of 5, biased by 15.
_HALF_MANTISSA_BITS = 10
_HALF_EXPONENT_FIELDS = 32
_HALF_EXPONENT_BIAS = 15
#: Rows of the sample whose similarities to the whole sample are held at once.
_SIMILARITY_ROWS = 512
#: Shards read, verified and decoded at once, ahead of the pass that consumes them.
_READERS = 3

#: ``progress(pass_number, shards_done, shards_total)``.
Progress = Callable[[int, int, int], None]
#: One shard's population rows: ``(md5s, float16 vectors)`` per decoded batch.
_Batches = list[tuple[np.ndarray, np.ndarray]]


class SpaceStatisticsError(ValueError):
    """The bank's shards cannot give the statistics of the population asked for."""


@dataclass(frozen=True)
class BankShard:
    """One shard of a bank, where it is on this node and what its manifest recorded."""

    path: Path
    sha256: str
    rows: int


# Reading the population from the shards ------------------------------------------


def population_array(md5s: Sequence[str]) -> np.ndarray:
    """The population's md5s, unique and sorted, as fixed-width bytes for fast lookups."""
    if not md5s:
        raise SpaceStatisticsError("the population is empty")
    bad = [m for m in md5s if len(m) != 32 or m.strip("0123456789abcdef")]
    if bad:
        raise SpaceStatisticsError(f"{len(bad)} population md5s are not 32 hex digits: {bad[:3]}")
    population: np.ndarray = np.unique(np.asarray(md5s, dtype="S32"))
    return population


def _positions(sorted_md5s: np.ndarray, md5s: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Where each of ``md5s`` sits in ``sorted_md5s``, and whether it is there."""
    where = np.minimum(np.searchsorted(sorted_md5s, md5s), len(sorted_md5s) - 1)
    return where, sorted_md5s[where] == md5s


def _population_of_shard(
    shard: BankShard, population: np.ndarray, dimension: int, batch_rows: int
) -> _Batches:
    """One shard's population rows, batch by batch, once its bytes hash as recorded."""
    blob = shard.path.read_bytes()
    got = sha256_of_bytes(blob)
    if got != shard.sha256:
        raise SpaceStatisticsError(f"{shard.path.name} hashes to {got}, not to {shard.sha256}")
    handle = pq.ParquetFile(pa.BufferReader(blob))
    width = handle.schema_arrow.field("vector").type.list_size
    if width != dimension:
        raise SpaceStatisticsError(f"{shard.path.name} holds vectors of {width}, not {dimension}")
    batches: _Batches = []
    for batch in handle.iter_batches(batch_size=batch_rows, columns=["md5", "vector"]):
        column = batch.column("vector")
        if column.null_count:
            raise SpaceStatisticsError(f"{shard.path.name} holds a null vector")
        md5s = np.asarray(batch.column("md5").to_pylist(), dtype="S32")
        _, keep = _positions(population, md5s)
        if keep.any():
            flat = column.flatten().to_numpy(zero_copy_only=False)
            batches.append((md5s[keep], flat.reshape(len(md5s), dimension)[keep]))
    return batches


def _rows_of(entry: tuple[int, Future[_Batches]]) -> Iterator[tuple[int, np.ndarray, np.ndarray]]:
    index, read = entry
    for md5s, vectors in read.result():
        yield index, md5s, vectors


def population_rows(
    shards: Sequence[BankShard],
    population: np.ndarray,
    dimension: int,
    batch_rows: int = BATCH_ROWS,
) -> Iterator[tuple[int, np.ndarray, np.ndarray]]:
    """``(shard index, md5s, float16 vectors)`` of the population's rows, in the shards' order.

    ``_READERS`` shards are read, verified and decoded at once while the pass
    consumes the earlier ones, and the rows always arrive in the shards' order,
    so a pass adds them up in the same order whatever the readers' timing.
    """
    with ThreadPoolExecutor(max_workers=_READERS) as readers:
        pending: deque[tuple[int, Future[_Batches]]] = deque()
        for index, shard in enumerate(shards):
            read = readers.submit(_population_of_shard, shard, population, dimension, batch_rows)
            pending.append((index, read))
            if len(pending) > _READERS:
                yield from _rows_of(pending.popleft())
        while pending:
            yield from _rows_of(pending.popleft())


def diagnostic_sample(
    population: np.ndarray, size: int = SAMPLE_SIZE, seed: int = SAMPLE_SEED
) -> np.ndarray:
    """The md5s of the seeded sample, sorted: the same sequences for every configuration."""
    count = min(size, len(population))
    chosen = np.random.default_rng(seed).choice(len(population), size=count, replace=False)
    sample: np.ndarray = population[np.sort(chosen)]
    return sample


# The two passes ------------------------------------------------------------------


def exponent_histogram(values: np.ndarray) -> np.ndarray:
    """How many float16 values carry each of the 32 exponent fields."""
    halves = np.ascontiguousarray(values, dtype=np.float16)
    fields = (halves.view(np.uint16) >> _HALF_MANTISSA_BITS) & (_HALF_EXPONENT_FIELDS - 1)
    histogram: np.ndarray = np.bincount(fields.ravel(), minlength=_HALF_EXPONENT_FIELDS)
    return histogram


def float16_rounding_variance(histogram: np.ndarray) -> float:
    """``nu16``: the mean of ``ulp16(x)^2 / 12`` over the values the histogram counts.

    A normal float16 with exponent field ``e`` lies in ``[2^(e-15), 2^(e-14))``,
    where consecutive numbers are ``2^(e-25)`` apart; zero and the subnormals,
    field 0, are ``2^-24`` apart. Field 31 holds infinities and NaN, refused.
    """
    counts = np.asarray(histogram, dtype=np.int64)
    if counts.shape != (_HALF_EXPONENT_FIELDS,) or counts.sum() == 0:
        raise SpaceStatisticsError("a float16 exponent histogram has 32 fields and some values")
    if counts[-1]:
        raise SpaceStatisticsError(f"{int(counts[-1])} stored values are not finite")
    fields = np.arange(_HALF_EXPONENT_FIELDS)
    spacing = np.where(
        fields == 0,
        2.0 ** (1 - _HALF_EXPONENT_BIAS - _HALF_MANTISSA_BITS),
        2.0 ** (fields - _HALF_EXPONENT_BIAS - _HALF_MANTISSA_BITS),
    )
    return float((counts * spacing**2).sum() / (12.0 * counts.sum()))


class _FirstPass:
    """The count, the sum, the exponents and the sample, and every member seen once."""

    def __init__(self, population: np.ndarray, sample: np.ndarray, dimension: int) -> None:
        self.population = population
        self.seen = np.zeros(len(population), dtype=bool)
        self.total = np.zeros(dimension, dtype=np.float64)
        self.histogram = np.zeros(_HALF_EXPONENT_FIELDS, dtype=np.int64)
        self.sample_md5s = sample
        self.sample = np.zeros((len(sample), dimension), dtype=np.float16)
        self.sampled = np.zeros(len(sample), dtype=bool)

    def add(self, md5s: np.ndarray, vectors: np.ndarray) -> None:
        where, _ = _positions(self.population, md5s)
        if self.seen[where].any() or len(np.unique(where)) != len(where):
            raise SpaceStatisticsError("a population sequence appears twice in the bank's shards")
        self.seen[where] = True
        self.total += vectors.sum(axis=0, dtype=np.float64)
        self.histogram += exponent_histogram(vectors)
        if len(self.sample_md5s):
            spot, hit = _positions(self.sample_md5s, md5s)
            self.sample[spot[hit]] = vectors[hit]
            self.sampled[spot[hit]] = True

    @property
    def n(self) -> int:
        return int(self.seen.sum())

    def check_complete(self) -> None:
        missing = len(self.population) - self.n
        if missing:
            example = self.population[~self.seen][:3].astype(str).tolist()
            raise SpaceStatisticsError(
                f"{missing} population sequences are in no shard of the bank (first {example})"
            )
        if not self.sampled.all():
            raise SpaceStatisticsError("a sampled sequence was not read back from the shards")


class _SecondPass:
    """The centred cross-products, their sum and the fourth moment of the norms."""

    def __init__(self, mean: np.ndarray) -> None:
        dimension = len(mean)
        self.mean = mean
        self.cross = np.zeros((dimension, dimension), dtype=np.float64)
        self.offset = np.zeros(dimension, dtype=np.float64)
        self.fourth = 0.0
        self.rows = 0

    def add(self, vectors: np.ndarray) -> None:
        centred = vectors.astype(np.float64) - self.mean
        self.cross += centred.T @ centred
        self.offset += centred.sum(axis=0)
        squares = np.einsum("ij,ij->i", centred, centred)
        self.fourth += float(squares @ squares)
        self.rows += len(centred)


# The spectrum --------------------------------------------------------------------


def ledoit_wolf_shrinkage(covariance: np.ndarray, fourth_moment_sum: float, n: int) -> float:
    """``delta*`` from ``S`` (divided by ``n``) and ``sum_k ||x_k - mu||^4``.

    With the norm ``||A||^2 = tr(A A^T) / d`` of Ledoit and Wolf (2004):
    ``m = tr(S) / d``, ``d2 = ||S - m I||^2`` and
    ``b2bar = (1 / n^2) sum_k ||x_k x_k^T - S||^2``, which equals
    ``(sum_k ||x_k||^4 / n - ||S||_F^2) / (n d)``; then ``b2 = min(b2bar, d2)``
    and ``delta* = b2 / d2``, zero when ``S`` is already a multiple of the identity.
    """
    dimension = covariance.shape[0]
    trace = float(np.trace(covariance))
    mean_eigenvalue = trace / dimension
    frobenius = float(np.sum(covariance * covariance))
    distance = (frobenius - dimension * mean_eigenvalue**2) / dimension
    spread = max((fourth_moment_sum / n - frobenius) / (n * dimension), 0.0)
    bounded = min(spread, distance)
    if distance <= 0.0 or bounded <= 0.0:
        return 0.0
    return bounded / distance


def profile_likelihood_elbow(values: np.ndarray) -> tuple[int, np.ndarray]:
    """Zhu and Ghodsi's (2006) elbow of a scree of ``values`` in descending order.

    For each ``q`` in ``1 .. p - 1`` the first ``q`` values and the other
    ``p - q`` are two normal samples with their own means and one common
    variance; with the maximum-likelihood estimates plugged in, the profile
    log-likelihood is ``-(p / 2) (log(2 pi sigma2_q) + 1)``, ``sigma2_q`` the
    pooled within-group sum of squares over ``p``. Returns the ``q`` that
    maximises it (the smallest on a tie) and the curve; ``0`` when ``p < 2``.
    """
    ordered = np.asarray(values, dtype=np.float64)
    p = ordered.size
    if p < 2:
        return 0, np.empty(0)
    within = np.empty(p - 1)
    for q in range(1, p):
        left, right = ordered[:q], ordered[q:]
        within[q - 1] = ((left - left.mean()) ** 2).sum() + ((right - right.mean()) ** 2).sum()
    with np.errstate(divide="ignore"):
        curve = -0.5 * p * (np.log(2.0 * np.pi * within / p) + 1.0)
    return int(np.argmin(within)) + 1, curve


def _non_negative(eigenvalues: np.ndarray) -> np.ndarray:
    """The spectrum with the rounding's negative eigenvalues read as zero."""
    clipped: np.ndarray = np.clip(np.asarray(eigenvalues, dtype=np.float64), 0.0, None)
    return clipped


def broken_stick_count(eigenvalues: np.ndarray) -> int:
    """Leading directions whose share of the variance beats the broken stick's expectation.

    The ``k``-th of ``p`` pieces of a randomly broken stick is expected to hold
    ``(1 / p) sum_{i=k}^{p} 1 / i`` of it; the count stops at the first
    direction that holds less.
    """
    values = _non_negative(eigenvalues)
    total = values.sum()
    if total <= 0.0:
        return 0
    p = values.size
    expected = np.cumsum((1.0 / np.arange(1, p + 1))[::-1])[::-1] / p
    above = values / total > expected
    return p if above.all() else int(np.argmin(above))


def participation_ratio(eigenvalues: np.ndarray) -> float:
    """``(sum lambda)^2 / sum lambda^2``, the spectrum's effective number of directions."""
    values = _non_negative(eigenvalues)
    squares = float((values**2).sum())
    return float(values.sum() ** 2 / squares) if squares > 0.0 else 0.0


def spectrum_share(eigenvalues: np.ndarray, count: int) -> float:
    """The share of the variance held by the ``count`` leading directions."""
    values = _non_negative(eigenvalues)
    total = float(values.sum())
    return float(values[:count].sum() / total) if total > 0.0 else 0.0


# The statistics ------------------------------------------------------------------


@dataclass(frozen=True)
class SpaceStatistics:
    """What the two passes measured: the transforms' parameters and the recorded values."""

    n: int
    parameters: SpaceParameters
    nu16: float
    profile_log_likelihood: np.ndarray
    sample_md5s: np.ndarray
    sample: np.ndarray

    @property
    def dimension(self) -> int:
        return self.parameters.dimension


def _eigen(covariance: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Eigenvalues descending, each eigenvector's largest component positive."""
    values, vectors = np.linalg.eigh(covariance)
    values, vectors = values[::-1].copy(), vectors[:, ::-1].copy()
    columns = np.arange(vectors.shape[1])
    signs = np.sign(vectors[np.argmax(np.abs(vectors), axis=0), columns])
    vectors *= np.where(signs == 0.0, 1.0, signs)
    return values, vectors


def _assemble(first: _FirstPass, second: _SecondPass) -> SpaceStatistics:
    n = first.n
    covariance = (second.cross - np.outer(second.offset, second.offset) / n) / n
    covariance = (covariance + covariance.T) / 2.0
    eigenvalues, eigenvectors = _eigen(covariance)
    removed, curve = profile_likelihood_elbow(_non_negative(eigenvalues))
    parameters = SpaceParameters(
        mean=second.mean + second.offset / n,
        variances=np.diag(covariance).copy(),
        eigenvalues=eigenvalues,
        eigenvectors=eigenvectors,
        shrinkage=ledoit_wolf_shrinkage(covariance, second.fourth, n),
        removed_directions=removed,
    )
    return SpaceStatistics(
        n=n,
        parameters=parameters,
        nu16=float16_rounding_variance(first.histogram),
        profile_log_likelihood=curve,
        sample_md5s=first.sample_md5s,
        sample=first.sample,
    )


def compute_space_statistics(
    shards: Sequence[BankShard],
    population: np.ndarray,
    dimension: int,
    *,
    batch_rows: int = BATCH_ROWS,
    progress: Progress | None = None,
) -> SpaceStatistics:
    """The statistics of ``population`` (sorted md5 bytes) over two passes of ``shards``."""
    if len(population) < 2:
        raise SpaceStatisticsError("a covariance needs at least two sequences")
    sample = diagnostic_sample(population, SAMPLE_SIZE, SAMPLE_SEED)
    first = _FirstPass(population, sample, dimension)
    for index, md5s, vectors in population_rows(shards, population, dimension, batch_rows):
        first.add(md5s, vectors)
        if progress is not None:
            progress(1, index, len(shards))
    first.check_complete()
    if progress is not None:
        progress(1, len(shards), len(shards))
    second = _SecondPass(first.total / first.n)
    for index, _, vectors in population_rows(shards, population, dimension, batch_rows):
        second.add(vectors)
        if progress is not None:
            progress(2, index, len(shards))
    if second.rows != first.n:
        raise SpaceStatisticsError(
            f"the second pass read {second.rows} rows and the first {first.n}: the shards changed"
        )
    if progress is not None:
        progress(2, len(shards), len(shards))
    return _assemble(first, second)


def spectrum_summary(statistics: SpaceStatistics) -> dict[str, Any]:
    """The recorded values of the spectrum, the counts and the cross-checks."""
    parameters = statistics.parameters
    eigenvalues = parameters.eigenvalues
    delta, mean_eigenvalue = parameters.shrinkage, parameters.mean_eigenvalue
    floor = delta * mean_eigenvalue
    shrunk = (1.0 - delta) * eigenvalues + floor
    exact = np.maximum(eigenvalues, statistics.nu16)
    with np.errstate(divide="ignore", invalid="ignore"):
        gain_ratio = np.sqrt(exact / shrunk)
    finite = gain_ratio[np.isfinite(gain_ratio)]
    count = parameters.fixed_count
    return {
        "trace": float(parameters.variances.sum()),
        "mean_eigenvalue": mean_eigenvalue,
        "largest_eigenvalue": float(eigenvalues[0]),
        "smallest_eigenvalue": float(eigenvalues[-1]),
        "negative_eigenvalues": int((eigenvalues < 0.0).sum()),
        "shrinkage": delta,
        "shrinkage_floor": floor,
        "nu16": statistics.nu16,
        "n_below_shrinkage": int((eigenvalues < floor).sum()),
        "n_below_storage": int((eigenvalues < statistics.nu16).sum()),
        "largest_relative_gain_difference": (
            float(np.max(np.abs(finite - 1.0))) if finite.size else None
        ),
        "removed_directions": parameters.removed_directions,
        "removed_directions_share": spectrum_share(eigenvalues, parameters.removed_directions),
        "broken_stick_count": broken_stick_count(eigenvalues),
        "participation_ratio": participation_ratio(eigenvalues),
        "fixed_count": count,
        "fixed_count_share": spectrum_share(eigenvalues, count),
    }


# The diagnostics -----------------------------------------------------------------


def _unit_rows(vectors: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    units: np.ndarray = np.divide(vectors, norms, out=np.zeros_like(vectors), where=norms > 0.0)
    return units


def _hubness_skewness(units: np.ndarray, neighbours: int) -> float | None:
    """The skewness of how often each row is among the ``neighbours`` nearest of the others."""
    size = units.shape[0]
    if size <= neighbours:
        return None
    single = units.astype(np.float32)
    occurrences = np.zeros(size, dtype=np.int64)
    for start in range(0, size, _SIMILARITY_ROWS):
        block = single[start : start + _SIMILARITY_ROWS] @ single.T
        rows = np.arange(block.shape[0])
        block[rows, rows + start] = -np.inf
        nearest = np.argpartition(block, -neighbours, axis=1)[:, -neighbours:]
        occurrences += np.bincount(nearest.ravel(), minlength=size)
    spread = occurrences - occurrences.mean()
    deviation = float(np.sqrt((spread**2).mean()))
    return float((spread**3).mean() / deviation**3) if deviation > 0.0 else 0.0


def _top_direction_share(units: np.ndarray) -> float:
    """The largest eigenvalue of the rows' covariance over its trace (that one alone is solved)."""
    centred = units - units.mean(axis=0)
    covariance = centred.T @ centred / units.shape[0]
    total = float(np.trace(covariance))
    if total <= 0.0:
        return 0.0
    last = covariance.shape[0] - 1
    largest = scipy.linalg.eigh(covariance, eigvals_only=True, subset_by_index=[last, last])
    return float(largest[0] / total)


def pair_diagnostics(
    vectors: np.ndarray, neighbours: int = NEIGHBOURS
) -> tuple[dict[str, Any], np.ndarray]:
    """The diagnostics of one space on the sample's rows, and each dimension's contribution.

    Every pair of distinct rows is a random pair: the mean pair cosine is the
    mean over all ``s (s - 1) / 2`` of them, and dimension ``j`` contributes
    ``((sum_i u_ij)^2 - sum_i u_ij^2) / (s (s - 1))`` of it, ``u`` the rows
    normalised (Timkey and van Schijndel 2021).
    """
    size = vectors.shape[0]
    if size < 2:
        raise SpaceStatisticsError("the diagnostics need at least two sampled sequences")
    units = _unit_rows(np.asarray(vectors, dtype=np.float64))
    sums = units.sum(axis=0)
    contributions = (sums**2 - (units**2).sum(axis=0)) / (size * (size - 1))
    largest = int(np.argmax(np.abs(contributions)))
    record = {
        "mean_pair_cosine": float(contributions.sum()),
        "largest_contribution": {"dimension": largest, "value": float(contributions[largest])},
        "hubness_skewness": _hubness_skewness(units, neighbours),
        "top_direction_share": _top_direction_share(units),
    }
    return record, contributions


def space_diagnostics(statistics: SpaceStatistics) -> tuple[dict[str, Any], np.ndarray]:
    """The diagnostics before and after each transform, and their contributions by variant.

    A transform the space cannot take (a dimension without variance to
    standardise, a singular shrunk covariance to whiten) is recorded with the
    reason it was refused, and its contributions are NaN.
    """
    sample = statistics.sample.astype(np.float64)
    records: dict[str, Any] = {}
    contributions = np.full((len(VARIANTS), statistics.dimension), np.nan)
    for row, variant in enumerate(VARIANTS):
        space = sample
        if variant != "raw":
            try:
                space = transformed_copy(sample, variant, statistics.parameters)
            except ValueError as refused:
                records[variant] = {"refused": str(refused)}
                continue
        records[variant], contributions[row] = pair_diagnostics(space, NEIGHBOURS)
    size = int(sample.shape[0])
    return {
        "sample_size": size,
        "sample_seed": SAMPLE_SEED,
        "neighbours": NEIGHBOURS,
        "pairs": size * (size - 1) // 2,
        "variants": records,
    }, contributions


# The artifact --------------------------------------------------------------------


def _npz_bytes(arrays: dict[str, np.ndarray]) -> bytes:
    """An ``.npz`` whose bytes depend on the arrays alone: sorted, stored, timeless."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_STORED, allowZip64=True) as archive:
        for name in sorted(arrays):
            entry = zipfile.ZipInfo(f"{name}.npy", date_time=(1980, 1, 1, 0, 0, 0))
            with archive.open(entry, "w", force_zip64=True) as handle:
                np.lib.format.write_array(handle, np.asarray(arrays[name]), allow_pickle=False)
    return buffer.getvalue()


def statistics_artifact(
    statistics: SpaceStatistics, record: dict[str, Any]
) -> tuple[bytes, dict[str, Any]]:
    """The artifact's bytes and its JSON document (``record`` names what was measured)."""
    diagnostics, contributions = space_diagnostics(statistics)
    document = {
        "format_version": FORMAT_VERSION,
        **record,
        "n": statistics.n,
        "d": statistics.dimension,
        "spectrum": spectrum_summary(statistics),
        "diagnostics": diagnostics,
    }
    text = json.dumps(document, sort_keys=True, allow_nan=False).encode("utf-8")
    parameters = statistics.parameters
    arrays: dict[str, np.ndarray] = {
        "format_version": np.asarray(FORMAT_VERSION, dtype=np.int64),
        "mean": parameters.mean,
        "variances": parameters.variances,
        "eigenvalues": parameters.eigenvalues,
        "eigenvectors": parameters.eigenvectors,
        "shrinkage": np.asarray(parameters.shrinkage, dtype=np.float64),
        "removed_directions": np.asarray(parameters.removed_directions, dtype=np.int64),
        "nu16": np.asarray(statistics.nu16, dtype=np.float64),
        "profile_log_likelihood": statistics.profile_log_likelihood,
        "contributions": contributions,
        "document": np.frombuffer(text, dtype=np.uint8),
    }
    return _npz_bytes(arrays), document


def read_statistics_artifact(blob: bytes) -> tuple[SpaceParameters, dict[str, Any]]:
    """The transforms' parameters and the JSON document of an artifact's bytes."""
    with np.load(io.BytesIO(blob), allow_pickle=False) as archive:
        version = int(archive["format_version"])
        if version != FORMAT_VERSION:
            raise SpaceStatisticsError(
                f"artifact format {version}, this code reads {FORMAT_VERSION}"
            )
        parameters = SpaceParameters(
            mean=archive["mean"],
            variances=archive["variances"],
            eigenvalues=archive["eigenvalues"],
            eigenvectors=archive["eigenvectors"],
            shrinkage=float(archive["shrinkage"]),
            removed_directions=int(archive["removed_directions"]),
        )
        document: dict[str, Any] = json.loads(archive["document"].tobytes().decode("utf-8"))
    if fixed_count(parameters.dimension) != document["spectrum"]["fixed_count"]:
        raise SpaceStatisticsError("the artifact's fixed count is not floor(d / 100)")
    return parameters, document
