"""The label-free statistics of a space: each value against a closed form or a direct computation.

The streaming passes read synthetic banks written in the ADR-D62 shard format
(``write_shard``, the builder's own writer), and every value they produce is
held to what numpy computes on the same rows in memory: the mean, the
variances, the covariance's spectrum, Ledoit and Wolf's shrinkage and the
float16 rounding variance. The shrinkage also meets a case worked by hand
(``17 / 18``), the elbow meets spectra whose split is known, and ``nu16`` meets
``numpy.spacing``.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pytest

from protea.core.embedding_bank.build import write_shard
from protea.core.embedding_space import statistics
from protea.core.embedding_space.statistics import (
    VARIANTS,
    BankShard,
    SpaceStatistics,
    SpaceStatisticsError,
    broken_stick_count,
    compute_space_statistics,
    diagnostic_sample,
    exponent_histogram,
    float16_rounding_variance,
    ledoit_wolf_shrinkage,
    pair_diagnostics,
    participation_ratio,
    population_array,
    profile_likelihood_elbow,
    read_statistics_artifact,
    space_diagnostics,
    spectrum_share,
    spectrum_summary,
    statistics_artifact,
)
from protea.core.embedding_space.transforms import SpaceParameters


def _md5(i: int) -> str:
    return hashlib.md5(f"sequence-{i}".encode(), usedforsecurity=False).hexdigest()


def _bank(
    tmp_path: Path, vectors: np.ndarray, cuts: list[int]
) -> tuple[list[str], list[BankShard]]:
    """``vectors`` stored as float16 shards split at ``cuts``; their md5s and shards."""
    md5s = [_md5(i) for i in range(len(vectors))]
    halves = np.asarray(vectors, dtype=np.float16)
    bounds = [0, *cuts, len(vectors)]
    shards = []
    for index, (start, end) in enumerate(zip(bounds[:-1], bounds[1:], strict=True)):
        path = write_shard(tmp_path, index, md5s[start:end], halves[start:end])
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        shards.append(BankShard(path, digest, end - start))
    return md5s, shards


def _anisotropic(n: int, d: int, seed: int = 7) -> np.ndarray:
    """Rows with a common offset and a decaying spectrum, the shape of a language model's space."""
    rng = np.random.default_rng(seed)
    scales = np.geomspace(1.0, 1e-3, d)
    return rng.normal(size=(n, d)) * scales + np.linspace(0.5, -0.2, d)


def _in_memory(rows: np.ndarray) -> dict[str, object]:
    """The same values computed on the rows held in memory."""
    centred = rows - rows.mean(axis=0)
    covariance = centred.T @ centred / len(rows)
    norms = np.einsum("ij,ij->i", centred, centred)
    return {
        "mean": rows.mean(axis=0),
        "covariance": covariance,
        "eigenvalues": np.sort(np.linalg.eigvalsh(covariance))[::-1],
        "shrinkage": ledoit_wolf_shrinkage(covariance, float(norms @ norms), len(rows)),
    }


# Ledoit and Wolf's shrinkage -----------------------------------------------------


class TestLedoitWolfShrinkage:
    #: Four centred rows in two dimensions, worked by hand: S = diag(2, 1/2), m = 5/4,
    #: d2 = 9/16, sum_k ||x_k||^4 = 34, ||S||_F^2 = 17/4, so b2bar = 17/32 and
    #: delta* = (17/32) / (9/16) = 17/18.
    ROWS = np.array([[2.0, 0.0], [-2.0, 0.0], [0.0, 1.0], [0.0, -1.0]])

    def test_the_closed_form_case(self) -> None:
        covariance = self.ROWS.T @ self.ROWS / 4
        norms = (self.ROWS**2).sum(axis=1)
        assert ledoit_wolf_shrinkage(covariance, float((norms**2).sum()), 4) == pytest.approx(
            17 / 18, abs=1e-15
        )

    def test_the_closed_form_case_through_the_shards(self, tmp_path: Path) -> None:
        md5s, shards = _bank(tmp_path, self.ROWS, [1, 3])
        result = compute_space_statistics(shards, population_array(md5s), 2, batch_rows=1)
        assert result.parameters.shrinkage == pytest.approx(17 / 18, abs=1e-15)
        assert result.parameters.mean_eigenvalue == pytest.approx(5 / 4)
        assert np.allclose(result.parameters.eigenvalues, [2.0, 0.5])

    def test_the_streaming_identity_equals_the_papers_sum(self) -> None:
        # b2bar = (1 / n^2) sum_k ||x_k x_k^T - S||^2 with ||A||^2 = tr(A A^T) / d,
        # computed term by term as Ledoit and Wolf define it.
        rows = _anisotropic(60, 5)
        centred = rows - rows.mean(axis=0)
        n, d = centred.shape
        covariance = centred.T @ centred / n
        mean_eigenvalue = np.trace(covariance) / d
        distance = np.sum((covariance - mean_eigenvalue * np.eye(d)) ** 2) / d
        spread = sum(np.sum((np.outer(x, x) - covariance) ** 2) / d for x in centred) / n**2
        expected = min(spread, distance) / distance
        norms = (centred**2).sum(axis=1)
        got = ledoit_wolf_shrinkage(covariance, float((norms**2).sum()), n)
        assert got == pytest.approx(expected, rel=1e-12)

    def test_it_agrees_with_scikit_learn(self) -> None:
        covariance_module = pytest.importorskip("sklearn.covariance")
        rows = _anisotropic(300, 12)
        assert _in_memory(rows)["shrinkage"] == pytest.approx(
            covariance_module.ledoit_wolf_shrinkage(rows), rel=1e-10
        )

    def test_a_covariance_already_spherical_is_not_shrunk(self) -> None:
        rows = np.array([[1.0, 0.0], [-1.0, 0.0], [0.0, 1.0], [0.0, -1.0]])
        covariance = rows.T @ rows / 4
        assert ledoit_wolf_shrinkage(covariance, 4.0, 4) == 0.0


# The spectrum's counts -----------------------------------------------------------


class TestTheElbow:
    def test_two_plateaus_split_where_they_meet(self) -> None:
        values = np.array([5.0, 5.0, 5.0] + [1.0] * 20)
        assert profile_likelihood_elbow(values)[0] == 3

    def test_a_noisy_two_level_spectrum(self) -> None:
        rng = np.random.default_rng(3)
        values = np.sort(np.r_[10 + rng.normal(0, 0.3, 4), 1 + rng.normal(0, 0.1, 60)])[::-1]
        assert profile_likelihood_elbow(values)[0] == 4

    def test_a_case_worked_by_hand(self) -> None:
        # [4, 3, 1, 0.5]: within-group sums of squares 3.5 (q=1), 0.625 (q=2), 14/3 (q=3).
        elbow, curve = profile_likelihood_elbow(np.array([4.0, 3.0, 1.0, 0.5]))
        within = np.array([3.5, 0.625, 14 / 3])
        assert elbow == 2
        assert np.allclose(curve, -2.0 * (np.log(2 * np.pi * within / 4) + 1))
        assert int(np.argmax(curve)) + 1 == elbow

    def test_a_tie_takes_the_fewest_directions(self) -> None:
        # [3, 2, 1]: both splits leave a within-group sum of squares of 0.5.
        assert profile_likelihood_elbow(np.array([3.0, 2.0, 1.0]))[0] == 1

    def test_one_value_has_no_elbow(self) -> None:
        elbow, curve = profile_likelihood_elbow(np.array([1.0]))
        assert elbow == 0 and curve.size == 0


class TestTheCrossChecks:
    def test_the_broken_stick_by_hand(self) -> None:
        # Shares 0.6, 0.2, 0.1, 0.1 against 25/48, 13/48, 7/48, 3/48: the first beats it.
        assert broken_stick_count(np.array([6.0, 2.0, 1.0, 1.0])) == 1

    def test_a_flat_spectrum_has_nothing_above_the_stick(self) -> None:
        assert broken_stick_count(np.ones(10)) == 0

    def test_the_participation_ratio(self) -> None:
        assert participation_ratio(np.array([6.0, 2.0, 1.0, 1.0])) == pytest.approx(100 / 42)
        assert participation_ratio(np.ones(7)) == pytest.approx(7.0)

    def test_negative_rounding_is_read_as_zero(self) -> None:
        assert participation_ratio(np.array([1.0, -1e-18])) == pytest.approx(1.0)
        assert spectrum_share(np.array([3.0, 1.0, -1e-17]), 1) == pytest.approx(0.75)


class TestTheFixedCount:
    @pytest.mark.parametrize(("d", "count"), [(4, 0), (99, 0), (100, 1), (768, 7), (2560, 25)])
    def test_it_is_floor_d_over_100(self, d: int, count: int) -> None:
        parameters = SpaceParameters(
            mean=np.zeros(d),
            variances=np.ones(d),
            eigenvalues=np.ones(d),
            eigenvectors=np.eye(d),
            shrinkage=0.0,
            removed_directions=0,
        )
        assert parameters.fixed_count == count

    def test_its_share_of_the_spectrum_is_recorded(self) -> None:
        d = 200
        eigenvalues = np.r_[[50.0, 30.0], np.ones(d - 2)]
        parameters = SpaceParameters(
            mean=np.zeros(d),
            variances=np.full(d, eigenvalues.sum() / d),
            eigenvalues=eigenvalues,
            eigenvectors=np.eye(d),
            shrinkage=0.01,
            removed_directions=2,
        )
        result = SpaceStatistics(2, parameters, 1e-9, np.empty(0), np.empty(0, "S32"), np.empty(0))
        summary = spectrum_summary(result)
        assert summary["fixed_count"] == 2
        assert summary["fixed_count_share"] == pytest.approx(80 / 278)
        assert summary["removed_directions_share"] == pytest.approx(80 / 278)


# The float16 rounding variance and the two counts --------------------------------


class TestTheFloat16RoundingVariance:
    def test_it_equals_the_mean_of_the_spacings_squared_over_twelve(self) -> None:
        rng = np.random.default_rng(11)
        values = np.r_[
            rng.normal(0, 0.05, 5000),
            rng.normal(0, 30.0, 500),
            [0.0, -0.0, 6e-8, -3e-6, 6.1e-5, 1000.0, -2.0],
        ].astype(np.float16)
        direct = float(np.mean(np.spacing(np.abs(values)).astype(np.float64) ** 2) / 12)
        assert float16_rounding_variance(exponent_histogram(values)) == pytest.approx(
            direct, rel=1e-12
        )

    def test_the_largest_half_reads_its_binade_spacing(self) -> None:
        values = np.array([65504.0], dtype=np.float16)
        assert float16_rounding_variance(exponent_histogram(values)) == 32.0**2 / 12

    def test_values_that_are_not_finite_are_refused(self) -> None:
        values = np.array([1.0, np.inf], dtype=np.float16)
        with pytest.raises(SpaceStatisticsError, match="not finite"):
            float16_rounding_variance(exponent_histogram(values))

    def test_the_counts_compare_eigenvalues_with_both_floors(self) -> None:
        d = 6
        eigenvalues = np.array([1.0, 0.1, 1e-3, 1e-6, 1e-9, 1e-12])
        parameters = SpaceParameters(
            mean=np.zeros(d),
            variances=np.full(d, eigenvalues.sum() / d),
            eigenvalues=eigenvalues,
            eigenvectors=np.eye(d),
            shrinkage=1e-4,
            removed_directions=1,
        )
        result = SpaceStatistics(9, parameters, 1e-10, np.empty(0), np.empty(0, "S32"), np.empty(0))
        summary = spectrum_summary(result)
        floor = 1e-4 * eigenvalues.sum() / d
        assert summary["shrinkage_floor"] == pytest.approx(floor)
        assert summary["n_below_shrinkage"] == int((eigenvalues < floor).sum()) == 3
        assert summary["n_below_storage"] == 1
        exact = np.maximum(eigenvalues, 1e-10)
        shrunk = (1 - 1e-4) * eigenvalues + floor
        assert summary["largest_relative_gain_difference"] == pytest.approx(
            float(np.max(np.abs(np.sqrt(exact / shrunk) - 1)))
        )


# The two passes ------------------------------------------------------------------


class TestStreamingEqualsInMemory:
    @pytest.mark.parametrize("batch_rows", [1, 7, 4096])
    def test_every_value_matches_whatever_the_batch(self, tmp_path: Path, batch_rows: int) -> None:
        rows = _anisotropic(400, 16)
        md5s, shards = _bank(tmp_path, rows, [90, 91, 250])
        result = compute_space_statistics(shards, population_array(md5s), 16, batch_rows=batch_rows)
        stored = rows.astype(np.float16).astype(np.float64)
        expected = _in_memory(stored)
        assert result.n == 400
        assert np.allclose(result.parameters.mean, expected["mean"], rtol=0, atol=1e-13)
        assert np.allclose(
            result.parameters.variances, np.diag(expected["covariance"]), rtol=1e-11, atol=0
        )
        assert np.allclose(
            result.parameters.eigenvalues, expected["eigenvalues"], rtol=1e-9, atol=1e-14
        )
        assert result.parameters.shrinkage == pytest.approx(expected["shrinkage"], rel=1e-9)
        assert result.nu16 == pytest.approx(
            float(np.mean(np.spacing(np.abs(rows.astype(np.float16))).astype(float) ** 2) / 12)
        )

    def test_the_eigenvectors_rebuild_the_covariance(self, tmp_path: Path) -> None:
        rows = _anisotropic(300, 12)
        md5s, shards = _bank(tmp_path, rows, [150])
        parameters = compute_space_statistics(shards, population_array(md5s), 12).parameters
        vectors, values = parameters.eigenvectors, parameters.eigenvalues
        covariance = _in_memory(rows.astype(np.float16).astype(float))["covariance"]
        assert np.allclose(vectors @ np.diag(values) @ vectors.T, covariance, atol=1e-12)
        assert np.allclose(vectors.T @ vectors, np.eye(12), atol=1e-12)
        assert (vectors[np.abs(vectors).argmax(axis=0), np.arange(12)] > 0).all()

    def test_only_the_population_is_read(self, tmp_path: Path) -> None:
        rows = _anisotropic(200, 8)
        md5s, shards = _bank(tmp_path, rows, [100])
        chosen = md5s[1::3]
        result = compute_space_statistics(shards, population_array(chosen), 8, batch_rows=5)
        expected = _in_memory(rows[1::3].astype(np.float16).astype(float))
        assert result.n == len(chosen)
        assert np.allclose(result.parameters.eigenvalues, expected["eigenvalues"], rtol=1e-9)

    def test_a_member_the_bank_lacks_is_refused(self, tmp_path: Path) -> None:
        rows = _anisotropic(20, 4)
        md5s, shards = _bank(tmp_path, rows, [10])
        population = population_array([*md5s, _md5(999)])
        with pytest.raises(SpaceStatisticsError, match="1 population sequences are in no shard"):
            compute_space_statistics(shards, population, 4)

    def test_a_member_in_two_shards_is_refused(self, tmp_path: Path) -> None:
        rows = _anisotropic(20, 4)
        md5s, shards = _bank(tmp_path, rows, [10])
        again = write_shard(tmp_path, 7, md5s[:1], rows[:1].astype(np.float16))
        extra = BankShard(again, hashlib.sha256(again.read_bytes()).hexdigest(), 1)
        with pytest.raises(SpaceStatisticsError, match="appears twice"):
            compute_space_statistics([*shards, extra], population_array(md5s), 4)

    def test_a_shard_that_does_not_hash_as_recorded_is_refused(self, tmp_path: Path) -> None:
        rows = _anisotropic(20, 4)
        md5s, shards = _bank(tmp_path, rows, [10])
        tampered = [shards[0], BankShard(shards[1].path, "0" * 64, shards[1].rows)]
        with pytest.raises(SpaceStatisticsError, match="hashes to"):
            compute_space_statistics(tampered, population_array(md5s), 4)

    def test_a_shard_of_another_width_is_refused(self, tmp_path: Path) -> None:
        rows = _anisotropic(20, 4)
        md5s, shards = _bank(tmp_path, rows, [10])
        with pytest.raises(SpaceStatisticsError, match="vectors of 4, not 5"):
            compute_space_statistics(shards, population_array(md5s), 5)

    def test_a_population_md5_that_is_not_one_is_refused(self) -> None:
        with pytest.raises(SpaceStatisticsError, match="32 hex digits"):
            population_array(["0" * 31, "a" * 32])


# The diagnostics -----------------------------------------------------------------


def _brute_force(units: np.ndarray, k: int) -> tuple[float, float]:
    """The mean pair cosine and the k-occurrence skewness, pair by pair."""
    s = len(units)
    similarities = units @ units.T
    mean = float(similarities[np.triu_indices(s, 1)].mean())
    np.fill_diagonal(similarities, -np.inf)
    occurrences = np.zeros(s)
    for row in similarities:
        occurrences[np.argsort(row)[-k:]] += 1
    spread = occurrences - occurrences.mean()
    return mean, float((spread**3).mean() / (spread**2).mean() ** 1.5)


class TestTheDiagnostics:
    def test_the_pair_cosine_and_the_hubness_by_brute_force(self) -> None:
        rows = _anisotropic(150, 6)
        record, contributions = pair_diagnostics(rows, 10)
        units = rows / np.linalg.norm(rows, axis=1, keepdims=True)
        mean, skewness = _brute_force(units, 10)
        assert record["mean_pair_cosine"] == pytest.approx(mean, rel=1e-10)
        assert contributions.sum() == pytest.approx(mean, rel=1e-10)
        assert record["hubness_skewness"] == pytest.approx(skewness, rel=1e-6)
        assert record["largest_contribution"]["dimension"] == int(np.argmax(np.abs(contributions)))

    def test_the_top_direction_share(self) -> None:
        rows = np.array([[3.0, 0.1], [-3.0, -0.1], [3.0, -0.1], [-3.0, 0.1]])
        units = rows / np.linalg.norm(rows, axis=1, keepdims=True)
        centred = units - units.mean(axis=0)
        values = np.linalg.eigvalsh(centred.T @ centred / 4)
        assert pair_diagnostics(rows, 1)[0]["top_direction_share"] == pytest.approx(
            values[-1] / values.sum()
        )

    def test_a_sample_too_small_for_the_neighbours_has_no_hubness(self) -> None:
        assert pair_diagnostics(_anisotropic(8, 3), 10)[0]["hubness_skewness"] is None

    def test_centring_takes_the_common_direction_out(self, tmp_path: Path) -> None:
        rng = np.random.default_rng(5)
        rows = rng.normal(size=(300, 10)) * np.geomspace(1.0, 1e-2, 10) + 1.0
        md5s, shards = _bank(tmp_path, rows, [100])
        result = compute_space_statistics(shards, population_array(md5s), 10)
        diagnostics, contributions = space_diagnostics(result)
        variants = diagnostics["variants"]
        assert set(variants) == set(VARIANTS) and contributions.shape == (len(VARIANTS), 10)
        assert variants["raw"]["mean_pair_cosine"] > 0.3
        for name in VARIANTS[1:]:
            assert abs(variants[name]["mean_pair_cosine"]) < 0.05, name

    def test_the_sample_is_the_same_sequences_for_every_configuration(self) -> None:
        population = population_array([_md5(i) for i in range(500)])
        first = diagnostic_sample(population, 40, 0)
        assert np.array_equal(first, diagnostic_sample(population, 40, 0))
        assert not np.array_equal(first, diagnostic_sample(population, 40, 1))
        assert np.array_equal(first, np.sort(first)) and len(np.unique(first)) == 40
        assert len(diagnostic_sample(population[:30], 40, 0)) == 30

    def test_a_transform_the_space_cannot_take_is_recorded_as_refused(self) -> None:
        d = 3
        parameters = SpaceParameters(
            mean=np.zeros(d),
            variances=np.array([1.0, 1.0, 0.0]),
            eigenvalues=np.array([1.0, 1.0, 0.0]),
            eigenvectors=np.eye(d),
            shrinkage=0.0,
            removed_directions=1,
        )
        sample = np.array([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [1.0, 1.0, 0.0]])
        result = SpaceStatistics(3, parameters, 1e-9, np.empty(0), np.empty(0, "S32"), sample)
        diagnostics, contributions = space_diagnostics(result)
        assert "refused" in diagnostics["variants"]["standardize"]
        assert "refused" in diagnostics["variants"]["whiten"]
        assert np.isnan(contributions[VARIANTS.index("whiten")]).all()
        assert "mean_pair_cosine" in diagnostics["variants"]["center"]


# The artifact --------------------------------------------------------------------


class TestTheArtifact:
    def _statistics(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> SpaceStatistics:
        monkeypatch.setattr(statistics, "SAMPLE_SIZE", 50)
        rows = _anisotropic(120, 6)
        md5s, shards = _bank(tmp_path, rows, [60])
        return compute_space_statistics(shards, population_array(md5s), 6)

    def test_it_round_trips(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        result = self._statistics(tmp_path, monkeypatch)
        blob, document = statistics_artifact(result, {"cut_release": 211})
        parameters, read = read_statistics_artifact(blob)
        assert read == json.loads(json.dumps(document)) and read["cut_release"] == 211
        for name in ("mean", "variances", "eigenvalues", "eigenvectors"):
            assert np.array_equal(getattr(parameters, name), getattr(result.parameters, name))
        assert parameters.shrinkage == result.parameters.shrinkage
        assert parameters.removed_directions == read["spectrum"]["removed_directions"]
        assert read["n"] == 120 and read["d"] == 6
        assert read["diagnostics"]["sample_size"] == 50 and read["diagnostics"]["pairs"] == 1225

    def test_its_bytes_depend_on_its_values_alone(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        result = self._statistics(tmp_path, monkeypatch)
        first, _ = statistics_artifact(result, {})
        assert first == statistics_artifact(result, {})[0]
        assert first != statistics_artifact(result, {"cut_release": 194})[0]

    def test_another_format_is_refused(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        result = self._statistics(tmp_path, monkeypatch)
        monkeypatch.setattr(statistics, "FORMAT_VERSION", 2)
        blob, _ = statistics_artifact(result, {})
        monkeypatch.setattr(statistics, "FORMAT_VERSION", 1)
        with pytest.raises(SpaceStatisticsError, match="artifact format 2"):
            read_statistics_artifact(blob)
