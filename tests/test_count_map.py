"""The count map's fit and reading against brute forces (ADR-D65).

The laboratory checked its vectorised fit against loops over donors, units and
every term; that brute force is kept here, with one for the reading: for each
target, each of its units read without the donors that share its sequence,
the best unit per term, the prior's candidates, the floor at pi, the denied
terms out and the best terms kept.
"""

from __future__ import annotations

import numpy as np
import pytest
import scipy.sparse as sp

from protea.core.count_map import reading
from protea.core.count_map.fit import StrengthFit, unit_counts
from protea.core.count_map.reading import CountMap, Targets, read


def _problem(seed: int, donors: int = 60, units: int = 12, terms: int = 25):
    """Random binary carriers and states, with groups of twins sharing their units."""
    rng = np.random.default_rng(seed)
    group = rng.integers(0, donors // 2, size=donors)  # about two donors per sequence
    seq_units = rng.random((donors // 2, units)) < 0.25
    carriers = sp.csr_matrix(seq_units[group].astype(np.float64))
    states = sp.csr_matrix((rng.random((donors, terms)) < 0.2).astype(np.float64))
    keep = np.flatnonzero(np.diff(states.indptr) > 0)
    carriers, states, group = carriers[keep], states[keep], group[keep]
    base_rate = np.asarray(states.sum(0)).ravel() / states.shape[0]
    return carriers, states, group, base_rate, seq_units


def _brute_bits(carriers, states, group, base_rate, m):
    """The laboratory's ``brute_loglik``: loops over units, donors and every term."""
    b, y = carriers.tocsc(), states.toarray()
    total = 0.0
    for u in range(b.shape[1]):
        members = b.indices[b.indptr[u] : b.indptr[u + 1]]
        for p in members:
            others = members[group[members] != group[p]]
            q = (y[others].sum(0) + m * base_rate) / (len(others) + m)
            total += float((y[p] * np.log2(q) + (1 - y[p]) * np.log2(1 - q)).sum())
    return total


@pytest.mark.parametrize("seed", [1, 2, 3])
@pytest.mark.parametrize("m", [0.5, 7.0, 120.0])
def test_the_fit_equals_the_brute_force(seed: int, m: float) -> None:
    carriers, states, group, base_rate, _ = _problem(seed)
    fit = StrengthFit(carriers, states, group, base_rate)
    want = _brute_bits(carriers, states, group, base_rate, m)
    assert fit.bits(m) == pytest.approx(want, rel=1e-9, abs=1e-6)


def test_the_counts_are_the_carriers_states() -> None:
    carriers, states, _, _, _ = _problem(4)
    counts, carried = unit_counts(carriers, states)
    b, y = carriers.toarray(), states.toarray()
    assert np.array_equal(counts.toarray(), b.T @ y)
    assert np.array_equal(carried, b.sum(0))


def test_the_best_strength_is_at_least_the_grid_best() -> None:
    carriers, states, group, base_rate, _ = _problem(5, donors=120)
    fit = StrengthFit(carriers, states, group, base_rate)
    m, bits, grid = fit.best()
    best_on_grid = max(grid["grid_bits"])
    # the golden search ends inside its last bracket: at a bound it stops a hair short of it
    assert bits >= best_on_grid - 1e-9 * abs(best_on_grid)
    assert bits == pytest.approx(fit.bits(m))
    assert 1e-3 <= m <= 1e4


def _targets(seed: int, seq_units, carriers, states, group, terms: int):
    """Targets: some sequences of the donors (with their groups out), some new ones."""
    rng = np.random.default_rng(seed)
    n_seq, n_units = seq_units.shape
    new_units = rng.random((6, n_units)) < 0.25
    target_seq = np.r_[np.arange(n_seq), -1 - np.arange(6)]
    units = np.vstack([seq_units, new_units])
    y = states.toarray()
    excluded = np.zeros((len(target_seq), terms))
    excluded_n = np.zeros(len(target_seq))
    for i, s in enumerate(target_seq):
        if s >= 0:
            excluded[i] = y[group == s].sum(0)
            excluded_n[i] = (group == s).sum()
    denied = sp.csr_matrix(rng.random((len(target_seq), terms)) < 0.05)
    return Targets(
        sp.csr_matrix(units.astype(np.float64)), sp.csr_matrix(excluded), excluded_n, denied
    )


def _brute_reading(carriers, states, group, base_rate, m, targets, target_seq, top):
    b, y = carriers.toarray().astype(bool), states.toarray()
    units, denied = targets.units.toarray().astype(bool), targets.denied.toarray()
    prior = np.argsort(-base_rate, kind="stable")[:top]
    out = {}
    for q in range(units.shape[0]):
        best: dict[int, tuple[float, int, float, float]] = {}
        for u in np.flatnonzero(units[q]):
            others = b[:, u] & (group != target_seq[q])
            k = y[others].sum(0)
            n = others.sum()
            for t in np.flatnonzero(k > 0):
                v = (k[t] + m * base_rate[t]) / (n + m)
                if t not in best or v > best[t][0]:
                    best[t] = (v, int(u), float(k[t]), float(n))
        rows = {}
        for t, (v, u, k_, n_) in best.items():
            rows[t] = (v, u, k_, n_) if v >= base_rate[t] else (base_rate[t], -1, np.nan, np.nan)
        for t in prior:
            if int(t) not in rows:
                rows[int(t)] = (base_rate[t], -1, np.nan, np.nan)
        kept = sorted(
            ((-v, t, u, k_, n_) for t, (v, u, k_, n_) in rows.items() if not denied[q, t])
        )
        out[q] = [(t, -nv, u) for nv, t, u, _, _ in kept[:top]]
    return out


@pytest.mark.parametrize("seed", [11, 12])
@pytest.mark.parametrize("top", [5, 100])
def test_the_reading_equals_the_brute_force(seed: int, top: int, monkeypatch) -> None:
    terms = 25
    carriers, states, group, base_rate, seq_units = _problem(seed, terms=terms)
    counts, carried = unit_counts(carriers, states)
    cmap = CountMap(counts, carried, base_rate, 3.5)
    targets = _targets(seed, seq_units, carriers, states, group, terms)
    target_seq = np.r_[np.arange(seq_units.shape[0]), -1 - np.arange(6)]
    monkeypatch.setattr(reading, "CHUNK_SUPPORT", 40)  # many chunks
    rows = read(cmap, targets, top=top)
    want = _brute_reading(carriers, states, group, base_rate, 3.5, targets, target_seq, top)
    for q, expected in want.items():
        got = np.flatnonzero(rows.target == q)
        got = got[np.lexsort((rows.term[got], -rows.score[got]))]
        assert [int(t) for t in rows.term[got]] == [t for t, _, _ in expected], q
        assert np.allclose(rows.score[got], [v for _, v, _ in expected])
        assert [int(u) for u in rows.unit[got]] == [u for _, _, u in expected], q


def test_an_exclusion_larger_than_its_unit_is_refused() -> None:
    counts = sp.csr_matrix(np.array([[1.0, 0.0]]))
    cmap = CountMap(counts, np.array([1.0]), np.array([0.5, 0.5]), 1.0)
    targets = Targets(
        sp.csr_matrix(np.array([[1.0]])),
        sp.csr_matrix(np.array([[0.0, 1.0]])),
        np.array([1.0]),
        sp.csr_matrix((1, 2)),
    )
    with pytest.raises(ValueError, match="took out more"):
        read(cmap, targets)


# ---------------------------------------------------------------- the matrices a cut becomes


def test_an_aspect_problem_keeps_donors_with_a_column_and_excludes_by_sequence() -> None:
    from protea.core.count_map.inputs import Columns, aspect_problem, columns_of

    cols = columns_of({"GO:0008150": "P", "GO:1": "P", "GO:2": "P", "GO:3": "F"})
    assert cols["P"] == Columns("P", ("GO:1", "GO:2")) and cols["F"].terms == ("GO:3",)
    positives = {
        "A": {"P": {"GO:1", "GO:0008150"}},
        "B": {"P": {"GO:2"}},
        "C": {"P": {"GO:0008150"}, "F": {"GO:3"}},  # only the root in P: not a donor of P
        "D": {"P": {"GO:1", "GO:2"}},
    }
    md5_of = {"A": "m1", "B": "m1", "C": "m2", "D": "m3", "Q": "m1", "R": "m9"}
    units = ({"m1": np.array([0, 2]), "m2": np.array([1]), "m3": np.array([2])}, 4)
    problem = aspect_problem(
        cols["P"],
        positives,
        {"Q": {"GO:2", "GO:3"}},
        md5_of,
        units,
        (["A", "B", "C", "D"], ["Q", "R", "A"]),
    )
    assert problem.donors == ["A", "B", "D"]
    assert problem.group[0] == problem.group[1] != problem.group[2]  # A and B share a sequence
    assert problem.states.toarray().tolist() == [[1, 0], [0, 1], [1, 1]]
    assert problem.carriers.toarray().tolist() == [[1, 0, 1, 0], [1, 0, 1, 0], [0, 0, 1, 0]]
    assert np.allclose(problem.base_rate, [2 / 3, 2 / 3])
    view = problem.target_view
    # Q shares m1 with A and B: both are taken out of its units; R shares nothing; A takes out A and B
    assert view.excluded.toarray().tolist() == [[1, 1], [0, 0], [1, 1]]
    assert view.excluded_n.tolist() == [2, 0, 2]
    assert view.units.toarray().tolist() == [[1, 0, 1, 0], [0, 0, 0, 0], [1, 0, 1, 0]]
    assert view.denied.toarray().tolist() == [[0, 1], [0, 0], [0, 0]]  # GO:3 is not a P column
