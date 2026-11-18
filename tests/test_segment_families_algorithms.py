"""The label-free steps of the segment families (ADR-D65, decision 2) on small known answers."""

from __future__ import annotations

import numpy as np
import pytest

from protea.core.segment_families import algorithms as fam
from protea.core.segment_families.algorithms import Hits


def _hits(rows: list[tuple[int, int, float, float, float, float]]) -> Hits:
    a = np.asarray(rows, dtype=np.float64).reshape(-1, 6)
    return Hits(
        a[:, 0].astype(np.int64), a[:, 1].astype(np.int64), a[:, 2], a[:, 3], a[:, 4], a[:, 5]
    )


def test_a_core_drops_non_match_runs_longer_than_its_match_count() -> None:
    # 4 match residues: a run of 3 inserts stays, a run of 5 goes
    columns = np.array([0, 1, -1, -1, -1, 2, -1, -1, -1, -1, -1, 3])
    assert fam.core_mask(columns).tolist() == [True] * 6 + [False] * 5 + [True]
    assert fam.core_mask(np.array([-1, -1])).tolist() == [False, False]  # no match: nothing kept


def test_components_and_the_set_cover() -> None:
    comps = sorted(sorted(c) for c in fam.components([(0, 1), (1, 2), (5, 6)]))
    assert comps == [[0, 1, 2], [5, 6]]
    # 0 links to 1, 2, 3; 3 links to 4: 0 represents 0-3 and 4 is alone, nothing chains
    rep = fam.set_cover(range(5), [(0, 1), (0, 2), (0, 3), (3, 4)])
    assert rep == {0: 0, 1: 0, 2: 0, 3: 0, 4: 4}
    # a tie on degree goes to the smaller index
    assert fam.set_cover(range(2), [(0, 1)]) == {0: 0, 1: 0}


def test_decoy_components_larger_than_ten_mask_their_cores() -> None:
    n = 20
    star = [(r, t, 1e-9, 50.0, 0.9, 0.9) for r in range(6) for t in range(10, 16)]  # 12 nodes
    small = [(16, 17, 1e-9, 50.0, 0.9, 0.9)]
    weak = [(18, 19, 1e-9, 50.0, 0.9, 0.2)]  # one side covered: not a decoy link
    masked = fam.decoy_mask(_hits(star + small + weak), n)
    assert set(np.flatnonzero(masked)) == set(range(6)) | set(range(10, 16))


def test_links_split_full_and_partial_and_skip_masked_cores() -> None:
    masked = np.zeros(6, dtype=bool)
    masked[5] = True
    hits = _hits(
        [
            (0, 1, 1e-5, 40.0, 0.8, 0.9),  # link
            (0, 2, 1e-5, 40.0, 0.8, 0.3),  # partial
            (1, 1, 1e-9, 90.0, 1.0, 1.0),  # self
            (2, 5, 1e-9, 90.0, 1.0, 1.0),  # masked target
            (3, 4, 1.0, 10.0, 1.0, 1.0),  # outside the scope
        ]
    )
    full, partial = fam.links(hits, 1e-3, masked)
    assert list(zip(full.q, full.t, strict=True)) == [(0, 1)]
    assert list(zip(partial.q, partial.t, strict=True)) == [(0, 2)]


def test_the_profile_constant_lets_at_most_three_reversed_hits_through() -> None:
    rev = _hits([(i, i + 1, 10.0 ** -(i + 3), 30.0, 0.9, 0.9) for i in range(6)])
    e = fam.profile_evalue(rev, nominal=1.0)
    assert int((np.sort(rev.e) < e).sum()) == 3
    assert fam.profile_evalue(_hits([]), nominal=0.5) == 0.5


def test_families_clans_and_the_targets_best_link() -> None:
    family = fam.join_families([0, 3, 6], _hits([(3, 0, 1e-9, 50.0, 0.9, 0.9)]), 1e-3)
    assert family == {0: 0, 3: 0, 6: 6}
    family_of_core = np.array([0, 0, 0, 0, 0, 0, 6, 6, -1])
    assert fam.clans(family_of_core, _hits([(1, 7, 1e-9, 50.0, 0.9, 0.2)])) == {0: 0, 6: 0}
    masked = np.zeros(9, dtype=bool)
    masked[8] = True
    queries = _hits(
        [
            (0, 1, 1e-9, 40.0, 0.9, 0.9),
            (0, 6, 1e-9, 60.0, 0.9, 0.9),  # more bits: target 0 takes family 6
            (1, 8, 1e-12, 99.0, 0.9, 0.9),  # masked core: no family for target 1
            (2, 2, 1e-12, 99.0, 0.9, 0.1),  # one side covered: none
        ]
    )
    out = fam.target_families(queries, 1e-3, masked, family_of_core, 3)
    assert out.tolist() == [6, -1, -1]


def test_scopes() -> None:
    assert fam.scope_evalue("strict", 1000) == pytest.approx(8.599e-8)
    assert fam.scope_evalue("permissive", 32768) == pytest.approx(1.0)
    with pytest.raises(ValueError):
        fam.scope_evalue("loose", 10)
