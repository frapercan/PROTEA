"""Term frequency, rung 0: LAFA's literal Naive and the propagated recipe variant.

The literal is checked against a reference written here from democafa's own
arithmetic (``naive.py`` at ``1102e34``, read as an oracle): the matrix of direct
rows, the column counts, the per-aspect denominator (the roots' counts, or the
proteins with a row in the aspect), float32 scores, and the cap that zeroes every
score at or below the ``int(n_terms / 3)``-th largest of an aspect with
``np.partition``. On random rows the two agree within 1e-7 and on the same term set.
The recipe variant is checked by hand: ``N_t / N_root``, roots out, the floor read
on the four-decimal rendering.
"""

from __future__ import annotations

from collections import Counter

import numpy as np
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from protea.core.lafa_frame import EndOntology
from protea.core.term_frequency import (
    GRID_FLOOR,
    LAFA_TERMS_PER_ASPECT,
    ROOT_OF,
    DirectRow,
    LiteralRefused,
    lafa_direct,
    propagated,
    rendered,
)

ROOTS = {aspect: root for aspect, root in ROOT_OF.items()}


def _ontology(live: dict[str, str], **renamings: dict[str, str]) -> EndOntology:
    return EndOntology(
        live={**{root: aspect for aspect, root in ROOTS.items()}, **live},
        parents={},
        alt_ids=renamings.get("alt_ids", {}),
        replaced_by=renamings.get("replaced_by", {}),
    )


def _democafa_naive(rows: list[DirectRow], ontology: EndOntology, n_terms: int) -> dict[str, float]:
    """democafa's arithmetic on rows already named: what ``naive.py`` writes, as a mapping."""
    kept = sorted({(r.protein, r.go_id, r.aspect) for r in rows})
    terms = sorted({t for _, t, _ in kept})
    index = {t: i for i, t in enumerate(terms)}
    holders: dict[str, set[str]] = {}
    for protein, term, _ in kept:
        holders.setdefault(term, set()).add(protein)
    frequencies = np.array([len(holders[t]) for t in terms])
    max_freqs = {a: frequencies[index[r]] for a, r in ROOTS.items() if r in index}
    if not max_freqs:
        by_aspect: dict[str, set[str]] = {}
        for protein, _, aspect in kept:
            by_aspect.setdefault(aspect, set()).add(protein)
        max_freqs = {a: len(p) for a, p in by_aspect.items()}
    assert len(max_freqs) == 3
    normalized = np.zeros_like(frequencies, dtype=np.float32)
    for aspect, max_freq in max_freqs.items():
        idx = [i for t, i in index.items() if ontology.live[t] == aspect]
        normalized[idx] = frequencies[idx] / max_freq
    top_k = int(n_terms / 3)
    for aspect in ROOTS:
        idx = [i for t, i in index.items() if ontology.live[t] == aspect]
        scores = normalized[idx]
        if len(scores) > top_k:
            threshold = np.partition(scores, -top_k)[-top_k]
            normalized[(normalized <= threshold) & np.isin(np.arange(len(normalized)), idx)] = 0.0
    return {t: float(normalized[i]) for t, i in index.items() if normalized[i] > 0}


def test_the_literal_counts_direct_rows_over_the_proteins_of_the_aspect() -> None:
    rows = [
        DirectRow("A", "GO:1", "P"),
        DirectRow("A", "GO:1", "P"),  # one row per protein, term and aspect
        DirectRow("B", "GO:1", "P"),
        DirectRow("B", "GO:2", "P"),
        DirectRow("C", "GO:3", "F"),
        DirectRow("D", "GO:3", "F"),
        DirectRow("E", "GO:4", "C"),  # not alive: dropped, and E counts nowhere
        DirectRow("F", "GO:6", "C"),
    ]
    ontology = _ontology({"GO:1": "P", "GO:2": "P", "GO:3": "F", "GO:6": "C"})
    got = lafa_direct(rows, ontology)
    assert got.record["branch"] == "proteins_per_aspect"
    assert got.denominators == {"P": 2, "F": 2, "C": 1}
    assert got.scores == {"GO:1": 1.0, "GO:2": 0.5, "GO:3": 1.0, "GO:6": 1.0}
    assert got.counted == frozenset({"A", "B", "C", "D", "F"})
    assert got.record["naming"] == {"row_dropped_not_live": 1}


def test_the_denominator_counts_by_the_rows_own_aspect() -> None:
    """democafa groups the rows' aspect column; the term's namespace picks its column."""
    ontology = _ontology({"GO:1": "P", "GO:3": "F", "GO:5": "F", "GO:6": "C"})
    rows = [
        DirectRow("A", "GO:1", "P"),
        DirectRow("B", "GO:3", "P"),  # a row whose letter is not its term's namespace
        DirectRow("C", "GO:3", "F"),
        DirectRow("X", "GO:5", "F"),
        DirectRow("D", "GO:6", "C"),
    ]
    got = lafa_direct(rows, ontology)
    assert got.denominators == {"P": 2, "F": 2, "C": 1}
    assert got.scores["GO:1"] == 0.5 and got.scores["GO:3"] == 1.0
    # Where that would put a score above 1, as democafa would write it, the literal refuses.
    rows.append(DirectRow("E", "GO:3", "P"))
    with pytest.raises(LiteralRefused, match="GO:3"):
        lafa_direct(rows, ontology)


def test_ids_are_named_as_the_t0_ontology_names_them() -> None:
    ontology = _ontology(
        {"GO:1": "P", "GO:3": "F", "GO:6": "C"},
        alt_ids={"GO:11": "GO:1"},
        replaced_by={"GO:33": "GO:3"},
    )
    rows = [
        DirectRow("A", "GO:11", "P"),
        DirectRow("A", "GO:1", "P"),
        DirectRow("B", "GO:33", "F"),
        DirectRow("C", "GO:6", "C"),
    ]
    got = lafa_direct(rows, ontology)
    assert got.proteins == {"GO:1": 1, "GO:3": 1, "GO:6": 1}
    assert got.record["naming"] == {"row_renamed_alt_id": 1, "row_renamed_replaced_by": 1}


def test_the_roots_counts_divide_when_all_three_carry_rows() -> None:
    ontology = _ontology({"GO:1": "P", "GO:3": "F", "GO:6": "C"})
    rows = [
        DirectRow("A", ROOTS["P"], "P"),
        DirectRow("B", ROOTS["P"], "P"),
        DirectRow("A", ROOTS["F"], "F"),
        DirectRow("A", ROOTS["C"], "C"),
        DirectRow("A", "GO:1", "P"),
        DirectRow("C", "GO:1", "P"),
        DirectRow("D", "GO:3", "F"),
        DirectRow("E", "GO:6", "C"),
    ]
    got = lafa_direct(rows, ontology)
    assert got.record["branch"] == "root_counts"
    assert got.denominators == {"P": 2, "F": 1, "C": 1}
    assert got.record["root_direct_proteins"] == {"P": 2, "F": 1, "C": 1}
    assert got.scores["GO:1"] == 1.0 and got.scores[ROOTS["P"]] == 1.0


def test_the_literal_refuses_where_democafa_would_stop() -> None:
    ontology = _ontology({"GO:1": "P", "GO:3": "F", "GO:6": "C"})
    with pytest.raises(LiteralRefused, match=r"root of \['P'\] and not of \['F', 'C'\]"):
        lafa_direct([DirectRow("A", ROOTS["P"], "P"), DirectRow("B", "GO:3", "F")], ontology)
    with pytest.raises(LiteralRefused, match=r"aspect \['C'\]"):
        lafa_direct([DirectRow("A", "GO:1", "P"), DirectRow("B", "GO:3", "F")], ontology)


def _aspect_with_counts(counts: list[int]) -> tuple[list[DirectRow], EndOntology]:
    """One P term per count, held by that many proteins, and one F and one C term."""
    rows, live = [], {"GO:F": "F", "GO:C": "C"}
    for i, count in enumerate(counts):
        term = f"GO:P{i:04d}"
        live[term] = "P"
        rows.extend(DirectRow(f"X{j}", term, "P") for j in range(count))
    rows += [DirectRow("Y", "GO:F", "F"), DirectRow("Z", "GO:C", "C")]
    return rows, _ontology(live)


def test_the_cap_keeps_the_terms_above_the_500th_largest_score() -> None:
    rows, ontology = _aspect_with_counts(list(range(1, 502)))
    got = lafa_direct(rows, ontology)
    kept = [t for t in got.scores if t.startswith("GO:P")]
    assert len(kept) == LAFA_TERMS_PER_ASPECT - 1
    assert got.record["cap"]["P"] == {
        "terms": 501,
        "capped": True,
        "threshold_proteins": 2,
        "kept": 499,
    }
    assert got.record["cap"]["F"] == {"terms": 1, "capped": False, "kept": 1}


def test_ties_at_the_500th_largest_score_all_go() -> None:
    counts = [1000 - i for i in range(495)] + [7] * 10
    rows, ontology = _aspect_with_counts(counts)
    got = lafa_direct(rows, ontology)
    kept = [t for t in got.scores if t.startswith("GO:P")]
    assert len(kept) == 495  # the five tied at the 500th place and the five past it all go
    assert got.record["cap"]["P"]["threshold_proteins"] == 7


@settings(max_examples=60, deadline=None)
@given(
    holdings=st.lists(
        st.tuples(
            st.sampled_from([f"A{i}" for i in range(12)]),
            st.sampled_from(["P", "F", "C"]),
            st.integers(min_value=0, max_value=11),
        ),
        min_size=3,
        max_size=150,
    ),
    cap=st.integers(min_value=1, max_value=6),
)
def test_the_literal_is_democafas_arithmetic(
    holdings: list[tuple[str, str, int]], cap: int
) -> None:
    rows = [DirectRow(p, f"GO:{a}{t}", a) for p, a, t in holdings]
    rows += [DirectRow("Q", f"GO:{a}0", a) for a in ("P", "F", "C")]
    ontology = _ontology({f"GO:{a}{t}": a for a in ("P", "F", "C") for t in range(12)})
    ours = lafa_direct(rows, ontology, cap=cap)
    theirs = _democafa_naive(rows, ontology, n_terms=3 * cap)
    assert set(ours.scores) == set(theirs)
    assert all(abs(ours.scores[t] - theirs[t]) <= 1e-7 for t in theirs)


def test_the_propagated_variant_divides_by_the_root_and_reads_the_floor() -> None:
    states = {
        "A": {"P": {ROOTS["P"], "GO:1", "GO:2"}, "F": {ROOTS["F"], "GO:3"}},
        "B": {"P": {ROOTS["P"], "GO:1"}},
        "C": {"P": {ROOTS["P"], "GO:1"}},
        "D": {"F": set()},
    }
    got = propagated(states)
    assert got.denominators == {"P": 3, "F": 1}
    assert got.scores == {"GO:1": 1.0, "GO:2": pytest.approx(1 / 3), "GO:3": 1.0}
    assert ROOTS["P"] not in got.scores and got.record["root_proteins"] == {"P": 3, "F": 1}
    assert got.counted == frozenset({"A", "B", "C"})


def test_the_floor_is_read_on_the_four_decimal_rendering() -> None:
    many = {f"N{i}": {"P": {ROOTS["P"], "GO:9"}} for i in range(10_000)}
    many["H1"] = {"P": {ROOTS["P"], "GO:1"}}  # 1 / 10,002 renders 0.0001: below
    for i in range(99):
        many[f"L{i}"] = {"P": {ROOTS["P"], "GO:2"}}  # 99 / 10,100 renders 0.0098: below
    for i in range(101):
        many[f"M{i}"] = {"P": {ROOTS["P"], "GO:3"}}  # 101 / 10,201 renders 0.0099: below
    got = propagated(many)
    assert "GO:9" in got.scores and not {"GO:1", "GO:2", "GO:3"} & set(got.scores)
    assert got.record["terms_below_the_floor"] == 3
    assert rendered(0.00996) >= GRID_FLOOR > rendered(0.00994)


def test_the_counts_of_the_literal_are_proteins_not_rows() -> None:
    ontology = _ontology({"GO:1": "P", "GO:3": "F", "GO:6": "C"})
    rows = [DirectRow("A", "GO:1", "P")] * 5 + [
        DirectRow("B", "GO:3", "F"),
        DirectRow("C", "GO:6", "C"),
    ]
    assert lafa_direct(rows, ontology).proteins == Counter({"GO:1": 1, "GO:3": 1, "GO:6": 1})
