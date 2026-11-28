"""Homology transfer's rules (rungs 2a and 2b), pure: the variants, the voters, the scores.

S1 and S2 are checked against a reference written here from their formulas (``sum
bits x I(t) / sum bits`` and ``sum bits x ID x I(t) / sum bits x ID``, every voter
carrying an admitted term in the denominator, or every voter carrying one in the
term's aspect) on random neighbourhoods, and the top hit against the largest
identity over the voters carrying a term. Donor states are closed, so a term never
scores below a descendant it implies, under every rule. Twins vote at identity 1
with the target's self score, and a twin with none is left out of a vote and
counted, never of the top hit.
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from protea.core.homology.transfer import (
    Hit,
    TransferRefused,
    TransferRule,
    global_identity,
    read_hits,
    score_target,
    twin_hit,
    voters_of,
)

#: Six terms in two aspects; each term's ancestors within its aspect (itself included).
ASPECT_OF = ["P", "P", "P", "F", "F", "F"]
ANCESTORS = {0: {0}, 1: {0, 1}, 2: {0, 1, 2}, 3: {3}, 4: {3, 4}, 5: {3, 5}}


def _closed(terms: set[int]) -> frozenset[int]:
    return frozenset(a for t in terms for a in ANCESTORS[t])


def _hit(md5: str, fident: float, bits: float, alnlen: int = 100) -> Hit:
    return Hit(md5, fident, alnlen, bits, 1e-10, False)


def test_the_variants_must_mean_something_under_the_rule() -> None:
    assert TransferRule("top_hit_identity", identity="local").is_vote is False
    with pytest.raises(TransferRefused, match="reads identity"):
        TransferRule("top_hit_identity")
    with pytest.raises(TransferRefused, match="reads no identity"):
        TransferRule("bitscore_vote", identity="global", denominator="any_aspect")
    with pytest.raises(TransferRefused, match="name the denominator"):
        TransferRule("bitscore_identity_vote", identity="global")
    with pytest.raises(TransferRefused, match="divides nothing"):
        TransferRule("top_hit_identity", identity="local", denominator="any_aspect")
    with pytest.raises(TransferRefused, match="multiplicity accession"):
        TransferRule("top_hit_identity", identity="local", multiplicity="sequence")
    with pytest.raises(TransferRefused, match="at least 1"):
        TransferRule("bitscore_vote", denominator="any_aspect", hit_cap=0)


def test_global_identity_is_identical_residues_over_the_longer_sequence() -> None:
    assert global_identity(0.5, 100, 200, 120) == 50 / 200
    assert global_identity(0.333, 3, 3, 9) == 1 / 9
    assert twin_hit("q", 312.0, 150).global_identity == 1.0


def test_hits_are_read_for_the_queries_asked_best_first_and_capped(tmp_path: Path) -> None:
    rows = [
        ("q1", "a", 0.9, 100, 1e-30, 200.0),
        ("q1", "b", 0.5, 100, 1e-10, 90.0),
        ("q1", "c", 0.7, 100, 1e-20, 120.0),
        ("q2", "a", 0.4, 80, 1e-5, 60.0),
        ("q3", "d", 0.4, 80, 1e-5, 60.0),
    ]
    names = ["query_md5", "target_md5", "fident", "alnlen", "evalue", "bits"]
    table = pa.table({n: [r[i] for r in rows] for i, n in enumerate(names)})
    path = tmp_path / "hits.parquet"
    pq.write_table(
        table.cast(
            pa.schema(
                [
                    ("query_md5", pa.large_string()),
                    ("target_md5", pa.large_string()),
                    ("fident", pa.float32()),
                    ("alnlen", pa.int32()),
                    ("evalue", pa.float64()),
                    ("bits", pa.float32()),
                ]
            )
        ),
        path,
        row_group_size=2,
    )
    every = read_hits(path, ["q1", "q2"])
    assert [h.md5 for h in every["q1"]] == ["a", "c", "b"] and set(every) == {"q1", "q2"}
    capped = read_hits(path, ["q1"], hit_cap=2)
    assert [h.md5 for h in capped["q1"]] == ["a", "c"]


def test_the_top_hit_scores_the_largest_identity_carrying_the_term() -> None:
    rule = TransferRule("top_hit_identity", identity="local")
    hits = [_hit("m1", 0.9, 300.0), _hit("m2", 0.4, 100.0), twin_hit("q", None, 100)]
    donors = {"m1": ["D1"], "m2": ["D2", "D3"], "q": ["T2"]}
    states = {"D1": _closed({1}), "D2": _closed({2, 4}), "D3": frozenset(), "T2": _closed({5})}
    voters = voters_of(hits, donors, states, rule)
    # The twin needs no self score for an identity; a donor with no state does not vote.
    assert voters.twins_without_self_score == 0 and len(voters.voters) == 3
    got = {t.term: t.score for t in score_target(voters.voters, ASPECT_OF, rule)}
    assert got == {0: 0.9, 1: 0.9, 2: 0.4, 3: 1.0, 4: 0.4, 5: 1.0}


def test_a_twin_without_a_self_score_is_left_out_of_a_vote_and_counted() -> None:
    rule = TransferRule("bitscore_vote", denominator="any_aspect")
    hits = [_hit("m1", 0.9, 300.0), twin_hit("q", None, 100)]
    states = {"D1": _closed({1}), "T1": _closed({2}), "T2": _closed({4})}
    voters = voters_of(hits, {"m1": ["D1"], "q": ["T1", "T2"]}, states, rule)
    assert voters.twins_without_self_score == 2 and [v.label for v in voters.voters] == ["D1"]
    with_score = voters_of(
        [_hit("m1", 0.9, 300.0), twin_hit("q", 500.0, 100)],
        {"m1": ["D1"], "q": ["T1"]},
        states,
        TransferRule("bitscore_identity_vote", identity="local", denominator="any_aspect"),
    )
    twin = next(v for v in with_score.voters if v.hit.twin)
    assert twin.identity == 1.0 and twin.weight == 500.0
    # A hit read under global identity must carry one: the operation fills it from lengths.
    with pytest.raises(TransferRefused, match="no global identity"):
        voters_of(
            [_hit("m1", 0.9, 300.0)],
            {"m1": ["D1"]},
            states,
            TransferRule("bitscore_identity_vote", identity="global", denominator="any_aspect"),
        )


def test_multiplicity_by_sequence_makes_one_voter_of_a_sequences_accessions() -> None:
    hits = [_hit("m1", 0.5, 100.0), _hit("m2", 0.5, 100.0)]
    donors = {"m1": ["D1", "D2"], "m2": ["D3"]}
    states = {"D1": _closed({1}), "D2": _closed({2}), "D3": _closed({4})}
    by_accession = TransferRule("bitscore_vote", denominator="any_aspect")
    by_sequence = TransferRule("bitscore_vote", denominator="any_aspect", multiplicity="sequence")
    accession = {
        t.term: t.score
        for t in score_target(
            voters_of(hits, donors, states, by_accession).voters, ASPECT_OF, by_accession
        )
    }
    sequence = {
        t.term: t.score
        for t in score_target(
            voters_of(hits, donors, states, by_sequence).voters, ASPECT_OF, by_sequence
        )
    }
    assert accession[1] == pytest.approx(2 / 3) and accession[2] == pytest.approx(1 / 3)
    assert sequence[1] == pytest.approx(1 / 2) and sequence[2] == pytest.approx(1 / 2)


def _reference(
    voters: list[tuple[float, float, frozenset[int]]], rule: TransferRule
) -> dict[int, float]:
    """The literature's formulas over (bits, identity, terms) voters, written out."""
    out: dict[int, float] = {}
    terms = {t for _, _, found in voters for t in found}
    for term in terms:
        carrying = [(b, i) for b, i, found in voters if term in found]
        if rule.rule == "top_hit_identity":
            out[term] = max(i for _, i in carrying)
            continue
        weight = (lambda b, i: b * i) if rule.rule == "bitscore_identity_vote" else (lambda b, i: b)
        pool = [
            (b, i)
            for b, i, found in voters
            if rule.denominator == "any_aspect"
            or any(ASPECT_OF[t] == ASPECT_OF[term] for t in found)
        ]
        out[term] = sum(weight(b, i) for b, i in carrying) / sum(weight(b, i) for b, i in pool)
    return out


_RULES = [
    TransferRule("top_hit_identity", identity="local"),
    TransferRule("bitscore_vote", denominator="any_aspect"),
    TransferRule("bitscore_vote", denominator="same_aspect"),
    TransferRule("bitscore_identity_vote", identity="local", denominator="any_aspect"),
    TransferRule("bitscore_identity_vote", identity="local", denominator="same_aspect"),
]


@settings(max_examples=80, deadline=None)
@given(
    neighbourhood=st.lists(
        st.tuples(
            st.floats(min_value=1.0, max_value=900.0),
            st.floats(min_value=0.05, max_value=1.0),
            st.sets(st.integers(min_value=0, max_value=5), min_size=1, max_size=3),
        ),
        min_size=1,
        max_size=12,
    ),
    which=st.integers(min_value=0, max_value=len(_RULES) - 1),
)
def test_the_rules_are_the_literatures_formulas_and_consistent_with_the_ontology(
    neighbourhood: list[tuple[float, float, set[int]]], which: int
) -> None:
    rule = _RULES[which]
    hits = [_hit(f"m{i}", ident, bits) for i, (bits, ident, _) in enumerate(neighbourhood)]
    states = {f"D{i}": _closed(terms) for i, (_, _, terms) in enumerate(neighbourhood)}
    donors = {f"m{i}": [f"D{i}"] for i in range(len(neighbourhood))}
    voters = voters_of(hits, donors, states, rule)
    got = {t.term: t.score for t in score_target(voters.voters, ASPECT_OF, rule)}
    expected = _reference(
        [(b, i, states[f"D{n}"]) for n, (b, i, _) in enumerate(neighbourhood)], rule
    )
    assert set(got) == set(expected)
    assert all(got[t] == pytest.approx(expected[t], rel=1e-12) for t in expected)
    assert all(0.0 < s <= 1.0 + 1e-12 for s in got.values())
    # Closed donor states make the scores hierarchy-consistent: an ancestor never below.
    for term, score in got.items():
        for ancestor in ANCESTORS[term]:
            assert got[ancestor] >= score - 1e-12


def test_the_best_voter_and_the_vote_are_recorded() -> None:
    rule = TransferRule("bitscore_identity_vote", identity="local", denominator="same_aspect")
    hits = [_hit("m1", 0.8, 100.0), _hit("m2", 0.5, 400.0)]
    states = {"D1": _closed({1}), "D2": _closed({1, 4})}
    voters = voters_of(hits, {"m1": ["D1"], "m2": ["D2"]}, states, rule)
    scored = {t.term: t for t in score_target(voters.voters, ASPECT_OF, rule)}
    assert scored[1].best.label == "D2" and scored[1].carrying == 2 and scored[1].voters == 2
    assert scored[4].denominator == pytest.approx(200.0) and scored[4].voters == 1
    assert scored[1].score == pytest.approx(1.0)


def test_weights_that_sum_to_nothing_score_nothing() -> None:
    rule = TransferRule("bitscore_vote", denominator="any_aspect")
    voters = voters_of([_hit("m1", 0.5, 0.0)], {"m1": ["D1"]}, {"D1": _closed({1})}, rule)
    assert score_target(voters.voters, ASPECT_OF, rule) == []


def test_a_neighbourhood_split_by_aspect() -> None:
    by_aspect: dict[str, float] = defaultdict(float)
    rule = TransferRule("bitscore_vote", denominator="same_aspect")
    hits = [_hit("m1", 0.5, 100.0), _hit("m2", 0.5, 300.0)]
    states = {"D1": _closed({1}), "D2": _closed({4})}
    for t in score_target(
        voters_of(hits, {"m1": ["D1"], "m2": ["D2"]}, states, rule).voters, ASPECT_OF, rule
    ):
        by_aspect[ASPECT_OF[t.term]] = max(by_aspect[ASPECT_OF[t.term]], t.score)
    # Each aspect's only voter carries its terms: every term of either aspect scores 1.
    assert dict(by_aspect) == {"P": 1.0, "F": 1.0}
