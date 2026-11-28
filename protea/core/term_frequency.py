"""Term frequency, rung 0 of the literature ladder: how often each term is held at t0.

The CAFA baseline every method is read against, the same vector of scores for every
target. Two variants, each a pure function of what the operation
(``protea.core.operations.run_term_frequency``) reads at the window's start:

``lafa_direct``, LAFA's literal. CAFA_forever (``modules/local/predictions.nf`` at
``2ce2457``) runs democafa's ``naive.py --n_terms 1500`` on ``train_terms.tsv``,
which ``retrieve_terms -sgc 'Experimental,IC,TAS'`` writes with no propagation.
So, read from democafa's code at ``1102e34`` (an oracle; nothing is copied):

1. the rows are direct: one per (protein, term, the row's own aspect), NOT rows
   dropped without negating anything, an alternative id named as its primary and
   an obsolete term as its replacement, a term not alive in the t0 ontology dropped;
2. ``N_t`` is the number of proteins with a row on ``t``;
3. democafa divides by the roots' own counts when a root is among the terms, and
   stops on an assertion unless all three are; when none is ("when terms were not
   propagated") it divides by ``D_aspect``, the number of proteins with any row
   whose aspect is the term's. The literal takes the same branches and refuses,
   naming the aspects, where democafa would stop;
4. ``top_k = int(n_terms / 3)`` = 500: in an aspect with more than 500 terms, every
   score at or below the 500th largest becomes zero, so at most 499 terms are kept
   and fewer with ties. Within an aspect every score shares one denominator, so
   ranking the counts ranks the scores, and the cap is computed on the counts
   exactly (democafa ranks float32 scores; two different counts below 2**24 never
   meet in float32 over one denominator).

``propagated``, the recipe's Naive (the CAFA definition): every protein's closed
state at t0 as LAFA's frame states it (NOT terms of t0 and their descendants out of
the direct terms first), ``N_t / N_root`` with ``N_root`` the proteins holding a
term in the aspect, roots out, and every term whose four-decimal rendering is at or
above the evaluator grid's first element. It differs from the literal, and the
method's recipe says how.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, NamedTuple

from protea.core.lafa_frame import EndOntology, named

#: The three roots, by the aspect each one roots.
ROOT_OF: Mapping[str, str] = {"P": "GO:0008150", "F": "GO:0003674", "C": "GO:0005575"}
ROOTS: frozenset[str] = frozenset(ROOT_OF.values())
ASPECTS: tuple[str, ...] = ("P", "F", "C")
#: democafa's ``top_k = int(n_terms / 3)`` with CAFA_forever's ``--n_terms 1500``.
LAFA_TERMS_PER_ASPECT = 500
#: The evaluator grid's first element at LAFA's ``th_step`` 0.01: a score whose
#: four-decimal rendering is below it is under every threshold the grid holds.
GRID_FLOOR = 0.01


class LiteralRefused(ValueError):
    """Rows on which democafa's Naive would stop, with the aspects that make it stop."""


class DirectRow(NamedTuple):
    """One direct annotation row: the protein, the term as stored, and the row's aspect."""

    protein: str
    go_id: str
    aspect: str


@dataclass(frozen=True)
class TermScores:
    """The vector every target receives, the counts behind it and what each step did.

    ``scores`` holds the terms written; ``proteins`` every counted term's ``N_t``;
    ``aspect_of`` each counted term's aspect; ``denominators`` the per-aspect
    denominator; ``counted`` the proteins that entered a count; ``record`` the
    branch taken and the counts of each step.
    """

    scores: dict[str, float]
    proteins: dict[str, int]
    aspect_of: dict[str, str]
    denominators: dict[str, int]
    counted: frozenset[str] = frozenset()
    record: dict[str, Any] = field(default_factory=dict)


def rendered(score: float) -> float:
    """The score as the evaluator reads it: rendered with four decimals."""
    return float(f"{score:.4f}")


def _named_rows(
    rows: Iterable[DirectRow], ontology: EndOntology, counts: Counter[str]
) -> set[tuple[str, str, str]]:
    """Step 1: each row's term as the t0 ontology names it, one row per protein, term and aspect."""
    kept: set[tuple[str, str, str]] = set()
    for protein, go_id, aspect in rows:
        for term in named(ontology, (go_id,), counts, "row"):
            kept.add((protein, term, aspect))
    return kept


def _literal_denominators(
    proteins: Mapping[str, int], proteins_by_aspect: Mapping[str, set[str]]
) -> tuple[str, dict[str, int], dict[str, int]]:
    """Step 3: democafa's branch, its denominators and the roots' direct counts."""
    root_rows = {aspect: proteins[root] for aspect, root in ROOT_OF.items() if root in proteins}
    if root_rows:
        missing = [aspect for aspect in ASPECTS if aspect not in root_rows]
        if missing:
            raise LiteralRefused(
                f"the rows hold the root of {sorted(root_rows)} and not of {missing}: democafa "
                "divides by the roots' counts when a root is among the terms and stops unless "
                "all three are, so the literal refuses where it would stop"
            )
        return "root_counts", dict(root_rows), root_rows
    missing = [aspect for aspect in ASPECTS if not proteins_by_aspect.get(aspect)]
    if missing:
        raise LiteralRefused(
            f"no protein has a row in aspect {missing}: democafa needs a denominator for each "
            "of the three aspects and stops without one"
        )
    return "proteins_per_aspect", {a: len(proteins_by_aspect[a]) for a in ASPECTS}, {}


def _capped(
    proteins: Mapping[str, int], aspect_of: Mapping[str, str], cap: int
) -> tuple[set[str], dict[str, Any]]:
    """Step 4: the terms that survive the cap, and per aspect what the cap did."""
    kept: set[str] = set()
    record: dict[str, Any] = {}
    for aspect in ASPECTS:
        terms = [t for t, a in aspect_of.items() if a == aspect]
        if len(terms) <= cap:
            kept.update(terms)
            record[aspect] = {"terms": len(terms), "capped": False, "kept": len(terms)}
            continue
        threshold = sorted((proteins[t] for t in terms), reverse=True)[cap - 1]
        survivors = [t for t in terms if proteins[t] > threshold]
        kept.update(survivors)
        record[aspect] = {
            "terms": len(terms),
            "capped": True,
            "threshold_proteins": threshold,
            "kept": len(survivors),
        }
    return kept, record


def lafa_direct(
    rows: Iterable[DirectRow], ontology: EndOntology, cap: int = LAFA_TERMS_PER_ASPECT
) -> TermScores:
    """LAFA's Naive on the population's direct rows, under the t0 ontology."""
    counts: Counter[str] = Counter()
    kept = _named_rows(rows, ontology, counts)
    holders: dict[str, set[str]] = defaultdict(set)
    proteins_by_aspect: dict[str, set[str]] = defaultdict(set)
    for protein, term, aspect in kept:
        holders[term].add(protein)
        proteins_by_aspect[aspect].add(protein)
    proteins = {term: len(found) for term, found in holders.items()}
    # The term's aspect is its namespace in the ontology, as democafa's
    # ``fetch_aspect`` reads it; the denominator counts by the row's own aspect.
    aspect_of = {term: ontology.live[term] for term in proteins}
    branch, denominators, root_rows = _literal_denominators(proteins, proteins_by_aspect)
    survivors, cap_record = _capped(proteins, aspect_of, cap)
    scores = {term: proteins[term] / denominators[aspect_of[term]] for term in sorted(survivors)}
    above = sorted(term for term, score in scores.items() if score > 1.0)
    if above:
        raise LiteralRefused(
            f"terms {above[:10]} are held by more proteins than their aspect's denominator: "
            "rows whose aspect is not their term's namespace, which democafa would score above 1"
        )
    counted = frozenset(protein for protein, _, _ in kept)
    record = {
        "variant": "lafa_direct",
        "branch": branch,
        "root_direct_proteins": root_rows,
        "proteins_with_a_row": len(counted),
        "rows": len(kept),
        "cap": cap_record,
        "naming": dict(sorted(counts.items())),
    }
    return TermScores(scores, proteins, aspect_of, denominators, counted, record)


def propagated(
    states: Mapping[str, Mapping[str, Iterable[str]]], floor: float = GRID_FLOOR
) -> TermScores:
    """The CAFA Naive on closed states: ``N_t / N_root`` per aspect, roots out, at the floor."""
    holders: Counter[str] = Counter()
    aspect_of: dict[str, str] = {}
    with_terms: Counter[str] = Counter()
    for by_aspect in states.values():
        for aspect, terms in by_aspect.items():
            found = set(terms)
            if not found:
                continue
            with_terms[aspect] += 1
            for term in found:
                holders[term] += 1
                aspect_of[term] = aspect
    denominators = {aspect: with_terms[aspect] for aspect in ASPECTS if with_terms[aspect]}
    proteins = {term: count for term, count in holders.items() if term not in ROOTS}
    scores: dict[str, float] = {}
    below = 0
    for term in sorted(proteins):
        score = proteins[term] / denominators[aspect_of[term]]
        if rendered(score) >= floor:
            scores[term] = score
        else:
            below += 1
    counted = frozenset(p for p, by_aspect in states.items() if any(by_aspect.values()))
    record = {
        "variant": "propagated",
        "proteins_with_a_term": len(counted),
        "floor": floor,
        "terms_below_the_floor": below,
        "root_proteins": {a: holders[ROOT_OF[a]] for a in ASPECTS if ROOT_OF[a] in holders},
    }
    return TermScores(
        scores, proteins, {t: aspect_of[t] for t in proteins}, denominators, counted, record
    )


__all__ = [
    "ASPECTS",
    "GRID_FLOOR",
    "LAFA_TERMS_PER_ASPECT",
    "ROOTS",
    "ROOT_OF",
    "DirectRow",
    "LiteralRefused",
    "TermScores",
    "lafa_direct",
    "propagated",
    "rendered",
]
