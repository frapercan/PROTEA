"""What the encoder is worth on its own, before any protein is involved.

Every number here has a companion that says what it would be without the
encoder. A rank of 300 out of 40,214 sounds excellent until you learn that
ranking by term frequency gets 400, and this project has already shipped one
measurement that meant nothing because nobody asked what the alternative was.
"""

from __future__ import annotations

import random
from dataclasses import dataclass

import numpy as np

from protea.core.ontology.dag import Dag
from protea.core.ontology.order_encoder import OrderEncoder


@dataclass(frozen=True)
class RankReport:
    """Where the true parent landed among all terms, over held-out edges."""

    n: int
    mrr: float
    hits1: float
    hits10: float
    hits100: float
    median_rank: float

    def line(self, tag: str) -> str:
        return (
            f"  {tag:22s} n={self.n:6,d}  MRR {self.mrr:.4f}  "
            f"h@1 {self.hits1:6.2%}  h@10 {self.hits10:6.2%}  "
            f"h@100 {self.hits100:6.2%}  rango p50 {self.median_rank:8,.0f}"
        )


def rank_held_out_parents(
    model: OrderEncoder,
    dag: Dag,
    test_edges: list[tuple[str, str]],
    *,
    sample: int,
    against_non_ancestors_only: bool = False,
) -> RankReport:
    """For each held-out edge, rank terms as candidate parents of the child.

    The child's OTHER parents are always removed. They are true answers too,
    and leaving them in would push the held-out parent down for being right in
    company.

    ``against_non_ancestors_only`` removes the child's whole ancestor set, and
    it is the measurement that means something for this architecture. An order
    embedding gives penalty zero to EVERY ancestor, not to the direct parent
    alone, because that is what subsumption is: the grandparent subsumes the
    child just as truly. Ranking against all terms therefore charges the
    encoder for ties it is right to produce. The held-out children have a
    median of 15 ancestors and the measured median rank against all terms is
    30, so about half the rank is legitimate tie. Removing the ancestors leaves
    only the error: how many terms that do NOT subsume the child were placed as
    if they did.
    """
    rng = random.Random(0)
    edges = test_edges if len(test_edges) <= sample else rng.sample(test_edges, sample)
    all_terms = list(dag.terms)
    ranks: list[int] = []
    for parent, child in edges:
        drop = (
            (dag.ancestors(child) | set(dag.parents_of(child))) - {parent}
            if against_non_ancestors_only
            else set(dag.parents_of(child)) - {parent}
        )
        cands = [t for t in all_terms if t != child and t not in drop]
        order = model.rank_parents(child, cands)
        pos = cands.index(parent)
        ranks.append(int(np.where(order == pos)[0][0]) + 1)
    r = np.array(ranks, dtype=float)
    return RankReport(
        n=len(r),
        mrr=float((1.0 / r).mean()),
        hits1=float((r <= 1).mean()),
        hits10=float((r <= 10).mean()),
        hits100=float((r <= 100).mean()),
        median_rank=float(np.median(r)),
    )


def _hard_negatives(
    dag: Dag, closure: set[tuple[str, str]], pos: list[tuple[str, str]], rng: random.Random
) -> list[tuple[str, str]]:
    """Negatives that are close to being true, which is the only kind that tests.

    Two random GO terms have nothing to do with each other, so a balanced set
    built from them measures whether the encoder can tell "related" from
    "unrelated" and calls the result subsumption accuracy. This project has
    already shipped one control where the defect could not appear.

    Two kinds are drawn here. A REVERSED pair takes a true (ancestor,
    descendant) and asks the encoder about (descendant, ancestor): the terms
    are as related as they can be and the answer is still no, so nothing but
    the asymmetry can separate them. A SIBLING pair takes two children of one
    parent: adjacent in the graph, neither subsuming the other.
    """
    out: list[tuple[str, str]] = []
    for up, lo in pos:
        if (lo, up) not in closure and lo != up:
            out.append((lo, up))
    for parent in rng.sample(dag.terms, min(len(dag.terms), 4 * len(pos))):
        kids = dag.children_of(parent)
        if len(kids) < 2:
            continue
        a, b = rng.sample(kids, 2)
        if (a, b) not in closure:
            out.append((a, b))
        if len(out) >= 2 * len(pos):
            break
    rng.shuffle(out)
    return out[: len(pos)]


def separates_ancestors(
    model: OrderEncoder,
    dag: Dag,
    closure: set[tuple[str, str]],
    *,
    n: int,
    seed: int,
    hard: bool = False,
) -> tuple[float, float]:
    """Can the penalty tell a true subsumption from a false one.

    Returns (accuracy at the best threshold, median false penalty over median
    true penalty). With ``hard``, the negatives are reversed true pairs and
    siblings rather than random pairs; see :func:`_hard_negatives`.
    """
    rng = random.Random(seed)
    pos = rng.sample(sorted(closure), min(n, len(closure)))
    if hard:
        neg = _hard_negatives(dag, closure, pos, rng)
        pos = pos[: len(neg)]
    else:
        neg = []
        while len(neg) < len(pos):
            a, b = rng.choice(dag.terms), rng.choice(dag.terms)
            if a != b and (a, b) not in closure:
                neg.append((a, b))
    sp, sn = model.score_pairs(pos), model.score_pairs(neg)
    cuts = np.unique(np.concatenate([sp, sn]))
    if len(cuts) > 2000:
        cuts = np.quantile(cuts, np.linspace(0, 1, 2000))
    acc = max(float(((sp <= c).sum() + (sn > c).sum()) / (len(sp) + len(sn))) for c in cuts)
    ratio = float(np.median(sn) / max(np.median(sp), 1e-9))
    return acc, ratio
