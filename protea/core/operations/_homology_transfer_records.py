"""What a homology transfer writes besides its scores: the evidence and the counts.

The evidence is one row per written (target, term): the score, the vote's
numerator and denominator (the largest identity and nothing, for the top hit), how
many voters the denominator summed and how many carried the term, the best voter
(its donor, sequence, identity, alignment length, bit score, e-value and whether
it is a twin) and the target's total weight, the sum of every voter's weight
(GOtcha's total evidence), which a reranker can read. The counts say what was read,
voted and left out, the twins without a self score among them.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, NamedTuple

import pyarrow as pa

from protea.core.homology.transfer import ScoredTerm, Voter, Voters


class EvidenceRow(NamedTuple):
    """One written (target, term) and how it was reached."""

    accession: str
    term: int
    score: float
    numerator: float
    denominator: float | None
    voters: int
    carrying: int
    best: Voter
    total_weight: float


def rows_of(
    scored: Iterable[tuple[str, Voters, list[ScoredTerm]]], data: Any
) -> tuple[list[EvidenceRow], dict[str, Any]]:
    """Every written row, and the counts of what the targets read and voted."""
    rows: list[EvidenceRow] = []
    targets = twins_voted = twins_left_out = targets_left_out = with_term = 0
    for accession, voters, terms in scored:
        targets += 1
        total = sum(v.weight for v in voters.voters)
        twins_voted += sum(1 for v in voters.voters if v.hit.twin)
        twins_left_out += voters.twins_without_self_score
        targets_left_out += voters.twins_without_self_score > 0
        with_term += bool(terms)
        rows.extend(
            EvidenceRow(
                accession,
                t.term,
                t.score,
                t.numerator,
                t.denominator,
                t.voters,
                t.carrying,
                t.best,
                total,
            )
            for t in terms
        )
    target_md5s = set(data.md5_of.values())
    counts = {
        "targets": len(data.targets),
        "targets_without_a_sequence": len(data.targets) - len(data.md5_of),
        "targets_scored": targets,
        "targets_with_a_term": with_term,
        "rows": len(rows),
        "hits_read": sum(len(h) for h in data.hits.values()),
        "donor_accessions": sum(len(d) for d in data.donors.values()),
        "donors_with_a_state": len(data.states.terms),
        "twins": {
            "voted": twins_voted,
            "left_out_without_a_self_score": twins_left_out,
            "targets_with_twins_left_out": targets_left_out,
        },
        "target_sequences_without_a_self_score": None
        if data.self_bits is None
        else len(target_md5s - set(data.self_bits)),
    }
    return rows, counts


def evidence_table(rows: list[EvidenceRow], data: Any) -> pa.Table:
    """The evidence parquet: one row per written (target, term)."""
    go_ids, aspects = data.states.go_ids, data.states.aspects
    return pa.table(
        {
            "accession": pa.array([r.accession for r in rows], pa.string()),
            "go_id": pa.array([go_ids[r.term] for r in rows], pa.string()),
            "aspect": pa.array([aspects[r.term] for r in rows], pa.string()),
            "score": pa.array([r.score for r in rows], pa.float64()),
            "numerator": pa.array([r.numerator for r in rows], pa.float64()),
            "denominator": pa.array([r.denominator for r in rows], pa.float64()),
            "voters": pa.array([r.voters for r in rows], pa.int32()),
            "voters_carrying": pa.array([r.carrying for r in rows], pa.int32()),
            "best_donor": pa.array([r.best.label for r in rows], pa.string()),
            "best_donor_md5": pa.array([r.best.hit.md5 for r in rows], pa.string()),
            "best_identity": pa.array([r.best.identity for r in rows], pa.float64()),
            "best_fident": pa.array([r.best.hit.fident for r in rows], pa.float64()),
            "best_alnlen": pa.array([r.best.hit.alnlen for r in rows], pa.int32()),
            "best_bits": pa.array([r.best.hit.bits for r in rows], pa.float64()),
            "best_evalue": pa.array([r.best.hit.evalue for r in rows], pa.float64()),
            "best_is_twin": pa.array([r.best.hit.twin for r in rows], pa.bool_()),
            "total_weight": pa.array([r.total_weight for r in rows], pa.float64()),
        }
    )


__all__ = ["EvidenceRow", "evidence_table", "rows_of"]
