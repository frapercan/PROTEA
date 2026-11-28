"""Homology transfer: a target scored from the donors its sequence hits (rungs 2a and 2b).

The cut's homology table (ADR-D61) holds, for every Swiss-Prot sequence of the
photo, its hits among every member sequence, keyed by md5, self hits dropped. A
target's voters are the donors behind its hits, and behind its own sequence: a
member that shares the target's sequence under another accession (a twin) is a hit
at identity 1 whose bit score is the target's self-alignment score
(``homology_self_scores``), since the table cannot hold it.

Three rules, from the literature, each a pure function here:

- ``top_hit_identity`` (CAFA1-3; LAFA's BLAST baseline, democafa's best HSP per pair
  read as ``pident / 100``): a term scores the largest identity among the voters
  that carry it.
- ``bitscore_vote`` (S1; BLAST-KNN, DiamondScore): ``sum bits x I(t) / sum bits``.
- ``bitscore_identity_vote`` (S2; Zhang and Freddolino 2024, InterLabelGO+'s
  AlignmentKNN): ``sum bits x ID x I(t) / sum bits x ID``.

Their variants:

- ``identity``: ``local`` is MMseqs2's ``fident``, the identity over the alignment;
  ``global`` is ``nident / max(qlen, slen)``, with ``nident = round(fident x
  alnlen)`` (the table's search ran with ``--seq-id-mode 0``). A twin is 1 either way.
- ``denominator`` of a vote: ``any_aspect`` sums the weight of every voter that
  carries an admitted term; ``same_aspect`` only of those carrying one in the
  term's aspect.
- ``multiplicity``: ``accession`` makes every donor accession a voter; ``sequence``
  makes every donor sequence one voter, carrying the union of its accessions' terms.
- ``hit_cap``: only the first hits of the table by bit score are read, which
  approximates a search run with ``--max-seqs`` at that value; twins are not hits
  of the table and are always read.

A voter's terms are its donor state at t0, closed and with its NOT terms out, so a
target's scores are consistent with the ontology by construction: a term never
scores below a descendant it implies.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, NamedTuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

Rule = Literal["top_hit_identity", "bitscore_vote", "bitscore_identity_vote"]
Identity = Literal["local", "global"]
Denominator = Literal["any_aspect", "same_aspect"]
Multiplicity = Literal["accession", "sequence"]

RULES: tuple[str, ...] = ("top_hit_identity", "bitscore_vote", "bitscore_identity_vote")
#: The rules that read an identity, and the rules that weigh by bit score.
READS_IDENTITY: frozenset[str] = frozenset({"top_hit_identity", "bitscore_identity_vote"})
READS_BITS: frozenset[str] = frozenset({"bitscore_vote", "bitscore_identity_vote"})
#: The columns of the table a transfer reads.
HIT_COLUMNS: tuple[str, ...] = ("query_md5", "target_md5", "fident", "alnlen", "evalue", "bits")


class TransferRefused(ValueError):
    """A rule or an input a homology transfer will not run with, and why."""


@dataclass(frozen=True)
class TransferRule:
    """A rule and its variants, refused when a variant means nothing under the rule."""

    rule: Rule
    identity: Identity | None = None
    denominator: Denominator | None = None
    multiplicity: Multiplicity = "accession"
    hit_cap: int | None = None

    def __post_init__(self) -> None:
        if self.rule not in RULES:
            raise TransferRefused(f"rule {self.rule!r} is not one of {list(RULES)}")
        if (self.identity is not None) != (self.rule in READS_IDENTITY):
            raise TransferRefused(
                f"{self.rule} {'reads' if self.rule in READS_IDENTITY else 'reads no'} identity: "
                f"{'name' if self.rule in READS_IDENTITY else 'leave out'} identity"
            )
        if (self.denominator is not None) != self.is_vote:
            raise TransferRefused(
                f"{self.rule} {'divides' if self.is_vote else 'divides nothing'}: "
                f"{'name' if self.is_vote else 'leave out'} the denominator"
            )
        if self.multiplicity == "sequence" and not self.is_vote:
            raise TransferRefused(
                "the largest identity over the donors is the same whether a sequence counts "
                "once or once per accession, so top_hit_identity takes multiplicity accession"
            )
        if self.hit_cap is not None and self.hit_cap < 1:
            raise TransferRefused(f"hit_cap {self.hit_cap} must be at least 1")

    @property
    def is_vote(self) -> bool:
        return self.rule in READS_BITS


class Hit(NamedTuple):
    """One donor sequence a target sequence aligns with: a hit of the table, or a twin."""

    md5: str
    fident: float
    alnlen: int
    #: The alignment's bit score; a twin's is the target's self score, None when missing.
    bits: float | None
    evalue: float | None
    twin: bool
    #: ``nident / max(qlen, slen)``, filled when the rule reads global identity.
    global_identity: float | None = None


def global_identity(fident: float, alnlen: int, qlen: int, slen: int) -> float:
    """``nident / max(qlen, slen)`` with ``nident = round(fident x alnlen)``."""
    return round(fident * alnlen) / max(qlen, slen)


def twin_hit(md5: str, self_bits: float | None, length: int | None) -> Hit:
    """The hit a twin stands for: the target's own sequence, aligned with itself."""
    return Hit(md5, 1.0, int(length or 0), self_bits, None, True, 1.0)


def read_hits(
    path: Path, queries: Iterable[str], hit_cap: int | None = None
) -> dict[str, list[Hit]]:
    """The table's hits of ``queries``, by query md5, best bit score first, ``hit_cap`` kept.

    Read row group by row group, keeping only the rows of a query asked about: a
    cut's table holds about ninety million hits and a toy window keeps under a
    million.
    """
    wanted = pa.array(sorted(set(queries)), pa.large_string())
    found: dict[str, list[Hit]] = defaultdict(list)
    handle = pq.ParquetFile(path)
    for group in range(handle.metadata.num_row_groups):
        table = handle.read_row_group(group, columns=list(HIT_COLUMNS))
        table = table.filter(pc.is_in(table["query_md5"], value_set=wanted))
        columns = [table[name].to_pylist() for name in HIT_COLUMNS]
        for query, target, fident, alnlen, evalue, bits in zip(*columns, strict=True):
            found[query].append(
                Hit(target, float(fident), int(alnlen), float(bits), float(evalue), False)
            )
    out: dict[str, list[Hit]] = {}
    for query, hits in found.items():
        ordered = sorted(hits, key=lambda h: (-(h.bits or 0.0), h.md5))
        out[query] = ordered[:hit_cap] if hit_cap is not None else ordered
    return out


class Voter(NamedTuple):
    """A donor accession (or, by sequence, a donor sequence) voting for its terms."""

    label: str
    hit: Hit
    weight: float
    identity: float
    terms: frozenset[int]


def _identity(hit: Hit, rule: TransferRule) -> float:
    if hit.twin:
        return 1.0
    if rule.identity == "global":
        if hit.global_identity is None:
            raise TransferRefused(f"the hit to {hit.md5} has no global identity")
        return hit.global_identity
    return hit.fident


def _weight(hit: Hit, identity: float, rule: TransferRule) -> float:
    if rule.rule == "top_hit_identity":
        return identity
    bits = float(hit.bits or 0.0)
    return bits * identity if rule.rule == "bitscore_identity_vote" else bits


class Voters(NamedTuple):
    """A target's voters, and the twins left out of the vote for want of a self score."""

    voters: list[Voter]
    twins_without_self_score: int


def voters_of(
    hits: Sequence[Hit],
    donors: Mapping[str, Sequence[str]],
    states: Mapping[str, frozenset[int]],
    rule: TransferRule,
) -> Voters:
    """Every voter behind ``hits``: the admitted donors with a state, by accession or by sequence.

    ``donors`` gives each hit's sequence its admitted donor accessions, the query's
    own accession (and, under sequence exclusion, its twins) already left out;
    ``states`` each donor's terms. A twin with no self score cannot be weighed by
    bit score, so under a vote it is left out and counted.
    """
    voters: list[Voter] = []
    skipped = 0
    for hit in hits:
        carrying = [(a, states[a]) for a in donors.get(hit.md5, ()) if states.get(a)]
        if not carrying:
            continue
        if hit.twin and rule.is_vote and hit.bits is None:
            skipped += len(carrying)
            continue
        identity = _identity(hit, rule)
        weight = _weight(hit, identity, rule)
        if rule.multiplicity == "sequence":
            terms = frozenset().union(*(terms for _, terms in carrying))
            voters.append(Voter(hit.md5, hit, weight, identity, terms))
        else:
            voters.extend(Voter(a, hit, weight, identity, terms) for a, terms in carrying)
    return Voters(voters, skipped)


class ScoredTerm(NamedTuple):
    """One term of a target: its score, how it was reached and its best voter."""

    term: int
    score: float
    numerator: float
    denominator: float | None
    voters: int
    carrying: int
    best: Voter


def _rank(voter: Voter) -> tuple[float, float, str]:
    """Best first: the larger weight, then the larger bit score, then the label."""
    return (-voter.weight, -float(voter.hit.bits or 0.0), voter.label)


def _denominators(
    voters: Sequence[Voter], aspect_of: Sequence[str], rule: TransferRule
) -> dict[str | None, tuple[float, int]]:
    """The vote's denominators and how many voters each sums: over every voter, or per aspect."""
    if rule.denominator == "any_aspect":
        return {None: (sum(v.weight for v in voters), len(voters))}
    weight: dict[str | None, float] = defaultdict(float)
    count: dict[str | None, int] = defaultdict(int)
    for voter in voters:
        for aspect in {aspect_of[t] for t in voter.terms}:
            weight[aspect] += voter.weight
            count[aspect] += 1
    return {aspect: (weight[aspect], count[aspect]) for aspect in weight}


def score_target(
    voters: Sequence[Voter], aspect_of: Sequence[str], rule: TransferRule
) -> list[ScoredTerm]:
    """The scored terms of one target from its voters, ascending by term index.

    ``aspect_of`` gives each term index its aspect. A term no voter carries is not
    scored, and neither is one whose denominator weighs nothing.
    """
    ordered = sorted(voters, key=_rank)
    best: dict[int, Voter] = {}
    numerator: dict[int, float] = defaultdict(float)
    carrying: dict[int, int] = defaultdict(int)
    for voter in ordered:
        for term in voter.terms:
            best.setdefault(term, voter)
            carrying[term] += 1
            if rule.is_vote:
                numerator[term] += voter.weight
            else:
                numerator[term] = max(numerator[term], voter.weight)
    if not rule.is_vote:
        return [
            ScoredTerm(t, numerator[t], numerator[t], None, len(ordered), carrying[t], best[t])
            for t in sorted(best)
            if numerator[t] > 0.0
        ]
    denominators = _denominators(ordered, aspect_of, rule)
    out: list[ScoredTerm] = []
    for term in sorted(best):
        key = None if rule.denominator == "any_aspect" else aspect_of[term]
        total, voting = denominators.get(key, (0.0, 0))
        if total <= 0.0:
            continue
        share = numerator[term] / total
        out.append(
            ScoredTerm(term, share, numerator[term], total, voting, carrying[term], best[term])
        )
    return out


__all__ = [
    "HIT_COLUMNS",
    "READS_BITS",
    "READS_IDENTITY",
    "RULES",
    "Denominator",
    "Hit",
    "Identity",
    "Multiplicity",
    "Rule",
    "ScoredTerm",
    "TransferRefused",
    "TransferRule",
    "Voter",
    "Voters",
    "global_identity",
    "read_hits",
    "score_target",
    "twin_hit",
    "voters_of",
]
