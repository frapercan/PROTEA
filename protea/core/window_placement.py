"""Place each target of a window on the sequence axes, as it stood when the window opened.

Two axes of :mod:`protea.core.strata` describe a target's sequence rather than a method:
its length, and its identity to the closest protein whose function had been measured
when the window opened. Read from a method's own neighbours, the identity axis was a
property of that method's retrieval: a method that retrieves nothing had no identity
to report, and two methods on one window placed the same protein in different bands.
Read here, once per window, it is a property of the window, and every method is
stratified on the same placement (the closure's question 4, answered on 2026-11-20).

**The rule, ``closest_truth_member``.** A target's closest truth member is the closest
OTHER protein of the photo of the window's opening cut that carries a truth row there,
a measurement on it (ADR-D59's ``truth`` set, the thirteen LAFA codes):

1. An identical sequence under another accession, when there is one: identity 100,
   by the membership table and with no alignment, as ADR-D61 decision 3 says ("two
   accessions with the same sequence are identical through the membership table").
   The target itself never counts, so a target with truth rows of its own (a
   limited- or partial-knowledge target) is not its own closest member.
2. Otherwise the best hit of the cut's homology table among the truth members'
   sequences: the highest bit score, then the highest identity, then the lowest
   e-value, then the lowest sequence md5, so the choice does not depend on the order
   the file was written in. The bit score ranks first because it grows with the
   length of the alignment as well as with its identity: the highest ``fident``
   alone would let a short local alignment at 100 % place a protein among the
   near-identical ones.
3. Otherwise nothing: :attr:`~protea.core.strata.HomologyBand.NONE`, a population
   and not a missing value.

The homology table applies MMseqs2's defaults and no threshold of ours (ADR-D61
decision 4), and neither does this rule.

**Identity is rounded to the precision MMseqs2 printed.** ``fident`` comes with three
decimals and the table stores it as float32, where 0.300 reads 0.30000001: times 100
that is above 30 and would cross the edge of the twilight band. The percentage is
rounded to one decimal, which is the printed value exactly.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import NamedTuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

RULE = "closest_truth_member"

#: The hit columns the rule reads, in the homology table's spelling (ADR-D61 decision 5).
HIT_COLUMNS = ("query_md5", "target_md5", "fident", "alnlen", "qcov", "tcov", "evalue", "bits")


class Hit(NamedTuple):
    """One alignment of the homology table: a query sequence against a member's."""

    target_md5: str
    fident: float
    alnlen: int
    qcov: float
    tcov: float
    evalue: float
    bits: float

    def rank(self) -> tuple[float, float, float, str]:
        """Smaller is closer: bit score, identity, e-value, then the md5 for a stable choice."""
        return (-self.bits, -self.fident, self.evalue, self.target_md5)


class Placement(NamedTuple):
    """Where one target sits: its sequence at the window's start, and its closest truth member."""

    accession: str
    sequence_md5: str
    residues: int
    closest_by: str | None
    closest_identity: float | None
    closest_md5: str | None
    bits: float | None
    evalue: float | None
    alnlen: int | None
    qcov: float | None
    tcov: float | None


def identity_percent(fident: float) -> float:
    """MMseqs2's ``fident`` as a percentage at the precision it was printed with."""
    return round(float(fident) * 100.0, 1)


def best_truth_hits(
    hits_path: Path, queries: Iterable[str], truth: Iterable[str]
) -> dict[str, Hit]:
    """Each query sequence's best hit among the truth members' sequences, by :meth:`Hit.rank`.

    Read row group by row group, keeping only the hits from a query asked about to a
    truth sequence: a cut's table holds about ninety million hits, and a window keeps
    under a million of them.
    """
    wanted = pa.array(sorted(set(queries)), pa.large_string())
    bank = pa.array(sorted(set(truth)), pa.large_string())
    best: dict[str, Hit] = {}
    handle = pq.ParquetFile(hits_path)
    for group in range(handle.metadata.num_row_groups):
        table = handle.read_row_group(group, columns=list(HIT_COLUMNS))
        keep = pc.and_(
            pc.is_in(table["query_md5"], value_set=wanted),
            pc.is_in(table["target_md5"], value_set=bank),
        )
        for row in table.filter(keep).to_pylist():
            hit = Hit(
                target_md5=row["target_md5"],
                fident=float(row["fident"]),
                alnlen=int(row["alnlen"]),
                qcov=float(row["qcov"]),
                tcov=float(row["tcov"]),
                evalue=float(row["evalue"]),
                bits=float(row["bits"]),
            )
            current = best.get(row["query_md5"])
            if current is None or hit.rank() < current.rank():
                best[row["query_md5"]] = hit
    return best


def place(
    targets: Mapping[str, str],
    residues: Mapping[str, int],
    truth_accessions: Mapping[str, frozenset[str]],
    best: Mapping[str, Hit],
) -> list[Placement]:
    """Place every target by :data:`RULE`, in accession order.

    ``targets`` maps a target's accession to its sequence md5 at the window's start,
    ``residues`` an md5 to its length, ``truth_accessions`` an md5 to the accessions of
    the photo's truth members with that sequence, and ``best`` a query md5 to its best
    hit among them (:func:`best_truth_hits`).
    """
    placed: list[Placement] = []
    for accession in sorted(targets):
        md5 = targets[accession]
        head = (accession, md5, int(residues[md5]))
        if truth_accessions.get(md5, frozenset()) - {accession}:
            placed.append(Placement(*head, "twin", 100.0, md5, None, None, None, None, None))
            continue
        hit = best.get(md5)
        if hit is None:
            placed.append(Placement(*head, None, None, None, None, None, None, None, None))
            continue
        placed.append(
            Placement(
                *head,
                "hit",
                identity_percent(hit.fident),
                hit.target_md5,
                hit.bits,
                hit.evalue,
                hit.alnlen,
                hit.qcov,
                hit.tcov,
            )
        )
    return placed


def needs_a_hit(
    targets: Mapping[str, str], truth_accessions: Mapping[str, frozenset[str]]
) -> set[str]:
    """The target sequences no twin places, which are the only ones the hits are read for."""
    return {
        md5
        for accession, md5 in targets.items()
        if not truth_accessions.get(md5, frozenset()) - {accession}
    }


__all__ = [
    "HIT_COLUMNS",
    "RULE",
    "Hit",
    "Placement",
    "best_truth_hits",
    "identity_percent",
    "needs_a_hit",
    "place",
]
