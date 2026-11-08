"""What each query's annotations ARE at both ends, which is the model's state.

IT IS NOT THE TRUTH OF THE WINDOW. ``gains`` is that: the terms a query gained,
trimmed by the terms of interest. This table is the STATE, and the difference
is why it carries more: **every evidence code at both moments**, the
non-experimental ones included, because the state a model conditions on
includes what is merely inferred. The laboratory asked for this and it was the
correction that mattered most: this table was being thought of as the truth of
each end, and it is not.

THE ORDER IS THREE STEPS, AND WHAT IT BUYS IS THE TWO COUNTS. Exactly: an
exact DISTINCT in the id space of each card's OWN snapshot, which is the GAF's
own duplication; then the rewrite into the axis; then a second DISTINCT on the
axis, which is two surrogates of one term meeting. ``go_term.id`` is a
surrogate PER SNAPSHOT, so a card read from release R names its term in R's id
space and has to be rewritten before it can be compared with one from another.

THE ROWS WOULD BE THE SAME EITHER WAY, and this is the honest statement of it.
The rewrite is a FUNCTION on cards, so rewriting and then removing duplicates
gives exactly the rows that removing duplicates and then rewriting does. What
the order buys is not rows: it is that the two collapses are ATTRIBUTABLE. One
DISTINCT at the end would give the right table and one unreadable number, and
the laboratory's session is the one that put it this way.

They are counted APART because they are two different facts about the data:
what the GAF itself wrote twice, and what became equal only once the axis named
it. One number for both would print the same digit for a bundle whose GAF
duplication changed and a bundle whose synonym map changed.

THE CARD IS FOUR FIELDS PLUS THE MOMENT, and ``assigned_by`` is NOT one of
them. That is not an omission. Production's own scorer sorts a donor's
annotations by ``(go_term_id, evidence_code, qualifier)`` and takes the
evidence code from the first row that matches, so the source is never read; and
inside the key it multiplies the cards by 1,177, measured over a sixteenth of
release 211, which is exactly the overlap that makes the laboratory's loader
reject a table. It stays in the database, recoverable by the card's own key.

THE NOT ROWS ARE KEPT, WITH THEIR QUALIFIER AS THE ROW CARRIES IT. The
qualifier in this series is a RELATION and not a NOT flag, and the regime
changes at release 202, from GAF 2.1 to 2.2, in the middle of the series. So it
goes through unnormalised: translating two regimes into one would make the
column say something neither file said. The laboratory's loader reads a
negation as "contains NOT", which works in both regimes.

THE POPULATION IS THE ONE ``queries`` HAS, with the same refusal ``gains``
makes, for the same reason: a state row for a protein ``queries`` excludes
would be a state for a query the loader does not know.

THE SHAPE, measured before the code. Over the 65 admissible windows this table
is **44.744.324 rows** before the two collapses, the largest window 3.074.761,
the mean 688.374, at 43 to 122 annotations per protein across both moments.
That is 4,6 times ``gains`` and the point where accumulating every column in
memory stops being obviously right, so it is written ONE ROW GROUP PER WINDOW
and released: the largest window bounds the memory.

AND STREAMING COSTS NO ATOMICITY, which is what makes it acceptable here. The
temp file is local and the only thing that turns it into an artifact is the
``put`` at the end, so a pass that fails halfway leaves a local file nobody
published. "A run writes the whole bundle or it fails" holds without
accumulating anything.
"""

from __future__ import annotations

from collections import Counter
from typing import NamedTuple

import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core._truth_remap import SideFrame
from protea.core.transitions_export._window_facts import WindowFacts

#: The six columns the laboratory's loader reads, in its order. EVERY ONE of
#: them is part of a row's identity, so the card is the whole row: that is why
#: the loader can refuse a repeated card without knowing anything about where
#: a row came from.
SCHEMA = pa.schema(
    [
        ("window", pa.string()),
        ("query", pa.string()),
        ("moment", pa.string()),
        ("go_id", pa.string()),
        ("evidence_code", pa.string()),
        ("qualifier", pa.string()),
    ]
)

#: The two moments, spelled as ADR-D56 spells them. ``t'`` and not ``t1``: the
#: laboratory's loader and its refusals already use this one, and a spelling
#: that is implemented beats a spelling that is only written.
MOMENTS: tuple[str, str] = ("t", "t'")

#: Every annotation of a window's queries at one end, with the GO id resolved
#: through that snapshot's own ``go_term`` row.
#:
#: ``go_term.id`` is a surrogate key PER SNAPSHOT, so the same GO term has a
#: different one in every release. Reading ``go_term_id`` as if it identified a
#: term is a measurement that was actually made and got wrong: it said an
#: annotation persisted in 1,1 releases of 48 instead of 19,2.
_STATE = text("""
    SELECT pga.protein_accession, gt.go_id, pga.evidence_code,
           COALESCE(pga.qualifier, '') AS qualifier
      FROM protein_go_annotation pga
      JOIN go_term gt ON gt.id = pga.go_term_id
     WHERE pga.annotation_set_id = :aset
       AND pga.protein_accession = ANY(:population)
""")

#: ``(query, moment, go_id, evidence_code, qualifier)``. Five and not four:
#: mypy caught the annotation saying four while the code appended five, so
#: ``f[4]`` was an index out of range in the TYPE while being right in the
#: data. A wrong type on a tuple is a comment the checker can refute, which is
#: the only kind of comment worth having.
_Row = tuple[str, str, str, str, str]


class Collapse(NamedTuple):
    """How many rows each of the two steps removed, counted apart.

    One number for both would be unreadable: a bundle whose GAF duplication
    changed and a bundle whose synonym map changed would print the same digit.
    """

    #: Cards the GAF itself wrote more than once, before anything was rewritten.
    gaf: int
    #: Cards that became equal only after the rewrite into the axis, which is
    #: two surrogates of one term meeting.
    remap: int


class Discards(NamedTuple):
    """Rows this table does not write, by reason. ``off_axis`` should be zero."""

    #: No live id on the axis and no published successor that has one. On the
    #: 65 windows published so far the axis IS release 211's own corpus
    #: ontology, so this is expected to be zero and a non-zero value is the
    #: thing to read before trusting the bundle.
    off_axis: int
    #: Rewritten to a successor the ontology named in a DIFFERENT aspect. Not a
    #: discard: the substitution happens, because the ontology said so. Counted
    #: because it moves a term between the groups the loader groups by.
    crossed_aspect: int
    #: No evidence code at all. ``protein_go_annotation.evidence_code`` is
    #: NULLABLE in the schema, so this branch is REACHABLE even though the live
    #: data does not take it: the top eight codes of a sample of release 211
    #: are all real ones. It is dropped rather than written, because the model
    #: reads an evidence CLASS and an empty class is not one, and the
    #: laboratory's loader would drop it as unknown anyway. Counted here so the
    #: number is visible on this side before it reaches that one.
    no_evidence: int


class WindowState(NamedTuple):
    """One window's state rows, with the counts that make it readable."""

    window: str
    rows: int
    by_moment: dict[str, int]
    #: Per moment, per code. The point of this table is that it is NOT the
    #: truth: if a moment shows thirteen codes or fewer it is carrying only the
    #: experimental ones and somebody narrowed it.
    evidence: dict[str, dict[str, int]]
    distinct_terms: int
    negated: int
    collapse: Collapse
    discards: Discards


def state_of(
    session: Session, facts: WindowFacts, population: list[str], axis: SideFrame
) -> tuple[pa.Table, WindowState]:
    """One window's state at both moments, as a table ready to be one row group.

    ``axis`` is the frame of the window's PIVOT, not of either end: the
    ``go_id`` column has to name a term of the declared axis or the loader
    cannot join it to ``terms``. Its liveness and its synonym map travel
    together in one :class:`SideFrame` for the reason that class exists, and
    its live set comes from the same function ``axis_of`` used to write
    ``terms``, which is what makes the join total.
    """
    cartas: set[_Row] = set()
    por_momento: dict[str, int] = {}
    evidencia: dict[str, dict[str, int]] = {}
    gaf = remapeo = fuera = cruzado = sin_evidencia = 0
    ends = (
        (MOMENTS[0], facts.eset.old_annotation_set_id),
        (MOMENTS[1], facts.eset.new_annotation_set_id),
    )
    for momento, aset in ends:
        crudas = list(session.execute(_STATE, {"aset": aset, "population": population}))
        con_evidencia = [f for f in crudas if f[2]]
        sin_evidencia += len(crudas) - len(con_evidencia)
        antes = {(acc, momento, go, ev, qu) for acc, go, ev, qu in con_evidencia}
        gaf += len(con_evidencia) - len(antes)
        puestas, vivas, d = _onto_axis(antes, axis)
        remapeo += vivas - len(puestas)
        fuera += d.off_axis
        cruzado += d.crossed_aspect
        por_momento[momento] = len(puestas)
        evidencia[momento] = dict(Counter(f[3] for f in puestas))
        cartas |= puestas
    cuentas = (
        Collapse(gaf=gaf, remap=remapeo),
        Discards(off_axis=fuera, crossed_aspect=cruzado, no_evidence=sin_evidencia),
    )
    return _as_table(facts, sorted(cartas), por_momento, evidencia, cuentas)


def _onto_axis(antes: set[_Row], axis: SideFrame) -> tuple[set[_Row], int, Discards]:
    """Rewrite each card's term into the axis, BEFORE anything is collapsed.

    Returns the rewritten cards, how many of the input cards survived, and what
    was dropped and why. The survivor count is returned rather than derived
    from the two set sizes because the difference between them is the remap
    collapse, and a caller that computed it from the discards would be mixing
    the two facts :class:`Collapse` exists to keep apart.
    """
    puestas: set[_Row] = set()
    vivas = fuera = cruzado = 0
    for acc, momento, go_id, evidence, qualifier in antes:
        if go_id in axis.live:
            puestas.add((acc, momento, go_id, evidence, qualifier))
            vivas += 1
            continue
        mapeado = axis.remap.by_alias.get(go_id)
        if mapeado is None or mapeado[0] not in axis.live:
            fuera += 1
            continue
        puestas.add((acc, momento, mapeado[0], evidence, qualifier))
        vivas += 1
        if go_id in axis.remap.crossed_aspect:
            cruzado += 1
    return puestas, vivas, Discards(off_axis=fuera, crossed_aspect=cruzado, no_evidence=0)


def _as_table(
    facts: WindowFacts,
    filas: list[_Row],
    por_momento: dict[str, int],
    evidencia: dict[str, dict[str, int]],
    cuentas: tuple[Collapse, Discards],
) -> tuple[pa.Table, WindowState]:
    """The six parallel columns, built directly from the sorted cards."""
    tabla = pa.table(
        {
            "window": [facts.window] * len(filas),
            "query": [f[0] for f in filas],
            "moment": [f[1] for f in filas],
            "go_id": [f[2] for f in filas],
            "evidence_code": [f[3] for f in filas],
            "qualifier": [f[4] for f in filas],
        },
        schema=SCHEMA,
    )
    colapso, descartes = cuentas
    return tabla, WindowState(
        window=facts.window,
        rows=len(filas),
        by_moment=por_momento,
        evidence=evidencia,
        distinct_terms=len({f[2] for f in filas}),
        # The qualifier is a RELATION and the regime changes at 202, so a
        # negation is "contains NOT" rather than an equality.
        negated=sum(1 for f in filas if "NOT" in f[4]),
        collapse=colapso,
        discards=descartes,
    )


class Writer:
    """A parquet opened once, one row group per window, closed at the end.

    A class rather than a function because the writer has to outlive the loop
    over windows: at 44,7 million rows the alternative is holding every column
    in memory until the end, and the largest single window is 3,07 million.
    """

    def __init__(self, path: str) -> None:
        self._writer = pq.ParquetWriter(path, SCHEMA)
        self.rows = 0
        self.windows = 0

    def write(self, tabla: pa.Table) -> None:
        self._writer.write_table(tabla)
        self.rows += tabla.num_rows
        self.windows += 1

    def close(self) -> int:
        self._writer.close()
        return self.rows
