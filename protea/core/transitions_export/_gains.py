"""The truth of a window: which terms each query gained, with its stratum.

THIS IS THE TABLE THAT CARRIES THE TRUTH, so the two rules that decide what a
gain IS are read from where the evaluation already applies them rather than
restated: the three scored maps come out of the window's own verified parquet,
already trimmed by the terms of interest, and the aspect comes from the same
axis the evaluation classified under.

THE POPULATION IS THE ONE ``queries`` HAS, and that is a guard rather than a
filter. ``queries`` excludes a protein with no stored sequence, because the
exported FASTA excludes it and the training rows have to be about the scored
population. A gain for a protein outside that set would be a gain for a query
the loader does not know, and the two tables would disagree about who the
queries are without either being wrong on its own. Measured 2026-11-08 over the
65 admissible windows: **zero** delta proteins lack a sequence, so this guard
changes nothing today and says so when that stops being true.

THE SHAPE, measured before the code was written. Over those 65 windows
``gains`` is **9.596.563 rows**, the largest window 691.553 and the mean
147.639. Six short columns at that count is the one place where building a
tuple per row would hurt: 9,6 million objects holding 58 million pointers. So
the columns are built as six parallel lists directly, which is why this module
looks like it does.
"""

from __future__ import annotations

from typing import NamedTuple

import pyarrow as pa
import pyarrow.parquet as pq

from protea.core.transitions_export._window_facts import WindowFacts

#: The six columns the laboratory's loader reads.
SCHEMA = pa.schema(
    [
        ("window", pa.string()),
        ("query", pa.string()),
        ("aspect", pa.string()),
        ("go_id", pa.string()),
        ("closure", pa.string()),
        ("stratum", pa.string()),
    ]
)

#: The three scored strata, in the order the evaluation names them. ``removed``
#: is not here: those proteins only LOST annotation, which the evaluation
#: reports and never scores.
STRATA: tuple[str, ...] = ("nk", "lk", "pk")


class WindowGains(NamedTuple):
    """One window's gains, with what the guards found."""

    window: str
    closure: str
    rows: int
    #: Per stratum, so a bundle whose strata shifted is visible from the event
    #: rather than from a diff of two parquets.
    by_stratum: dict[str, int]
    distinct_terms: int


def closure_of(facts: WindowFacts) -> str:
    """Which graphs this window's two ends close in, DERIVED, or a refusal.

    IT IS NOT ``truth_profile``, AND THE FIRST VERSION OF THIS FUNCTION MADE
    EXACTLY THAT MISTAKE. It mapped ``projected`` to ``pivot`` and
    ``as_closed`` to ``native``, which ADR-D56 says in as many words is wrong:
    ``truth_profile`` says HOW the truth is derived and ``closure`` says WHICH
    GRAPHS the two ends close in, and they vary independently.

    The proof is in the database. The (A) and (B) twins of ``160 -> 177`` are
    BOTH ``projected`` and differ only in their natives, so under the mapping
    the (B) one, ``e84dfec8``, would have been written as ``pivot``. The
    desktop session caught it by reading the ADR that this project's own
    earlier PR had written, and it had made the same mistake a day before and
    withdrawn it.

    So the value comes from comparing the window's two natives against its
    pivot and against the ontology each end's corpus was loaded under, the way
    the window inventory already derives it, and a combination that is neither
    is refused rather than guessed. Measured over the 131 evaluation sets:
    122 are ``native`` and 9 are ``pivot``.
    """
    eset = facts.eset
    pivote = eset.pivot_snapshot_id
    viejo, nuevo = eset.old_native_snapshot_id, eset.new_native_snapshot_id
    if viejo == pivote and nuevo == pivote:
        return "pivot"
    if viejo == facts.old_end_snapshot and nuevo == facts.new_end_snapshot:
        return "native"
    raise ValueError(
        f"window {facts.window} has natives {str(viejo)[:8]} and {str(nuevo)[:8]} "
        f"against pivot {str(pivote)[:8]} and corpus ontologies "
        f"{str(facts.old_end_snapshot)[:8]} and {str(facts.new_end_snapshot)[:8]}: "
        "neither both-on-the-pivot nor each-on-its-own-era. The closure column says "
        "which graphs the two ends close in, and a third combination has no name yet, "
        "so it is refused rather than written as one of the two"
    )


def gains_of(
    facts: WindowFacts, aspect: dict[str, str], population: set[str], columns: dict[str, list]
) -> WindowGains:
    """Append one window's gains to the shared columns, and say what it added.

    It appends rather than returning rows: at 9,6 million rows over 65 windows,
    a list of tuples per window and a concatenation afterwards is the one
    avoidable cost in this table.
    """
    cierre = closure_of(facts)
    por_estrato: dict[str, int] = {}
    terminos: set[str] = set()
    fuera: set[str] = set()
    sin_aspecto: set[str] = set()
    for estrato in STRATA:
        n = 0
        for query, gos in sorted((getattr(facts.data, estrato, {}) or {}).items()):
            if query not in population:
                fuera.add(query)
                continue
            for go in sorted(gos):
                if go not in aspect:
                    sin_aspecto.add(go)
                    continue
                columns["window"].append(facts.window)
                columns["query"].append(query)
                columns["aspect"].append(aspect[go])
                columns["go_id"].append(go)
                columns["closure"].append(cierre)
                columns["stratum"].append(estrato)
                terminos.add(go)
                n += 1
        por_estrato[estrato] = n
    _refuse_gaps(facts, fuera, sin_aspecto)
    return WindowGains(
        window=facts.window,
        closure=cierre,
        rows=sum(por_estrato.values()),
        by_stratum=por_estrato,
        distinct_terms=len(terminos),
    )


def _refuse_gaps(facts: WindowFacts, fuera: set[str], sin_aspecto: set[str]) -> None:
    """A gain outside the query population, or on a term the axis cannot name.

    BOTH ARE GUARDS AND NEITHER FILTERS ANYTHING TODAY, measured. They refuse
    rather than drop because dropping would make ``gains`` quietly smaller than
    the truth it is a copy of, and a row count is the only thing a reader could
    compare it against.
    """
    if fuera:
        raise ValueError(
            f"window {facts.window}: {len(fuera)} proteins have gains and are not in "
            f"the query population: {sorted(fuera)[:5]}. queries excludes a protein "
            "with no stored sequence because the scored FASTA does, so a gain for one "
            "would be a gain for a query the loader does not know"
        )
    if sin_aspecto:
        raise ValueError(
            f"window {facts.window}: {len(sin_aspecto)} gained terms are not on the "
            f"declared axis: {sorted(sin_aspecto)[:5]}. The loader groups by aspect, "
            "so a term the axis cannot name is a row it cannot place"
        )


def new_columns() -> dict[str, list]:
    """The six parallel lists, empty, in the schema's order."""
    return {name: [] for name in SCHEMA.names}


def write_table(columns: dict[str, list], path: str) -> int:
    """Write the accumulated columns, already in a stable order.

    The order is stable by construction and not by a final sort: the caller
    walks the windows sorted, and within a window the strata in their declared
    order, each query sorted and each term sorted. Sorting 9,6 million rows
    again at the end would cost more than the property is worth when the
    property is already there.
    """
    pq.write_table(pa.table(columns, schema=SCHEMA), path)
    return len(columns["window"])
