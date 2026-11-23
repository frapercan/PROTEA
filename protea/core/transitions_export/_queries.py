"""The proteins a window asks about, with the release and span they sit in.

THE POPULATION IS NOT DEFINED HERE. It is ``delta_accessions``, imported from
``export_evaluation_targets``, which is the rule that already decides which
proteins the FASTA the grid and LAFA both score contains: the union of the
no-knowledge, limited-knowledge and prior-knowledge maps, the proteins that
GAINED annotation. Writing that union again here would make the training set's
population a second rule that agrees with the first until it does not.

AND A PROTEIN WITH NO STORED SEQUENCE IS EXCLUDED, for the same reason and not
for convenience: the exported FASTA excludes it, so including it here would
make the training rows about a population the scored file does not contain. The
count is emitted per window, so the exclusion is visible rather than inferred
from a row count that does not add up.

``version_t`` and ``horizon_days`` come from the window's facts, read once, and
a window missing either is refused there rather than written as NULL.
"""

from __future__ import annotations

from typing import NamedTuple

import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core.transitions_export._window_facts import WindowFacts
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.sequence.sequence import Sequence

#: The six columns the laboratory's loader reads, in its order.
SCHEMA = pa.schema(
    [
        ("window", pa.string()),
        ("query", pa.string()),
        ("sequence_id", pa.int64()),
        ("length", pa.int32()),
        ("version_t", pa.string()),
        ("horizon_days", pa.int32()),
    ]
)


class QueryRow(NamedTuple):
    """One protein a window asks about."""

    window: str
    query: str
    sequence_id: int
    length: int
    version_t: str
    horizon_days: int


class WindowQueries(NamedTuple):
    """One window's rows, with what was left out."""

    window: str
    rows: list[QueryRow]
    asked: int
    without_sequence: int


def _sequences(session: Session, accessions: list[str]) -> dict[str, tuple[int, int]]:
    """Accession to ``(sequence_id, length)``, for those that have one.

    The id comes from the protein's own row and the length from that sequence,
    so neither is derived from the published bytes: the FASTA is canonicalised
    (upper case, gaps removed) and its length can differ from the stored one.
    """
    rows = session.execute(
        select(Protein.accession, Protein.sequence_id, Sequence.sequence)
        .join(Sequence, Sequence.id == Protein.sequence_id)
        .where(Protein.accession.in_(accessions))
    ).all()
    return {accession: (sequence_id, len(seq)) for accession, sequence_id, seq in rows}


def queries_of(session: Session, facts: WindowFacts, categories: list[str]) -> WindowQueries:
    """The window's delta proteins as rows, and how many had no sequence."""
    from protea.core.operations.export_evaluation_targets import delta_accessions

    accessions = delta_accessions(facts.data, categories)
    if not accessions:
        raise ValueError(
            f"window {facts.window} has no delta proteins in {categories}, so it would "
            "contribute no queries and the bundle would carry a window nothing asks about"
        )
    conocidas = _sequences(session, accessions)
    rows = [
        QueryRow(
            window=facts.window,
            query=a,
            sequence_id=conocidas[a][0],
            length=conocidas[a][1],
            version_t=facts.version_t,
            horizon_days=facts.horizon_days,
        )
        for a in accessions
        if a in conocidas
    ]
    return WindowQueries(
        window=facts.window,
        rows=rows,
        asked=len(accessions),
        without_sequence=len(accessions) - len(rows),
    )


def write_table(results: list[WindowQueries], path: str) -> int:
    """Write the six columns in a stable order, and say how many rows.

    Sorted by window and then by accession, for the reason the sibling table is
    sorted: the sha of this file is what a consumer quotes, and a digest over
    whatever order a query plan produced moves without the data moving.
    """
    columnas: dict[str, list] = {name: [] for name in SCHEMA.names}
    for r in sorted(results, key=lambda r: r.window):
        for row in sorted(r.rows, key=lambda row: row.query):
            for name, value in zip(SCHEMA.names, row, strict=True):
                columnas[name].append(value)
    pq.write_table(pa.table(columnas, schema=SCHEMA), path)
    return len(columnas["window"])
