"""protein carries its UniProt dates

Revision ID: e4b7a19c0f83
Revises: d1fc5b3c53f7
Create Date: 2026-10-05 18:10:00.000000

Adds ``date_created``, ``date_sequence_modified`` and ``sequence_version`` to
``protein``. All three come from UniProt in the same batch request that already
fetches the sequence, so they cost no extra traffic.

Why they are worth a migration
------------------------------

``date_created`` answers "did this protein exist at release N", which separates
KNOWLEDGE GAIN from ENTRY CREATION. Without it, a protein first seen in UniProt in
2022 sits in the universe of a 2016 release with no annotations, and a delta reads
that as NK -> gained. It is not: the entry did not exist. That is a defect in the
training signal, not in the corpus size.

``date_sequence_modified`` turns the sequence leak into a NAMED SUBSET. The
sequence we embed comes from today's UniProt. If the sequence was last modified at
or before the window's t0, today's sequence IS the sequence of then and there is no
leak for that protein. If it is later, that protein is flagged and can be excluded
from the window or reported as a sensitivity check. Measured against Swiss-Prot
2024_02: 350 of 73.863 differ, 0,47% -- small, and now enumerable one by one
instead of being an unknown.

``sequence_version`` is the same signal UniProt uses internally and is free in the
same response: 1 means the sequence never changed in the entry's life, so no window
can leak through it. Measured over 109.320 canonical accessions: 71,5% are at
version 1.

What is deliberately NOT stored
-------------------------------

``first_release``. It is derivable -- the earliest release whose publication date is
at or after ``date_created`` -- and the calendar to derive it from is already in this
database: ``annotation_set.source_version`` with ``source_published_at``. A stored
copy would be a second source of truth for a value two existing columns already
determine.

What it must NOT be derived from is the order the passes run in. Phase 1 runs
descending (235 -> 156), so "the release whose pass admitted this protein" would be
235 for nearly every row, which is the opposite of what the column would be read as
meaning.

Downgrade drops the three columns.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "e4b7a19c0f83"
down_revision: str | Sequence[str] | None = "d1fc5b3c53f7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("protein", sa.Column("date_created", sa.Date(), nullable=True))
    op.add_column("protein", sa.Column("date_sequence_modified", sa.Date(), nullable=True))
    op.add_column("protein", sa.Column("sequence_version", sa.Integer(), nullable=True))
    # date_created se consulta por rango para "existia en la release N", y
    # date_sequence_modified para marcar las posteriores a t0.
    op.create_index("ix_protein_date_created", "protein", ["date_created"])
    op.create_index(
        "ix_protein_date_sequence_modified", "protein", ["date_sequence_modified"]
    )


def downgrade() -> None:
    op.drop_index("ix_protein_date_sequence_modified", table_name="protein")
    op.drop_index("ix_protein_date_created", table_name="protein")
    op.drop_column("protein", "sequence_version")
    op.drop_column("protein", "date_sequence_modified")
    op.drop_column("protein", "date_created")
