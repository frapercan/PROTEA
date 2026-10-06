"""protein records the release that admitted it

Revision ID: f2a8c41d9e37
Revises: e4b7a19c0f83
Create Date: 2026-10-06 13:40:00.000000

Adds ``first_admitted_release`` to ``protein``: the lowest GOA release number
whose GAF admitted this accession into the universe under the declared criterion.
Written by ``extract_goa_universe``, which is the only code that observes it.

Why this is NOT the column the previous migration refused
---------------------------------------------------------

``e4b7a19c0f83``, one day earlier, declined to store ``first_release`` and gave
two reasons. Both are about a DIFFERENT quantity, and the name is changed here so
the two cannot be confused.

Its ``first_release`` meant *the earliest release that existed once this entry
existed* -- "the earliest release whose publication date is at or after
``date_created``". That is ENTRY CREATION, it is derivable from ``date_created``
plus ``annotation_set.source_published_at``, and storing it would indeed have been
a second source of truth for something two columns already determine. That
argument stands and this migration does not touch it.

``first_admitted_release`` means *the earliest release at which this protein met
the admission criterion*: the release whose GAF first carried a reliable evidence
code for it, or first named it as a reviewed entry. That is KNOWLEDGE GAIN, which
is the event the campaign measures. A protein can have existed in UniProt since
1998 and have acquired its first experimental annotation in 2019; ``date_created``
says 1998 and cannot say 2019.

The second reason was that the pass order made the number meaningless: phase 1
ran DESCENDING (235 -> 156), so "the release whose pass admitted this protein"
would have read 235 for nearly every row. The order was reversed on 2026-10-06
precisely so the series is walked from its first release upward, which is what
makes this observable at all. The write is a minimum rather than a first-writer
-- ``WHERE first_admitted_release IS NULL OR first_admitted_release > N`` -- so
the column holds the true minimum even if the releases are ever processed out of
order, and a resumed or repeated pass cannot raise it.

Why it cannot be recovered later
--------------------------------

For a protein admitted on an evidence code, the value is in principle derivable
after phase 2, as a minimum over ``annotation_set.source_version`` for its rows
carrying one of the admitted codes -- expensive over a table of this size, but
possible.

For a protein admitted ONLY as a reviewed entry of its release (tier
``swissprot_of_release``: no reliable annotation at all, which is 527.149 of the
accessions on GOA 156) it is derivable from nothing in this database. Swiss-Prot
membership per release is read from the entry name the GAF itself carries, and no
table stores it. Recovering that one column would mean reading the 802 GB series
again. It costs nothing to write now and cannot be written later, which is the
whole argument for doing it in the same pass.

Downgrade drops the column and its index.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "f2a8c41d9e37"
down_revision: str | Sequence[str] | None = "e4b7a19c0f83"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "protein",
        sa.Column("first_admitted_release", sa.Integer(), nullable=True),
    )
    # Indexed because the window construction filters on it: "the proteins the
    # corpus already knew at release N" is the population every temporal split
    # has to name, and it is a range scan over this one column.
    op.create_index(
        "ix_protein_first_admitted_release",
        "protein",
        ["first_admitted_release"],
    )


def downgrade() -> None:
    op.drop_index("ix_protein_first_admitted_release", table_name="protein")
    op.drop_column("protein", "first_admitted_release")
