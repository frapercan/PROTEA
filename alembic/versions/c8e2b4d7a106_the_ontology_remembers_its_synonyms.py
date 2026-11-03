"""The ontology remembers the ids it no longer uses as primary

``go_term`` holds ``go_id``, ``name``, ``aspect``, ``definition`` and
``is_obsolete``, and nothing about identity across releases. The OBO files
carry two statements this table has been dropping on the floor since the
first load: ``alt_id`` (a secondary id of a live term) and ``replaced_by``
(the successor an obsolete term names), plus ``consider``, which is the
ontology declining to name one.

WHAT IT COSTS TO NOT HAVE THEM, measured on this store:

* a temporal window cannot ask "does this t1 term have a node at t0", because
  the answer can be yes through a successor it cannot see. On the 211 -> 227
  validation window that is nine entries and three proteins of 40,531
  (``A2AF31``, ``P62962``, ``Q8BPT6``), each one a term that was obsolete in
  2022 with a successor and that GO REVIVED in 2025;
* a series over releases reads every merge as a retraction plus a gain. 1,657
  of 2016's terms are secondary ids by 2026, all with a known successor, and
  the churn figures of the whole GOA series carry that inflation.

A SIDE TABLE, NOT TWO COLUMNS, and not a reload. ``alt_id`` is one-to-many
(3,590 over the 47,328 terms of 2022-07-01), so a scalar column cannot hold
it and an array column would put a list where a join belongs. ``go_term`` is
3.1 million rows over 66 snapshots, 1,158 MB, and it is not touched: this is
about 4,100 rows per snapshot, around 250,000 in all, filled by an operation
that re-reads each archived OBO in 0.2 seconds.

Revision ID: c8e2b4d7a106
Revises: b7c4e1a9f280
Create Date: 2026-10-27 14:30:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c8e2b4d7a106"
down_revision: str | Sequence[str] | None = "b7c4e1a9f280"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "go_term_alias",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("ontology_snapshot_id", sa.UUID(as_uuid=True), nullable=False),
        sa.Column("alias_go_id", sa.String(length=15), nullable=False),
        sa.Column("primary_go_id", sa.String(length=15), nullable=False),
        sa.Column("kind", sa.String(length=16), nullable=False),
        sa.ForeignKeyConstraint(
            ["ontology_snapshot_id"], ["ontology_snapshot.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "ontology_snapshot_id",
            "alias_go_id",
            "primary_go_id",
            "kind",
            name="uq_go_term_alias_identity",
        ),
    )
    op.create_index(
        "ix_go_term_alias_lookup",
        "go_term_alias",
        ["ontology_snapshot_id", "alias_go_id"],
    )
    op.create_check_constraint(
        "ck_go_term_alias_kind",
        "go_term_alias",
        "kind IN ('alt_id', 'replaced_by', 'consider')",
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_go_term_alias_lookup", table_name="go_term_alias")
    op.drop_table("go_term_alias")
