"""a result carries the frame it was scored under

Revision ID: c4e88a1b6d30
Revises: a1f7c39b2e04

``evaluation_result.frame`` is ``varchar(8)`` under a check constraint admitting
exactly ``lafa`` or ``internal``. That is a harness label, and the census
operation says in its own docstring why a label is not enough: it marks which
harness, not which parameters, so two rows both reading ``lafa`` can still be
incomparable. Two results differing only in which accretion table weighted their
terms move by up to 0.0185 of weighted micro F, which is more than the distance
cap moves them and as much as layer depth.

The label is not wrong, it is insufficient, so this adds a column beside it
rather than redefining it. ``frame_digest`` holds a content address of the
fields that have to match for two results to be comparable: the window, the
ontology pivot, the accretion set and the evaluation caps. Equal digests mean
comparable numbers, and a reader needs to know nothing about which fields the
frame is made of to compare two of them.

Nullable, because the column has to be addable to a table that already holds
rows nobody can attribute. A row whose producing job is gone cannot recover the
accretion set that weighted it, and a default would claim an attribution that
does not exist. What the column promises is that a non-null value is a real
frame, not that every row has one.
"""

import sqlalchemy as sa

from alembic import op

revision = "c4e88a1b6d30"
down_revision = "a1f7c39b2e04"
branch_labels = None
depends_on = None

#: Width of a truncated sha256, plus room for a prefix a later seal may add.
#: Sized once here rather than left to whatever the first writer happens to
#: emit, which is how the existing column came to be eight characters wide.
_DIGEST_WIDTH = 32


def upgrade() -> None:
    op.add_column(
        "evaluation_result",
        sa.Column("frame_digest", sa.String(length=_DIGEST_WIDTH), nullable=True),
    )
    # Partial: only sealed rows are worth indexing, and the unsealed ones are
    # the majority of any table this lands on. Queries ask which results share a
    # frame; none of them asks for the null ones by digest.
    op.create_index(
        "ix_evaluation_result_frame_digest",
        "evaluation_result",
        ["frame_digest"],
        unique=False,
        postgresql_where=sa.text("frame_digest IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_evaluation_result_frame_digest", table_name="evaluation_result")
    op.drop_column("evaluation_result", "frame_digest")
