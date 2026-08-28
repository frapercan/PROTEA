"""Depth is counted in sequences, not in proteins

The corpus holds 616,846 proteins over 528,294 distinct sequences. 38,694 of
those sequences belong to more than one protein and one of them belongs to 114.
Two proteins with the same sequence have the same embedding: they are one point
of the space, and a neighbourhood counted in protein ranks can therefore spend
many of its slots looking at that one point again.

NULLABLE, AND NULL IS NOT ZERO. Every row retrieved before this column existed
gets null, which says the retrieval predates the question. Zero would say the
term was reachable at sequence rank zero, which is not a thing, and one would
say it came from the nearest sequence, which nobody measured. The producer that
fills it lands with this migration rather than after it, because a NOT NULL
column whose writer does not exist is how the last schema-first change broke
every insert into this table.

WHY IT IS STORED AND THE VOTE AGGREGATES ARE NOT. This rank is fixed by the
retrieval: truncating the neighbourhood later does not renumber the rows that
survive. ``vote_count`` and the ``neighbor_*`` columns are the opposite, they are
functions of the cut, and storing them is why a depth sweep run on 2026-08-27
scored a ten-neighbour consensus over a two-neighbour candidate set. Those move
to being computed at scoring time; this one belongs on the row.

NOT DEDUPLICATED. Among shared sequences that carry annotations, 18.7 per cent
have proteins with different term sets. A shared sequence is one point of the
space and several donors of annotation, so the rank counts points while the
donor accession keeps counting sources.

Revision ID: e3c8b7a1f592
Revises: d7f21a9c4e08
"""

import sqlalchemy as sa

from alembic import op

revision = "e3c8b7a1f592"
down_revision = "d7f21a9c4e08"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "go_prediction",
        sa.Column("sequence_rank", sa.Integer(), nullable=True),
    )
    op.create_index(
        "ix_go_prediction_sequence_rank",
        "go_prediction",
        ["prediction_set_id", "sequence_rank"],
        postgresql_where=sa.text("sequence_rank IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_go_prediction_sequence_rank", table_name="go_prediction")
    op.drop_column("go_prediction", "sequence_rank")
