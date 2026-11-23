"""A prediction set is summarised once

``prediction_set.summary`` holds what a set's pages read of it (its rows, its
proteins, its totals per aspect, its most frequent terms) and
``prediction_set_protein`` each protein's terms and closest distance. A set
does not change after it is written; its pages counted its whole set of rows on
every visit, up to 20 s on a set of 1.8 million candidate pairs. Both start
empty and are filled on a set's first read or by ``summarize_prediction_sets``.

Revision ID: a5e954c5bab4
Revises: e0399c98f515
Create Date: 2026-11-21 19:30:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a5e954c5bab4"
down_revision: str | Sequence[str] | None = "e0399c98f515"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "prediction_set",
        sa.Column("summary", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.create_table(
        "prediction_set_protein",
        sa.Column("prediction_set_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("protein_accession", sa.String(), nullable=False),
        sa.Column("go_count", sa.Integer(), nullable=False),
        sa.Column("min_distance", sa.Float(), nullable=True),
        sa.ForeignKeyConstraint(
            ["prediction_set_id"],
            ["prediction_set.id"],
            name="fk_prediction_set_protein_set",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("prediction_set_id", "protein_accession"),
    )


def downgrade() -> None:
    op.drop_table("prediction_set_protein")
    op.drop_column("prediction_set", "summary")
