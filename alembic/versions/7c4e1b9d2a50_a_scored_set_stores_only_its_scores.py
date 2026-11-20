"""A scored set stores only its scores

A scored set held its scores as ``go_prediction`` rows, about 593 bytes each on
the development store, to say three things per (protein, term): the protein, the
term and the score. ``prediction_score`` holds one row per protein of a scored
set with its terms and their scores as two aligned arrays, 20.2 bytes per pair
as measured on the same store, and the candidates and their evidence stay in
the candidate set.

The downgrade refuses while a scored set holds scores here: dropping the table
would leave sets whose record says they are scored and that hold nothing.

Revision ID: 7c4e1b9d2a50
Revises: 2b7e4d9a6c31
Create Date: 2026-11-20 21:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "7c4e1b9d2a50"
down_revision: str | Sequence[str] | None = "2b7e4d9a6c31"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: The model's checks, by the same text; a test compares them.
ALIGNED_CHECK = "cardinality(go_term_ids) = cardinality(scores)"
NOT_EMPTY_CHECK = "cardinality(go_term_ids) > 0"


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "prediction_score",
        sa.Column("prediction_set_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("protein_accession", sa.String(), nullable=False),
        sa.Column("go_term_ids", postgresql.ARRAY(sa.BigInteger()), nullable=False),
        sa.Column("scores", postgresql.ARRAY(postgresql.DOUBLE_PRECISION()), nullable=False),
        sa.ForeignKeyConstraint(
            ["prediction_set_id"],
            ["prediction_set.id"],
            name="fk_prediction_score_prediction_set_id",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("prediction_set_id", "protein_accession"),
        sa.CheckConstraint(ALIGNED_CHECK, name="ck_prediction_score_aligned"),
        sa.CheckConstraint(NOT_EMPTY_CHECK, name="ck_prediction_score_not_empty"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    rows = op.get_bind().execute(sa.text("select count(*) from prediction_score")).scalar_one()
    if rows:
        raise RuntimeError(
            f"prediction_score holds {rows} rows of scored sets; dropping it would leave "
            "sets whose record says they are scored and that hold nothing. Delete those "
            "sets first."
        )
    op.drop_table("prediction_score")
