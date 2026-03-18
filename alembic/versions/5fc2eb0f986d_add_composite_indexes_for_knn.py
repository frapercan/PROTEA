"""add composite indexes for KNN performance

Revision ID: 5fc2eb0f986d
Revises: 54e758c210c8
Create Date: 2026-03-18 12:00:00.000000

"""
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "5fc2eb0f986d"
down_revision: str = "54e758c210c8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Composite index for KNN GO transfer: queries are always scoped to
    # a single annotation_set_id and filtered by protein_accession.
    op.create_index(
        "ix_pga_set_accession",
        "protein_go_annotation",
        ["annotation_set_id", "protein_accession"],
    )

    # Composite index for prediction export and evaluation: queries filter
    # by prediction_set_id then protein_accession.
    op.create_index(
        "ix_go_prediction_set_accession",
        "go_prediction",
        ["prediction_set_id", "protein_accession"],
    )


def downgrade() -> None:
    op.drop_index("ix_go_prediction_set_accession", table_name="go_prediction")
    op.drop_index("ix_pga_set_accession", table_name="protein_go_annotation")
