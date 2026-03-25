"""drop_go_prediction_protein_fk

Revision ID: e5f6a7b8c9d0
Revises: d4e5f6a7b8c9
Create Date: 2026-03-10 00:00:00.000000

"""
from collections.abc import Sequence

from alembic import op

revision: str = "e5f6a7b8c9d0"
down_revision: str | Sequence[str] | None = "d4e5f6a7b8c9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_constraint(
        "go_prediction_protein_accession_fkey",
        "go_prediction",
        type_="foreignkey",
    )


def downgrade() -> None:
    op.create_foreign_key(
        "go_prediction_protein_accession_fkey",
        "go_prediction",
        "protein",
        ["protein_accession"],
        ["accession"],
        ondelete="CASCADE",
    )
