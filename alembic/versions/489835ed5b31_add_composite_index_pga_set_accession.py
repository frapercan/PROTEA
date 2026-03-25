"""add_composite_index_pga_set_accession

Revision ID: 489835ed5b31
Revises: 7737a352d4fe
Create Date: 2026-03-15 11:17:30.865922

"""
from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = '489835ed5b31'
down_revision: str | Sequence[str] | None = '7737a352d4fe'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_index(
        "ix_pga_set_accession",
        "protein_go_annotation",
        ["annotation_set_id", "protein_accession"],
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_pga_set_accession", table_name="protein_go_annotation")
