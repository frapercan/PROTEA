"""add taxonomy_id and species to query_set_entry

Revision ID: c4d5e6f7a8b9
Revises: b2c3d4e5f6a7
Create Date: 2026-04-11

Adds two nullable columns to ``query_set_entry`` so user-uploaded FASTA
sequences can carry their UniProt header taxonomy (``OX=`` / ``OS=``) even
when the accession is not present in the ``protein`` table and therefore
has no ``ProteinUniProtMetadata`` counterpart.

The populating helper lives in ``protea.api.routers.query_sets`` and is a
silent no-op for non-UniProt headers.
"""
from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'c4d5e6f7a8b9'
down_revision: str | Sequence[str] | None = 'b2c3d4e5f6a7'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('query_set_entry', sa.Column('taxonomy_id', sa.Integer(), nullable=True))
    op.add_column('query_set_entry', sa.Column('species', sa.String(), nullable=True))
    op.create_index(
        'ix_query_set_entry_taxonomy_id',
        'query_set_entry',
        ['taxonomy_id'],
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index('ix_query_set_entry_taxonomy_id', table_name='query_set_entry')
    op.drop_column('query_set_entry', 'species')
    op.drop_column('query_set_entry', 'taxonomy_id')
