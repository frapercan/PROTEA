"""add display metadata columns to embedding_config

Revision ID: b2c3d4e5f6a7
Revises: 3505bfa74df6
Create Date: 2026-04-10

Adds three nullable columns to ``embedding_config`` so the benchmark UI can
show a human-readable label, a family tag, and the approximate parameter
count without having to infer everything from the raw HuggingFace
``model_name`` at render time.

All columns are nullable — existing rows can be backfilled later with
``UPDATE embedding_config SET display_name = ..., family = ..., param_count = ...``
or left as NULL (the router falls back to the Python-side derivation).
"""
from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'b2c3d4e5f6a7'
down_revision: str | Sequence[str] | None = '3505bfa74df6'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('embedding_config', sa.Column('display_name', sa.String(), nullable=True))
    op.add_column('embedding_config', sa.Column('family', sa.String(), nullable=True))
    op.add_column('embedding_config', sa.Column('param_count', sa.BigInteger(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('embedding_config', 'param_count')
    op.drop_column('embedding_config', 'family')
    op.drop_column('embedding_config', 'display_name')
