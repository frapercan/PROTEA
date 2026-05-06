"""migrate sequence_embedding.embedding from vector to halfvec

Revision ID: b1a1f4ec0e42
Revises: f7a004f5f2c7
Create Date: 2026-04-14 22:00:00.000000
"""
from __future__ import annotations

from alembic import op

revision: str = "b1a1f4ec0e42"
down_revision: str = "f7a004f5f2c7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF (
                SELECT udt_name
                FROM information_schema.columns
                WHERE table_name = 'sequence_embedding'
                  AND column_name = 'embedding'
            ) = 'vector' THEN
                ALTER TABLE sequence_embedding
                ALTER COLUMN embedding TYPE halfvec
                USING embedding::halfvec;
            END IF;
        END $$;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF (
                SELECT udt_name
                FROM information_schema.columns
                WHERE table_name = 'sequence_embedding'
                  AND column_name = 'embedding'
            ) = 'halfvec' THEN
                ALTER TABLE sequence_embedding
                ALTER COLUMN embedding TYPE vector
                USING embedding::vector;
            END IF;
        END $$;
        """
    )
