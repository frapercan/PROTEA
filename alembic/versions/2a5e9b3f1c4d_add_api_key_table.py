"""add api_key table

Revision ID: 2a5e9b3f1c4d
Revises: 1e4aee32e677
Create Date: 2026-05-11 12:00:00.000000

T5.6a — first iteration of authn. Adds the ``api_key`` table that backs
the ``require_api_key`` FastAPI dependency. The raw key is never stored;
only its sha256 hash (``key_hash``) and the 8-char display prefix
(``prefix``). The unique constraint on ``key_hash`` doubles as a
defence against accidental duplicate inserts.
"""
from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2a5e9b3f1c4d"
down_revision: str | Sequence[str] | None = "1e4aee32e677"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "api_key",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("key_hash", sa.String(length=64), nullable=False),
        sa.Column("prefix", sa.String(length=8), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("key_hash"),
    )
    op.create_index(op.f("ix_api_key_prefix"), "api_key", ["prefix"], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f("ix_api_key_prefix"), table_name="api_key")
    op.drop_table("api_key")
