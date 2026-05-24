"""FEAT-AUTH: add ``role`` column to ``api_key``

Revision ID: c4e8a1b9d2f6
Revises: d8e2f4b6a3c1
Create Date: 2026-05-23 19:00:00.000000

WAVE-2 FEAT-AUTH-ROLES. Adds a single nullable ``role`` column to the
``api_key`` table so the JWT minted by ``POST /auth/login`` can carry
a role claim. NULL is interpreted as ``viewer`` at decode time so
keys minted before this migration keep working without a DML update.

The column is small (``String(16)``) and unindexed: role is read at
login time only (one row lookup by hash) and is never used as a WHERE
filter on its own.
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c4e8a1b9d2f6"
down_revision: str | Sequence[str] | None = "d8e2f4b6a3c1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "api_key",
        sa.Column("role", sa.String(length=16), nullable=True),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("api_key", "role")
