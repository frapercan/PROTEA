"""F-EVAL-PROTOCOL .a: add window_role to evaluation_set

Adds the rolling-origin protocol window marker (ADR D40) to
``evaluation_set``. The column is nullable (``"valid"`` | ``"test"`` |
``NULL``) and guarded by a CHECK constraint so the vocabulary stays
closed without a native enum. Metadata only: it does not change how the
delta is computed.

Revision ID: f2a4c6e8b0d1
Revises: a7b3c8d2e1f4
Create Date: 2026-06-06 00:00:00.000000

"""
from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'f2a4c6e8b0d1'
down_revision: str | Sequence[str] | None = 'a7b3c8d2e1f4'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        'evaluation_set',
        sa.Column('window_role', sa.String(length=8), nullable=True),
    )
    op.create_check_constraint(
        'ck_evaluation_set_window_role',
        'evaluation_set',
        "window_role IS NULL OR window_role IN ('valid', 'test')",
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint(
        'ck_evaluation_set_window_role', 'evaluation_set', type_='check'
    )
    op.drop_column('evaluation_set', 'window_role')
