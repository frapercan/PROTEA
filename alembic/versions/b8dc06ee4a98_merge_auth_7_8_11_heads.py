"""merge_auth_7_8_11_heads

Revision ID: b8dc06ee4a98
Revises: b202e7d5, b203e8d6, b2e4f1a9c3d7
Create Date: 2026-05-24 19:24:02.799581

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b8dc06ee4a98'
down_revision: Union[str, Sequence[str], None] = ('b202e7d5', 'b203e8d6', 'b2e4f1a9c3d7')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
