"""merge_scoring_config_branch

Revision ID: 7737a352d4fe
Revises: 47de89cf6fec, b1c2d3e4f5a6
Create Date: 2026-03-15 10:11:56.507967

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7737a352d4fe'
down_revision: Union[str, Sequence[str], None] = ('47de89cf6fec', 'b1c2d3e4f5a6')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
