"""fix evidence_code varchar length

Revision ID: 350d9f18ca10
Revises: dc0cb8499090
Create Date: 2026-03-07 21:34:13.851839

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '350d9f18ca10'
down_revision: Union[str, Sequence[str], None] = 'dc0cb8499090'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        "protein_go_annotation",
        "evidence_code",
        type_=sa.String(),
        existing_nullable=True,
    )


def downgrade() -> None:
    op.alter_column(
        "protein_go_annotation",
        "evidence_code",
        type_=sa.String(length=10),
        existing_nullable=True,
    )
