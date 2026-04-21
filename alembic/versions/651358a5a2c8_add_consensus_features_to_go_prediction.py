"""add consensus features to go_prediction

Revision ID: 651358a5a2c8
Revises: b1a1f4ec0e42
Create Date: 2026-04-16 10:00:00.000000
"""
from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision: str = "651358a5a2c8"
down_revision: str = "b1a1f4ec0e42"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "go_prediction",
        sa.Column("neighbor_vote_fraction", sa.Float(), nullable=True),
    )
    op.add_column(
        "go_prediction",
        sa.Column("neighbor_min_distance", sa.Float(), nullable=True),
    )
    op.add_column(
        "go_prediction",
        sa.Column("neighbor_mean_distance", sa.Float(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("go_prediction", "neighbor_mean_distance")
    op.drop_column("go_prediction", "neighbor_min_distance")
    op.drop_column("go_prediction", "neighbor_vote_fraction")
