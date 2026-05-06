"""add visitor_event table

Revision ID: f7a004f5f2c7
Revises: c4d5e6f7a8b9
Create Date: 2026-04-12 20:50:00.000000
"""
from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision: str = "f7a004f5f2c7"
down_revision: str = "c4d5e6f7a8b9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "visitor_event",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("day", sa.Date(), nullable=False),
        sa.Column("visitor_hash", sa.String(length=16), nullable=False),
        sa.Column("path", sa.String(length=255), nullable=False),
        sa.Column("method", sa.String(length=8), nullable=False),
        sa.Column("status", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_visitor_event_day_hash", "visitor_event", ["day", "visitor_hash"])
    op.create_index("ix_visitor_event_created_at", "visitor_event", ["created_at"])
    op.create_index("ix_visitor_event_path", "visitor_event", ["path"])


def downgrade() -> None:
    op.drop_index("ix_visitor_event_path", table_name="visitor_event")
    op.drop_index("ix_visitor_event_created_at", table_name="visitor_event")
    op.drop_index("ix_visitor_event_day_hash", table_name="visitor_event")
    op.drop_table("visitor_event")
