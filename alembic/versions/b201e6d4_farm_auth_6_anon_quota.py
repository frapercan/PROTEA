"""farm_auth_6_anon_quota

Revision ID: b201e6d4
Revises: a1f2b3c4d5e6
Create Date: 2026-05-24 20:00:00.000000

FARM-AUTH.6 -- anonymous quick-annotate IP-hash daily quota table.

Creates ``anon_quota``: one row per (ip_hash, day) pair that tracks how
many unauthenticated requests a given IP hash has consumed today.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

from alembic import op

revision: str = "b201e6d4"
down_revision: str = "a1f2b3c4d5e6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "anon_quota",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("ip_hash", sa.String(length=64), nullable=False),
        sa.Column("day", sa.Date(), nullable=False),
        sa.Column("count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("ip_hash", "day", name="uq_anon_quota_ip_day"),
    )
    op.create_index("ix_anon_quota_ip_hash_day", "anon_quota", ["ip_hash", "day"])


def downgrade() -> None:
    op.drop_index("ix_anon_quota_ip_hash_day", table_name="anon_quota")
    op.drop_table("anon_quota")
