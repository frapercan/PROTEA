"""farm_auth_7_user_quota

Revision ID: b202e7d5
Revises: b201e6d4
Create Date: 2026-05-24 20:00:00.000000

FARM-AUTH.7 -- per-user daily operation quota table.

Creates the ``user_quota`` table that counts how many times each
authenticated user has called an expensive operation (predict,
export_research_dataset, run_cafa_evaluation) on a given UTC calendar day.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "b202e7d5"
down_revision: str | None = "b201e6d4"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_table(
        "user_quota",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("user.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("day", sa.Date(), nullable=False),
        sa.Column("operation", sa.Text(), nullable=False),
        sa.Column("count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_unique_constraint(
        "uq_user_quota_user_day_op", "user_quota", ["user_id", "day", "operation"]
    )
    op.create_index("ix_user_quota_user_day", "user_quota", ["user_id", "day"])
    op.create_index("ix_user_quota_operation", "user_quota", ["operation"])


def downgrade() -> None:
    op.drop_index("ix_user_quota_operation", table_name="user_quota")
    op.drop_index("ix_user_quota_user_day", table_name="user_quota")
    op.drop_constraint("uq_user_quota_user_day_op", "user_quota", type_="unique")
    op.drop_table("user_quota")
