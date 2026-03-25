"""add_query_set_id_to_prediction_set

Revision ID: d4e5f6a7b8c9
Revises: c3d4e5f6a7b8
Create Date: 2026-03-10 00:00:00.000000

"""
from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "d4e5f6a7b8c9"
down_revision: str | Sequence[str] | None = "c3d4e5f6a7b8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "prediction_set",
        sa.Column("query_set_id", sa.UUID(), nullable=True),
    )
    op.create_foreign_key(
        "fk_prediction_set_query_set_id",
        "prediction_set",
        "query_set",
        ["query_set_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        op.f("ix_prediction_set_query_set_id"),
        "prediction_set",
        ["query_set_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_prediction_set_query_set_id"), table_name="prediction_set")
    op.drop_constraint("fk_prediction_set_query_set_id", "prediction_set", type_="foreignkey")
    op.drop_column("prediction_set", "query_set_id")
