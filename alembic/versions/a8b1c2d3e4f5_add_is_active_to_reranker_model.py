"""serve-offline-reconciliation S3: add is_active to reranker_model

Adds the operator-controlled serve-selection flag to ``reranker_model``.
The serving path picks the ACTIVE booster per (category, aspect) slot,
falling back to latest-by-created_at when none is active. The column is
NOT NULL with a ``FALSE`` server default, so every existing row is
inactive on upgrade and serve behaviour is unchanged until an operator
marks a row active (data-preserving).

A partial unique index enforces at most one active booster per
(category, COALESCE(aspect, '')) slot, scoped to the active rows only so
the table keeps any number of inactive history rows.

Revision ID: a8b1c2d3e4f5
Revises: d1e2f3a4b5c6
Create Date: 2026-06-30 00:00:00.000000

"""
from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a8b1c2d3e4f5"
down_revision: str | Sequence[str] | None = "d1e2f3a4b5c6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "reranker_model",
        sa.Column(
            "is_active",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.create_index(
        "uq_reranker_model_active_slot",
        "reranker_model",
        ["category", sa.text("COALESCE(aspect, '')")],
        unique=True,
        postgresql_where=sa.text("is_active"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("uq_reranker_model_active_slot", table_name="reranker_model")
    op.drop_column("reranker_model", "is_active")
