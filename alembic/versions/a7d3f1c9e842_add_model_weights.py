"""Add model_weights: one row per file of fitted weights in the artifact store (ADR-D54)

The lab's weights reach the platform as files that an operation finds through an
address in its payload, and nothing listed them. This table registers each file
with its kind, its artifact store key, its sha256 and size, free metadata and the
annotation set it was fitted on, so the instrument can list weights, upload them
and launch the operations that read them.

Additive, so downgrade is a clean drop.

Revision ID: a7d3f1c9e842
Revises: e3c9a7f15b20
Create Date: 2026-11-03 09:30:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a7d3f1c9e842"
down_revision: str | Sequence[str] | None = "e3c9a7f15b20"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "model_weights",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("kind", sa.String(length=32), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("format", sa.String(length=32), nullable=False),
        sa.Column("uri", sa.String(length=512), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("meta", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("trained_on_annotation_set_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("created_by", sa.String(length=200), nullable=True),
        sa.ForeignKeyConstraint(
            ["trained_on_annotation_set_id"], ["annotation_set.id"], ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("uri", name="uq_model_weights_uri"),
    )
    op.create_index("ix_model_weights_kind", "model_weights", ["kind"])
    op.create_index("ix_model_weights_sha256", "model_weights", ["sha256"])


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_model_weights_sha256", table_name="model_weights")
    op.drop_index("ix_model_weights_kind", table_name="model_weights")
    op.drop_table("model_weights")
