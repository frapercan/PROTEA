"""Add residue_code: where the per-residue code of a sequence lives (ADR-D51)

The platform keeps one vector per sequence in ``sequence_embedding``. A residue
vocabulary produces a code for EVERY residue, which is too large for rows (about
300 million residues times 144 active features), so the codes go to compressed
shards in the artifact store and this table is their index: one row per sequence
and configuration, naming the shard, the sequence's position in it, its residue
and entry counts, and the shard's sha256.

Additive, so downgrade is a clean drop.

Revision ID: e3c9a7f15b20
Revises: d5a1c8e3f706
Create Date: 2026-11-03 01:30:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e3c9a7f15b20"
down_revision: str | Sequence[str] | None = "d5a1c8e3f706"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "residue_code",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("embedding_config_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("shard_key", sa.String(length=512), nullable=False),
        sa.Column("shard_sha256", sa.String(length=64), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column("residue_count", sa.Integer(), nullable=False),
        sa.Column("entry_count", sa.BigInteger(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["sequence_id"], ["sequence.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["embedding_config_id"], ["embedding_config.id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "sequence_id", "embedding_config_id", name="uq_residue_code_sequence_config"
        ),
    )
    op.create_index("ix_residue_code_sequence_id", "residue_code", ["sequence_id"])
    op.create_index("ix_residue_code_embedding_config_id", "residue_code", ["embedding_config_id"])
    op.create_index("ix_residue_code_shard_key", "residue_code", ["shard_key"])


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_residue_code_shard_key", table_name="residue_code")
    op.drop_index("ix_residue_code_embedding_config_id", table_name="residue_code")
    op.drop_index("ix_residue_code_sequence_id", table_name="residue_code")
    op.drop_table("residue_code")
