"""The lexicon bank of the photos

ADR-D63. The bank is decoded on the cluster, one lexicon per card, and loaded
by ``load_lexicon_bank``: this adds the table that declares each bank. Its
outputs (the cut and the theme mass of every shard) are files in the artifact
store, listed with their URI and sha256 in ``shards``.

Revision ID: e7c4a9b2d5f1
Revises: c3a9e7d1f5b2
Create Date: 2026-11-17 20:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e7c4a9b2d5f1"
down_revision: str | Sequence[str] | None = "c3a9e7d1f5b2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: The model's checks as they stand at this revision, frozen here.
SHORT_NAME_CHECK = "short_name ~ '^[a-z0-9][a-z0-9-]*$' AND length(short_name) <= 64"
DESCRIPTIONS_CHECK = "length(description) > 0"
DIGESTS_CHECK = "length(manifest_sha256) = 64 AND length(sequences_sha256) = 64"
COUNTS_CHECK = "sequences > 0 AND residues > 0"


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "lexicon_bank",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "model_weights_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("model_weights.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("name", sa.String(length=64), nullable=False),
        sa.Column("photos", postgresql.JSONB(), nullable=False),
        sa.Column("sequences", sa.Integer(), nullable=False),
        sa.Column("residues", sa.BigInteger(), nullable=False),
        sa.Column("sequences_sha256", sa.String(length=64), nullable=False),
        sa.Column("shards", postgresql.JSONB(), nullable=False),
        sa.Column("passes", postgresql.JSONB(), nullable=False),
        sa.Column("parity", postgresql.JSONB(), nullable=False),
        sa.Column("manifest", postgresql.JSONB(), nullable=False),
        sa.Column("manifest_uri", sa.String(length=512), nullable=False),
        sa.Column("manifest_sha256", sa.String(length=64), nullable=False),
        sa.Column("short_name", sa.String(length=64), nullable=False, unique=True),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("code_revision", sa.String(length=64), nullable=True),
        sa.Column("builder_revision", sa.String(length=64), nullable=True),
        sa.Column(
            "job_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("job.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint(
            "model_weights_id", "manifest_sha256", name="uq_lexicon_bank_weights_manifest"
        ),
        sa.CheckConstraint(SHORT_NAME_CHECK, name="ck_lexicon_bank_short_name"),
        sa.CheckConstraint(DESCRIPTIONS_CHECK, name="ck_lexicon_bank_descriptions"),
        sa.CheckConstraint(DIGESTS_CHECK, name="ck_lexicon_bank_digests"),
        sa.CheckConstraint(COUNTS_CHECK, name="ck_lexicon_bank_counts"),
    )
    op.create_index("ix_lexicon_bank_model_weights_id", "lexicon_bank", ["model_weights_id"])


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_lexicon_bank_model_weights_id", table_name="lexicon_bank")
    op.drop_table("lexicon_bank")
