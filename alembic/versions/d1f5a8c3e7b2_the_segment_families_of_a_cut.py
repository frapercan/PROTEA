"""The segment families of a cut

ADR-D65, decision 2. The families are built on the cluster from a photo and a
lexicon bank and loaded by ``load_segment_families``: this adds the table that
declares them. Their outputs (the segments and the families of each scope) are
files in the artifact store, listed with their URI and sha256 in ``outputs``.

Revision ID: d1f5a8c3e7b2
Revises: e7c4a9b2d5f1
Create Date: 2026-11-18 17:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d1f5a8c3e7b2"
down_revision: str | Sequence[str] | None = "e7c4a9b2d5f1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: The model's checks as they stand at this revision, frozen here.
SHORT_NAME_CHECK = "short_name ~ '^[a-z0-9][a-z0-9-]*$' AND length(short_name) <= 64"
DESCRIPTIONS_CHECK = "length(description) > 0"
DIGESTS_CHECK = "length(manifest_sha256) = 64"
COUNTS_CHECK = "segments > 0"


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "segment_families",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "cut_photo_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("cut_photo.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "lexicon_bank_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("lexicon_bank.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("cut_release", sa.Integer(), nullable=False),
        sa.Column("scopes", postgresql.JSONB(), nullable=False),
        sa.Column("segments", sa.Integer(), nullable=False),
        sa.Column("outputs", postgresql.JSONB(), nullable=False),
        sa.Column("counts", postgresql.JSONB(), nullable=False),
        sa.Column("mmseqs", postgresql.JSONB(), nullable=False),
        sa.Column("manifest", postgresql.JSONB(), nullable=False),
        sa.Column("manifest_uri", sa.String(length=512), nullable=False),
        sa.Column("manifest_sha256", sa.String(length=64), nullable=False, unique=True),
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
            "cut_photo_id", "lexicon_bank_id", name="uq_segment_families_photo_lexicon_bank"
        ),
        sa.CheckConstraint(SHORT_NAME_CHECK, name="ck_segment_families_short_name"),
        sa.CheckConstraint(DESCRIPTIONS_CHECK, name="ck_segment_families_descriptions"),
        sa.CheckConstraint(DIGESTS_CHECK, name="ck_segment_families_digests"),
        sa.CheckConstraint(COUNTS_CHECK, name="ck_segment_families_counts"),
    )
    op.create_index("ix_segment_families_cut_photo_id", "segment_families", ["cut_photo_id"])
    op.create_index("ix_segment_families_lexicon_bank_id", "segment_families", ["lexicon_bank_id"])


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_segment_families_lexicon_bank_id", table_name="segment_families")
    op.drop_index("ix_segment_families_cut_photo_id", table_name="segment_families")
    op.drop_table("segment_families")
