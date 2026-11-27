"""A bank records the statistics of its space

The search can run in a declared space (centred, standardised, whitened, or
with its dominant directions removed), and every such space is read from the
label-free statistics of the configuration's space at a cut, computed by
``compute_embedding_space_statistics`` from the loaded bank's shards: this adds
the table that declares them.

- ``embedding_space_statistics``: one row per (configuration, cut photo), with
  the bank it was read from, the population's size ``n`` and the dimension
  ``d``, the artifact (an ``.npz`` holding every value) by URI and sha256, the
  description its writer derives, the job and the code revision. The three
  foreign keys it derives from are ``RESTRICT``: a configuration, a photo or a
  bank cannot vanish while statistics name it.

The downgrade drops the table; the artifacts stay in the store.

Revision ID: f2e0fc2717f7
Revises: 5d2e8c1b4a73
Create Date: 2026-11-28 12:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f2e0fc2717f7"
down_revision: str | Sequence[str] | None = "5d2e8c1b4a73"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: The model's checks as they stand at this revision, frozen here.
DESCRIPTIONS_CHECK = "length(description) > 0"
DIGESTS_CHECK = "length(artifact_sha256) = 64"
COUNTS_CHECK = "n > 1 AND d > 0"


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "embedding_space_statistics",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "embedding_config_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("embedding_config.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "cut_photo_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("cut_photo.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "embedding_bank_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("embedding_bank.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("n", sa.Integer(), nullable=False),
        sa.Column("d", sa.Integer(), nullable=False),
        sa.Column("artifact_uri", sa.String(length=512), nullable=False),
        sa.Column("artifact_sha256", sa.String(length=64), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column(
            "job_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("job.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("code_revision", sa.String(length=64), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint(
            "embedding_config_id",
            "cut_photo_id",
            name="uq_embedding_space_statistics_config_photo",
        ),
        sa.CheckConstraint(DESCRIPTIONS_CHECK, name="ck_embedding_space_statistics_descriptions"),
        sa.CheckConstraint(DIGESTS_CHECK, name="ck_embedding_space_statistics_digests"),
        sa.CheckConstraint(COUNTS_CHECK, name="ck_embedding_space_statistics_counts"),
    )
    op.create_index(
        "ix_embedding_space_statistics_cut_photo_id",
        "embedding_space_statistics",
        ["cut_photo_id"],
    )
    op.create_index(
        "ix_embedding_space_statistics_embedding_bank_id",
        "embedding_space_statistics",
        ["embedding_bank_id"],
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(
        "ix_embedding_space_statistics_embedding_bank_id",
        table_name="embedding_space_statistics",
    )
    op.drop_index(
        "ix_embedding_space_statistics_cut_photo_id",
        table_name="embedding_space_statistics",
    )
    op.drop_table("embedding_space_statistics")
