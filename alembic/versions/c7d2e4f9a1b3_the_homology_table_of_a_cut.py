"""The homology table of a cut (ADR-D61)

One row per cut photo, declaring the hits parquet a cluster built: the photo's
superset searched against all its members with a pinned MMseqs2, keyed by
sequence md5. The hits live in the artifact store by URI and sha256; this table
holds the row that declares them, the MMseqs2 version and binary sha256, the
command line, the counts and the manifest. A cut has one table.

Revision ID: c7d2e4f9a1b3
Revises: e5b1c9d3a7f2
Create Date: 2026-11-17 09:30:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c7d2e4f9a1b3"
down_revision: str | Sequence[str] | None = "e5b1c9d3a7f2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: The model's checks, by the same text; a test compares them.
SHORT_NAME_CHECK = "short_name ~ '^[a-z0-9][a-z0-9-]*$' AND length(short_name) <= 64"
DESCRIPTIONS_CHECK = "length(description) > 0"
DIGESTS_CHECK = (
    "length(manifest_sha256) = 64 AND length(hits_sha256) = 64 "
    "AND length(mmseqs_binary_sha256) = 64"
)
COUNTS_CHECK = (
    "queries > 0 AND bank > 0 AND hits >= 0 AND self_hits_dropped >= 0 "
    "AND queries_with_hits >= 0 AND queries_with_hits <= queries "
    "AND members_without_sequence >= 0"
)


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "homology_table",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "cut_photo_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("cut_photo.id", ondelete="RESTRICT"),
            nullable=False,
            unique=True,
        ),
        sa.Column("cut_release", sa.Integer(), nullable=False, unique=True),
        sa.Column("mmseqs_version", sa.String(length=64), nullable=False),
        sa.Column("mmseqs_binary_sha256", sa.String(length=64), nullable=False),
        sa.Column("command", postgresql.JSONB(), nullable=False),
        sa.Column("queries", sa.Integer(), nullable=False),
        sa.Column("bank", sa.Integer(), nullable=False),
        sa.Column("hits", sa.BigInteger(), nullable=False),
        sa.Column("self_hits_dropped", sa.BigInteger(), nullable=False),
        sa.Column("queries_with_hits", sa.Integer(), nullable=False),
        sa.Column("members_without_sequence", sa.Integer(), nullable=False),
        sa.Column("hits_uri", sa.String(length=512), nullable=False),
        sa.Column("hits_sha256", sa.String(length=64), nullable=False),
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
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("cut_release > 0", name="ck_homology_table_cut_release"),
        sa.CheckConstraint(SHORT_NAME_CHECK, name="ck_homology_table_short_name"),
        sa.CheckConstraint(DESCRIPTIONS_CHECK, name="ck_homology_table_descriptions"),
        sa.CheckConstraint(DIGESTS_CHECK, name="ck_homology_table_digests"),
        sa.CheckConstraint(COUNTS_CHECK, name="ck_homology_table_counts"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    bind = op.get_bind()
    tables = bind.execute(sa.text("select count(*) from homology_table")).scalar_one()
    if tables:
        raise RuntimeError(
            f"{tables} homology tables are loaded; dropping the table would orphan their "
            "files in the store. Remove them first."
        )
    op.drop_table("homology_table")
