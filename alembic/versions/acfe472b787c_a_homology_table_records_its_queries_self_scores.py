"""A homology table records its queries' self scores

A homology transfer weighs a target's voters by bit score, and a voter that
shares the target's sequence under another accession (a twin) is not a hit of
the cut's homology table, which drops the hit of a sequence with itself. Its bit
score is the target's self-alignment score, computed on the cluster by forced
self alignments of every query of the table: this adds the table that declares
those scores.

- ``homology_self_scores``: one row per homology table (``homology_table_id``,
  unique, ``RESTRICT``: a table cannot vanish while its self scores name it), with
  the MMseqs2 version and binary sha256, the command lines, the table's query
  count and the queries the forced alignment could not score, the scores parquet
  and the manifest by URI and sha256, ADR-D58's descriptors, the job, the loader's
  revision and the builder's.

The downgrade refuses while a row exists: the homology transfers written with
these scores pin them by sha256, and dropping the table would leave pins that
name nothing.

Revision ID: acfe472b787c
Revises: 532d2c1db500
Create Date: 2026-11-28 21:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "acfe472b787c"
down_revision: str | Sequence[str] | None = "532d2c1db500"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: The model's checks as they stand at this revision, frozen here.
SHORT_NAME_CHECK = "short_name ~ '^[a-z0-9][a-z0-9-]*$' AND length(short_name) <= 64"
DESCRIPTIONS_CHECK = "length(description) > 0"
DIGESTS_CHECK = (
    "length(manifest_sha256) = 64 AND length(scores_sha256) = 64 "
    "AND length(mmseqs_binary_sha256) = 64"
)
COUNTS_CHECK = (
    "queries > 0 AND queries_without_self_score >= 0 AND queries_without_self_score <= queries"
)


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "homology_self_scores",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "homology_table_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("homology_table.id", ondelete="RESTRICT"),
            nullable=False,
            unique=True,
        ),
        sa.Column("mmseqs_version", sa.String(length=64), nullable=False),
        sa.Column("mmseqs_binary_sha256", sa.String(length=64), nullable=False),
        sa.Column("command", postgresql.JSONB(), nullable=False),
        sa.Column("queries", sa.Integer(), nullable=False),
        sa.Column("queries_without_self_score", sa.Integer(), nullable=False),
        sa.Column("scores_uri", sa.String(length=512), nullable=False),
        sa.Column("scores_sha256", sa.String(length=64), nullable=False),
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
        sa.CheckConstraint(SHORT_NAME_CHECK, name="ck_homology_self_scores_short_name"),
        sa.CheckConstraint(DESCRIPTIONS_CHECK, name="ck_homology_self_scores_descriptions"),
        sa.CheckConstraint(DIGESTS_CHECK, name="ck_homology_self_scores_digests"),
        sa.CheckConstraint(COUNTS_CHECK, name="ck_homology_self_scores_counts"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    rows = op.get_bind().execute(sa.text("select count(*) from homology_self_scores")).scalar_one()
    if rows:
        raise RuntimeError(
            f"homology_self_scores holds {rows} rows; the homology transfers written with them "
            "pin them by sha256, so dropping the table would leave pins that name nothing. "
            "Delete those rows first."
        )
    op.drop_table("homology_self_scores")
