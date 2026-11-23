"""Each window places its targets on the sequence axes

Two tables. ``window_placement`` declares, per window, the photo of its opening cut
and the homology table of that cut it was read from, the rule, the counts, the job
and the revision. ``window_placement_target`` holds one row per target: its
sequence md5 and length at the window's start, and the closest other protein of
the photo with a truth row (an identical sequence under another accession, or the
best hit of the homology table by bit score), with that hit's alignment.

The downgrade refuses while a placement is stored: the strata of every evaluation
on that window are read from it.

Revision ID: 2ee1c12a5e0c
Revises: 7c4e1b9d2a50
Create Date: 2026-11-21 04:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2ee1c12a5e0c"
down_revision: str | Sequence[str] | None = "7c4e1b9d2a50"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: The model's checks, by the same text; a test compares them.
SHORT_NAME_CHECK = "short_name ~ '^[a-z0-9][a-z0-9-]*$' AND length(short_name) <= 64"
DESCRIPTIONS_CHECK = "length(description) > 0"
RULES_CHECK = "rule IN ('closest_truth_member')"
COUNTS_CHECK = (
    "targets > 0 AND targets_with_twin >= 0 AND targets_with_hit >= 0 "
    "AND targets_without_homolog >= 0 "
    "AND targets_with_twin + targets_with_hit + targets_without_homolog = targets"
)
CLOSEST_CHECK = (
    "(closest_by IS NULL AND closest_identity IS NULL AND closest_md5 IS NULL "
    "AND bits IS NULL) "
    "OR (closest_by = 'twin' AND closest_identity = 100 AND closest_md5 = sequence_md5 "
    "AND bits IS NULL) "
    "OR (closest_by = 'hit' AND closest_identity BETWEEN 0 AND 100 "
    "AND closest_md5 IS NOT NULL AND closest_md5 <> sequence_md5 AND bits IS NOT NULL)"
)


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "window_placement",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "evaluation_set_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("evaluation_set.id", ondelete="RESTRICT"),
            nullable=False,
            unique=True,
        ),
        sa.Column(
            "cut_photo_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("cut_photo.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "homology_table_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("homology_table.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("rule", sa.String(length=32), nullable=False),
        sa.Column("targets", sa.Integer(), nullable=False),
        sa.Column("targets_with_twin", sa.Integer(), nullable=False),
        sa.Column("targets_with_hit", sa.Integer(), nullable=False),
        sa.Column("targets_without_homolog", sa.Integer(), nullable=False),
        sa.Column("short_name", sa.String(length=64), nullable=False, unique=True),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("code_revision", sa.String(length=64), nullable=True),
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
        sa.CheckConstraint(SHORT_NAME_CHECK, name="ck_window_placement_short_name"),
        sa.CheckConstraint(DESCRIPTIONS_CHECK, name="ck_window_placement_descriptions"),
        sa.CheckConstraint(RULES_CHECK, name="ck_window_placement_rule"),
        sa.CheckConstraint(COUNTS_CHECK, name="ck_window_placement_counts"),
    )
    op.create_table(
        "window_placement_target",
        sa.Column(
            "placement_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("window_placement.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("accession", sa.String(length=64), primary_key=True),
        sa.Column("sequence_md5", sa.String(length=32), nullable=False),
        sa.Column("residues", sa.Integer(), nullable=False),
        sa.Column("closest_by", sa.String(length=8), nullable=True),
        sa.Column("closest_identity", sa.Float(), nullable=True),
        sa.Column("closest_md5", sa.String(length=32), nullable=True),
        sa.Column("bits", sa.Float(), nullable=True),
        sa.Column("evalue", sa.Double(), nullable=True),
        sa.Column("alnlen", sa.Integer(), nullable=True),
        sa.Column("qcov", sa.Float(), nullable=True),
        sa.Column("tcov", sa.Float(), nullable=True),
        sa.CheckConstraint("residues > 0", name="ck_window_placement_target_residues"),
        sa.CheckConstraint(CLOSEST_CHECK, name="ck_window_placement_target_closest"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    bind = op.get_bind()
    placements = bind.execute(sa.text("select count(*) from window_placement")).scalar_one()
    if placements:
        raise RuntimeError(
            f"{placements} window placements are stored, and the strata of every evaluation "
            "on those windows are read from them. Remove them first."
        )
    op.drop_table("window_placement_target")
    op.drop_table("window_placement")
