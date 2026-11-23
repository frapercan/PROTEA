"""A photo's members as rows: cut_photo_member replaces cut_photo_cluster and cut_photo_cluster_profile

ADR-D59, decision 1 and 3, amended 2026-11-16: the annotated UniRef50 neighbours of
the superset enter the photo as proteins with all their GOA rows, so the
per-cluster profiles, which counted exactly these neighbours, are retired, and so is
the separate cluster table: every member now carries its UniRef50 cluster.

``cut_photo_member`` is one row per member of a photo: the sets it entered by
(superset, truth, curated inference, neighbour), its cluster when it has one, and the
md5 of its sequence in the paired release. Neighbours enter by UniRef, which no
annotation records, so who is in a photo is stored rather than derived.

``cut_photo`` loses the descriptors of the profile set (``profile_short_name``,
``profile_description``), and its description check names the one description left.

The two retired tables are dropped with their rows: a photo loaded before this
revision is a trial of the old format, and the loader refuses its manifest now (it
lacks outputs the builder writes). Downgrading refuses while any photo exists,
rather than inventing the profile descriptors it would need.

Revision ID: 384568a09847
Revises: f0e6749f94cb
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

from alembic import op

revision = "384568a09847"
down_revision = "f0e6749f94cb"
branch_labels = None
depends_on = None

#: The same texts the ORM model declares; a test compares the two.
MEMBER_SETS_CHECK = "superset OR truth OR curated OR neighbour"
NEIGHBOUR_ALONE_CHECK = "NOT neighbour OR NOT (superset OR truth OR curated)"
DESCRIPTIONS_CHECK = "length(description) > 0"
#: The checks this revision removes, restated for the downgrade.
_OLD_DESCRIPTIONS_CHECK = "length(description) > 0 AND length(profile_description) > 0"
_OLD_PROFILE_SHORT_NAME_CHECK = (
    "profile_short_name ~ '^[a-z0-9][a-z0-9-]*$' AND length(profile_short_name) <= 64"
)
_OLD_PROFILE_CHANNEL_CHECK = (
    "channel IN ('curated', 'orthology_electronic', 'phylogenetic', 'truth')"
)


def upgrade() -> None:
    op.create_table(
        "cut_photo_member",
        sa.Column(
            "photo_id",
            UUID(as_uuid=True),
            sa.ForeignKey("cut_photo.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("accession", sa.String(32), primary_key=True),
        sa.Column("superset", sa.Boolean(), nullable=False),
        sa.Column("truth", sa.Boolean(), nullable=False),
        sa.Column("curated", sa.Boolean(), nullable=False),
        sa.Column("neighbour", sa.Boolean(), nullable=False),
        sa.Column("cluster", sa.String(64), nullable=True),
        sa.Column("sequence_md5", sa.String(32), nullable=True),
        sa.CheckConstraint(MEMBER_SETS_CHECK, name="ck_cut_photo_member_sets"),
        sa.CheckConstraint(NEIGHBOUR_ALONE_CHECK, name="ck_cut_photo_member_neighbour_alone"),
    )
    op.create_index(
        "ix_cut_photo_member_photo_cluster", "cut_photo_member", ["photo_id", "cluster"]
    )
    op.drop_table("cut_photo_cluster_profile")
    op.drop_index("ix_cut_photo_cluster_photo_cluster", table_name="cut_photo_cluster")
    op.drop_table("cut_photo_cluster")
    op.drop_constraint("ck_cut_photo_profile_short_name", "cut_photo", type_="check")
    op.drop_constraint("ck_cut_photo_descriptions", "cut_photo", type_="check")
    op.drop_column("cut_photo", "profile_short_name")
    op.drop_column("cut_photo", "profile_description")
    op.create_check_constraint("ck_cut_photo_descriptions", "cut_photo", DESCRIPTIONS_CHECK)


def downgrade() -> None:
    photos = op.get_bind().execute(sa.text("SELECT count(*) FROM cut_photo")).scalar_one()
    if photos:
        raise RuntimeError(
            f"{photos} photos exist: the old format needs profile descriptors they never had"
        )
    op.drop_constraint("ck_cut_photo_descriptions", "cut_photo", type_="check")
    op.add_column("cut_photo", sa.Column("profile_description", sa.Text(), nullable=False))
    op.add_column(
        "cut_photo", sa.Column("profile_short_name", sa.String(64), nullable=False, unique=True)
    )
    op.create_check_constraint("ck_cut_photo_descriptions", "cut_photo", _OLD_DESCRIPTIONS_CHECK)
    op.create_check_constraint(
        "ck_cut_photo_profile_short_name", "cut_photo", _OLD_PROFILE_SHORT_NAME_CHECK
    )
    op.create_table(
        "cut_photo_cluster",
        sa.Column(
            "photo_id",
            UUID(as_uuid=True),
            sa.ForeignKey("cut_photo.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("accession", sa.String(32), primary_key=True),
        sa.Column("cluster", sa.String(64), nullable=False),
    )
    op.create_index(
        "ix_cut_photo_cluster_photo_cluster", "cut_photo_cluster", ["photo_id", "cluster"]
    )
    op.create_table(
        "cut_photo_cluster_profile",
        sa.Column(
            "photo_id",
            UUID(as_uuid=True),
            sa.ForeignKey("cut_photo.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("cluster", sa.String(64), primary_key=True),
        sa.Column("go_id", sa.String(16), primary_key=True),
        sa.Column("channel", sa.String(32), primary_key=True),
        sa.Column("negated", sa.Boolean(), primary_key=True),
        sa.Column("members", sa.Integer(), nullable=False),
        sa.CheckConstraint(_OLD_PROFILE_CHANNEL_CHECK, name="ck_cut_photo_cluster_profile_channel"),
        sa.CheckConstraint("members > 0", name="ck_cut_photo_cluster_profile_members"),
    )
    op.drop_index("ix_cut_photo_member_photo_cluster", table_name="cut_photo_member")
    op.drop_table("cut_photo_member")
