"""The photo of a cut: cut_photo, cut_photo_cluster and cut_photo_cluster_profile

ADR-D59, decision 3. Three new tables and nothing else changes: the photo's rows
go to the existing ``annotation_set`` / ``protein_go_annotation`` /
``protein`` / ``sequence`` tables, and its superset is the paired release's
``swissprot_membership``, referenced and not copied.

``cut_photo`` is one row per cut, with ADR-D58's descriptors for the photo and
for its profile set, its manifest by URI and sha256, the job, the loader's
revision and the builder's. ``cut_photo_cluster`` gives each superset protein
its UniRef50 cluster; ``cut_photo_cluster_profile`` holds counts per (cluster,
GO term, channel, negation). The two are counted entities and carry no
descriptors.

Nothing is backfilled: no photo exists before this revision.

Revision ID: d4a7c2e9f310
Revises: b71e2396e5c3
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID

from alembic import op

revision = "d4a7c2e9f310"
down_revision = "b71e2396e5c3"
branch_labels = None
depends_on = None

#: The same texts the ORM model declares; a test compares the two.
PROFILE_CHANNEL_CHECK = "channel IN ('curated', 'orthology_electronic', 'phylogenetic', 'truth')"
SHORT_NAME_CHECK = "short_name ~ '^[a-z0-9][a-z0-9-]*$' AND length(short_name) <= 64"
PROFILE_SHORT_NAME_CHECK = (
    "profile_short_name ~ '^[a-z0-9][a-z0-9-]*$' AND length(profile_short_name) <= 64"
)
DESCRIPTIONS_CHECK = "length(description) > 0 AND length(profile_description) > 0"
DIGESTS_CHECK = (
    "length(manifest_sha256) = 64 AND length(channel_table_sha256) = 64 "
    "AND length(uniref_clusters_sha256) = 64"
)


def upgrade() -> None:
    op.create_table(
        "cut_photo",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("cut_release", sa.Integer(), nullable=False),
        sa.Column("cut_date", sa.Date(), nullable=False),
        sa.Column(
            "annotation_set_id",
            UUID(as_uuid=True),
            sa.ForeignKey("annotation_set.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "ontology_snapshot_id",
            UUID(as_uuid=True),
            sa.ForeignKey("ontology_snapshot.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "swissprot_release_id",
            UUID(as_uuid=True),
            sa.ForeignKey("swissprot_release.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("uniref_release", sa.String(16), nullable=False),
        sa.Column("uniref_published_at", sa.Date(), nullable=False),
        sa.Column("uniref_clusters_sha256", sa.String(64), nullable=False),
        sa.Column("goref_list_read_on", sa.Date(), nullable=False),
        sa.Column("channel_table_sha256", sa.String(64), nullable=False),
        sa.Column("manifest", JSONB(), nullable=False),
        sa.Column("manifest_uri", sa.String(512), nullable=False),
        sa.Column("manifest_sha256", sa.String(64), nullable=False),
        sa.Column("short_name", sa.String(64), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("profile_short_name", sa.String(64), nullable=False),
        sa.Column("profile_description", sa.Text(), nullable=False),
        sa.Column("code_revision", sa.String(64), nullable=True),
        sa.Column("builder_revision", sa.String(64), nullable=True),
        sa.Column(
            "job_id",
            UUID(as_uuid=True),
            sa.ForeignKey("job.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("cut_release", name="cut_photo_cut_release_key"),
        sa.UniqueConstraint("annotation_set_id", name="cut_photo_annotation_set_id_key"),
        sa.UniqueConstraint("manifest_sha256", name="cut_photo_manifest_sha256_key"),
        sa.UniqueConstraint("short_name", name="cut_photo_short_name_key"),
        sa.UniqueConstraint("profile_short_name", name="cut_photo_profile_short_name_key"),
        sa.CheckConstraint("cut_release > 0", name="ck_cut_photo_cut_release"),
        sa.CheckConstraint(SHORT_NAME_CHECK, name="ck_cut_photo_short_name"),
        sa.CheckConstraint(PROFILE_SHORT_NAME_CHECK, name="ck_cut_photo_profile_short_name"),
        sa.CheckConstraint(DESCRIPTIONS_CHECK, name="ck_cut_photo_descriptions"),
        sa.CheckConstraint(DIGESTS_CHECK, name="ck_cut_photo_digests"),
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
        sa.CheckConstraint(PROFILE_CHANNEL_CHECK, name="ck_cut_photo_cluster_profile_channel"),
        sa.CheckConstraint("members > 0", name="ck_cut_photo_cluster_profile_members"),
    )


def downgrade() -> None:
    op.drop_table("cut_photo_cluster_profile")
    op.drop_index("ix_cut_photo_cluster_photo_cluster", table_name="cut_photo_cluster")
    op.drop_table("cut_photo_cluster")
    op.drop_table("cut_photo")
