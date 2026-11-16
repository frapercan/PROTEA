"""The photo of a cut (ADR-D59, decision 3).

``cut_photo`` is one row per cut: the GOA annotation set the photo's rows are
in, the ontology snapshot and the paired Swiss-Prot release it was loaded
against, the UniRef release its clusters come from, the channel table it
classified with, and its manifest by URI and sha256, with the job, the loader's
revision and the builder's. It carries ADR-D58's two descriptors, and two more
for its profile set, which ADR-D59 names a cited entity of its own: a profile
depends on the GOA cut AND on the UniRef clustering, so both are in its name.

``cut_photo_cluster`` gives each superset protein its UniRef50 cluster, so a
target finds its profile. ``cut_photo_cluster_profile`` holds, per cluster, how
many members outside the base carry each GO term in each profile channel:
counts, not proteins. Both are counted entities, named by their photo.

The superset itself is not copied: it is the paired release's
``swissprot_membership``, accession and sequence md5 included.

Append-only: a cut has at most one photo, and a later photo adds rows without
rewriting an earlier one.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Any

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from protea.infrastructure.orm.base import Base

#: The channels a profile may count: ``protea.core.cut_photo.channels.PROFILE_CHANNELS``,
#: restated so the schema does not import the builder (a test pins the two equal).
PROFILE_CHANNEL_CHECK = "channel IN ('curated', 'orthology_electronic', 'phylogenetic', 'truth')"

#: ADR-D58, decision 2: a short descriptor is a lowercase slug of at most 64 characters.
SHORT_NAME_CHECK = "short_name ~ '^[a-z0-9][a-z0-9-]*$' AND length(short_name) <= 64"
PROFILE_SHORT_NAME_CHECK = (
    "profile_short_name ~ '^[a-z0-9][a-z0-9-]*$' AND length(profile_short_name) <= 64"
)
DESCRIPTIONS_CHECK = "length(description) > 0 AND length(profile_description) > 0"
DIGESTS_CHECK = (
    "length(manifest_sha256) = 64 AND length(channel_table_sha256) = 64 "
    "AND length(uniref_clusters_sha256) = 64"
)


class CutPhoto(Base):
    """One cut's photo, as its verified manifest declared it."""

    __tablename__ = "cut_photo"
    __table_args__ = (
        CheckConstraint("cut_release > 0", name="ck_cut_photo_cut_release"),
        CheckConstraint(SHORT_NAME_CHECK, name="ck_cut_photo_short_name"),
        CheckConstraint(PROFILE_SHORT_NAME_CHECK, name="ck_cut_photo_profile_short_name"),
        CheckConstraint(DESCRIPTIONS_CHECK, name="ck_cut_photo_descriptions"),
        CheckConstraint(DIGESTS_CHECK, name="ck_cut_photo_digests"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    #: The GOA release of the cut, and its publication date, which is the cut's date.
    cut_release: Mapped[int] = mapped_column(Integer, nullable=False, unique=True)
    cut_date: Mapped[date] = mapped_column(Date, nullable=False)
    annotation_set_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("annotation_set.id", ondelete="RESTRICT"),
        nullable=False,
        unique=True,
    )
    ontology_snapshot_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("ontology_snapshot.id", ondelete="RESTRICT"), nullable=False
    )
    swissprot_release_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("swissprot_release.id", ondelete="RESTRICT"), nullable=False
    )
    uniref_release: Mapped[str] = mapped_column(String(16), nullable=False)
    uniref_published_at: Mapped[date] = mapped_column(Date, nullable=False)
    #: The sha256 of the cluster table the profiles were counted over.
    uniref_clusters_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    #: The GO_REF list the channel table was checked against, and the table's digest.
    goref_list_read_on: Mapped[date] = mapped_column(Date, nullable=False)
    channel_table_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    manifest: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    manifest_uri: Mapped[str] = mapped_column(String(512), nullable=False)
    manifest_sha256: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    #: ADR-D58, derived by the loader from the cut, never typed.
    short_name: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    profile_short_name: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    profile_description: Mapped[str] = mapped_column(Text, nullable=False)
    #: The loader's revision; the builder's is the one its manifest recorded.
    code_revision: Mapped[str | None] = mapped_column(String(64), nullable=True)
    builder_revision: Mapped[str | None] = mapped_column(String(64), nullable=True)
    job_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("job.id", ondelete="SET NULL"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class CutPhotoCluster(Base):
    """The UniRef50 cluster of one superset protein in one photo."""

    __tablename__ = "cut_photo_cluster"
    __table_args__ = (Index("ix_cut_photo_cluster_photo_cluster", "photo_id", "cluster"),)

    photo_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("cut_photo.id", ondelete="CASCADE"), primary_key=True
    )
    accession: Mapped[str] = mapped_column(String(32), primary_key=True)
    cluster: Mapped[str] = mapped_column(String(64), nullable=False)


class CutPhotoClusterProfile(Base):
    """How many members of a cluster outside the base carry a term in a channel."""

    __tablename__ = "cut_photo_cluster_profile"
    __table_args__ = (
        CheckConstraint(PROFILE_CHANNEL_CHECK, name="ck_cut_photo_cluster_profile_channel"),
        CheckConstraint("members > 0", name="ck_cut_photo_cluster_profile_members"),
    )

    photo_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("cut_photo.id", ondelete="CASCADE"), primary_key=True
    )
    cluster: Mapped[str] = mapped_column(String(64), primary_key=True)
    go_id: Mapped[str] = mapped_column(String(16), primary_key=True)
    channel: Mapped[str] = mapped_column(String(32), primary_key=True)
    negated: Mapped[bool] = mapped_column(Boolean, primary_key=True)
    members: Mapped[int] = mapped_column(Integer, nullable=False)
