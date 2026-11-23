"""The photo of a cut (ADR-D59, decision 3).

``cut_photo`` is one row per cut: the GOA annotation set the photo's rows are
in, the ontology snapshot and the paired Swiss-Prot release it was loaded
against, the UniRef release its clusters come from, the channel table it
classified with, and its manifest by URI and sha256, with the job, the loader's
revision and the builder's. It carries ADR-D58's two descriptors.

``cut_photo_member`` is one row per member of the photo: the sets it entered by
(the superset, truth, curated inference, or as an annotated UniRef50 neighbour),
its UniRef50 cluster when it is in one that holds a superset protein, and the md5
of its sequence in the paired release when the release holds it. Neighbours enter
by UniRef, which no annotation records, so who is in a photo is stored rather
than derived (ADR-D59, decision 3, amended 2026-11-16, which retired the
per-cluster profiles and the separate cluster table). A counted entity, named by
its photo. The superset flags agree with the paired release's
``swissprot_membership`` by construction: the loader refuses a photo whose
superset is not that release.

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

#: ADR-D58, decision 2: a short descriptor is a lowercase slug of at most 64 characters.
SHORT_NAME_CHECK = "short_name ~ '^[a-z0-9][a-z0-9-]*$' AND length(short_name) <= 64"
DESCRIPTIONS_CHECK = "length(description) > 0"
#: A full photo names the UniRef release its neighbours came from; a truth-only photo
#: ends a window, has no neighbours and names none.
KIND_CHECK = (
    "(kind = 'full' AND uniref_release IS NOT NULL AND uniref_published_at IS NOT NULL "
    "AND uniref_clusters_sha256 IS NOT NULL) OR (kind = 'truth_only' AND uniref_release IS NULL "
    "AND uniref_published_at IS NULL AND uniref_clusters_sha256 IS NULL)"
)
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
        CheckConstraint(DESCRIPTIONS_CHECK, name="ck_cut_photo_descriptions"),
        CheckConstraint(DIGESTS_CHECK, name="ck_cut_photo_digests"),
        CheckConstraint(KIND_CHECK, name="ck_cut_photo_kind"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    #: ``full``, or ``truth_only`` for a cut that only ends a window (ADR-D59).
    kind: Mapped[str] = mapped_column(String(16), nullable=False, server_default="full")
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
    #: The UniRef release of the neighbours and the sha256 of its measured cluster table;
    #: NULL on a truth-only photo (``KIND_CHECK``).
    uniref_release: Mapped[str | None] = mapped_column(String(16), nullable=True)
    uniref_published_at: Mapped[date | None] = mapped_column(Date, nullable=True)
    uniref_clusters_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    #: The GO_REF list the channel table was checked against, and the table's digest.
    goref_list_read_on: Mapped[date] = mapped_column(Date, nullable=False)
    channel_table_sha256: Mapped[str] = mapped_column(
        String(64),
        ForeignKey(
            "evidence_channel_table.sha256",
            ondelete="RESTRICT",
            name="fk_cut_photo_channel_table",
        ),
        nullable=False,
    )
    manifest: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    manifest_uri: Mapped[str] = mapped_column(String(512), nullable=False)
    manifest_sha256: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    #: ADR-D58, derived by the loader from the cut, never typed.
    short_name: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    #: The loader's revision; the builder's is the one its manifest recorded.
    code_revision: Mapped[str | None] = mapped_column(String(64), nullable=True)
    builder_revision: Mapped[str | None] = mapped_column(String(64), nullable=True)
    job_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("job.id", ondelete="SET NULL"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


#: Every member entered by at least one set, and a neighbour by that set alone.
MEMBER_SETS_CHECK = "superset OR truth OR curated OR neighbour"
NEIGHBOUR_ALONE_CHECK = "NOT neighbour OR NOT (superset OR truth OR curated)"


class CutPhotoMember(Base):
    """One member of one photo: its sets, its UniRef50 cluster and its paired sequence."""

    __tablename__ = "cut_photo_member"
    __table_args__ = (
        CheckConstraint(MEMBER_SETS_CHECK, name="ck_cut_photo_member_sets"),
        CheckConstraint(NEIGHBOUR_ALONE_CHECK, name="ck_cut_photo_member_neighbour_alone"),
        Index("ix_cut_photo_member_photo_cluster", "photo_id", "cluster"),
    )

    photo_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("cut_photo.id", ondelete="CASCADE"), primary_key=True
    )
    accession: Mapped[str] = mapped_column(String(32), primary_key=True)
    superset: Mapped[bool] = mapped_column(Boolean, nullable=False)
    truth: Mapped[bool] = mapped_column(Boolean, nullable=False)
    curated: Mapped[bool] = mapped_column(Boolean, nullable=False)
    neighbour: Mapped[bool] = mapped_column(Boolean, nullable=False)
    cluster: Mapped[str | None] = mapped_column(String(64), nullable=True)
    #: The md5 of its sequence in the paired release; NULL when the release lacks it.
    sequence_md5: Mapped[str | None] = mapped_column(String(32), nullable=True)
    #: Its own NCBI taxon as the cut's GOA rows name it (protea.core.cut_photo.taxa);
    #: NULL for a member without rows at the cut or whose rows name two own taxa.
    taxon_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
