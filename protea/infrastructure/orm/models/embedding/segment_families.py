"""The segment families of a cut under one lexicon (ADR-D65, decision 2).

One row per cut photo and lexicon bank: the cores of the lexicon's segments of
the photo's donors (its ``truth`` members) and targets (its other superset
members), linked by a pinned MMseqs2 into families under two scopes, as
``protea.core.segment_families.build`` wrote them on the cluster. The outputs
are files in the artifact store, not rows, as the homology table's hits are
(ADR-D61): the segments and the families of each scope, each by URI and sha256
in ``outputs``. The row carries the photo and the lexicon bank it was built
from, the MMseqs2 binary and the commands, the counts, the manifest by URI and
sha256, the job, the loader's revision and the builder's, and ADR-D58's two
descriptors.

Written only by ``load_segment_families``.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation.cut_photo import (
    DESCRIPTIONS_CHECK,
    SHORT_NAME_CHECK,
)

DIGESTS_CHECK = "length(manifest_sha256) = 64"
COUNTS_CHECK = "segments > 0"


class SegmentFamilies(Base):
    """The families of one cut's segments under one lexicon, as its verified manifest declared them."""

    __tablename__ = "segment_families"
    __table_args__ = (
        UniqueConstraint(
            "cut_photo_id", "lexicon_bank_id", name="uq_segment_families_photo_lexicon_bank"
        ),
        CheckConstraint(SHORT_NAME_CHECK, name="ck_segment_families_short_name"),
        CheckConstraint(DESCRIPTIONS_CHECK, name="ck_segment_families_descriptions"),
        CheckConstraint(DIGESTS_CHECK, name="ck_segment_families_digests"),
        CheckConstraint(COUNTS_CHECK, name="ck_segment_families_counts"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    #: The full photo whose truth members are the donors and whose superset gives the targets.
    cut_photo_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("cut_photo.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    #: The lexicon bank whose cut gave the segments.
    lexicon_bank_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("lexicon_bank.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    cut_release: Mapped[int] = mapped_column(Integer, nullable=False)
    #: The scopes whose families it holds (``strict``, ``permissive``).
    scopes: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    #: One row per segment of a donor or a target, in every output.
    segments: Mapped[int] = mapped_column(Integer, nullable=False)
    #: Every output: name, rows, uri and sha256.
    outputs: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    #: What the passes counted: prepare, search and the families of each scope.
    counts: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    #: The MMseqs2 binary (version and sha256) and the commands it ran.
    mmseqs: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    manifest: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    manifest_uri: Mapped[str] = mapped_column(String(512), nullable=False)
    manifest_sha256: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    #: ADR-D58, derived by the loader from the lexicon and the cut, never typed.
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
