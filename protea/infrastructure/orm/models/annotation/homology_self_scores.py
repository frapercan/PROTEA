"""The self-alignment bit scores of a homology table's queries.

A homology table (ADR-D61) drops the hit of a sequence with itself, so it cannot
say how strongly a target aligns with a member that shares its sequence under
another accession (a twin). A homology transfer that weighs its voters by bit
score needs that number, and it is the target's own self-alignment score. This
table declares, for one homology table, the bit score of every query sequence
aligned with itself: forced self alignments on the cluster
(``protea.core.homology.self_scores``, run by ``deploy/cluster/homology_self_scores.sh``),
with the table's MMseqs2 and its scoring.

The scores are a parquet in the artifact store, referenced by URI and sha256, as
the hits are: a cut's table has about 480,000 queries. The row names the
homology table it scores (a ``RESTRICT`` foreign key, unique: one row per table),
the MMseqs2 version and binary sha256, the command lines, how many queries the
table has and how many the forced alignment could not score (a degenerate
sequence), the manifest by URI and sha256, the job, the loader's revision and the
builder's, and ADR-D58's two descriptors.

Written only by ``load_homology_self_scores``, append-only.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation.cut_photo import (
    DESCRIPTIONS_CHECK,
    SHORT_NAME_CHECK,
)

DIGESTS_CHECK = (
    "length(manifest_sha256) = 64 AND length(scores_sha256) = 64 "
    "AND length(mmseqs_binary_sha256) = 64"
)
COUNTS_CHECK = (
    "queries > 0 AND queries_without_self_score >= 0 AND queries_without_self_score <= queries"
)


class HomologySelfScores(Base):
    """The self-alignment bit score of every query of one homology table."""

    __tablename__ = "homology_self_scores"
    __table_args__ = (
        CheckConstraint(SHORT_NAME_CHECK, name="ck_homology_self_scores_short_name"),
        CheckConstraint(DESCRIPTIONS_CHECK, name="ck_homology_self_scores_descriptions"),
        CheckConstraint(DIGESTS_CHECK, name="ck_homology_self_scores_digests"),
        CheckConstraint(COUNTS_CHECK, name="ck_homology_self_scores_counts"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    #: The homology table whose queries were aligned with themselves.
    homology_table_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("homology_table.id", ondelete="RESTRICT"),
        nullable=False,
        unique=True,
    )
    #: What ``mmseqs version`` answered and the sha256 of the binary that ran.
    mmseqs_version: Mapped[str] = mapped_column(String(64), nullable=False)
    mmseqs_binary_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    #: The command lines of the forced alignment, with paths reduced to file names.
    command: Mapped[list[Any]] = mapped_column(JSONB, nullable=False)
    #: The table's queries, and those the forced alignment could not score.
    queries: Mapped[int] = mapped_column(Integer, nullable=False)
    queries_without_self_score: Mapped[int] = mapped_column(Integer, nullable=False)
    scores_uri: Mapped[str] = mapped_column(String(512), nullable=False)
    scores_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
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
