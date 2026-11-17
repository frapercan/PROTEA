"""The homology table of a cut (ADR-D61).

One row per cut photo, declaring the hits the cluster built: the photo's superset
searched against all its members with a pinned MMseqs2, keyed by sequence md5. The
hits themselves are a parquet in the artifact store, referenced by URI and sha256,
as the information accretion tables are (ADR-D46): nobody joins them in SQL, and
tens of millions of rows per cut would cost the database more than any reader
gains. The row carries the MMseqs2 version and binary sha256, the command line, the
counts, the manifest by URI and sha256, the job, the loader's revision and the
builder's, and ADR-D58's two descriptors.

A cut has one table, as it has one photo: written only by ``load_homology_table``,
append-only.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation.cut_photo import (
    DESCRIPTIONS_CHECK,
    SHORT_NAME_CHECK,
)

DIGESTS_CHECK = (
    "length(manifest_sha256) = 64 AND length(hits_sha256) = 64 "
    "AND length(mmseqs_binary_sha256) = 64"
)
COUNTS_CHECK = (
    "queries > 0 AND bank > 0 AND hits >= 0 AND self_hits_dropped >= 0 "
    "AND queries_with_hits >= 0 AND queries_with_hits <= queries "
    "AND members_without_sequence >= 0"
)


class HomologyTable(Base):
    """One cut's homology table, as its verified manifest declared it."""

    __tablename__ = "homology_table"
    __table_args__ = (
        CheckConstraint("cut_release > 0", name="ck_homology_table_cut_release"),
        CheckConstraint(SHORT_NAME_CHECK, name="ck_homology_table_short_name"),
        CheckConstraint(DESCRIPTIONS_CHECK, name="ck_homology_table_descriptions"),
        CheckConstraint(DIGESTS_CHECK, name="ck_homology_table_digests"),
        CheckConstraint(COUNTS_CHECK, name="ck_homology_table_counts"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    #: The photo it was built from: its superset are the queries, its members the bank.
    cut_photo_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("cut_photo.id", ondelete="RESTRICT"),
        nullable=False,
        unique=True,
    )
    cut_release: Mapped[int] = mapped_column(Integer, nullable=False, unique=True)
    #: What ``mmseqs version`` answered and the sha256 of the binary that ran.
    mmseqs_version: Mapped[str] = mapped_column(String(64), nullable=False)
    mmseqs_binary_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    #: The command line, with paths reduced to file names.
    command: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    #: Unique query and bank sequences, and what the search returned.
    queries: Mapped[int] = mapped_column(Integer, nullable=False)
    bank: Mapped[int] = mapped_column(Integer, nullable=False)
    hits: Mapped[int] = mapped_column(BigInteger, nullable=False)
    self_hits_dropped: Mapped[int] = mapped_column(BigInteger, nullable=False)
    queries_with_hits: Mapped[int] = mapped_column(Integer, nullable=False)
    members_without_sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    hits_uri: Mapped[str] = mapped_column(String(512), nullable=False)
    hits_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
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
