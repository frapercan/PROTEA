"""The lexicon bank of a set of photos under one lexicon (ADR-D63).

One row per bank the cluster computed and the platform loaded: every member's
sequence of the photos it names, decoded with one lexicon by the platform's
decoder. The outputs are files in the artifact store, not rows, as the homology
table's hits are (ADR-D61): per shard, the cut (segments and columns) and the
theme mass, each by URI and sha256 in ``shards``. The row carries the lexicon's
registered weights, the photos by their manifests' sha256, the counts, the card
and the software of each pass, the parity record measured against the
laboratory's code, the manifest by URI and sha256, the job, the loader's
revision and the builder's, and ADR-D58's two descriptors.

Written only by ``load_lexicon_bank``.
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

DIGESTS_CHECK = "length(manifest_sha256) = 64 AND length(sequences_sha256) = 64"
COUNTS_CHECK = "sequences > 0 AND residues > 0"


class LexiconBank(Base):
    """One lexicon's bank over a set of photos, as its verified manifest declared it."""

    __tablename__ = "lexicon_bank"
    __table_args__ = (
        UniqueConstraint(
            "model_weights_id", "manifest_sha256", name="uq_lexicon_bank_weights_manifest"
        ),
        CheckConstraint(SHORT_NAME_CHECK, name="ck_lexicon_bank_short_name"),
        CheckConstraint(DESCRIPTIONS_CHECK, name="ck_lexicon_bank_descriptions"),
        CheckConstraint(DIGESTS_CHECK, name="ck_lexicon_bank_digests"),
        CheckConstraint(COUNTS_CHECK, name="ck_lexicon_bank_counts"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    #: The lexicon's registered weights (kind ``lexicon``): what decoded every sequence.
    model_weights_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("model_weights.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    #: The lexicon's semantic code (``lexico-93k``), as the bank's paths spell it.
    name: Mapped[str] = mapped_column(String(64), nullable=False)
    #: The photos whose members it covers: cut, kind and manifest sha256 of each.
    photos: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    sequences: Mapped[int] = mapped_column(Integer, nullable=False)
    residues: Mapped[int] = mapped_column(BigInteger, nullable=False)
    #: The sha256 of the cluster's ``sequences.parquet``: the population, in its order.
    sequences_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    #: Every shard of every pass: pass, shard, rows, uri and sha256.
    shards: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    #: Per pass: its runs, its card and its software.
    passes: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    #: The decoder against the laboratory's code, as measured before the bank ran.
    parity: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    manifest: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    manifest_uri: Mapped[str] = mapped_column(String(512), nullable=False)
    manifest_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    #: ADR-D58, derived by the loader from the lexicon and the cuts, never typed.
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
