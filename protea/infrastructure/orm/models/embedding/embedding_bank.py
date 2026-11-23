"""The embedding bank of a set of photos under one configuration (ADR-D62).

One row per bank the cluster computed and the platform loaded: every member's
sequence of the photos it names, embedded with one configuration's model through
the padded pass of its protea-backends plugin. The vectors are rows of
``sequence_embedding``, each of which names the bank that wrote it
(``sequence_embedding.embedding_bank_id``); a row that was already there when the
bank loaded keeps its own provenance and is counted, not overwritten.

The row carries the photos by their manifests' sha256, the model's revision, the
software and the card, the token budgets of the padded pass, the counts, the
manifest by URI and sha256, the job, the loader's revision and the builder's, and
ADR-D58's two descriptors. ``status`` is ``loading`` while the shards go in, one
transaction each (``shards_loaded`` is where a rerun resumes), and ``loaded`` once
the last one has.

Written only by ``load_embedding_bank``.
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
COUNTS_CHECK = (
    "sequences > 0 AND residues > 0 AND dim > 0 AND shards > 0 "
    "AND shards_loaded >= 0 AND shards_loaded <= shards "
    "AND rows_inserted >= 0 AND rows_present >= 0 "
    "AND rows_inserted + rows_present <= sequences"
)
STATUS_CHECK = (
    "(status = 'loading' AND loaded_at IS NULL) "
    "OR (status = 'loaded' AND loaded_at IS NOT NULL AND shards_loaded = shards "
    "AND rows_inserted + rows_present = sequences)"
)


class EmbeddingBank(Base):
    """One configuration's bank over a set of photos, as its verified manifest declared it."""

    __tablename__ = "embedding_bank"
    __table_args__ = (
        UniqueConstraint(
            "embedding_config_id", "manifest_sha256", name="uq_embedding_bank_config_manifest"
        ),
        CheckConstraint(SHORT_NAME_CHECK, name="ck_embedding_bank_short_name"),
        CheckConstraint(DESCRIPTIONS_CHECK, name="ck_embedding_bank_descriptions"),
        CheckConstraint(DIGESTS_CHECK, name="ck_embedding_bank_digests"),
        CheckConstraint(COUNTS_CHECK, name="ck_embedding_bank_counts"),
        CheckConstraint(STATUS_CHECK, name="ck_embedding_bank_status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    #: The recipe every vector of the bank was computed with.
    embedding_config_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("embedding_config.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    #: The photos whose members it covers: cut, kind and manifest sha256 of each.
    photos: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    #: Unique member sequences, their residues, the vector's width and the shards.
    sequences: Mapped[int] = mapped_column(Integer, nullable=False)
    residues: Mapped[int] = mapped_column(BigInteger, nullable=False)
    dim: Mapped[int] = mapped_column(Integer, nullable=False)
    shards: Mapped[int] = mapped_column(Integer, nullable=False)
    #: Shards written so far, in the manifest's order: where a rerun resumes.
    shards_loaded: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    #: Rows this bank wrote, and rows that were already there and were kept.
    rows_inserted: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    rows_present: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    #: The sha256 of the cluster's ``sequences.parquet``: the population, in its order.
    sequences_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    #: What computed it: the weights' commit, the software, the card, the batch budgets.
    model_revision: Mapped[str] = mapped_column(String(64), nullable=False)
    software: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    card: Mapped[str] = mapped_column(String(128), nullable=False)
    token_budgets: Mapped[list[int]] = mapped_column(JSONB, nullable=False)
    manifest: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    manifest_uri: Mapped[str] = mapped_column(String(512), nullable=False)
    manifest_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="loading")
    #: ADR-D58, derived by the loader from the model and the cuts, never typed.
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
    loaded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
