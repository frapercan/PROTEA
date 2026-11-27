"""The label-free statistics of a configuration's embedding space at a cut.

One row per configuration and cut photo: the statistics of the space of the
photo's distinct member sequences with a vector under the configuration, read
from the shards of the configuration's loaded bank in two streaming passes
(:mod:`protea.core.embedding_space.statistics`). The population depends on no
donor policy, so every breadth level of a pool searches one geometry.

The values live in one artifact, an ``.npz`` stored under
``embedding-space-statistics/<config>/<photo>/``: the mean, the per-dimension
variances, the eigendecomposition of the covariance, Ledoit and Wolf's
shrinkage, the removed directions of the scree elbow, the float16 rounding
variance and the counts and diagnostics, with a JSON document of every
recorded value. The row names the configuration,
the photo and the bank it was read from, the population's size ``n`` and the
dimension ``d``, the artifact by URI and sha256, a description its writer
derives, the job and the code revision.

Written only by ``compute_embedding_space_statistics``.
"""

from __future__ import annotations

import uuid
from datetime import datetime

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
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation.cut_photo import DESCRIPTIONS_CHECK

DIGESTS_CHECK = "length(artifact_sha256) = 64"
COUNTS_CHECK = "n > 1 AND d > 0"


class EmbeddingSpaceStatistics(Base):
    """The statistics of one configuration's space over one photo's member sequences."""

    __tablename__ = "embedding_space_statistics"
    __table_args__ = (
        UniqueConstraint(
            "embedding_config_id",
            "cut_photo_id",
            name="uq_embedding_space_statistics_config_photo",
        ),
        CheckConstraint(DESCRIPTIONS_CHECK, name="ck_embedding_space_statistics_descriptions"),
        CheckConstraint(DIGESTS_CHECK, name="ck_embedding_space_statistics_digests"),
        CheckConstraint(COUNTS_CHECK, name="ck_embedding_space_statistics_counts"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    #: The configuration whose space it measures.
    embedding_config_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("embedding_config.id", ondelete="RESTRICT"),
        nullable=False,
    )
    #: The photo whose distinct member sequences are the population.
    cut_photo_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("cut_photo.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    #: The bank whose shards gave the vectors.
    embedding_bank_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("embedding_bank.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    #: The population's size and the space's dimension.
    n: Mapped[int] = mapped_column(Integer, nullable=False)
    d: Mapped[int] = mapped_column(Integer, nullable=False)
    #: The ``.npz`` that holds every value, by URI and sha256.
    artifact_uri: Mapped[str] = mapped_column(String(512), nullable=False)
    artifact_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    #: What it is, derived by its writer from what it measured, never typed.
    description: Mapped[str] = mapped_column(Text, nullable=False)
    job_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("job.id", ondelete="SET NULL"), nullable=True
    )
    code_revision: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
