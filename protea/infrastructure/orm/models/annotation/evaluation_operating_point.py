"""The reading of one evaluation result at a declared threshold, per cell and stratum.

One row per (evaluation result, declared threshold): the result is read at
each cell's threshold of a weights row that declares per-cell thresholds
(``operating_threshold``, and every fitted reading that declares its own). Per
cell (``cells``): the threshold and whether it sits at the grid's edge;
``f_micro_w`` with ``f_w``, the precisions and recalls, the coverage and S in
both weightings at that threshold; S's minimum and its threshold; the oracle
maximum with its threshold and edge, and its distance from half its value. Per
stratum (the identity band by length band inside each cell): the same numbers
with the stratum's size, a stratum under the population floor flagged and never
withheld, in a parquet named by URI and sha256.

``t0_category_disagreement`` is the 3 x 3 table of the t0 category (what a
method may key by, :mod:`protea.core.t0_category`) against the evaluator's
category over the window's benchmark pairs; NULL when the window's frame states
no t0 category.

Written only by ``read_operating_point``.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, String, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation.cut_photo import DESCRIPTIONS_CHECK

DIGESTS_CHECK = "length(strata_sha256) = 64"


class EvaluationOperatingPoint(Base):
    """An evaluation result read at the per-cell thresholds one weights row declares."""

    __tablename__ = "evaluation_operating_point"
    __table_args__ = (
        UniqueConstraint(
            "evaluation_result_id",
            "declared_threshold_id",
            name="uq_evaluation_operating_point_result_threshold",
        ),
        CheckConstraint(DESCRIPTIONS_CHECK, name="ck_evaluation_operating_point_descriptions"),
        CheckConstraint(DIGESTS_CHECK, name="ck_evaluation_operating_point_digests"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    #: The result read.
    evaluation_result_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("evaluation_result.id", ondelete="RESTRICT"),
        nullable=False,
    )
    #: The weights whose artifact declares the per-cell thresholds it was read at.
    declared_threshold_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("model_weights.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    #: Per cell (``"NK:BPO"``...), the numbers read at the cell's declared threshold.
    cells: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    #: The t0 category against the evaluator's, over the window's benchmark pairs.
    t0_category_disagreement: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    #: The per-stratum parquet, by URI and sha256.
    strata_uri: Mapped[str] = mapped_column(String(512), nullable=False)
    strata_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    #: What it is, derived by its writer from what it read, never typed.
    description: Mapped[str] = mapped_column(Text, nullable=False)
    job_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("job.id", ondelete="SET NULL"), nullable=True
    )
    code_revision: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
