from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import DateTime, ForeignKey, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from protea.infrastructure.orm.base import Base

if TYPE_CHECKING:
    from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
    from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
    from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig
    from protea.infrastructure.orm.models.job import Job


class EvaluationResult(Base):
    """Fmax / PR / RC results from running the CAFA evaluator on a prediction set.

    Stores per-setting (NK/LK/PK) and per-namespace (BPO/MFO/CCO) metrics
    in a JSONB column so they can be displayed in the UI without additional
    queries.

    ``results`` structure::

        {
          "NK": {
            "BPO": {"fmax": 0.45, "precision": 0.51, "recall": 0.40,
                    "tau": 0.32, "coverage": 0.95, "n_proteins": 100},
            "MFO": {...},
            "CCO": {...}
          },
          "LK": {...},
          "PK": {...}
        }
    """

    __tablename__ = "evaluation_result"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    evaluation_set_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("evaluation_set.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    prediction_set_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("prediction_set.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    scoring_config_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("scoring_config.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    job_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("job.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    results: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)

    evaluation_set: Mapped[EvaluationSet] = relationship("EvaluationSet")
    prediction_set: Mapped[PredictionSet] = relationship("PredictionSet")
    scoring_config: Mapped[ScoringConfig | None] = relationship("ScoringConfig")
    job: Mapped[Job | None] = relationship("Job")
