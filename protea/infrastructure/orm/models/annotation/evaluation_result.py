from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import DateTime, ForeignKey, Index, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from protea.infrastructure.orm.base import Base

if TYPE_CHECKING:
    from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
    from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
    from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel
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
                    "tau": 0.32, "coverage": 0.95, "n_proteins": 100,
                    "f_micro": 0.43, "fmax_w": 0.30,
                    "f_micro_w": 0.26, "precision_w": 0.33,
                    "recall_w": 0.22, "coverage_w": 0.95},
            "MFO": {...},
            "CCO": {...}
          },
          "LK": {...},
          "PK": {...}
        }

    ``f_micro_w`` is the IA-weighted micro-averaged F-measure, the headline
    metric shared with the LAFA / CAFA scorer, with its companion weighted
    micro precision / recall / coverage (``precision_w`` / ``recall_w`` /
    ``coverage_w``). The plain ``fmax`` / ``precision`` / ``recall`` are the
    unweighted equivalents kept for history; they are not LAFA-comparable
    (see docs/EVAL_LAFA_PARITY.md). The ``_w`` keys are populated only when a
    real IA file was supplied to cafaeval; with the uniform IC=1 fallback
    they collapse onto the unweighted values.
    """

    __tablename__ = "evaluation_result"
    __table_args__ = (
        # T3.5: ``list_evaluation_results`` filters by
        # ``evaluation_set_id`` and orders by ``created_at DESC``;
        # the composite turns that into a single index scan.
        Index(
            "ix_evaluation_result_eval_set_created_at",
            "evaluation_set_id",
            "created_at",
        ),
    )

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
    reranker_model_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("reranker_model.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    reranker_config: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
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
    reranker_model: Mapped[RerankerModel | None] = relationship("RerankerModel")
    job: Mapped[Job | None] = relationship("Job")
