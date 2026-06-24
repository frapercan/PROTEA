from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, String, func
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

    **Method-surface provenance** (slice F-METHOD-EVAL-SURFACE). Four
    optional markers make every benchmark number on ``/benchmark`` and
    ``/evaluation`` self-describing without joining back to the run that
    produced it. All four are nullable with no server default, so legacy
    rows and existing flows are untouched (they read back as ``None`` and
    the UI shows an "unknown" empty state):

    - ``frame``: the scoring frame the metrics live in. ``"lafa"`` marks
      the parity-locked LAFA-frame harness (the leaderboard-comparable
      number); ``"internal"`` marks the lab / full-GT frame. ``None`` for
      rows scored before the frame was recorded.
    - ``temporal_window``: the rolling-origin window label, e.g.
      ``"SELECT_220_227"`` (selection window) or ``"FINAL_227_230"`` (the
      report-once test window). Free text so new windows do not need a
      schema change; ``"other"`` / ``None`` cover ad-hoc episodes.
    - ``arms_enabled``: which method arms contributed, as a flag dict
      (``{"knn": true, "reranker": true, "mlp_tower": false,
      "interpro": false}``). A dict (not a bitmask) so the set of arms can
      grow without a migration; ``None`` means the composition was not
      recorded.
    - ``leakage_role``: the leakage-hygiene role of this evaluation under
      ADR D40. ``"select"`` = a result used for model / threshold
      selection (must be on the SELECT window), ``"test"`` = the
      report-once measurement on the held-out window, ``"probe"`` = an
      exploratory read that must not feed selection. ``None`` for rows
      that predate the protocol marker.
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
        # F-METHOD-EVAL-SURFACE: keep the frame + leakage-role vocabularies
        # closed at the DB level without a native enum type (cheaper to
        # extend later, mirrors ``ck_evaluation_set_window_role``).
        CheckConstraint(
            "frame IS NULL OR frame IN ('lafa', 'internal')",
            name="ck_evaluation_result_frame",
        ),
        CheckConstraint(
            "leakage_role IS NULL OR leakage_role IN ('select', 'test', 'probe')",
            name="ck_evaluation_result_leakage_role",
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
    # F-METHOD-EVAL-SURFACE provenance (all nullable, no server default so
    # legacy rows and existing flows are unaffected). See the class
    # docstring for the vocabulary of each field.
    frame: Mapped[str | None] = mapped_column(String(8), nullable=True)
    temporal_window: Mapped[str | None] = mapped_column(String(32), nullable=True)
    arms_enabled: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    leakage_role: Mapped[str | None] = mapped_column(String(8), nullable=True)

    evaluation_set: Mapped[EvaluationSet] = relationship("EvaluationSet")
    prediction_set: Mapped[PredictionSet] = relationship("PredictionSet")
    scoring_config: Mapped[ScoringConfig | None] = relationship("ScoringConfig")
    reranker_model: Mapped[RerankerModel | None] = relationship("RerankerModel")
    job: Mapped[Job | None] = relationship("Job")
