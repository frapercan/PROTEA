"""Why a method predicted a term for a protein (ADR-D60, decision 3).

One row per (prediction set, accession, GO term), the key ``go_prediction``
is unique on, holding the explanation as JSON. Only a method whose spec says it
``explains`` writes here, and what the payload holds is that method's own
schema. The rows live and die with their prediction set, like the predictions.
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import JSON, BigInteger, ForeignKey, String
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from protea.infrastructure.orm.base import Base

_JSONB = JSONB().with_variant(JSON(), "sqlite")


class PredictionExplanation(Base):
    """The explanation of one predicted term for one protein."""

    __tablename__ = "prediction_explanation"

    prediction_set_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("prediction_set.id", ondelete="CASCADE"),
        primary_key=True,
    )
    accession: Mapped[str] = mapped_column(String, primary_key=True)
    go_term_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("go_term.id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )
    payload: Mapped[dict[str, Any]] = mapped_column(_JSONB, nullable=False)
