from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, Float, ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from protea.infrastructure.orm.base import Base

if TYPE_CHECKING:
    from protea.infrastructure.orm.models.annotation.go_term import GOTerm
    from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet


class GOPrediction(Base):
    """One predicted GO term for a protein within a prediction set.

    The prediction is derived by transferring annotations from the nearest
    reference protein (``ref_protein_accession``) in embedding space. The
    ``distance`` field records the cosine distance to that neighbor, which
    serves as a proxy for prediction confidence (lower = more similar).
    """

    __tablename__ = "go_prediction"
    __table_args__ = (
        UniqueConstraint(
            "prediction_set_id",
            "protein_accession",
            "go_term_id",
            name="uq_go_prediction_set_protein_term",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    prediction_set_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("prediction_set.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    protein_accession: Mapped[str] = mapped_column(
        String,
        nullable=False,
        index=True,
    )
    go_term_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("go_term.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    ref_protein_accession: Mapped[str] = mapped_column(String, nullable=False)
    distance: Mapped[float] = mapped_column(Float, nullable=False)
    qualifier: Mapped[str | None] = mapped_column(String, nullable=True)
    evidence_code: Mapped[str | None] = mapped_column(String, nullable=True)

    prediction_set: Mapped[PredictionSet] = relationship(
        "PredictionSet", back_populates="predictions"
    )
    go_term: Mapped[GOTerm] = relationship("GOTerm")
