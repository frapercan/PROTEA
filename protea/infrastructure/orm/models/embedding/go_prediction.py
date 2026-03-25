from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, Float, ForeignKey, Index, Integer, String, UniqueConstraint
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
        Index("ix_go_prediction_set_accession", "prediction_set_id", "protein_accession"),
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

    # --- Alignment features (Needleman–Wunsch global) ---
    identity_nw: Mapped[float | None] = mapped_column(Float, nullable=True)
    similarity_nw: Mapped[float | None] = mapped_column(Float, nullable=True)
    alignment_score_nw: Mapped[float | None] = mapped_column(Float, nullable=True)
    gaps_pct_nw: Mapped[float | None] = mapped_column(Float, nullable=True)
    alignment_length_nw: Mapped[float | None] = mapped_column(Float, nullable=True)

    # --- Alignment features (Smith–Waterman local) ---
    identity_sw: Mapped[float | None] = mapped_column(Float, nullable=True)
    similarity_sw: Mapped[float | None] = mapped_column(Float, nullable=True)
    alignment_score_sw: Mapped[float | None] = mapped_column(Float, nullable=True)
    gaps_pct_sw: Mapped[float | None] = mapped_column(Float, nullable=True)
    alignment_length_sw: Mapped[float | None] = mapped_column(Float, nullable=True)

    # --- Sequence lengths (populated when alignments are computed) ---
    length_query: Mapped[int | None] = mapped_column(Integer, nullable=True)
    length_ref: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # --- Re-ranker features ---
    vote_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    k_position: Mapped[int | None] = mapped_column(Integer, nullable=True)
    go_term_frequency: Mapped[int | None] = mapped_column(Integer, nullable=True)
    ref_annotation_density: Mapped[int | None] = mapped_column(Integer, nullable=True)
    neighbor_distance_std: Mapped[float | None] = mapped_column(Float, nullable=True)

    # --- Taxonomy features ---
    query_taxonomy_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    ref_taxonomy_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    taxonomic_lca: Mapped[int | None] = mapped_column(Integer, nullable=True)
    taxonomic_distance: Mapped[int | None] = mapped_column(Integer, nullable=True)
    taxonomic_common_ancestors: Mapped[int | None] = mapped_column(Integer, nullable=True)
    taxonomic_relation: Mapped[str | None] = mapped_column(String(20), nullable=True)

    prediction_set: Mapped[PredictionSet] = relationship(
        "PredictionSet", back_populates="predictions"
    )
    go_term: Mapped[GOTerm] = relationship("GOTerm")
