from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, Float, ForeignKey, Index, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
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

    # --- Consensus features (per candidate term, computed over voting neighbors) ---
    neighbor_vote_fraction: Mapped[float | None] = mapped_column(Float, nullable=True)
    neighbor_min_distance: Mapped[float | None] = mapped_column(Float, nullable=True)
    neighbor_mean_distance: Mapped[float | None] = mapped_column(Float, nullable=True)

    # --- Taxonomy features ---
    query_taxonomy_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    ref_taxonomy_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    taxonomic_lca: Mapped[int | None] = mapped_column(Integer, nullable=True)
    taxonomic_distance: Mapped[int | None] = mapped_column(Integer, nullable=True)
    taxonomic_common_ancestors: Mapped[int | None] = mapped_column(Integer, nullable=True)
    taxonomic_relation: Mapped[str | None] = mapped_column(String(20), nullable=True)

    # --- Anc2Vec semantic-coherence features (GO 2020-10 pretrained) ---
    anc2vec_neighbor_cos: Mapped[float | None] = mapped_column(Float, nullable=True)
    anc2vec_neighbor_maxcos: Mapped[float | None] = mapped_column(Float, nullable=True)
    anc2vec_has_emb: Mapped[float | None] = mapped_column(Float, nullable=True)
    anc2vec_query_known_cos: Mapped[float | None] = mapped_column(Float, nullable=True)
    anc2vec_query_known_maxcos: Mapped[float | None] = mapped_column(Float, nullable=True)
    anc2vec_query_known_count: Mapped[float | None] = mapped_column(Float, nullable=True)

    # --- Taxonomic consensus across voting neighbors ---
    tax_voters_same_frac: Mapped[float | None] = mapped_column(Float, nullable=True)
    tax_voters_close_frac: Mapped[float | None] = mapped_column(Float, nullable=True)
    tax_voters_mean_common_ancestors: Mapped[float | None] = mapped_column(Float, nullable=True)

    # --- LAFA-system scalars (classifier / self-prior / association).
    # These six were the last feature families living only in the
    # ``features`` JSONB blob, outside the typed-column space. Promoting
    # them here makes the blob fully redundant so it can be dropped in a
    # separate reviewed step (ADR-D45). Nullable because legacy rows and
    # exports where the producer did not run carry a missing value, not a
    # zero; the export path emits NaN, never a well-defined 0.0.
    classifier_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    classifier_present: Mapped[float | None] = mapped_column(Float, nullable=True)
    self_prior_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    association_total: Mapped[float | None] = mapped_column(Float, nullable=True)
    association_cross: Mapped[float | None] = mapped_column(Float, nullable=True)
    association_present: Mapped[float | None] = mapped_column(Float, nullable=True)

    # --- T3.1a dual-write target: every feature column above is also
    # mirrored into this JSONB blob (see ``build_feature_jsonb``).
    # Readers stay on the typed columns until T3.1b cuts them over;
    # the column is nullable because legacy rows predate the dual-write.
    features: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # --- T3.1 dual-write target: the prediction tuple itself
    # (``go_term_id``, score, evidence) mirrored into a compact JSONB
    # blob via ``protea.core.jsonb_dual_write.maybe_jsonb``. Gated by
    # ``PROTEA_GO_PREDICTION_JSONB_WRITE_ENABLED``; off by default so
    # the scaffolding lands without touching production writers.
    # Readers stay on the typed columns until T3.3. The column is
    # nullable because legacy rows predate the dual-write; a GIN
    # index (``ix_go_prediction_jsonb_gin``) supports JSONB lookups.
    predictions_jsonb: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # --- Sequence-embedding PCA: per-query projection (16 components) ---
    emb_pca_query_0: Mapped[float | None] = mapped_column(Float, nullable=True)
    emb_pca_query_1: Mapped[float | None] = mapped_column(Float, nullable=True)
    emb_pca_query_2: Mapped[float | None] = mapped_column(Float, nullable=True)
    emb_pca_query_3: Mapped[float | None] = mapped_column(Float, nullable=True)
    emb_pca_query_4: Mapped[float | None] = mapped_column(Float, nullable=True)
    emb_pca_query_5: Mapped[float | None] = mapped_column(Float, nullable=True)
    emb_pca_query_6: Mapped[float | None] = mapped_column(Float, nullable=True)
    emb_pca_query_7: Mapped[float | None] = mapped_column(Float, nullable=True)
    emb_pca_query_8: Mapped[float | None] = mapped_column(Float, nullable=True)
    emb_pca_query_9: Mapped[float | None] = mapped_column(Float, nullable=True)
    emb_pca_query_10: Mapped[float | None] = mapped_column(Float, nullable=True)
    emb_pca_query_11: Mapped[float | None] = mapped_column(Float, nullable=True)
    emb_pca_query_12: Mapped[float | None] = mapped_column(Float, nullable=True)
    emb_pca_query_13: Mapped[float | None] = mapped_column(Float, nullable=True)
    emb_pca_query_14: Mapped[float | None] = mapped_column(Float, nullable=True)
    emb_pca_query_15: Mapped[float | None] = mapped_column(Float, nullable=True)

    prediction_set: Mapped[PredictionSet] = relationship(
        "PredictionSet", back_populates="predictions"
    )
    go_term: Mapped[GOTerm] = relationship("GOTerm")
