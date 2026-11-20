"""The scores of a scored set: per protein, its terms and their scores.

A scored set (``score_prediction_set``) is a candidate set's evidence turned into
one number per (protein, term) by a named arithmetic. The candidates and their
evidence stay where they are, in the candidate set's ``go_prediction`` rows; the
scored set holds the scores and nothing else.

WHY NOT GO_PREDICTION ROWS. A ``go_prediction`` row costs about 593 bytes on the
development store (2026-11-20: 24.2 million rows, 7.1 GB of table and 6.6 GB of
its eight indexes), most of it columns a score leaves empty and indexes a scored
set never uses. Here a scored set is one row per protein with two aligned
arrays: measured on the same store, the 691,226 pairs of one nearest-neighbour
set scored with ``embedding_only`` take 13.97 MB, 20.2 bytes per pair, about 29
times less than that average, and a scoring sweep over several configurations
and every language model stays small.

Scores are ``double precision``, as ``go_prediction.method_score`` is, so the
prediction file written from either form is the same to the last digit.
"""

from __future__ import annotations

import uuid

from sqlalchemy import BigInteger, CheckConstraint, ForeignKey, String
from sqlalchemy.dialects.postgresql import ARRAY, DOUBLE_PRECISION, UUID
from sqlalchemy.orm import Mapped, mapped_column

from protea.infrastructure.orm.base import Base

#: The two arrays describe the same pairs: the n-th score is the n-th term's.
ALIGNED_CHECK = "cardinality(go_term_ids) = cardinality(scores)"
#: A protein with no scored term has no row, rather than an empty one.
NOT_EMPTY_CHECK = "cardinality(go_term_ids) > 0"


class PredictionScore(Base):
    """One protein of a scored set: its terms, ascending, and their scores."""

    __tablename__ = "prediction_score"
    __table_args__ = (
        CheckConstraint(ALIGNED_CHECK, name="ck_prediction_score_aligned"),
        CheckConstraint(NOT_EMPTY_CHECK, name="ck_prediction_score_not_empty"),
    )

    prediction_set_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("prediction_set.id", ondelete="CASCADE"),
        primary_key=True,
    )
    protein_accession: Mapped[str] = mapped_column(String, primary_key=True)
    #: ``go_term.id`` of each scored term, ascending. An array cannot carry a
    #: foreign key; the writer takes them from the candidate rows, which do.
    go_term_ids: Mapped[list[int]] = mapped_column(ARRAY(BigInteger), nullable=False)
    scores: Mapped[list[float]] = mapped_column(ARRAY(DOUBLE_PRECISION), nullable=False)
