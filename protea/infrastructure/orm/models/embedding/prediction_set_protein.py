"""One protein of a prediction set, as the set's pages list it.

A set does not change after it is written, so how many terms each protein holds
in it and its closest neighbour's distance are counted once
(``protea.services._prediction_set_summary``) and read from here: the set's page
used to group its whole set of rows by protein on every visit, up to 12 s on a
set of 1.8 million candidate pairs.
"""

from __future__ import annotations

import uuid

from sqlalchemy import Float, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from protea.infrastructure.orm.base import Base


class PredictionSetProtein(Base):
    """A protein's terms in one set, and its closest neighbour's distance."""

    __tablename__ = "prediction_set_protein"

    prediction_set_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("prediction_set.id", ondelete="CASCADE", name="fk_prediction_set_protein_set"),
        primary_key=True,
    )
    protein_accession: Mapped[str] = mapped_column(String, primary_key=True)
    #: The set's (protein, term) pairs for this protein: candidate rows, or the
    #: length of a scored set's array.
    go_count: Mapped[int] = mapped_column(Integer, nullable=False)
    #: The closest neighbour's distance; NULL where the set records none (a
    #: scored set's arrays, a method's rows).
    min_distance: Mapped[float | None] = mapped_column(Float, nullable=True)
