from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, ForeignKey, Index, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from protea.infrastructure.orm.base import Base

if TYPE_CHECKING:
    from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot

#: An id the snapshot lists as a secondary id of a live term: the same term
#: under an older name. Resolving it is not a guess.
KIND_ALT_ID = "alt_id"
#: The successor an obsolete term names. A judgement the ontology published,
#: which is why it is kept apart from ``alt_id``: one says "this IS that", the
#: other says "that is what this became".
KIND_REPLACED_BY = "replaced_by"
#: A term an obsolete one says to CONSIDER. Not a successor: the ontology
#: declined to name one, so a remapping that used it would be inventing a
#: decision. Recorded because the absence of a successor is itself the
#: evidence that an annotation cannot be carried forward.
KIND_CONSIDER = "consider"

KINDS: tuple[str, ...] = (KIND_ALT_ID, KIND_REPLACED_BY, KIND_CONSIDER)


class GOTermAlias(Base):
    """What one ontology snapshot says about an id it no longer uses as primary.

    THE GAP THIS FILLS, MEASURED. ``go_term`` holds ``go_id``, ``name``,
    ``aspect``, ``definition`` and ``is_obsolete``, and nothing about
    identity across releases. Two consequences, and the second is the
    expensive one:

    * a temporal window cannot ask "does this t1 term have a node at t0",
      because the answer is yes through a successor it cannot see. On
      211 -> 227 that is nine entries and three proteins of 40,531;
    * a series over releases reads every MERGE as a retraction plus a gain.
      1,657 of 2016's terms are secondary ids by 2026, all of them with a
      known successor, and the churn figures of the whole GOA series carry
      that inflation.

    WHY A SIDE TABLE AND NOT TWO COLUMNS. ``alt_id`` is one-to-many: the
    2022-07-01 snapshot lists 3,590 of them over 47,328 terms, so a scalar
    column could not hold them and an array column would put a list where a
    join belongs. And ``go_term`` has 3.1 million rows across 66 snapshots,
    1,158 MB: this is 4,100 rows per snapshot, around 250,000 in all, and it
    is written without touching that table.

    One row per (snapshot, alias, kind): the same old id can be both the
    secondary id of one term and the ``consider`` of another, and collapsing
    them would lose which statement the ontology actually made.
    """

    __tablename__ = "go_term_alias"
    __table_args__ = (
        UniqueConstraint(
            "ontology_snapshot_id",
            "alias_go_id",
            "primary_go_id",
            "kind",
            name="uq_go_term_alias_identity",
        ),
        # The lookup the remapping does: given an id and a snapshot, what did
        # that snapshot say about it.
        Index("ix_go_term_alias_lookup", "ontology_snapshot_id", "alias_go_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ontology_snapshot_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("ontology_snapshot.id", ondelete="CASCADE"),
        nullable=False,
    )
    #: The id that is NOT primary in this snapshot: the one an annotation may
    #: still carry.
    alias_go_id: Mapped[str] = mapped_column(String(15), nullable=False)
    #: What this snapshot points it at. For ``alt_id`` the live term that
    #: lists it; for ``replaced_by`` and ``consider`` what the obsolete term
    #: names.
    primary_go_id: Mapped[str] = mapped_column(String(15), nullable=False)
    #: Which of the three statements the ontology made. NOT interchangeable:
    #: see the module constants.
    kind: Mapped[str] = mapped_column(String(16), nullable=False)

    ontology_snapshot: Mapped[OntologySnapshot] = relationship("OntologySnapshot")
