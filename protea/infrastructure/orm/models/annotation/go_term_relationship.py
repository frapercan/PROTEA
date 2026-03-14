from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from protea.infrastructure.orm.base import Base

if TYPE_CHECKING:
    from protea.infrastructure.orm.models.annotation.go_term import GOTerm
    from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot


class GOTermRelationship(Base):
    """Directed edge in the GO DAG for a specific ontology snapshot.

    ``child_go_term_id`` → ``parent_go_term_id`` with a given ``relation_type``
    (``is_a``, ``part_of``, ``regulates``, ``negatively_regulates``,
    ``positively_regulates``).
    """

    __tablename__ = "go_term_relationship"
    __table_args__ = (
        UniqueConstraint(
            "child_go_term_id", "parent_go_term_id", "relation_type",
            name="uq_go_term_relationship",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    child_go_term_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("go_term.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    parent_go_term_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("go_term.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    relation_type: Mapped[str] = mapped_column(String(40), nullable=False)
    ontology_snapshot_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("ontology_snapshot.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    child: Mapped[GOTerm] = relationship("GOTerm", foreign_keys=[child_go_term_id])
    parent: Mapped[GOTerm] = relationship("GOTerm", foreign_keys=[parent_go_term_id])
    ontology_snapshot: Mapped[OntologySnapshot] = relationship("OntologySnapshot")
