from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, Boolean, ForeignKey, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from protea.infrastructure.orm.base import Base

if TYPE_CHECKING:
    from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
    from protea.infrastructure.orm.models.annotation.protein_go_annotation import (
        ProteinGOAnnotation,
    )


class GOTerm(Base):
    """One row per GO term per ontology snapshot.

    GO terms are scoped to an ``OntologySnapshot`` so that the meaning of a
    term at a specific ontology release is preserved. ``(go_id,
    ontology_snapshot_id)`` is unique — the same GO:XXXXXXX can exist in
    multiple snapshots with potentially different names or definitions.
    """

    __tablename__ = "go_term"
    __table_args__ = (
        UniqueConstraint("go_id", "ontology_snapshot_id", name="uq_go_term_snapshot"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    go_id: Mapped[str] = mapped_column(String(15), nullable=False, index=True)
    name: Mapped[str | None] = mapped_column(String, nullable=True)
    aspect: Mapped[str | None] = mapped_column(String(1), nullable=True)
    definition: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_obsolete: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    ontology_snapshot_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("ontology_snapshot.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    ontology_snapshot: Mapped[OntologySnapshot] = relationship(
        "OntologySnapshot", back_populates="go_terms"
    )
    annotations: Mapped[list[ProteinGOAnnotation]] = relationship(
        "ProteinGOAnnotation", back_populates="go_term"
    )
