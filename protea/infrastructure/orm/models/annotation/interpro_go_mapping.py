from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    DateTime,
    Index,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from protea.infrastructure.orm.base import Base


class InterProGoMapping(Base):
    """One InterPro entry to GO term mapping row (InterPro2GO).

    IP.4a of the executor plan (F-IP annotation ensemble). Persists the
    EBI ``interpro2go`` flat file as rows keyed by
    (``ipr_accession``, ``go_id``). Each line of the upstream file
    declares that the InterPro entry on the left implies the GO term on
    the right; the same IPR can map to many GO ids, and a single GO id
    can be claimed by many IPR entries, so the natural key is the pair.

    Loaded by :class:`LoadInterProGoMappingOperation` (idempotent
    upsert keyed by the ``uq_interpro_go_mapping_pair`` unique
    constraint). Released as a versioned snapshot via
    ``source_version`` so callers can pin the InterPro release used by
    a given prediction batch.

    Intentionally NOT FK-linked to ``GOTerm``: the EBI mapping file
    is independent of any specific OBO snapshot, so it can reference
    GO ids that are missing or obsolete in the active OntologySnapshot.
    Consumers join in application code where appropriate, so a release
    skew between InterPro2GO and the GO OBO is observable rather than
    a hard load error.
    """

    __tablename__ = "interpro_go_mapping"
    __table_args__ = (
        UniqueConstraint(
            "ipr_accession",
            "go_id",
            "source_version",
            name="uq_interpro_go_mapping_pair",
        ),
        Index("idx_interpro_go_mapping_ipr", "ipr_accession"),
        Index("idx_interpro_go_mapping_go", "go_id"),
        Index("idx_interpro_go_mapping_version", "source_version"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    ipr_accession: Mapped[str] = mapped_column(String(32), nullable=False)
    go_id: Mapped[str] = mapped_column(String(16), nullable=False)
    evidence: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_version: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    def __repr__(self) -> str:
        return (
            "<InterProGoMapping("
            f"ipr_accession={self.ipr_accession}, "
            f"go_id={self.go_id}, "
            f"source_version={self.source_version})>"
        )
