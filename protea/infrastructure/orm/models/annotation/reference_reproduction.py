"""One claim that a window's truth reproduces an external reference's.

WHY A ROW AND NOT A TEST. The comparison lived in
``tests/test_long_window_acceptance.py``, which is the right place to keep a
contract from rotting and the wrong place to keep a finding: a test says the
claim held on somebody's machine at some commit, and the platform could not
state it at all. The headline result of a campaign has to be something the
platform produced, with a job behind it and a digest beside it, or it is a
sentence in a chat log.

WHAT MAKES ONE CLAIM DIFFERENT FROM ANOTHER. The window, the reference, and
the reference's BYTES. The third is in the key because an acceptance contract
is a file that can be regenerated: a reference whose counters changed is a
different claim about the same window, and overwriting the first would lose
the disagreement that the change may have introduced. So a new digest writes
a new row and the old one stays readable.

WHAT IS STORED IS THE WHOLE VERDICT, not a boolean. ``compared`` is the
denominator any claim has to quote, ``absent`` names the counters the
reference published that this record does not keep, and ``disagreements``
carries both values for each counter that differed. A row saying "reproduces"
with no denominator is the kind of number this project keeps deleting.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import JSON, DateTime, ForeignKey, Index, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from protea.infrastructure.orm.base import Base

_JSONB = JSONB().with_variant(JSON(), "sqlite")


class ReferenceReproduction(Base):
    """A verified comparison between one window and one external reference."""

    __tablename__ = "reference_reproduction"
    __table_args__ = (
        # The window, the reference and the reference's bytes. See the module
        # docstring: a regenerated contract is a new claim, not an update.
        UniqueConstraint(
            "evaluation_set_id",
            "reference_key",
            "reference_digest",
            name="uq_reference_reproduction_claim",
        ),
        Index("ix_reference_reproduction_set", "evaluation_set_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    evaluation_set_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("evaluation_set.id"), nullable=False
    )
    #: What the reference calls this comparison, from its own contract -- the
    #: long window's is ``valid-211-227``. Read from the file and not coined
    #: here, so two sides naming one window differently is visible.
    reference_key: Mapped[str] = mapped_column(String(128), nullable=False)
    #: sha256 of the contract file as read, verified against the digest the
    #: caller declared before anything was compared.
    reference_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    #: The reference's own commit, when its contract states one. Null rather
    #: than empty: a reference that does not say is different from one that
    #: says nothing is pinned.
    reference_commit: Mapped[str | None] = mapped_column(String(64), nullable=True)
    #: Counters readable on both sides. The denominator.
    compared: Mapped[int] = mapped_column(Integer, nullable=False)
    agreed: Mapped[int] = mapped_column(Integer, nullable=False)
    #: ``[{counter, reference, ours}]`` for each counter that differed.
    disagreements: Mapped[list[dict[str, Any]]] = mapped_column(
        _JSONB, nullable=False, default=list
    )
    #: Counters the reference published that no window stores, each with why.
    absent: Mapped[dict[str, str]] = mapped_column(_JSONB, nullable=False, default=dict)
    job_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("job.id"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
