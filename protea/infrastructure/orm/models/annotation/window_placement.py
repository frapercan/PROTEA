"""Where each target of a window sits on the sequence axes, read once at the window's start.

Every number is reported per stratum (:mod:`protea.core.strata`), and two of the axes
are properties of a target's sequence rather than of a method: its length, and its
identity to the closest protein whose function had been measured when the window
opened. Both are read here once per window, from the photo of the window's opening
cut (ADR-D59) and the homology table of that cut (ADR-D61), so every method evaluated
on the window is stratified on the same placement.

``window_placement`` declares one placement: the window, the photo and the homology
table it was read from, the rule, the counts, the job and the revision, and ADR-D58's
two descriptors. ``window_placement_target`` holds one row per target. Both are
written only by ``place_window_targets``, append-only: a window has one placement.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Double,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation.cut_photo import (
    DESCRIPTIONS_CHECK,
    SHORT_NAME_CHECK,
)

#: The rules a placement can be read by. One so far; a second one is a new row of
#: this vocabulary and a new placement, never a rewrite of the rows already written.
RULES_CHECK = "rule IN ('closest_truth_member')"
COUNTS_CHECK = (
    "targets > 0 AND targets_with_twin >= 0 AND targets_with_hit >= 0 "
    "AND targets_without_homolog >= 0 "
    "AND targets_with_twin + targets_with_hit + targets_without_homolog = targets"
)
#: A target is placed by an identical sequence under another accession (a twin, at
#: identity 100 and with no alignment), by a hit of the homology table (with its
#: alignment), or by nothing at all (no identity, no alignment).
CLOSEST_CHECK = (
    "(closest_by IS NULL AND closest_identity IS NULL AND closest_md5 IS NULL "
    "AND bits IS NULL) "
    "OR (closest_by = 'twin' AND closest_identity = 100 AND closest_md5 = sequence_md5 "
    "AND bits IS NULL) "
    "OR (closest_by = 'hit' AND closest_identity BETWEEN 0 AND 100 "
    "AND closest_md5 IS NOT NULL AND closest_md5 <> sequence_md5 AND bits IS NOT NULL)"
)


class WindowPlacement(Base):
    """One window's placement on the sequence axes, and what it was read from."""

    __tablename__ = "window_placement"
    __table_args__ = (
        CheckConstraint(SHORT_NAME_CHECK, name="ck_window_placement_short_name"),
        CheckConstraint(DESCRIPTIONS_CHECK, name="ck_window_placement_descriptions"),
        CheckConstraint(RULES_CHECK, name="ck_window_placement_rule"),
        CheckConstraint(COUNTS_CHECK, name="ck_window_placement_counts"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    #: The window whose targets are placed. One placement per window.
    evaluation_set_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("evaluation_set.id", ondelete="RESTRICT"),
        nullable=False,
        unique=True,
    )
    #: The full photo of the window's opening cut: the targets' sequences and the
    #: members with a truth row.
    cut_photo_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("cut_photo.id", ondelete="RESTRICT"), nullable=False
    )
    #: The homology table of that cut, whose hits place the targets without a twin.
    homology_table_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("homology_table.id", ondelete="RESTRICT"), nullable=False
    )
    rule: Mapped[str] = mapped_column(String(32), nullable=False)
    targets: Mapped[int] = mapped_column(Integer, nullable=False)
    targets_with_twin: Mapped[int] = mapped_column(Integer, nullable=False)
    targets_with_hit: Mapped[int] = mapped_column(Integer, nullable=False)
    targets_without_homolog: Mapped[int] = mapped_column(Integer, nullable=False)
    #: ADR-D58, derived from the window and its cuts, never typed.
    short_name: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    code_revision: Mapped[str | None] = mapped_column(String(64), nullable=True)
    job_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("job.id", ondelete="SET NULL"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class WindowPlacementTarget(Base):
    """One target of a window: its sequence at the window's start, and the closest truth member.

    ``closest_identity`` is a percentage. For a hit it is MMseqs2's ``fident`` times
    100, and ``bits``, ``evalue``, ``alnlen``, ``qcov`` and ``tcov`` are that hit's.
    """

    __tablename__ = "window_placement_target"
    __table_args__ = (
        CheckConstraint("residues > 0", name="ck_window_placement_target_residues"),
        CheckConstraint(CLOSEST_CHECK, name="ck_window_placement_target_closest"),
    )

    placement_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("window_placement.id", ondelete="CASCADE"),
        primary_key=True,
    )
    accession: Mapped[str] = mapped_column(String(64), primary_key=True)
    sequence_md5: Mapped[str] = mapped_column(String(32), nullable=False)
    residues: Mapped[int] = mapped_column(Integer, nullable=False)
    closest_by: Mapped[str | None] = mapped_column(String(8), nullable=True)
    closest_identity: Mapped[float | None] = mapped_column(Float, nullable=True)
    closest_md5: Mapped[str | None] = mapped_column(String(32), nullable=True)
    bits: Mapped[float | None] = mapped_column(Float, nullable=True)
    evalue: Mapped[float | None] = mapped_column(Double, nullable=True)
    alnlen: Mapped[int | None] = mapped_column(Integer, nullable=True)
    qcov: Mapped[float | None] = mapped_column(Float, nullable=True)
    tcov: Mapped[float | None] = mapped_column(Float, nullable=True)
