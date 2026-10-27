from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from protea.infrastructure.orm.base import Base

if TYPE_CHECKING:
    from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
    from protea.infrastructure.orm.models.job import Job


class EvaluationSet(Base):
    """Result of comparing two AnnotationSets to produce CAFA-style evaluation data.

    Stores metadata and statistics about the delta between an old and a new GOA
    annotation set.  The actual ground-truth rows are computed on-demand from
    the stored annotation sets and streamed directly to the client.

    ``old_annotation_set_id`` is the reference (training) snapshot.
    ``new_annotation_set_id`` is the evaluation (ground-truth) snapshot.
    Delta proteins are those that gained at least one new experimental annotation
    between old → new.  NK/LK classification and NOT-qualifier propagation are
    applied during both generation and export.

    ``window_role`` binds this set to one of the rolling-origin protocol
    windows of ADR D40: ``"valid"`` marks the selection / threshold-tuning
    window (t-1 -> t0), ``"test"`` marks the report-once window (t0 -> t1),
    and ``None`` leaves the set unbound (ad-hoc episodes). A cell can be
    scored on the VALID set first and only then on the TEST set, which is
    the structural piece that lets selection happen off the reported
    window (no winner's-curse). The field is metadata only: it does not
    change how the delta is computed.

    ``pivot_snapshot_id``, ``old_native_snapshot_id`` and
    ``new_native_snapshot_id`` are the opposite: they change the delta, and
    they are part of the row's identity for that reason. Each side's ancestors
    are closed under its native DAG and the result is intersected with the
    pivot's term universe, so a rewiring between two ontology releases can put
    a term in the gain set that nobody annotated. Measured on GOA 220 -> 227,
    choosing native-per-side over pivot-for-both moves the PK bucket by 21
    percent of its annotations and by 51 percent of its proteins. Two rows
    differing only in these three are two different measurements and coexist.
    """

    __tablename__ = "evaluation_set"
    __table_args__ = (
        # T3.5: list endpoints order by ``created_at DESC``.
        Index("ix_evaluation_set_created_at", "created_at"),
        # What identifies one evaluation episode. The (old, new) pair alone
        # used to, and it does not: the same pair under a different
        # propagation graph is a different measurement, by 21 percent of the
        # PK bucket's annotations on the 220 -> 227 window. The pivot fixes
        # the term universe both sides are read into and the two natives fix
        # the DAG each side's ancestors are closed under, so all five are the
        # key. Enforced at the DB level via alembic revision
        # ``d4b8c2f10a37_evaluation_set_identity_includes_graphs``.
        # ``truth_profile`` joined them for the same reason the two natives
        # did: it decides what the delta IS. Under ``projected`` a t1 term
        # with no node at t0 is replaced by its first live ancestors instead
        # of being dropped by the closure, which on the 211 -> 227 window is
        # 646 proteins and 2,610 entries of difference. Without it in the key
        # the two truths could not coexist: the five ids are the same, so the
        # second one would be refused as a duplicate of the first.
        UniqueConstraint(
            "old_annotation_set_id",
            "new_annotation_set_id",
            "pivot_snapshot_id",
            "old_native_snapshot_id",
            "new_native_snapshot_id",
            "truth_profile",
            "toi_rule",
            "not_codes",
            name="uq_evaluation_set_identity",
        ),
        # ADR D40: keep the protocol-window vocabulary closed at the DB
        # level without a native enum type (cheaper to extend later).
        CheckConstraint(
            "window_role IS NULL OR window_role IN ('valid', 'test')",
            name="ck_evaluation_set_window_role",
        ),
        # Same reasoning, and NOT NULL rather than nullable: a NULL would be
        # distinct from every other NULL in the unique constraint above, so
        # two windows with no profile declared would both be accepted and the
        # key would stop being one.
        CheckConstraint(
            "truth_profile IN ('as_closed', 'projected')",
            name="ck_evaluation_set_truth_profile",
        ),
        CheckConstraint(
            "toi_rule IN ('pivot_terms', 'live_in_both_ends')",
            name="ck_evaluation_set_toi_rule",
        ),
        CheckConstraint(
            "not_codes IN ('any', 'experimental')",
            name="ck_evaluation_set_not_codes",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    old_annotation_set_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("annotation_set.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    new_annotation_set_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("annotation_set.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    # The three graphs that decide the delta, stored RESOLVED rather than as
    # the overrides that were requested: a row says which DAG actually ran, so
    # it can be read without its producing job. See the class docstring.
    pivot_snapshot_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("ontology_snapshot.id", ondelete="RESTRICT"),
        nullable=False,
    )
    old_native_snapshot_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("ontology_snapshot.id", ondelete="RESTRICT"),
        nullable=False,
    )
    new_native_snapshot_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("ontology_snapshot.id", ondelete="RESTRICT"),
        nullable=False,
    )
    job_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("job.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    stats: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    groundtruth_uri: Mapped[str | None] = mapped_column(String(512), nullable=True)
    # ADR D40: rolling-origin protocol window this set is bound to.
    # ``"valid"`` | ``"test"`` | ``None`` (unbound). Nullable so legacy
    # rows and ad-hoc episodes carry no role; a CHECK constraint keeps
    # the value set closed without a native enum (cheaper to evolve).
    window_role: Mapped[str | None] = mapped_column(String(8), nullable=True)
    #: How this window's t1 truth was built. ``as_closed`` is what every row
    #: written before this column existed did: a term the pivot cannot name
    #: is dropped by the closure intersection and the gain goes with it.
    #: ``projected`` replaces such a term by its first live ancestors at t0
    #: (see :mod:`protea.core._truth_projection`) before the binding rule and
    #: the closure.
    #:
    #: It is part of the identity above, not metadata: unlike
    #: ``window_role``, this one changes the delta. The counters of what the
    #: projection did land in ``stats``, because they are a property of the
    #: build and not of the truth.
    truth_profile: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default="as_closed"
    )
    #: Which terms the GAIN may contain. ``pivot_terms`` is what every row
    #: written before this column did: the gain is whatever the pivot can
    #: name, and the restriction to the terms of interest happens later, when
    #: cafaeval is told what to score. ``live_in_both_ends`` restricts it
    #: BEFORE the delta is classified, which is what the lab's frame does: a
    #: protein whose only gain is a term obsolete by t1 is not a target at
    #: all.
    #:
    #: In the identity for the same reason as ``truth_profile``, and
    #: SEPARATE from it for a different one: the projection decides what a
    #: gain CAN be and this decides which gains COUNT. Four combinations, not
    #: two, and folding them into one field would be the single-field
    #: comparison this project keeps paying for.
    toi_rule: Mapped[str] = mapped_column(
        String(24), nullable=False, server_default="pivot_terms"
    )
    #: Which NOT annotations may take truth away. ``any`` is what every row
    #: written before this column did: every NOT row whatever its evidence,
    #: so a ``NOT|IBA`` removes a term from a truth whose positives are
    #: experimental only. ``experimental`` restricts the negation to the
    #: thirteen experimental codes, which is the lab's frame and what
    #: Francisco declared for the long validation window on 2026-10-27.
    #:
    #: In the identity for the same reason as the other two: it changes the
    #: delta. Measured on 211 -> 227 it is three proteins of the delta, two
    #: of them LK, and nine projected entries: small, and exactly the
    #: difference between reproducing the lab's truth and missing it.
    not_codes: Mapped[str] = mapped_column(String(16), nullable=False, server_default="any")

    old_annotation_set: Mapped[AnnotationSet] = relationship(
        "AnnotationSet", foreign_keys=[old_annotation_set_id]
    )
    new_annotation_set: Mapped[AnnotationSet] = relationship(
        "AnnotationSet", foreign_keys=[new_annotation_set_id]
    )
    job: Mapped[Job | None] = relationship("Job")
