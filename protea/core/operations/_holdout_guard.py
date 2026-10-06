"""One place that asks whether a window may inform a decision.

Two operations can touch the holdout and they touch it differently:
:mod:`generate_evaluation_set` DEFINES a window, and :mod:`run_cafa_evaluation`
produces a NUMBER against one. Both have to ask, because a window defined
before this guard existed can still be scored today, and the leak happens when
the number is produced rather than when the window is named.

They ask through here rather than each resolving the date itself, so there is
one answer to "which end of the window is compared against the mark" instead of
two that can drift. The rule itself lives in
:func:`protea.core.split_registry.assert_window_may_inform`; this only knows how
to find the date it needs in the database.
"""

from __future__ import annotations

from datetime import date, datetime
from uuid import UUID

from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn
from protea.core.split_registry import assert_window_may_inform
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet


def _as_date(value: date | datetime) -> date:
    return value.date() if isinstance(value, datetime) else value


def refuse_if_the_set_reads_the_holdout(
    new_set: AnnotationSet | None, *, waiver: str | None, context: str, emit: EmitFn | None = None
) -> None:
    """The rule, for a caller that already holds the corpus it is ending at.

    Separate from the id form so a caller does not pay a second lookup for a row
    it has already resolved. That is not only cost: an extra query inside an
    operation is an extra thing a test double has to expect, and the first
    version of this guard broke three existing tests by asking the session for a
    row the operation had in hand.
    """
    if new_set is None or new_set.source_published_at is None:
        # PASSING FOR LACK OF A DATE IS NOT THE SAME AS PASSING, and it must not
        # be silent. The original justification was that
        # ``refresh_goa_release_dates`` "has run for every set this platform
        # holds". That was true when this was written and is false during a
        # rebuild: phase 2 of the clean campaign creates 75 annotation sets and
        # that job has not run on any of them, so this branch would be the COMMON
        # case and the guard would be inert exactly when it is needed. A guard
        # that cannot decide has to say so.
        if emit is not None:
            emit(
                "holdout_guard.undecidable",
                "the holdout guard could not decide: no publication date on the corpus",
                {
                    "context": context,
                    "annotation_set": getattr(new_set, "source_version", None),
                    "remedy": "run refresh_goa_release_dates before building windows",
                },
                "warning",
            )
        return
    assert_window_may_inform(
        _as_date(new_set.source_published_at),
        waiver=waiver,
        context=f"{context} {new_set.source_version}",
    )


def refuse_if_it_reads_the_holdout(
    session: Session,
    new_annotation_set_id: UUID,
    *,
    waiver: str | None,
    context: str,
    emit: EmitFn | None = None,
) -> None:
    """Raise unless the window ending at this corpus may inform a decision.

    A corpus with no recorded publication date is passed rather than refused,
    because the date is what the rule compares and refusing every window on a set
    that predates the date column would take down the tune windows to protect the
    holdout from a case that cannot be evaluated either way.

    But it is passed LOUDLY: see the sibling function. The absence is not narrow
    during a rebuild, when no set has a date yet, and a guard that passes in
    silence for lack of an input is indistinguishable from a guard that approved.
    """
    refuse_if_the_set_reads_the_holdout(
        session.get(AnnotationSet, new_annotation_set_id),
        waiver=waiver,
        context=context,
        emit=emit,
    )
