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

from protea.core.split_registry import assert_window_may_inform
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet


def _as_date(value: date | datetime) -> date:
    return value.date() if isinstance(value, datetime) else value


def refuse_if_it_reads_the_holdout(
    session: Session,
    new_annotation_set_id: UUID,
    *,
    waiver: str | None,
    context: str,
) -> None:
    """Raise unless the window ending at this corpus may inform a decision.

    A corpus with no recorded publication date is passed rather than refused.
    The date is what the rule compares, so without one there is nothing to
    compare, and refusing every window on a set that predates the date column
    would take down the tune windows to protect the holdout from a case that
    cannot be evaluated either way. The absence is narrow and visible:
    ``refresh_goa_release_dates`` fills the column and has run for every set
    this platform holds.
    """
    new_set = session.get(AnnotationSet, new_annotation_set_id)
    if new_set is None or new_set.source_published_at is None:
        return
    assert_window_may_inform(
        _as_date(new_set.source_published_at),
        waiver=waiver,
        context=f"{context} {new_set.source_version}",
    )
