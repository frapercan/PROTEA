"""A depth counted in a unit the candidates do not carry scores nothing.

``sequence_rank`` is null on every row retrieved before the column
existed. SQL being what it is, ``sequence_rank <= 2`` is null for those
rows and null is not true, so a sequence-depth cut against them selects
**no rows at all**. The evaluation then runs on an empty candidate set,
writes its metrics and reports success. This is the shape of failure
this project keeps meeting, and it is worth naming precisely: the
system goes on working while doing nothing, and the zero it produces
does not carry where it came from.

So the unit is checked against the prediction set before anything is
scored, once per run rather than per row. Per row would be worse than
useless: a run that recounted some rows and inherited others would
produce a number that is not comparable with itself, which no label can
repair.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from sqlalchemy import func, select

from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

__all__ = (
    "DepthUnitUnavailable",
    "assert_depth_unit_is_available",
    "ledger_coverage",
    "why_the_unit_is_unavailable",
)


class DepthUnitUnavailable(RuntimeError):
    """A depth was asked for in a unit the stored candidates cannot answer."""


def ledger_coverage(session: Session, pred_set_id: uuid.UUID) -> tuple[int, int, int]:
    """How much of one prediction set can answer a depth question.

    Returns:
        ``(rows, with_sequence_rank, with_donor_ledger)``. Reported as
        three counts rather than a verdict so the caller can say how far
        off it is, which is the difference between "this set predates the
        column" and "half of it was written by a stale worker".
    """
    row = session.execute(
        select(
            func.count(GOPrediction.id),
            func.count(GOPrediction.sequence_rank),
            func.count(GOPrediction.donor_count),
        ).where(GOPrediction.prediction_set_id == pred_set_id)
    ).one()
    return int(row[0]), int(row[1]), int(row[2])


def why_the_unit_is_unavailable(
    rows: int, ranked: int, max_sequence_rank: int, pred_set_id: object
) -> str | None:
    """The verdict, given the coverage. Separated from the query on purpose.

    The query and the decision fail in different ways and are worth
    testing apart: a decision tested through a fake query proves nothing
    about the query, and a query whose result nothing reads returns a
    zero that means "my lookup missed" rather than "there is nothing
    there". Both halves have been wrong here before.

    Returns:
        None when the depth can be answered, otherwise the sentence
        explaining what is missing.
    """
    if rows == 0:
        return (
            f"prediction set {pred_set_id} holds no candidates, so a depth of "
            f"{max_sequence_rank} sequences would score an empty set and report it"
        )
    if ranked == rows:
        return None
    return (
        f"a depth of {max_sequence_rank} sequences was asked of prediction set "
        f"{pred_set_id}, which carries a sequence rank on {ranked} of its {rows} "
        f"candidates. The unranked ones cannot answer in that unit and SQL would "
        f"drop them silently, so the run would score "
        f"{'nothing at all' if ranked == 0 else 'only the ranked part'} and "
        f"report success. Re-retrieve the set, or count this depth in proteins."
    )


def assert_depth_unit_is_available(
    session: Session,
    pred_set_id: uuid.UUID,
    *,
    max_sequence_rank: int | None,
) -> None:
    """Refuse a sequence-depth cut the candidates cannot honour.

    Args:
        session: An open session on the store holding the candidates.
        pred_set_id: The prediction set about to be scored.
        max_sequence_rank: The requested depth in sequences, or None when
            the run counts in proteins and this check does not apply.

    Raises:
        DepthUnitUnavailable: When the set carries no sequence ranks, or
            carries them on only part of itself. Partial coverage is
            refused as firmly as none: a cut over a partly ranked set
            silently drops the unranked half and the surviving half looks
            like a complete answer.
    """
    if max_sequence_rank is None:
        return
    rows, ranked, _ = ledger_coverage(session, pred_set_id)
    complaint = why_the_unit_is_unavailable(
        rows, ranked, max_sequence_rank, pred_set_id
    )
    if complaint is not None:
        raise DepthUnitUnavailable(complaint)
