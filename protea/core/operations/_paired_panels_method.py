"""The seal on the method side, applied to one paired comparison.

Split out of :mod:`compare_paired_panels` when it passed the file budget, and
the split is along a real seam: this module answers whether the two arms were
PRODUCED by methods differing in one named way, while the frame gate that stays
behind answers whether their numbers were MEASURED the same way. The two
questions are independent, and on 2026-08-28 the first one was the one nobody
asked.

The classification of what a method is lives in :mod:`protea.core.method_seal`,
which has no operation in it and can be read by any surface that compares two
prediction sets. This module is only the wiring: fetch both rows, refuse in the
operation's own error type, and say in an event what the comparison turned out
to be of.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn
from protea.core.method_seal import (
    METHOD_IDENTITY_FIELDS,
    CrossedMethodAxes,
    assert_one_axis,
)
from protea.core.operations._paired_panels_artifact import PanelComparabilityError

if TYPE_CHECKING:
    from protea.core.operations.compare_paired_panels import ComparePairedPanelsPayload


#: The whole prediction_set row behind one side, columns and all.
#:
#: SELECT * and not a named column list, deliberately. The method seal is the
#: COMPLEMENT of the declared axis: it requires every field it has not been told
#: about to agree. A named list would decide here which fields the seal can ever
#: see, so a column added to prediction_set next month would be invisible to it
#: and free to vary -- the silent widening the seal exists to prevent, moved from
#: the classification into the query. With the star, an unclassified column
#: arrives at ``method_identity`` and is refused by name.
_PREDICTION_SET_SQL = text("SELECT * FROM prediction_set WHERE id = CAST(:psid AS uuid)")


def _prediction_set(session: Session, prov: dict[str, Any]) -> dict[str, Any]:
    """The whole prediction_set row behind one evaluation result.

    Both absences are refusals and neither is a fallback to an empty method.
    A result that names no prediction set, or names one that is gone, cannot be
    shown to have run the same method as anything: treating it as a blank row
    would make it agree with every other blank row, which is the failure the
    seal on the evaluation side already refuses by name.
    """
    pset = prov.get("prediction_set_id")
    if not pset:
        raise PanelComparabilityError(
            f"evaluation result {prov['id']} names no prediction set, so what method "
            "produced its numbers cannot be read. A comparison of two systems that "
            "cannot say what either system was is not a comparison."
        )
    row = session.execute(_PREDICTION_SET_SQL, {"psid": pset}).mappings().first()
    if row is None:
        raise PanelComparabilityError(
            f"prediction set {pset}, behind evaluation result {prov['id']}, is not in "
            "prediction_set. The method it ran is unrecoverable, so nothing can be shown "
            "to differ from it in one named way."
        )
    return dict(row)


def _method_gate(
    session: Session,
    p: ComparePairedPanelsPayload,
    prov: tuple[dict[str, Any], dict[str, Any]],
    emit: EmitFn,
) -> tuple[str, ...]:
    """Refuse unless the two arms differ in exactly the method fields declared.

    THE COMPARISON THIS PREVENTS. On 2026-08-28 this operation was pointed at
    two arms believed to differ in the donor bank alone. They also differed in
    ``expand_votes_to_ancestors``, ``aspect_separated_knn``, ``code_revision``
    and ``ontology_snapshot_id``; the first of those alone moved candidates per
    protein by a factor of 2.76, and that was read as an ontology effect for
    five hours. Nine deltas with intervals were published off the pair.
    ``compare_paired_panels._frame_gate`` could not have caught it: it seals the
    EVALUATION side, and all four of those fields are upstream of it, on the
    prediction set.

    The refusal is raised as ``PanelComparabilityError`` like the frame gate's,
    so a caller that already handles one incomparable pair handles both.
    """
    a, b = (_prediction_set(session, prov[0]), _prediction_set(session, prov[1]))
    try:
        axis = assert_one_axis(p.method_axis, a, b, names=(str(a["id"]), str(b["id"])))
    except CrossedMethodAxes as exc:
        raise PanelComparabilityError(str(exc)) from exc
    emit(
        "compare_paired_panels.method_axis",
        f"the two arms differ in {list(axis) or 'no method field'} and nothing else",
        {
            "axis": list(axis),
            "a_prediction_set_id": str(a["id"]),
            "b_prediction_set_id": str(b["id"]),
            "sealed_fields": [f for f in METHOD_IDENTITY_FIELDS if f not in axis],
        },
        "info",
    )
    return axis
