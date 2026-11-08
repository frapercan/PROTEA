"""Which declared window a dump reads, said out loud instead of guessed.

THE AMBIGUITY IS REAL AND IT IS THE NORMAL CASE. A pair of annotation sets
used to identify one ``EvaluationSet``, enforced by a UNIQUE constraint. It no
longer does: the three propagation graphs and the three truth rules joined the
key, because the same pair read under a different graph or a different rule is
a different delta. On this campaign's database **62 of 69 distinct pairs
already carry more than one window**, so a resolver keyed on the pair alone
does not work on 90 percent of the series.

WHY ONE HELPER AND NOT TWO. The train path learned this on 2026-05-12, the
LB.1 incident: ``one_or_none()`` on the bare pair raised
``MultipleResultsFound`` from deep inside SQLAlchemy, which is unreadable from
the outside. It was repaired to collect candidates and name them. **Its twin
on the test path was not**, and still called ``one_or_none()`` when this
module was written, so the same failure was one test pair away. Two copies of
one rule is how the second one rots; there is one copy now and both callers
use it.

WHAT NAMING BUYS, beyond not crashing. A dump that names its windows records
WHICH truth it trained on, and the manifest can carry each window's pivot,
its three rules and the revision of the code that built it. Before this, a
dataset said which annotation sets it spanned and nothing about how their
delta had been constructed, which is the part that moves.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def window_identity(eset: Any) -> dict[str, str | None]:
    """What a manifest has to say about one window to make it findable again.

    The three rules and the pivot, because those decide the delta, plus
    ``code_revision``, because the rules decide it by declaration and the code
    decides it by implementation and the two are different quantities. NULL
    there means "ask the dated declaration", never "the same as this other
    one".
    """
    return {
        "evaluation_set_id": str(eset.id),
        "pivot_snapshot_id": str(eset.pivot_snapshot_id),
        "old_native_snapshot_id": str(eset.old_native_snapshot_id),
        "new_native_snapshot_id": str(eset.new_native_snapshot_id),
        "truth_profile": eset.truth_profile,
        "toi_rule": eset.toi_rule,
        "not_codes": eset.not_codes,
        "code_revision": getattr(eset, "code_revision", None),
    }


def _describe(candidates: list[Any]) -> str:
    return "; ".join(
        f"{e.id} (pivot {str(e.pivot_snapshot_id)[:8]}, "
        f"natives {str(e.old_native_snapshot_id)[:8]}/{str(e.new_native_snapshot_id)[:8]}, "
        f"{e.truth_profile}/{e.toi_rule}/{e.not_codes})"
        for e in candidates
    )


def _choose(candidates: list[Any], named_ids: set[str], label: str) -> Any:
    """The one window among the candidates, or a refusal naming them all.

    ``named_ids`` is a SET and not a list parallel to the pairs: the pairs are
    generated from the version list, and a position nobody can check is a
    position that silently shifts. Naming is order-free, and a named id that
    belongs to no pair of this run is caught here rather than ignored.
    """
    if named_ids:
        chosen = [e for e in candidates if str(e.id) in named_ids]
        if len(chosen) > 1:
            raise RuntimeError(
                f"the payload names {len(chosen)} evaluation sets for {label}: "
                f"{_describe(chosen)}. They hold different deltas, so naming both "
                "says nothing. Name one."
            )
        if not chosen:
            raise RuntimeError(
                f"the payload names evaluation sets, but none of them is a window of "
                f"{label}, whose candidates are: {_describe(candidates)}. A named id "
                "that matches no pair of this run is a typo or a window from another "
                "series, and either way this dump would quietly fall back to guessing."
            )
        return chosen[0]
    if len(candidates) > 1:
        raise RuntimeError(
            f"{len(candidates)} EvaluationSets exist for {label}, differing in their "
            f"propagation graphs or their truth rules: {_describe(candidates)}. They "
            "hold different deltas, so the pair alone does not say which one this dump "
            "should use. Name one in the payload's evaluation_set_ids."
        )
    return candidates[0]


def _refuse_wrong_pivot(eset: Any, pivot_must_be: uuid.UUID | str, label: str) -> None:
    """A window on another term axis than the one the dump declares.

    Refused rather than noted: the manifest would be wrong about its own
    vocabulary, and a reader has no way to tell from the parquet.
    """
    if str(eset.pivot_snapshot_id) == str(pivot_must_be):
        return
    raise RuntimeError(
        f"{label} resolves to window {eset.id}, whose pivot is "
        f"{str(eset.pivot_snapshot_id)[:8]}, and this dump declares "
        f"ontology_snapshot_id {str(pivot_must_be)[:8]}. A window pivoting on another "
        "snapshot expresses its truth on another term axis, so the dataset's manifest "
        "would be wrong about its own vocabulary. Declare the axis the windows "
        "actually use, or name windows that use the declared one."
    )


def resolve_window(
    session: Session,
    old_set_id: uuid.UUID,
    new_set_id: uuid.UUID,
    named_ids: set[str],
    pivot_must_be: uuid.UUID | str | None,
    label: str,
) -> Any:
    """The one window this dump means for a pair, or a refusal that says why.

    ``pivot_must_be`` is the dump's declared ``ontology_snapshot_id``, and
    ``None`` skips that check for a caller with no axis to declare.
    """
    from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet

    candidates = (
        session.query(EvaluationSet)
        .filter_by(old_annotation_set_id=old_set_id, new_annotation_set_id=new_set_id)
        .all()
    )
    if not candidates:
        raise RuntimeError(
            f"EvaluationSet missing for {label}. Materialize it via "
            "scripts/materialize_lab_intervals.py or POST "
            "/annotations/evaluation-sets/generate before retrying."
        )
    eset = _choose(candidates, named_ids, label)
    if pivot_must_be is not None:
        _refuse_wrong_pivot(eset, pivot_must_be, label)
    return eset
