"""The state of every protein at a window's start, read from that end alone (ADR-D65).

The count maps read the donors' states and the targets' denied terms at the
cut. The window's own truth unions the NOT terms of both ends and takes them out
of both, which is right for a gain and wrong for a state: a NOT written at t1
would take a term out of a protein's state at t0, and that is information from
after the cut.

So this builds the t0 side exactly as the evaluator does (the thirteen codes,
the binding rule, the t0 side's remap, the closure under its native graph, the
pivot's universe and aspects, the NOT descendants under both graphs) and takes
out t0's NOT terms alone. Nothing is reimplemented: the steps are the
evaluator's own functions, called on one side.
"""

from __future__ import annotations

from collections import Counter
from typing import NamedTuple

from sqlalchemy.orm import Session

from protea.core import lafa_frame
from protea.core._evaluation_graph import _load_children_by_go_id, _load_pivot_term_universe
from protea.core._lafa_frame_data import load_end_ontology, load_end_rows
from protea.core._truth_rules import (
    ReconciledWindow,
    _projection_request,
    _sides_of,
    refuse_unknown_rules,
)
from protea.core.evaluation import (
    _apply_negatives,
    _reconcile_experimental_side,
    _reconcile_not_side,
)
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet


class CutState(NamedTuple):
    """Every protein's state at the window's start, and the pivot's aspects.

    ``positives`` holds, per protein and aspect, its closed experimental terms
    without its own NOT terms; ``negatives`` the terms it is denied (its NOT
    terms at t0 and their descendants); ``aspect`` the pivot's aspect per term.
    """

    positives: dict[str, dict[str, set[str]]]
    negatives: dict[str, set[str]]
    aspect: dict[str, str]


def window_of(eset: EvaluationSet) -> ReconciledWindow:
    """The reconciled window a stored evaluation set was built as."""
    return ReconciledWindow(
        eset.old_annotation_set_id,
        eset.new_annotation_set_id,
        eset.old_native_snapshot_id,
        eset.new_native_snapshot_id,
        eset.pivot_snapshot_id,
        eset.truth_profile,
        eset.toi_rule,
        eset.not_codes,
    )


def cut_state(
    session: Session, window: ReconciledWindow, raw: dict[str, set[str]] | None = None
) -> CutState:
    """The t0 side of ``window`` as the evaluator builds it, with t0's NOT terms only.

    ``raw`` (``{protein: {native go_id}}``, t0's direct annotations of other
    evidence) is reconciled in place of the experimental rows, by the same steps.
    """
    refuse_unknown_rules(window)
    pivot_go_ids, pivot_aspect = _load_pivot_term_universe(session, window.pivot_snapshot_id)
    pivot_children = _load_children_by_go_id(session, window.pivot_snapshot_id)
    project = _projection_request(session, window) if window.truth_profile == "projected" else None
    old, _ = _sides_of(session, window, project)
    # As the evaluator does for the t0 side: it remaps and never projects.
    t0_project = project._replace(graph=project.graph._replace(live=set())) if project else None
    positives, _ = _reconcile_experimental_side(
        session, old, pivot_go_ids, pivot_aspect, t0_project, raw=raw
    )
    negatives, _ = _reconcile_not_side(
        session,
        old,
        pivot_go_ids,
        pivot_children,
        project,
        window.not_codes == "experimental",
    )
    return CutState(_apply_negatives(positives, negatives), negatives, pivot_aspect)


def lafa_cut_state(session: Session, eset: EvaluationSet) -> CutState:
    """The t0 side of a window of LAFA's frame, as that frame states an end, with t0's NOT terms only.

    The frame has no pivot: each end is read under its own ontology
    (:mod:`protea.core.lafa_frame`). t0's experimental rows are named under t0's
    ontology, each NOT term of t0 and everything under it is taken out of the
    direct terms, and what is left is closed upwards. Only t0's NOT rows count,
    by the window's ``not_codes``: the window's own truth unions the NOT rows of
    both ends, and one written at t1 is information from after the cut. The
    aspects are t0's.
    """
    t0 = load_end_ontology(session, eset.old_native_snapshot_id)
    rows, not_rows = load_end_rows(
        session, eset.old_annotation_set_id, eset.not_codes == "experimental"
    )
    positives, negatives = lafa_frame.end_state(t0, rows, not_rows, Counter(), "t0")
    return CutState(positives, negatives, dict(t0.live))


def window_state(session: Session, eset: EvaluationSet) -> CutState:
    """The state of every protein at a window's start, as the window's own frame states it."""
    if eset.truth_profile == lafa_frame.PROFILE:
        return lafa_cut_state(session, eset)
    return cut_state(session, window_of(eset))


__all__ = ["CutState", "cut_state", "lafa_cut_state", "window_of", "window_state"]
