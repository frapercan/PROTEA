"""The report of an exploration, generated from rows (ADR-D55, sections 6 and 7).

A light server step: it reads the frozen rules, the receipt, the linked
results and the linked comparisons, answers the automatic half of the fixed
checklist, and from those computes the verdict and the grade. The whole report
goes into the ``report`` event with its sha256, so the log holds the report the
reviewer read and the author accepted, byte for byte, and nothing can edit it.

Every figure in it is drawn from a row. Free text is the reviewer's and the
author's, in their own events, never a result.

FIRST VERSIONS, written down so the review can change them on purpose:

* The verdict reads the panels the rules declare in ``deciding_panels``, or
  every compared panel when none are declared: ``blocked`` when the receipt or
  a blocking check forbids a verdict, ``against`` when the base wins any
  deciding panel, ``for`` when the candidate wins all of them, ``equal`` when
  every one is a null with power, and ``underpowered`` otherwise. An
  instrument reads its predicate instead: ``for`` when every deciding delta is
  within the rules' ``tolerance``, zero by default, and ``against`` otherwise.
* The grade: ``exploratory`` when the rules were frozen post hoc or a blocking
  check fails; ``check`` for an instrument; ``thesis`` on a test window;
  ``development`` on a validation window; ``exploratory`` otherwise. Whether a
  test window was read only once is not recorded yet.
* A check whose machinery does not exist yet answers ``not recorded``, and that
  does not block. Population, the machine and the coverage are such checks.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

import protea.services.exploration_receipt as receipt
from protea.core.explorations import rules_fingerprint
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.exploration.exploration import Exploration
from protea.infrastructure.orm.models.exploration.exploration_link import ExplorationLink
from protea.infrastructure.orm.models.job import JobEvent

__all__ = ["CHECKLIST_VERSION", "build", "grade", "verdict"]

#: The version of the fixed checklist the report answers. A report reviewed
#: against another version is another review.
CHECKLIST_VERSION = "1"

PASS, FAIL, NOT_RECORDED, NOT_APPLICABLE = "pass", "fail", "not recorded", "not applicable"


def _comparisons(session: Session, exploration: Exploration) -> list[dict[str, Any]]:
    """Each linked comparison job's stored result: its axis, its panels and its tally."""
    jobs = session.execute(
        select(ExplorationLink.job_id).where(
            ExplorationLink.exploration_id == exploration.id,
            ExplorationLink.role == "comparison",
            ExplorationLink.job_id.is_not(None),
        )
    ).scalars()
    out = []
    for job_id in jobs:
        fields = session.execute(
            select(JobEvent.fields)
            .where(JobEvent.job_id == job_id, JobEvent.event == "job.succeeded")
            .order_by(JobEvent.id.desc())
            .limit(1)
        ).scalar()
        result = (fields or {}).get("result") or {}
        out.append(
            {
                "job_id": str(job_id),
                "method_axis": result.get("method_axis"),
                "panels": {
                    key: {k: panel.get(k) for k in ("status", "verdict", "delta", "n_paired")}
                    for key, panel in (result.get("panels") or {}).items()
                },
                "tally": result.get("verdict"),
            }
        )
    return out


def _release(session: Session, annotation_set_id: Any) -> int | None:
    if annotation_set_id is None:
        return None
    version = session.execute(
        select(AnnotationSet.source_version).where(AnnotationSet.id == annotation_set_id)
    ).scalar()
    return int(version) if version and str(version).isdigit() else None


def _no_leak(
    session: Session, exploration: Exploration, window: EvaluationSet | None
) -> dict[str, Any]:
    """The weights' cut is not after the window's start, and corpus membership is declared."""
    if exploration.kind == "instrument":
        return {"answer": NOT_APPLICABLE, "blocking": True}
    start = _release(session, window.old_annotation_set_id) if window else None
    sets = receipt.arm_rows(session, exploration)
    if start is None or not sets:
        return {"answer": NOT_RECORDED, "blocking": True}
    problems = []
    for arm, row in sets.items():
        config = receipt.read_row(session, receipt.CONFIG_SQL, row["embedding_config_id"]) or {}
        cut = _release(session, config.get("trained_on_annotation_set_id"))
        if cut is not None and cut > start:
            problems.append(f"{arm}: fitted on release {cut}, after the window's start {start}")
        policy = (row.get("meta") or {}).get("donor_policy") or {}
        member_by = policy.get("admitted_by_release")
        if member_by is None:
            problems.append(f"{arm}: the bank's corpus membership is not declared")
        elif int(member_by) > start:
            problems.append(f"{arm}: donors admitted by {member_by}, after the start {start}")
    return {"answer": FAIL if problems else PASS, "blocking": True, "problems": problems}


def _layer_answer(layer: dict[str, Any], passing: tuple[str, ...]) -> str:
    status = layer.get("status")
    if status == receipt.NOT_RECORDED:
        return NOT_RECORDED
    return PASS if status in passing else FAIL


def checklist(
    session: Session, exploration: Exploration, layers: dict[str, Any], window: EvaluationSet | None
) -> dict[str, dict[str, Any]]:
    """The automatic half of the fixed checklist. The human half is in the events."""
    if exploration.kind == "one_axis":
        one_axis = {"answer": _layer_answer(layers["method"], ("one axis",)), "blocking": True}
    else:
        one_axis = {"answer": NOT_APPLICABLE, "blocking": True}
    return {
        "one_axis": one_axis,
        "one_frame": {"answer": _layer_answer(layers["frame"], ("one frame",)), "blocking": True},
        "one_population": {"answer": NOT_RECORDED, "blocking": True},
        "rules_before_numbers": {
            "answer": FAIL if exploration.post_hoc else PASS,
            "blocking": False,
        },
        "machine": {"answer": NOT_RECORDED, "blocking": True},
        "no_leak": _no_leak(session, exploration, window),
        # By construction: a link names a platform row, or a store key with its sha256.
        "everything_inside": {"answer": PASS, "blocking": True},
        # By construction: every figure in this report is drawn from a row.
        "figures_with_origin": {"answer": PASS, "blocking": True},
    }


def _instrument_verdict(panels: list[dict[str, Any]], tolerance: float) -> str:
    """An instrument reads its predicate, not its power.

    Two runs that agree byte for byte give a delta of exactly zero in every
    pair, so the paired bootstrap is degenerate and every panel comes back
    ``unresolvable``. Read for power that is ``underpowered``, which says the
    opposite of what happened. An instrument holds when every deciding panel's
    delta is within the tolerance its rules declare, zero by default.
    """
    deltas = [p.get("delta") for p in panels]
    if any(d is None for d in deltas):
        return "underpowered"
    within = all(d is not None and abs(float(d)) <= tolerance for d in deltas)
    return "for" if within else "against"


def verdict(
    blocks: list[str],
    comparisons: list[dict[str, Any]],
    deciding: list[str] | None,
    *,
    instrument_tolerance: float | None = None,
) -> str | None:
    """The reading of the deciding panels, or None when nothing was compared.

    ``instrument_tolerance`` is set for an instrument, whose verdict reads its
    predicate; see :func:`_instrument_verdict`.
    """
    if blocks:
        return "blocked"
    panels = [
        panel
        for comparison in comparisons
        for key, panel in comparison["panels"].items()
        if not deciding or key in deciding
    ]
    if not panels:
        return None
    if instrument_tolerance is not None:
        return _instrument_verdict(panels, instrument_tolerance)
    if any(p.get("verdict") == "b_greater" for p in panels):
        return "against"
    if all(p.get("verdict") == "a_greater" for p in panels):
        return "for"
    if all(p.get("status") == "null_with_power" for p in panels):
        return "equal"
    return "underpowered"


def grade(exploration: Exploration, window_role: str | None, failed_blocking: bool) -> str:
    """Where the figure may be used: thesis, development, check or exploratory."""
    if exploration.post_hoc or failed_blocking:
        return "exploratory"
    if exploration.kind == "instrument":
        return "check"
    if window_role == "test":
        return "thesis"
    if window_role == "valid":
        return "development"
    return "exploratory"


def build(session: Session, exploration: Exploration) -> tuple[dict[str, Any], str]:
    """The report and its sha256, taken over the report itself and nothing else."""
    window = session.get(EvaluationSet, exploration.evaluation_set_id)
    computed = receipt.compute(session, exploration)
    checks = checklist(session, exploration, computed["layers"], window)
    failed = [k for k, c in checks.items() if c["blocking"] and c["answer"] == FAIL]
    comparisons = _comparisons(session, exploration)
    deciding = (exploration.rules or {}).get("deciding_panels")
    blocks = list(computed["blocks_verdict"]) + [f"check failed: {k}" for k in failed]
    report = {
        "exploration": {
            "id": str(exploration.id),
            "number": exploration.number,
            "title": exploration.title,
            "kind": exploration.kind,
            "question": exploration.question,
            "hypothesis": exploration.hypothesis,
            "evaluation_set_id": str(exploration.evaluation_set_id),
            "window_role": window.window_role if window else None,
        },
        "rules": {"frozen": exploration.rules, "sha256": exploration.rules_sha256},
        "post_hoc": exploration.post_hoc,
        "receipt": computed,
        "comparisons": comparisons,
        "deciding_panels": deciding or "every compared panel; none declared",
        "checklist": {"version": CHECKLIST_VERSION, "items": checks},
        "blocks_verdict": blocks,
        "verdict": verdict(
            blocks,
            comparisons,
            deciding,
            instrument_tolerance=(
                float((exploration.rules or {}).get("tolerance", 0.0))
                if exploration.kind == "instrument"
                else None
            ),
        ),
        "grade": grade(exploration, window.window_role if window else None, bool(failed)),
    }
    return report, rules_fingerprint(report)
