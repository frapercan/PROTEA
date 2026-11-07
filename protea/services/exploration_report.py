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
  instrument reads its predicate instead, because a repeatability test is an
  assertion of identity and power does not apply to it: ``for`` when its two
  runs hold the same rows, with zero tolerance, and every deciding panel's
  delta is within the rules' ``panel_tolerance``, zero by default; ``against``
  otherwise, naming the first row that differs. Its two runs must have loaded
  their banks from one and the same source, the rules' ``bank_load`` when
  declared, or no verdict is read: a cold run against a warm one is two
  pipelines.
* The grade: ``exploratory`` when the rules were frozen post hoc or a blocking
  check fails; ``check`` for an instrument; ``thesis`` on a test window;
  ``development`` on a validation window; ``exploratory`` otherwise. Whether a
  test window was read only once is not recorded yet.
* A check whose machinery does not exist yet answers ``not recorded``, and that
  does not block. Population, the machine and the coverage are such checks.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import select, text
from sqlalchemy.orm import Session

import protea.services.exploration_receipt as receipt
from protea.core.domain.aspect import Aspect
from protea.core.explorations import rules_fingerprint
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.exploration.exploration import Exploration
from protea.infrastructure.orm.models.exploration.exploration_link import ExplorationLink
from protea.infrastructure.orm.models.job import JobEvent

__all__ = ["CHECKLIST_VERSION", "bank_load_problems", "build", "grade", "verdict"]

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


#: The producer's own record of a namespace it refused to write into the
#: per-protein grid, for every evaluation result this exploration links. Read
#: over all of them because a panel is comparable only when BOTH arms wrote it:
#: one arm refusing is enough to leave nothing to pair.
_REFUSED_SQL = text(
    """
    SELECT DISTINCT e.fields ->> 'setting'   AS setting,
                    e.fields ->> 'namespace' AS namespace,
                    e.fields ->> 'code'      AS code
      FROM exploration_link l
      JOIN evaluation_result r ON r.id = l.evaluation_result_id
      JOIN job_event e ON e.job_id = r.job_id
     WHERE l.exploration_id = CAST(:eid AS uuid)
       AND l.evaluation_result_id IS NOT NULL
       AND e.event = 'run_cafa_evaluation.per_protein_grid_namespace_dropped'
"""
)


def _panel_coverage(
    session: Session,
    exploration: Exploration,
    comparisons: list[dict[str, Any]],
    deciding: list[str] | None,
) -> tuple[dict[str, Any], list[str]]:
    """Each deciding panel's state, and the absences that block a verdict.

    THREE STATES AND NOT TWO. A panel the comparison read is ``computed``. A
    panel it could not read because the evaluation refused to write that
    namespace is ``refused``, and the producer's own ``code`` says why: a
    limit recorded at the time, not a run that did not happen. Only a panel
    that is neither blocks, because then nothing in the record says why its
    number is missing.

    Both halves of that distinction are load-bearing, and each one alone is
    wrong. ``PK:BPO`` cannot be written at all while the weighted and
    unweighted branches score two populations out of one row per protein, so a
    rule that blocked on absence would block that exploration for ever. And
    :func:`verdict` reads the deciding panels through a comprehension that
    simply skips the ones that never arrived, so a rule that ignored absence
    publishes a verdict over part of its declared evidence without saying
    which part.

    With nothing declared the asked-for set stands in, which is what
    ``verdict`` already falls back to, and the refusals then say why nine
    requested panels became six.
    """
    asked = {key for comparison in comparisons for key in comparison["panels"]}
    read = {
        key
        for comparison in comparisons
        for key, panel in comparison["panels"].items()
        if panel.get("status") != "empty"
    }
    refused = {
        (row.setting, row.namespace): row.code
        for row in session.execute(_REFUSED_SQL, {"eid": str(exploration.id)})
    }
    panels: dict[str, Any] = {}
    blockers: list[str] = []
    for key in list(deciding) if deciding else sorted(asked):
        if key in read:
            panels[key] = {"state": "computed"}
            continue
        setting, _, cafa = key.partition(":")
        code = refused.get((setting, _namespace_of(cafa)))
        if code is not None:
            panels[key] = {"state": "refused", "code": code}
            continue
        panels[key] = {"state": "uncomputed"}
        blockers.append(f"the deciding panel {key} was never computed")
    return {"panels": panels}, blockers


def _namespace_of(cafa: str) -> str | None:
    """The long namespace a CAFA code names, or ``None`` when it names none.

    A deciding panel is a string in frozen rules, so it can carry a typo that
    no aspect answers to. That returns ``None`` rather than raising: the panel
    then has no recorded refusal, falls through to ``uncomputed`` and blocks,
    which is the safe direction for a panel nobody can even name.
    """
    try:
        return Aspect.from_cafa(cafa).namespace
    except KeyError:
        return None


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


def _instrument_verdict(
    identity: dict[str, Any], panels: list[dict[str, Any]], panel_tolerance: float
) -> str | None:
    """An instrument reads its predicate: identical rows, panels within their tolerance.

    A repeatability test is an assertion of identity, and power does not apply
    to it: runs that agree byte for byte give a delta of exactly zero in every
    pair, the paired bootstrap is degenerate, and every panel comes back
    ``unresolvable``, which read for power says the opposite of what happened.
    The rows decide, with zero tolerance (see ``exploration_identity``). The
    panels only corroborate, because stored metrics are rounded to four
    decimals: a panel delta beyond the declared tolerance breaks the instrument,
    and a panel without a delta takes nothing away from identical rows.
    """
    if identity.get("identical") is None:
        return None
    if not identity["identical"]:
        return "against"
    deltas = [p.get("delta") for p in panels]
    within = all(d is None or abs(float(d)) <= panel_tolerance for d in deltas)
    return "for" if within else "against"


def verdict(
    blocks: list[str],
    comparisons: list[dict[str, Any]],
    deciding: list[str] | None,
    *,
    instrument: dict[str, Any] | None = None,
) -> str | None:
    """The reading of the deciding panels, or None when nothing was compared.

    ``instrument`` carries an instrument's row identity and its panel
    tolerance; its verdict reads them instead (see :func:`_instrument_verdict`).
    """
    if blocks:
        return "blocked"
    panels = [
        panel
        for comparison in comparisons
        for key, panel in comparison["panels"].items()
        if not deciding or key in deciding
    ]
    if instrument is not None:
        return _instrument_verdict(instrument["identity"], panels, instrument["panel_tolerance"])
    if not panels:
        return None
    if any(p.get("verdict") == "b_greater" for p in panels):
        return "against"
    if all(p.get("verdict") == "a_greater" for p in panels):
        return "for"
    if all(p.get("status") == "null_with_power" for p in panels):
        return "equal"
    return "underpowered"


_LOADS_SQL = text(
    "SELECT fields ->> 'source' AS source, fields ->> 'host' AS host FROM job_event "
    "WHERE job_id = CAST(:job AS uuid) "
    "AND event = 'child.predict_go_terms_batch.load_references_done'"
)


def _bank_loads(session: Session, rows: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Where each arm's bank came from, read off its producing job's events.

    ``source`` is ``database`` for a cold load and ``disk_cache`` for a warm
    one, and the two are different code: a warm load never runs the pool
    query, so it never runs the ORDER BY that fixes the bank's order, which is
    what an instrument exists to certify. The hosts are recorded beside it.
    """
    out: dict[str, dict[str, Any]] = {}
    for arm, row in rows.items():
        job = (row.get("meta") or {}).get("job_id")
        found = session.execute(_LOADS_SQL, {"job": str(job)}).all() if job else []
        out[arm] = {
            "job_id": str(job) if job else None,
            "sources": sorted({r.source for r in found if r.source}),
            "hosts": sorted({r.host for r in found if r.host}),
        }
    return out


def bank_load_problems(loads: dict[str, dict[str, Any]], declared: str | None) -> list[str]:
    """What forbids reading an instrument's runs as one pipeline: their banks' sources.

    Each arm must record its bank load, from one source, the one the rules
    declare in ``bank_load`` when they declare one, and the same across every
    arm. A cold run against a warm run compares two pipelines, not one twice,
    and that holds for the third and fourth arm as much as for the second.
    """
    problems = []
    for arm, load in loads.items():
        if not load["sources"]:
            problems.append(f"{arm}: no bank load recorded")
        elif len(load["sources"]) > 1:
            problems.append(f"{arm}: its bank came from {' and '.join(load['sources'])}")
        elif declared and load["sources"][0] != declared:
            problems.append(f"{arm}: bank from {load['sources'][0]}, the rules require {declared}")
    if len({tuple(load["sources"]) for load in loads.values()}) > 1:
        seen = "; ".join(
            f"{arm}={'+'.join(load['sources']) or 'none'}" for arm, load in loads.items()
        )
        problems.append(f"the runs loaded their banks from different sources: {seen}")
    return problems


def _instrument(session: Session, exploration: Exploration) -> tuple[dict[str, Any], list[str]]:
    """An instrument's runs: their row identity, their bank loads, and what blocks.

    **Two arms used to be the only shape this read, and that cost a verdict.**
    Exploration 3 ran four arms on one revision; ``len(rows) != 2`` refused
    them, the report came back with no predicate at all, and its reading had to
    be written by hand as a job event. A figure whose predicate is an event
    somebody wrote is not the same object as one an operation computed, and the
    first cannot be re-derived from the database.

    **N arms need N-1 comparisons, not N(N-1)/2.** ``row_identity`` is exact
    equality, which is transitive, so every arm identical to one reference arm
    means every arm identical to every other. The reference is the first in
    sorted arm order, so which pairs a report names does not depend on dict
    order, and ``pairs`` carries every comparison rather than only its
    conclusion: an instrument that fails should say which arm moved.

    A report built here for a two-arm instrument carries more keys than it did
    before, so its sha256 differs from one stored earlier. The structure
    changed, not the measurement.
    """
    from protea.services.exploration_identity import row_identity

    rules = exploration.rules or {}
    rows = receipt.arm_rows(session, exploration)
    if len(rows) < 2:
        detail = (
            "an instrument compares at least two runs with a prediction set each; "
            f"{len(rows)} linked"
        )
        return {"identical": None, "detail": detail}, []
    arms = sorted(rows)
    reference, others = arms[0], arms[1:]
    ref_id = str(rows[reference]["id"])
    pairs = {arm: row_identity(session, ref_id, str(rows[arm]["id"])) for arm in others}
    first = next((p["first_difference"] for p in pairs.values() if not p["identical"]), None)
    loads = _bank_loads(session, rows)
    identity = {
        "arms": arms,
        "reference": reference,
        "bank_loads": loads,
        "identical": all(p["identical"] for p in pairs.values()),
        "rows": {reference: next(iter(pairs.values()))["rows"]["a"]}
        | {arm: p["rows"]["b"] for arm, p in pairs.items()},
        "pairs": {
            arm: {"identical": p["identical"], "first_difference": p["first_difference"]}
            for arm, p in pairs.items()
        },
        "first_difference": first,
    }
    return identity, bank_load_problems(loads, rules.get("bank_load"))


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
    coverage, uncomputed = _panel_coverage(session, exploration, comparisons, deciding)
    blocks = list(computed["blocks_verdict"]) + [f"check failed: {k}" for k in failed] + uncomputed
    instrument = None
    if exploration.kind == "instrument":
        identity, load_problems = _instrument(session, exploration)
        blocks += load_problems
        instrument = {
            "identity": identity,
            "panel_tolerance": float((exploration.rules or {}).get("panel_tolerance", 0.0)),
        }
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
        "panel_coverage": coverage,
        "checklist": {"version": CHECKLIST_VERSION, "items": checks},
        "blocks_verdict": blocks,
        "identity": instrument["identity"] if instrument else None,
        "verdict": verdict(blocks, comparisons, deciding, instrument=instrument),
        "grade": grade(exploration, window.window_role if window else None, bool(failed)),
    }
    return report, rules_fingerprint(report)
