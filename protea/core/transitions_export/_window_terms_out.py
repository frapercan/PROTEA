"""The axis terms a window's rule leaves OUT, which is the table and not its name.

WHY THE COMPLEMENT AND NOT THE SET. The loader draws negatives from the term
axis. Without this table it draws them from the WHOLE axis, so a term created
after an early window shows up as a negative there, and the model learns that
new terms are never gained -- an artefact of the rule, not biology. What the
loader needs is exactly the axis terms it must not treat as negatives.

AND IT IS ``axis - TOI``, NOT "the complement of the TOI". Measured on
2026-11-08 over the 64 windows of revision ``d5416224``, one axis of 43.558
live terms:

    axis - TOI      min 207, median 1.339, max 4.087, 106.010 rows in total
    TOI - axis      min 0, median 2.481, max 3.286

**The TOI is not a subset of the axis, and in 42 of those 64 windows it holds
MORE terms than the axis does.** The terms of interest are what is live in both
ENDS, and a term can be live in both ends while being obsolete in the pivot's
era. Those terms are irrelevant here because the loader never sees them: they
are not on the axis it draws from. Saying "the complement" would invite a
reader to expect ``|TOI| + |out| == |axis|``, which is false for most windows.

The 106.010 rows are also the argument for the shape: storing each window's TOI
instead would be 64 x 43.558, about 2,79 million rows, for the same
information.

THE RULE IS READ AND NEVER REIMPLEMENTED. ``_terms_of_interest`` is imported
from where the evaluation already uses it, private name and all, because a
second spelling of a rule is the project's own recurring defect: the exported
negatives would then be about a slightly different TOI from the one the gain
was trimmed by, and nothing in either artefact would say so.
"""

from __future__ import annotations

import uuid
from typing import NamedTuple

import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy.orm import Session

from protea.core._evaluation_graph import _live_terms_with_aspect
from protea.core._truth_rules import (
    ReconciledWindow,
    _terms_of_interest,
    toi_restricts_the_gain,
)
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet

#: ``(window, go_id)``, the two columns the laboratory's loader reads.
SCHEMA = pa.schema([("window", pa.string()), ("go_id", pa.string())])


class WindowTerms(NamedTuple):
    """One window's result, with the counts that make it readable."""

    window: str
    out: list[str]
    axis_size: int
    toi_size: int
    toi_off_axis: int
    #: Whether this window's rule trims the gain at all. ``False`` means the
    #: gain was never restricted, so ``out`` is empty BY RULE and not because
    #: the intersection happened to cover the axis. The two look identical in
    #: the parquet and are different facts.
    restricts: bool
    #: Distinct terms across the window's NK, LK and PK maps, read from its
    #: own sealed parquet. 0 when the invariant was not checked.
    gain_terms: int


def axis_live_terms(session: Session, cut: uuid.UUID) -> set[str]:
    """The non-obsolete ids of the declared term axis.

    Refuses an empty answer rather than writing every window's whole axis as
    "out": an unknown or unloaded snapshot would otherwise produce a table
    that is complete, consistent and about nothing.
    """
    live, _ = _live_terms_with_aspect(session, cut)
    if not live:
        raise ValueError(
            f"OntologySnapshot {cut} has no live terms, so it cannot be the term axis. "
            "Every window would export its entire TOI complement as empty and the "
            "dataset would say the loader may use the whole axis as negatives"
        )
    return live


def _refuse_gains_outside_toi(session: Session, eset: EvaluationSet, out: set[str]) -> int:
    """No term of a gain may be in its own window's ``window_terms_out``.

    THE CROSS-TABLE INVARIANT, and the only check that can see in the DATA
    what the function-identity test cannot. A gain is ``new - old`` restricted
    to ``(new & TOI) | (new & old)``; a gained term is by definition not in
    ``old``, so it has to come from ``new & TOI`` and therefore lies inside the
    terms of interest. If one ever turns up here, the rule that trimmed the
    gain and the rule that wrote this table are not the same rule, and
    importing the same function would not have told us.

    Read through the one door the truth has, so this cannot drift from the
    verified read. Measured on 2026-11-08 over four windows: 0 violations
    against 3.346 to 5.241 gained terms each.

    Returns the number of distinct gained terms, which is also the number that
    makes a silent empty gain visible.
    """
    from protea.core.evaluation import load_evaluation_data_for_set

    data, _ = load_evaluation_data_for_set(session, eset)
    ganados: set[str] = set()
    for categoria in ("nk", "lk", "pk"):
        for por_proteina in (getattr(data, categoria, {}) or {}).values():
            ganados |= set(por_proteina)
    violan = sorted(ganados & out)
    if violan:
        raise ValueError(
            f"window {eset.id} has {len(violan)} gained terms inside its own "
            f"window_terms_out: {violan[:5]}. A gained term is not in the old side, "
            "so it can only come from the terms of interest. The rule that trimmed "
            "this gain and the rule writing this table are not the same rule"
        )
    return len(ganados)


def terms_out(session: Session, eset: EvaluationSet, axis: set[str]) -> WindowTerms:
    """The axis terms this window's rule leaves out, with its counts.

    A WINDOW WHOSE RULE DOES NOT TRIM GETS AN EMPTY TABLE, not the
    intersection. ``_terms_of_interest`` ignores ``toi_rule`` and always
    intersects the two ends, and ``pivot_terms`` is the DEFAULT rule, so
    writing that intersection for such a window would remove terms from the
    loader's negatives that its gain never excluded. The 64 windows of this
    campaign are all ``live_in_both_ends``, so nothing would have shown it.
    """
    window = ReconciledWindow(
        eset.old_annotation_set_id,
        eset.new_annotation_set_id,
        eset.old_native_snapshot_id,
        eset.new_native_snapshot_id,
        eset.pivot_snapshot_id,
        eset.truth_profile,
        eset.toi_rule,
        eset.not_codes,
    )
    restringe = toi_restricts_the_gain(eset.toi_rule)
    toi = _terms_of_interest(session, window)
    fuera = (axis - toi) if restringe else set()
    return WindowTerms(
        window=str(eset.id),
        out=sorted(fuera),
        axis_size=len(axis),
        toi_size=len(toi),
        toi_off_axis=len(toi - axis),
        restricts=restringe,
        gain_terms=_refuse_gains_outside_toi(session, eset, fuera),
    )


def write_table(results: list[WindowTerms], path: str) -> int:
    """Write ``(window, go_id)`` in a stable order, and say how many rows.

    Sorted by window and then by id so the same windows always produce the same
    bytes: the sha of this file is what a consumer quotes, and a sha over an
    order the query plan happened to pick is a sha that changes without the
    data changing. That is a measured hazard in this project, not a worry.

    IT SORTS THE IDS HERE TOO, and not because the caller does not. ``terms_out``
    returns them sorted, so this is redundant today -- and the first draft left
    it out for exactly that reason, which made the promise above depend on a
    caller keeping a habit. A test that passed unsorted ids wrote them unsorted.
    """
    ventanas: list[str] = []
    terminos: list[str] = []
    for r in sorted(results, key=lambda r: r.window):
        ids = sorted(r.out)
        ventanas.extend([r.window] * len(ids))
        terminos.extend(ids)
    pq.write_table(pa.table({"window": ventanas, "go_id": terminos}, schema=SCHEMA), path)
    return len(ventanas)
