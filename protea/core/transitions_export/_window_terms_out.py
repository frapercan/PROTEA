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
from protea.core._truth_rules import ReconciledWindow, _terms_of_interest
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


def terms_out(session: Session, eset: EvaluationSet, axis: set[str]) -> WindowTerms:
    """The axis terms this window's rule leaves out, with its counts."""
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
    toi = _terms_of_interest(session, window)
    return WindowTerms(
        window=str(eset.id),
        out=sorted(axis - toi),
        axis_size=len(axis),
        toi_size=len(toi),
        toi_off_axis=len(toi - axis),
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
