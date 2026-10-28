"""Why a panel has no per-protein rows, said in the producer's own words.

``run_cafa_evaluation`` does not write a namespace into the per-protein grid
when it cannot number that namespace's rows against the pooled cell, and it
says so: one ``per_protein_grid_namespace_dropped`` job event per drop, with a
machine-readable ``code`` and a prose ``reason``. The producer has done that
from the start.

The stratum endpoint was the half that did not read it. A namespace absent from
the grid left ``_sliced_at_the_published_tau`` building a frame from an empty
list, which carries NO COLUMNS, so the first column access raised
``KeyError: 'protein_accession'`` and the page answered 500. Measured on the
live record: three of the nine published cells of the 211 -> 227 window, one per
knowledge setting, each with its reason already written down.

Its own module for the reason :mod:`_graph_dates` is: the file it came out of
had reached its size budget, and this is one cohesive thing rather than a
leftover. The thing is a relay, and naming it that way is the point: nothing
here decides whether a namespace should have been written, it only repeats what
the evaluation recorded.
"""

from __future__ import annotations

import uuid
from typing import Any

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

#: The producer's own record of a namespace it refused to write into the grid.
#:
#: Matched on the three fields the event carries that identify a cell: the
#: result's job, the setting and the namespace. ``LIMIT 1`` because one drop per
#: cell is what the producer writes and a second would name the same refusal
#: twice.
DROPPED = text(
    """
    SELECT e.fields ->> 'code'   AS code,
           e.fields ->> 'reason' AS reason
      FROM evaluation_result r
      JOIN job_event e ON e.job_id = r.job_id
     WHERE r.id = :rid
       AND e.event = 'run_cafa_evaluation.per_protein_grid_namespace_dropped'
       AND e.fields ->> 'setting'   = :setting
       AND e.fields ->> 'namespace' = :namespace
     LIMIT 1
"""
)


def refuse_a_dropped_namespace(
    session: Session, result_id: uuid.UUID, setting: str, namespace: str
) -> None:
    """Relay the producer's refusal, or return and let the read proceed.

    Checked BEFORE the cache, for the reason the artefact-existence check is: a
    refusal raised inside the producer is swallowed the moment a stale page for
    another cell is in the store, and this one has to survive that.

    A 404 and not a 409: the rows for this cell were deliberately not written,
    which is an absence with a recorded cause rather than a damaged artefact.
    The code goes in the body beside the prose so a caller can branch on it
    without parsing a sentence, and the message says the pooled cell still
    stands, because it does: what cannot be opened is the detail behind it.
    """
    row = (
        session.execute(DROPPED, {"rid": result_id, "setting": setting, "namespace": namespace})
        .mappings()
        .first()
    )
    if row is None:
        return
    raise HTTPException(
        status_code=404,
        detail=(
            f"{setting}/{namespace} has no per-protein rows for evaluation result "
            f"{result_id}: the evaluation refused to write them and recorded why "
            f"({row['code']}). {row['reason']} The pooled cell for this namespace "
            "is still reported; what cannot be opened is the per-protein detail "
            "behind it."
        ),
    )


def refuse_when_the_grid_and_the_record_disagree(
    rows: list[dict[str, Any]], *, result_id: str, setting: str, namespace: str
) -> None:
    """Refuse a cell the panel reports and the grid does not hold.

    Reached only when nothing else explains the absence. The best-F table
    reports this namespace, so the run DID score it; the grid holds no row for
    it; and :func:`refuse_a_dropped_namespace` found no recorded drop, so the
    producer is not the one who left it out. The artefact and the record
    disagree about what was written, which is worse than either on its own and
    is reported rather than smoothed over with an empty page.
    """
    if rows:
        return
    raise HTTPException(
        status_code=409,
        detail=(
            f"evaluation result {result_id}, setting {setting}: the panel reports "
            f"{namespace} and the per-protein grid holds no row for it, and the "
            "evaluation recorded no reason for leaving it out. The artefact and the "
            "record disagree, so the page is refused rather than shown empty"
        ),
    )


def arm_for_an_openable_cell(
    session: Session, arm_sql: Any, result_id: uuid.UUID, setting: str, namespace: str
) -> Any:
    """The arm's identity, or the reason this cell cannot be opened at all.

    Three refusals in the order they become knowable, which is also the order of
    how much they cost to answer: no such result, then a result that names no
    prediction set, then a namespace the producer refused to write. All three
    are 404s and all three are absences with a cause, so they belong together
    and in front of the cache rather than scattered through the handler.
    """
    arm = session.execute(arm_sql, {"rid": result_id}).mappings().first()
    if arm is None:
        raise HTTPException(status_code=404, detail=f"no evaluation result {result_id}")
    if arm["prediction_set_id"] is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"evaluation result {result_id} names no prediction set, "
                "so its proteins cannot be placed on the retrieval axes"
            ),
        )
    refuse_a_dropped_namespace(session, result_id, setting, namespace)
    return arm
