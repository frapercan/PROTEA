"""Retired surface, kept answering so nothing that calls it falls over.

WHAT THIS USED TO BE. This endpoint served the ladder model of the
campaign: an ordered sequence of steps, each one a grid of jobs that
declared the same step and window in their meta, with the grid's cells
counted and a headline read off whichever cell scored best. The whole
answer was derived at request time by joining ``job`` to
``prediction_set`` to ``evaluation_result``.

WHY IT IS GONE. The derivation outlived its evidence. The join's left
side is the job table, which is append-only and still holds every job
that campaign dispatched; its right side is ``evaluation_result``, whose
rows for that campaign were deleted. A LEFT JOIN does not notice that.
The endpoint went on counting the jobs, reporting a full grid in which
every cell had succeeded, while not one scored result behind it still
existed. A surface that reports a complete measurement out of an empty
table is worse than a surface that reports nothing, because a reader has
no way to tell the two apart.

The counting is therefore deleted rather than disabled. Left in place and
merely unreferenced it would be one call away from being restored by
somebody who saw an empty response and assumed a bug.

WHY IT STILL ANSWERS 200. Two checks in ``scripts/deploy-check.sh``
constrain this. One walks every router module in the tree, reads its
declared prefix and fails the deploy if that prefix is absent from the
served OpenAPI document, so the route has to stay registered and in the
schema. The other curls a handful of endpoints and treats anything other
than 200 as a failure, so a 410 would turn every deploy red. Retirement
is a fact about meaning, not about reachability, and it is reported in
the body where a reader looks rather than in a status code that would
break a check that is right to be strict.

WHY THE BODY STILL CARRIES AN EMPTY LIST. The one client, ``getRungs``
in ``apps/web/lib/rungs.ts``, rejects any 200 whose body has no array
under ``rungs``: it was written that way after a mock replied 200 with
``[]`` to every route it did not know and took a server render down with
it. Every consumer of that list already has an empty branch and renders
nothing on it. Keeping the key means the retirement travels through the
existing paths instead of through the error path.

The experiment graph at ``/v1/graph`` replaces this.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query

router = APIRouter(prefix="/rungs", tags=["rungs"])

#: Where a caller should go instead.
_SUPERSEDED_BY = "/v1/graph"


@router.get("", deprecated=True)
def list_rungs(
    metric: str = Query(
        default="f_micro_w",
        description=(
            "Accepted and echoed back so an existing caller's URL still "
            "parses. It selects nothing: this endpoint no longer reads the "
            "database."
        ),
    ),
) -> dict[str, Any]:
    """Report that this surface is retired, and name its replacement.

    Reads nothing and writes nothing. The empty list is the whole of the
    answer, not a database that happened to come back empty, and the
    ``retired`` block says which of the two it is so a caller is never
    left guessing.
    """
    return {
        "rungs": [],
        "metric": metric,
        "retired": {
            "retired": True,
            "superseded_by": _SUPERSEDED_BY,
            "reason": (
                "This endpoint described the ladder model of the campaign, "
                "which has been withdrawn in favour of the experiment graph. "
                "It also counted jobs from a campaign whose evaluation "
                "results were deleted, so its grid reported a complete "
                "measurement with no surviving evidence behind it."
            ),
            "empty_means": (
                "retired, not unpopulated. This response is a constant. No "
                "query runs behind it, so an empty list here says nothing "
                "about what the database holds."
            ),
        },
    }
