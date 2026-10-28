"""How a panel gets its retrieval neighbourhoods, and how long it keeps them.

The expensive half of opening a cell. MEASURED on the live store 2026-10-28:
over the retrieval arm of the 211 -> 227 window, 1.145.411 candidate rows,
``neighbourhoods_for`` took **635,0 s** and returned 37.467 neighbourhoods. The
other half, slicing the grid and attaching the bands, was 0,110 s with these
already resident.

Its own module because the two halves want different lifetimes and the router
had reached its size budget. The comment that used to carry one number for both
said the neighbourhood query "takes seconds".
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session, sessionmaker

from protea.api.cache import cached
from protea.core.operations._run_cafa_strata import neighbourhoods_for

#: THESE DO NOT EXPIRE, and the five minutes they used to share with the panel
#: slice was wrong for them by more than an order of magnitude: 635 s of work
#: under a 300 s lifetime is stale the moment it is stored. The cache could not
#: hit even once on that arm. Every visitor started a rebuild that had already
#: expired before the next request arrived and paid the whole ten and a half
#: minutes again, so the stratified grid -- computed, stored, 96 cells of it --
#: was unreachable from the page that exists to show it.
#:
#: What makes an unbounded lifetime CORRECT rather than merely convenient is the
#: key, and the old comment said it without drawing the conclusion: the value
#: "only changes when a run is rescored". A rescore does not mutate a prediction
#: set; it writes a new ``evaluation_result`` that names one, and a set's
#: candidate rows are written once and never updated. So the key names an
#: immutable thing, the entry cannot become wrong, and there is no invalidation
#: to get right because there is none to do.
#:
#: Bounded by the number of prediction sets, 9 on this deployment, each entry a
#: small tuple of floats per query that retrieved a donor. The process-local
#: store empties on restart, which is when a redeploy happens anyway.
HOODS_TTL = float("inf")


def hoods(factory: sessionmaker[Session], prediction_set_id: str) -> dict[str, Any]:
    """The retrieval neighbourhood of a run, cached across its arms.

    Eight evaluation results can share one prediction set, and the
    neighbourhood is a property of the retrieval rather than of the scoring, so
    switching arms inside a cell must not recompute it. Keying by the
    prediction set is what buys that, and it is the same property that makes
    :data:`HOODS_TTL` safe.
    """

    def produce() -> dict[str, Any]:
        with factory() as session:
            return neighbourhoods_for(session, prediction_set_id)

    return cached(f"stratum:hoods:{prediction_set_id}", HOODS_TTL, produce, serve_stale_on_error=True)
