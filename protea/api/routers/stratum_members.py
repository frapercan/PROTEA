"""Which proteins are in a stratum, so a cell can be opened rather than read.

The strata panel says a cell holds 778 proteins and scores 0.0842. It does
not say which proteins, so the chain from a published number to the thing
it is about stops at the count. A reader who distrusts a cell, or who
wants to see why the twilight band is hard, has nowhere to go.

Membership is read from the window's placement (:mod:`protea.core.window_placement`),
the same rows the stratification reads, so the bands here and the bands in the
parquet cannot drift: a second implementation would.

What it filters on and what it does not. Length and homology are the window's
and are filtered: the population is every target of the window in that band,
whatever the method. Category and aspect are ASSERTED by the caller: whether a
target is no-knowledge in a given aspect is a fact about the evaluation's ground
truth, and this router does not hold it. So the count returned is the population
of the BAND, and it will exceed the cell the strata panel reports for the same
coordinates. The field is named for that rather than left to be assumed.

Beside each target it shows what the prediction set's own retrieval found: the
identity of its most homologous non-self donor and that donor's evidence. Those
place nothing; they are there so a reader can compare a method's neighbours with
the window's closest measured protein.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session, sessionmaker

from protea.api.deps import get_session_factory
from protea.api.routers._stratum_hoods import hoods as retrieval_neighbourhoods
from protea.core.domain.aspect import Aspect
from protea.core.strata import UNREAD_NEIGHBOURHOOD, stratum_for
from protea.core.window_placement import (
    TargetPlacement,
    WindowNotPlacedError,
    placement_of_window,
)

router = APIRouter(prefix="/stratum", tags=["strata"])

#: Both spellings of an aspect, mapped to the wire code the band vocabulary
#: takes. The CAFA form is first because it is the one every other surface
#: in this product emits.
_ASPECT_CODES: dict[str, str] = {
    **{a.cafa: a.code for a in Aspect},
    **{a.code: a.code for a in Aspect},
}


def _aspect_code(raw: str) -> str:
    """Accept the code the caller was shown, not only the one the DB stores.

    An aspect has two spellings in this system and they are not
    interchangeable in code. ``Aspect.code`` is the single character in
    ``go_term.aspect`` and in the OBO file. ``Aspect.cafa`` is the three
    letters cafaeval writes, which the evaluation results JSON carries and
    which every user-facing surface renders: the benchmark matrix, the
    strata panel, the axis labels.

    ``stratum_for`` takes the wire code. So opening a cell with the value
    printed in that cell raised ``KeyError('BPO')``, and the caller got a
    500 that named nothing. Anyone wiring a screen to this endpoint would
    have hit it on the first click, because the value they hold is the one
    the table displayed.

    Both are accepted rather than one chosen. The CAFA form is what a
    screen holds; the wire form is what the database column holds, and a
    caller reading from there is not wrong either. What is refused is
    refused as a 422 that names the vocabulary, because an unrecognised
    aspect is a bad request and not a server fault.
    """
    code = _ASPECT_CODES.get(raw.strip().upper())
    if code is None:
        raise HTTPException(
            status_code=422,
            detail=(
                f"aspect {raw!r} is not a GO aspect. Accepted: " + ", ".join(sorted(_ASPECT_CODES))
            ),
        )
    return code


@dataclass
class _Where:
    """The cell to open, as query parameters."""

    category: str = Query(
        description=(
            "Knowledge category, ASSERTED rather than verified. It is passed to the "
            "band computation because the stratum vocabulary takes it, and membership "
            "is not filtered on it: whether a query is NK in this aspect is an "
            "evaluation fact and not a retrieval one."
        )
    )
    aspect: str = Query(description="GO aspect, asserted on the same terms as category.")
    evaluation_set_id: uuid.UUID = Query(
        description="The window whose placement places its targets on the length and homology axes."
    )
    length: str | None = Query(default=None, description="Length band, or unset for any.")
    homology: str | None = Query(default=None, description="Identity band, or unset for any.")


def _walk(
    placement: dict[str, TargetPlacement],
    hoods: dict[str, Any],
    at: _Where,
    aspect: str,
    limit: int,
) -> tuple[list[dict[str, Any]], int]:
    """Place every target on the axes and keep the ones in this band.

    Returns the capped rows and the UNCAPPED count, because a caller told
    only the page size cannot tell a small cell from a truncated one.
    """
    members: list[dict[str, Any]] = []
    population = 0
    for accession, where in placement.items():
        hood = hoods.get(accession)
        stratum = stratum_for(
            category=at.category,
            aspect=aspect,
            residues=where.residues,
            identity=where.identity,
            retrieval=hood if hood is not None else UNREAD_NEIGHBOURHOOD,
        )
        if at.length is not None and str(stratum.length) != at.length:
            continue
        if at.homology is not None and str(stratum.homology) != at.homology:
            continue
        population += 1
        members.append(
            {
                "accession": accession,
                "residues": where.residues,
                "length_band": str(stratum.length),
                "homology_band": str(stratum.homology),
                "identity": where.identity,
                "best_identity": None if hood is None else hood.best_identity,
                "donor_is_experimental": None if hood is None else hood.donor_is_experimental,
                "taxonomic_relation": None if hood is None else hood.taxonomic_relation,
            }
        )
    # Weakest identity first: the reason to open a cell is usually to see
    # what makes it hard, and the hardest cases are the ones to show.
    members.sort(
        key=lambda m: (m["identity"] if m["identity"] is not None else -1.0, m["accession"])
    )
    return members[:limit], population


@router.get("/{prediction_set_id}/members", summary="Proteins in one stratum")
def stratum_members(
    prediction_set_id: uuid.UUID,
    at: _Where = Depends(),
    limit: int = Query(default=200, ge=1, le=2000, description="Cap on rows returned."),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """The targets a stratum is made of, with what put each one there.

    Every row carries the identity that placed it, to the closest protein
    measured when the window opened, because that is what the band IS: a reader
    who cannot see what put a protein in a band cannot check the band, and a
    band nobody can check is a label. Beside it, what the prediction set's own
    retrieval found, which places nothing.

    It does NOT yet carry WHICH donor the arm retrieved. ``Neighbourhood``
    resolves five facts about the retrieved donors and the accession is not
    among them, so naming it here would mean changing what retrieval returns.
    """
    with factory() as session:
        try:
            placement = placement_of_window(session, str(at.evaluation_set_id))
        except WindowNotPlacedError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
    # Cached for the process, as the panel drawer caches them: a set's candidate
    # rows are written once, and reading its neighbours costs minutes.
    hoods = retrieval_neighbourhoods(factory, str(prediction_set_id))
    members, population = _walk(placement, hoods, at, _aspect_code(at.aspect), limit)

    return {
        "prediction_set_id": str(prediction_set_id),
        "evaluation_set_id": str(at.evaluation_set_id),
        "where": {
            "category": at.category,
            "aspect": at.aspect,
            "length": at.length,
            "homology": at.homology,
        },
        # Named band_population rather than total, because it is not the
        # cell's population: category and aspect are asserted, not filtered,
        # so this counts every query in the length and homology band. It
        # will exceed the strata panel's n_proteins for the same
        # coordinates, and a field called "total" would read as a
        # contradiction rather than as a different quantity.
        "band_population": population,
        "returned": len(members),
        # Named rather than implied: a truncated list that does not say so
        # reads as the whole cell.
        "truncated": population > len(members),
        "members": members,
    }
