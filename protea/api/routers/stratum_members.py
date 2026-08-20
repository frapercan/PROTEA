"""Which proteins are in a stratum, so a cell can be opened rather than read.

The strata panel says a cell holds 778 proteins and scores 0.0842. It does
not say which proteins, so the chain from a published number to the thing
it is about stops at the count. A reader who distrusts a cell, or who
wants to see why the twilight band is hard, has nowhere to go.

Membership is recomputed rather than stored, because the strata artefact
holds cells and measures and not the population behind them. It uses the
same two functions the stratification itself uses, so the bands here and
the bands in the parquet cannot drift: a second implementation would.

What it filters on and what it does not. Length and homology are computed
from the retrieval and are filtered. Category and aspect are ASSERTED by
the caller: whether a query is no-knowledge in a given aspect is a fact
about the evaluation's ground truth, not about the neighbourhood, and this
router does not hold it. So the count returned is the population of the
BAND, and it will exceed the cell the strata panel reports for the same
coordinates. The field is named for that rather than left to be assumed.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from protea.api.deps import get_session_factory
from protea.core.operations._run_cafa_strata import neighbourhoods_for
from protea.core.strata import stratum_for

router = APIRouter(prefix="/stratum", tags=["strata"])

#: Sequence length per query, which the band needs and the neighbourhood
#: does not carry.
_LENGTHS = text(
    """
    SELECT p.accession AS accession, length(s.sequence) AS residues
    FROM protein p JOIN sequence s ON s.id = p.sequence_id
    WHERE p.accession = ANY(:accessions)
    """
)


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
    length: str | None = Query(default=None, description="Length band, or unset for any.")
    homology: str | None = Query(default=None, description="Identity band, or unset for any.")


def _walk(
    hoods: dict[str, Any], lengths: dict[str, int], at: _Where, limit: int
) -> tuple[list[dict[str, Any]], int]:
    """Place every query on the axes and keep the ones in this band.

    Returns the capped rows and the UNCAPPED count, because a caller told
    only the page size cannot tell a small cell from a truncated one.
    """
    members: list[dict[str, Any]] = []
    population = 0
    for accession, hood in hoods.items():
        residues = lengths.get(accession)
        if residues is None:
            continue
        stratum = stratum_for(
            category=at.category, aspect=at.aspect, residues=residues, neighbourhood=hood
        )
        if at.length is not None and str(stratum.length) != at.length:
            continue
        if at.homology is not None and str(stratum.homology) != at.homology:
            continue
        population += 1
        if len(members) < limit:
            members.append(
                {
                    "accession": accession,
                    "residues": residues,
                    "length_band": str(stratum.length),
                    "homology_band": str(stratum.homology),
                    "best_identity": hood.best_identity,
                    "donor_is_experimental": hood.donor_is_experimental,
                    "taxonomic_relation": hood.taxonomic_relation,
                }
            )
    # Weakest identity first: the reason to open a cell is usually to see
    # what makes it hard, and the hardest cases are the ones to show.
    return sorted(members, key=lambda m: (m["best_identity"] or -1.0)), population


@router.get("/{prediction_set_id}/members", summary="Proteins in one stratum")
def stratum_members(
    prediction_set_id: uuid.UUID,
    at: _Where = Depends(),
    limit: int = Query(default=200, ge=1, le=2000, description="Cap on rows returned."),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """The proteins a stratum is made of, with what put each one there.

    Every row carries its nearest donor and the identity to it, because
    that is what the band IS: a reader who cannot see the donor cannot
    check the band, and a band nobody can check is a label.
    """
    with factory() as session:
        hoods = neighbourhoods_for(session, str(prediction_set_id))
        if not hoods:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"prediction set {prediction_set_id} retrieved no non-self donors, "
                    "so no query in it can be placed on the homology axis"
                ),
            )
        lengths = {
            r["accession"]: r["residues"]
            for r in session.execute(
                _LENGTHS, {"accessions": list(hoods)}
            ).mappings()
        }

    members, population = _walk(hoods, lengths, at, limit)

    return {
        "prediction_set_id": str(prediction_set_id),
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
