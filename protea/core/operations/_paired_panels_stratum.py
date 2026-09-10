"""Restricting a paired comparison to one stratum, and naming what it restricted to.

Split out of :mod:`compare_paired_panels` because the two concerns are
different: that module decides what a difference between two arms means, and
this one decides which proteins the difference is over. Keeping them together
pushed the operation past the file budget, which is the budget doing its job.
"""

from __future__ import annotations

from typing import Any, NamedTuple

from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn
from protea.core.operations._paired_panels_artifact import PanelComparabilityError
from protea.core.operations._run_cafa_strata import neighbourhoods_for
from protea.core.operations.stratify_evaluation import _protein_lengths
from protea.core.strata import (
    NEIGHBOURHOOD_AXES,
    Aspect,
    Category,
    DonorEvidence,
    HomologyBand,
    LengthBand,
    Neighbourhood,
    PropagationBand,
    Stratum,
    TaxonomyBand,
    stratum_for,
)

#: Stands in when no donor axis was requested, so the one placement path serves
#: both kinds of restriction. Only the sequence axes may be read off a stratum
#: built with it -- which is exactly the case in which it is used.
_NO_DONOR_READ = Neighbourhood(best_identity=None, donor_is_experimental=None)

_BAND_TYPE: dict[str, type] = {
    "category": Category,
    "aspect": Aspect,
    "length": LengthBand,
    "homology": HomologyBand,
    "donor_evidence": DonorEvidence,
    "taxonomy": TaxonomyBand,
    "propagation": PropagationBand,
}


class Restriction(NamedTuple):
    """Which proteins a panel is narrowed to, and the name that narrowing is reported under.

    The two travel together on purpose. A restricted delta reported under the
    unrestricted panel's name is a number that cannot be told apart from the one
    it is not, so nothing may carry the population without also carrying the label.
    """

    keep: frozenset[str]
    label: str


def validate_stratum(value: dict[str, str] | None) -> dict[str, str] | None:
    """A restriction is a claim about a population, so an unreadable one is refused.

    Lives beside :data:`_BAND_TYPE` rather than in the payload, so the closed
    vocabularies are known in one place: a validator that kept its own copy
    would keep passing after the enums moved on.

    Both halves are checked. An unknown AXIS would silently restrict nothing and
    report a whole-panel delta under a stratum's name; an unknown BAND would
    restrict to the empty set and report a refusal that looks like a sparse
    stratum rather than like a typo.
    """
    if value is None:
        return None
    if not value:
        raise ValueError(
            "omit restrict_to_stratum to compare the whole panel; an empty mapping "
            "asks for a restriction and names none"
        )
    unknown = sorted(k for k in value if k not in Stratum._fields)
    if unknown:
        raise ValueError(f"unknown stratum axes {unknown}; the seven are {list(Stratum._fields)}")
    for axis, band in value.items():
        allowed = [b.value for b in _BAND_TYPE[axis]]
        if band not in allowed:
            raise ValueError(f"{axis}={band!r} is not one of {allowed}")
    return dict(value)


def stratum_label(key: str, restrict: dict[str, str] | None) -> str:
    """``NK:MFO`` unrestricted, ``NK:MFO@length=512-1024`` restricted.

    Axes are sorted so the same restriction always produces the same string:
    a key that varied with dict order would file one stratum under two names.
    """
    if not restrict:
        return key
    inner = ",".join(f"{a}={restrict[a]}" for a in sorted(restrict))
    return f"{key}@{inner}"


def _place(
    lengths: dict[str, int],
    hoods: dict[str, Any],
    restrict: dict[str, str],
    *,
    needs_donor: bool,
) -> tuple[set[str], int]:
    """Which proteins sit in the stratum, and how many could not be placed at all.

    The two are returned together because reporting the first without the second
    would let a stratum thinned by missing alignments read as a stratum that is
    genuinely small.

    ``category`` and ``aspect`` are fixed here rather than matched: they vary per
    ROW of the artefact, not per protein, so the panel key already carries them
    and matching them again on a protein would be matching the wrong thing.
    """
    keep: set[str] = set()
    unplaceable = 0
    axes = [a for a in restrict if a in NEIGHBOURHOOD_AXES or a == "length"]
    for acc, residues in lengths.items():
        if not residues:
            continue
        hood = hoods.get(acc)
        if hood is None:
            if needs_donor:
                unplaceable += 1
                continue
            hood = _NO_DONOR_READ
        st = stratum_for(
            category=Category.NO_KNOWLEDGE,
            aspect=Aspect.MOLECULAR_FUNCTION,
            residues=residues,
            neighbourhood=hood,
        )
        if all(getattr(st, a) == restrict[a] for a in axes):
            keep.add(acc)
    return keep, unplaceable


def stratum_population(
    session: Session,
    restrict: dict[str, str],
    baseline_prediction_set_id: str,
    emit: EmitFn,
) -> frozenset[str]:
    """The accessions that sit in the requested stratum, read off the BASELINE.

    WHOSE NEIGHBOURHOOD DEFINES MEMBERSHIP. For the three sequence axes --
    category, aspect, length -- the question does not arise: they are properties
    of the query itself and both arms answer identically. The four donor axes
    are properties of a RETRIEVAL, and the two arms retrieved different donors,
    so the same protein can sit in ``<=30`` for one arm and ``30-50`` for the
    other.

    Membership is therefore always taken from the baseline, never from the arm
    under test. "Among the proteins the baseline found hard, does the challenger
    help?" is a question with an answer. "Among the proteins the challenger
    placed in the twilight zone" is the challenger choosing its own population,
    and a method that retrieves worse would be handed an easier stratum to be
    measured on. The choice is recorded on the job so a reader is never left to
    infer which arm the band came from.
    """
    needs_donor = bool(NEIGHBOURHOOD_AXES & set(restrict))
    lengths = _protein_lengths(session)
    hoods = neighbourhoods_for(session, baseline_prediction_set_id) if needs_donor else {}

    keep, unplaceable = _place(lengths, hoods, restrict, needs_donor=needs_donor)

    emit(
        "compare_paired_panels.stratum",
        f"population restricted to {restrict}",
        {
            "restrict_to_stratum": dict(restrict),
            "membership_from": "baseline",
            "baseline_prediction_set_id": baseline_prediction_set_id,
            "n_in_stratum": len(keep),
            "n_unplaceable": unplaceable,
            "reads_a_donor": needs_donor,
        },
        "info",
    )
    if not keep:
        raise PanelComparabilityError(
            f"no protein sits in stratum {restrict}: the restriction selects an empty "
            "population, so there is nothing to resample. That is a refusal and not a "
            "zero delta."
        )
    return frozenset(keep)


