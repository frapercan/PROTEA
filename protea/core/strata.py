"""The four axes every result is reported along, and the refusal to pool them.

No number in this campaign is reported for a population as a whole. Results are
reported per stratum, and a stratum is a point on four axes:

1. **category**, the prior-knowledge partition, from :mod:`protea.core.domain.category`
2. **aspect**, the sub-ontology, from :mod:`protea.core.domain.aspect`
3. **length band**, from the model context limits, see :class:`LengthBand`
4. **homology band crossed with donor provenance**, see :class:`HomologyBand`
   and :class:`DonorEvidence`

The first two are imported rather than restated, because they already exist as
typed enums and a second spelling of a closed set is a second thing to keep in
sync.

**Why pooling is refused rather than discouraged.** An unweighted mean across
strata is itself a reweighting: the strata hold populations that differ by more
than an order of magnitude, so a plain average silently promotes the smallest
ones. It also moves in the flattering direction, because the smallest strata are
the easiest. This has been reported wrongly before, which is why
:func:`pooled_mean` refuses to run without population sizes and
:func:`assert_stratified` refuses a result that names no stratum.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import NamedTuple

from protea.core.domain.aspect import Aspect
from protea.core.domain.category import Category

__all__ = [
    "DonorEvidence",
    "HomologyBand",
    "LengthBand",
    "Stratum",
    "UnstratifiedResultError",
    "all_strata",
    "assert_stratified",
    "homology_band_for",
    "length_band_for",
    "pooled_mean",
    "report_order",
    "stratum_for",
]


class UnstratifiedResultError(ValueError):
    """A result was reported for a population rather than for a stratum."""


@dataclass(frozen=True)
class _Band:
    """A half-open interval ``(lower, upper]`` with a label."""

    label: str
    lower: float
    upper: float

    def contains(self, value: float) -> bool:
        return self.lower < value <= self.upper


class LengthBand(StrEnum):
    """Sequence length band.

    The boundaries are the model context limits rather than quantiles of the
    corpus. A protein longer than the context is truncated before it is
    embedded, so the bands separate sequences the representation saw whole from
    sequences it saw in part, which is a different question from whether a
    protein is long. Quantile bands would mix the two and move whenever the
    corpus did.
    """

    SHORT = "<=512"
    MEDIUM = "512-1024"
    LONG = "1024-2048"
    TRUNCATED = ">2048"


_LENGTH_BANDS: tuple[tuple[LengthBand, _Band], ...] = (
    (LengthBand.SHORT, _Band("<=512", -1.0, 512.0)),
    (LengthBand.MEDIUM, _Band("512-1024", 512.0, 1024.0)),
    (LengthBand.LONG, _Band("1024-2048", 1024.0, 2048.0)),
    (LengthBand.TRUNCATED, _Band(">2048", 2048.0, float("inf"))),
)


def length_band_for(residues: int) -> LengthBand:
    """Return the length band for a sequence of ``residues`` residues."""
    if residues <= 0:
        raise ValueError(f"a sequence cannot have {residues} residues")
    for band, interval in _LENGTH_BANDS:
        if interval.contains(float(residues)):
            return band
    raise AssertionError("the bands are exhaustive")  # pragma: no cover


class HomologyBand(StrEnum):
    """How close the nearest annotated donor is, by sequence identity.

    The boundaries mark where transfer by similarity is known to change
    character rather than where the corpus happens to split. Below about thirty
    percent identity is the twilight zone, where alignment alone stops implying
    shared function. Above about ninety percent, transfer is close to a copy
    and says little about whether a method generalises. The middle is where
    the interesting decisions live, so it is split once.

    ``NONE`` is a band, not a missing value. A query with no donor at all is a
    real and reportable population, and folding it into the lowest identity
    band would hide the retrieval failures inside the modelling results.

    Every band is half-open, ``(lower, upper]``, and the labels say so: an
    identity of exactly thirty falls in the twilight band and exactly ninety
    falls in the close band. A label that disagreed with its own cut would
    mis-title a column in every table that reported it.
    """

    NONE = "none"
    TWILIGHT = "<=30"
    DISTANT = "30-60"
    CLOSE = "60-90"
    NEAR_IDENTICAL = ">90"


_HOMOLOGY_BANDS: tuple[tuple[HomologyBand, _Band], ...] = (
    (HomologyBand.TWILIGHT, _Band("<=30", -1.0, 30.0)),
    (HomologyBand.DISTANT, _Band("30-60", 30.0, 60.0)),
    (HomologyBand.CLOSE, _Band("60-90", 60.0, 90.0)),
    (HomologyBand.NEAR_IDENTICAL, _Band(">90", 90.0, 100.0)),
)


def homology_band_for(best_identity: float | None) -> HomologyBand:
    """Return the band for the best donor identity, as a percentage.

    ``None`` means no donor was retrieved at all, which is
    :attr:`HomologyBand.NONE` rather than an identity of zero.
    """
    if best_identity is None:
        return HomologyBand.NONE
    if not 0.0 <= best_identity <= 100.0:
        raise ValueError(f"identity {best_identity} is not a percentage in [0, 100]")
    for band, interval in _HOMOLOGY_BANDS:
        if interval.contains(best_identity):
            return band
    raise AssertionError("the bands are exhaustive")  # pragma: no cover


class DonorEvidence(StrEnum):
    """Whether the nearest donor's annotation rests on experiment.

    The fourth axis is the homology band **crossed with this**, because the two
    together are what a homology threshold actually means. A close donor whose
    own annotation was itself inferred computationally transfers a guess, and
    reporting it beside a close donor with experimental support would average
    two different claims into one number.
    """

    EXPERIMENTAL = "exp"
    OTHER = "other"
    NONE = "none"


class Stratum(NamedTuple):
    """One point on the four axes. The unit every result is reported for."""

    category: Category
    aspect: Aspect
    length: LengthBand
    homology: HomologyBand
    donor_evidence: DonorEvidence

    def __str__(self) -> str:
        return (
            f"{self.category.code}/{self.aspect.code}/{self.length.value}/"
            f"{self.homology.value}/{self.donor_evidence.value}"
        )


def stratum_for(
    *,
    category: Category | str,
    aspect: Aspect | str,
    residues: int,
    best_identity: float | None,
    donor_is_experimental: bool | None,
) -> Stratum:
    """Place one observation on the four axes.

    ``donor_is_experimental`` is ``None`` when there is no donor, which pairs
    with :attr:`HomologyBand.NONE`. Passing a verdict alongside no donor is a
    contradiction and is refused rather than silently resolved.
    """
    homology = homology_band_for(best_identity)
    if (homology is HomologyBand.NONE) != (donor_is_experimental is None):
        raise ValueError(
            "the donor band and the donor evidence disagree about whether a "
            f"donor exists: band={homology.value}, evidence={donor_is_experimental!r}"
        )
    if donor_is_experimental is None:
        evidence = DonorEvidence.NONE
    else:
        evidence = DonorEvidence.EXPERIMENTAL if donor_is_experimental else DonorEvidence.OTHER
    return Stratum(
        category=Category.from_code(category) if isinstance(category, str) else category,
        aspect=Aspect.from_code(aspect) if isinstance(aspect, str) else aspect,
        length=length_band_for(residues),
        homology=homology,
        donor_evidence=evidence,
    )


def all_strata() -> tuple[Stratum, ...]:
    """Every stratum the axes can express, in a stable order.

    Derived from the axes rather than written out, so adding a band cannot
    leave a report silently covering fewer strata than it claims to.
    """
    out: list[Stratum] = []
    for category in Category:
        for aspect in Aspect:
            for length in LengthBand:
                for homology in HomologyBand:
                    evidences = (
                        (DonorEvidence.NONE,)
                        if homology is HomologyBand.NONE
                        else (DonorEvidence.EXPERIMENTAL, DonorEvidence.OTHER)
                    )
                    for evidence in evidences:
                        out.append(Stratum(category, aspect, length, homology, evidence))
    return tuple(out)


def assert_stratified(results: Mapping[Stratum, object], *, context: str) -> None:
    """Raise if a reported result names no stratum.

    The cheapest way to violate the rule is to report one number and call it
    the result. This is what a reporting path calls to say it did not.
    """
    if not results:
        raise UnstratifiedResultError(
            f"{context}: no stratum was reported. Every result in this campaign "
            f"is reported per stratum, because the populations differ by more "
            f"than an order of magnitude and a single number over them is an "
            f"unweighted mean wearing a different name."
        )


def pooled_mean(
    values: Mapping[Stratum, float],
    populations: Mapping[Stratum, int],
    *,
    context: str,
) -> float:
    """Combine per-stratum values, weighted by population.

    There is no unweighted variant on purpose. An unweighted mean across strata
    promotes the smallest ones, and the smallest strata here are the easiest, so
    the error moves in the flattering direction. Anything calling this has to
    produce the population sizes, which is also what has to be printed beside
    the number.
    """
    missing = set(values) - set(populations)
    if missing:
        raise UnstratifiedResultError(
            f"{context}: {len(missing)} stratum/strata have a value but no "
            f"population size, so they cannot be weighted: "
            f"{', '.join(sorted(str(s) for s in missing)[:3])}. "
            f"The population sizes are not optional here; they are what makes "
            f"the combination a mean of the population rather than a mean of "
            f"the strata."
        )
    total = sum(populations[s] for s in values)
    if total <= 0:
        raise UnstratifiedResultError(f"{context}: the reported strata hold no observations")
    return sum(values[s] * populations[s] for s in values) / total


def report_order(strata: Sequence[Stratum]) -> tuple[Stratum, ...]:
    """Return ``strata`` in the canonical axis order, for stable tables."""
    index = {stratum: i for i, stratum in enumerate(all_strata())}
    return tuple(sorted(strata, key=lambda s: index[s]))
