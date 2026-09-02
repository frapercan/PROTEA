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
    "Neighbourhood",
    "PropagationBand",
    "TaxonomyBand",
    "UnstratifiedResultError",
    "all_strata",
    "assert_stratified",
    "homology_band_for",
    "length_band_for",
    "pooled_mean",
    "propagation_band_for",
    "report_order",
    "reportable_strata",
    "stratum_for",
    "taxonomy_band_for",
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


class TaxonomyBand(StrEnum):
    """How far the nearest donor sits from the query in the tree of life.

    The values mirror ``go_prediction.taxonomic_relation``, which is already
    computed and stored, so this axis costs a reduction rule rather than a
    computation.

    The rule is the part that matters. The nearest donor of all is almost always
    the query itself, retrieved from the earlier annotation bank under the
    temporal protocol, and its taxonomic relation is trivially ``SAME``. Measured
    on prot_t5 at K=30, that self hit is the nearest experimentally supported
    donor for 83.8 percent of proteins. So this band is resolved over donors
    whose sequence differs from the query's, which is the only reading under
    which "same species" is a fact about the neighbourhood rather than a fact
    about the protocol.
    """

    SAME = "same"
    CLOSE = "close"
    INTERMEDIATE = "intermediate"
    DISTANT = "distant"
    ROOT_ONLY = "root-only"
    NONE = "none"


_TAXONOMY_ALIASES: Mapping[str, TaxonomyBand] = {
    "same": TaxonomyBand.SAME,
    "close": TaxonomyBand.CLOSE,
    "intermediate": TaxonomyBand.INTERMEDIATE,
    "distant": TaxonomyBand.DISTANT,
    "root-only": TaxonomyBand.ROOT_ONLY,
    # ancestor and descendant are lineal relations rather than distances. They
    # are 0.04 percent of donor rows each, too few to carry a band of their own,
    # and folding them into DISTANT would overstate the separation, so they join
    # the band that makes the weaker claim.
    "ancestor": TaxonomyBand.INTERMEDIATE,
    "descendant": TaxonomyBand.INTERMEDIATE,
    # child and parent are the one-step cases of the same lineal relation and
    # take the same band for the same reason. They are listed because
    # feature_engineering can emit them: a vocabulary that covers only what has
    # been seen so far fails on the first run that sees the rest.
    "child": TaxonomyBand.INTERMEDIATE,
    "parent": TaxonomyBand.INTERMEDIATE,
    # "unrelated" is emitted when either taxon is missing, not when two taxa are
    # far apart (feature_engineering returns it before any lineage is compared).
    # It is therefore absence of information, and belongs with the other NONE
    # rather than in a distance band, which would assert a separation nobody
    # measured.
    "unrelated": TaxonomyBand.NONE,
}


def taxonomy_band_for(relation: str | None) -> TaxonomyBand:
    """Return the taxonomy band for a stored ``taxonomic_relation``.

    ``None`` means no donor with a resolvable lineage, which is a population
    rather than a missing value, so it is :attr:`TaxonomyBand.NONE`.
    """
    if relation is None:
        return TaxonomyBand.NONE
    try:
        return _TAXONOMY_ALIASES[relation]
    except KeyError:
        raise ValueError(
            f"unknown taxonomic relation {relation!r}; known relations: "
            f"{', '.join(sorted(_TAXONOMY_ALIASES))}"
        ) from None


class PropagationBand(StrEnum):
    """How far an annotation had to travel to reach the query.

    Not the homology band. That one asks how close the nearest donor of any kind
    is. This one asks how much further you must go to reach a donor whose own
    annotation rests on experiment, which is the gap between two distances:
    the nearest donor of any kind, and the nearest experimentally supported one.
    A protein can sit in a dense neighbourhood and still be far from any
    experimental evidence, and that gap is the distance the annotation travelled.

    ``ZERO`` is a band and not a boundary case. Measured on prot_t5 at K=30 over
    donors of differing sequence, 53.5 percent of proteins have a gap of exactly
    zero: their nearest neighbour already carries experimental support. Quantile
    cuts on this distribution would put the median, the lower quartile and the
    upper quartile all at zero and produce three bands holding the same
    population, so the cuts here are fixed and the zero mass is separated first.

    The positive tail is cut at 0.05 and 0.15. The 95th percentile of the gap is
    0.133, so the last band is the tail beyond nearly all of the mass rather
    than a band that happens to be sparse.
    """

    NONE = "none"
    ZERO = "0"
    NEAR = "0-0.05"
    MID = "0.05-0.15"
    FAR = ">0.15"


_PROPAGATION_BANDS: tuple[tuple[PropagationBand, _Band], ...] = (
    (PropagationBand.NEAR, _Band("0-0.05", 0.0, 0.05)),
    (PropagationBand.MID, _Band("0.05-0.15", 0.05, 0.15)),
    (PropagationBand.FAR, _Band(">0.15", 0.15, float("inf"))),
)


def propagation_band_for(
    nearest_any: float | None,
    nearest_experimental: float | None,
) -> PropagationBand:
    """Return the band for the gap between the two nearest-donor distances.

    Both distances are over donors whose sequence differs from the query's. The
    self hit and same-sequence duplicates are excluded before this is called,
    because including them collapses ``nearest_any`` to approximately zero and
    turns the gap into a restatement of whether the protein was already
    annotated, which the category axis carries.

    ``nearest_experimental`` of ``None`` means no experimentally supported donor
    was retrieved at all. That is right-censored rather than infinite, and it is
    reported as :attr:`PropagationBand.NONE` so it can never be averaged into
    the tail.
    """
    if nearest_any is None or nearest_experimental is None:
        return PropagationBand.NONE
    gap = nearest_experimental - nearest_any
    if gap < 0.0:
        raise ValueError(
            f"the nearest experimental donor is closer than the nearest donor "
            f"of any kind ({nearest_experimental} < {nearest_any}), which means "
            f"the two distances were resolved over different donor sets"
        )
    if gap <= 0.0:
        return PropagationBand.ZERO
    for band, interval in _PROPAGATION_BANDS:
        if interval.contains(gap):
            return band
    raise AssertionError("the bands are exhaustive")  # pragma: no cover


class Neighbourhood(NamedTuple):
    """What the retrieved neighbourhood looks like for one query.

    Five facts about the donors, bundled because they are read from the same
    retrieval and are only meaningful together: a homology band without the
    donor's evidence describes a transfer whose provenance is unknown, and a
    propagation gap without the taxonomy says how far the annotation travelled
    but not through what.

    All of it is resolved over donors whose sequence differs from the query's.
    Under the temporal protocol the query retrieves itself at distance zero,
    and a caller that includes that hit measures the protocol rather than the
    neighbourhood.
    """

    best_identity: float | None
    donor_is_experimental: bool | None
    taxonomic_relation: str | None = None
    nearest_any: float | None = None
    nearest_experimental: float | None = None


class Stratum(NamedTuple):
    """One point on the six axes. The unit every result is reported for."""

    category: Category
    aspect: Aspect
    length: LengthBand
    homology: HomologyBand
    donor_evidence: DonorEvidence
    taxonomy: TaxonomyBand
    propagation: PropagationBand

    def __str__(self) -> str:
        return (
            f"{self.category.code}/{self.aspect.code}/{self.length.value}/"
            f"{self.homology.value}/{self.donor_evidence.value}/"
            f"{self.taxonomy.value}/{self.propagation.value}"
        )


def stratum_for(
    *,
    category: Category | str,
    aspect: Aspect | str,
    residues: int,
    neighbourhood: Neighbourhood,
) -> Stratum:
    """Place one observation on the six axes.

    ``donor_is_experimental`` is ``None`` when there is no donor, which pairs
    with :attr:`HomologyBand.NONE`. Passing a verdict alongside no donor is a
    contradiction and is refused rather than silently resolved.

    The three donor-neighbourhood arguments are all resolved over donors whose
    sequence differs from the query's. Under the temporal protocol the query is
    present in the earlier annotation bank and retrieves itself at a distance of
    approximately zero, so a caller that passes distances including the self hit
    will place almost every protein in the same two bands and the axes will
    report the protocol rather than the neighbourhood.
    """
    n = neighbourhood
    homology = homology_band_for(n.best_identity)
    if (homology is HomologyBand.NONE) != (n.donor_is_experimental is None):
        raise ValueError(
            "the donor band and the donor evidence disagree about whether a "
            f"donor exists: band={homology.value}, evidence={n.donor_is_experimental!r}"
        )
    if n.donor_is_experimental is None:
        evidence = DonorEvidence.NONE
    else:
        evidence = DonorEvidence.EXPERIMENTAL if n.donor_is_experimental else DonorEvidence.OTHER
    return Stratum(
        category=Category.from_code(category) if isinstance(category, str) else category,
        aspect=Aspect.from_code(aspect) if isinstance(aspect, str) else aspect,
        length=length_band_for(residues),
        homology=homology,
        donor_evidence=evidence,
        taxonomy=taxonomy_band_for(n.taxonomic_relation),
        propagation=propagation_band_for(n.nearest_any, n.nearest_experimental),
    )


def all_strata() -> tuple[Stratum, ...]:
    """Every stratum the axes can express, in a stable order.

    Derived from the axes rather than written out, so adding a band cannot
    leave a report silently covering fewer strata than it claims to.

    This is an ordering key and not a reporting frame. The six axes cross to
    9,720 strata, and the evaluation set holds 6,216 proteins, so most of these
    can never be occupied and the cross is not a claim that they should be.

    The five protein-level axes admit 1,080 combinations, not the 1,800 a free
    product of the enums would give, because homology and donor evidence are
    coupled here rather than crossed: :attr:`HomologyBand.NONE` admits only
    :attr:`DonorEvidence.NONE`, and each of the four bands that do have a donor
    admits only EXPERIMENTAL or OTHER, never all three. That pair therefore
    contributes 1 + 4x2 = 9 states rather than 15, and the protein-level count
    is 4 lengths x 9 donor states x 6 taxonomy bands x 5 propagation bands =
    1,080. The 9,720 above already implies it: 9 category-aspect pairs x 1,080.

    Of those 1,080, exactly 77 were populated when this was last measured, on
    prot_t5 at K=30, because length, homology, taxonomy and propagation covary
    strongly. That measurement predates the campaign wipe of 2026-08-27 and no
    result now in the database is older than the wipe, so the 77 describes a
    window that no longer exists and has not been re-measured on the current
    one. Read it as evidence that the populated fraction is small, not as a
    current count. Report from :func:`reportable_strata`, which enumerates what
    the data actually holds, and use this only to sort what that returns.
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
                        for taxonomy in TaxonomyBand:
                            for propagation in PropagationBand:
                                out.append(
                                    Stratum(
                                        category,
                                        aspect,
                                        length,
                                        homology,
                                        evidence,
                                        taxonomy,
                                        propagation,
                                    )
                                )
    return tuple(out)


def reportable_strata(
    populations: Mapping[Stratum, int],
    *,
    min_population: int,
) -> tuple[tuple[Stratum, ...], tuple[Stratum, ...]]:
    """Split observed strata into those big enough to report and those not.

    Returns ``(reportable, withheld)``, both in canonical axis order.

    A stratum holding three proteins produces a number, and that number will sit
    in a table next to one computed over two thousand. The floor exists so the
    thin ones are named as withheld rather than printed at the same weight, and
    both halves are returned so a caller can say how much of the population it
    is not showing. Dropping them silently is the failure this replaces: with
    six axes most of the cross is empty, and a frame that only prints what
    survived looks identical to a frame that covered everything.
    """
    if min_population < 1:
        raise ValueError(f"min_population must be at least 1, got {min_population}")
    reportable = [s for s, n in populations.items() if n >= min_population]
    withheld = [s for s, n in populations.items() if n < min_population]
    return report_order(reportable), report_order(withheld)


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
