"""The four axes, and the refusal to report a number that names no stratum."""

from __future__ import annotations

import pytest

from protea.core.domain.aspect import Aspect
from protea.core.domain.category import Category
from protea.core.strata import (
    DonorEvidence,
    HomologyBand,
    LengthBand,
    Stratum,
    UnstratifiedResultError,
    all_strata,
    assert_stratified,
    homology_band_for,
    length_band_for,
    pooled_mean,
    report_order,
    stratum_for,
)


class TestLengthBandsFollowTheContextLimit:
    """The bands separate what the representation saw whole from what it did not."""

    @pytest.mark.parametrize(
        ("residues", "expected"),
        [
            (1, LengthBand.SHORT),
            (512, LengthBand.SHORT),
            (513, LengthBand.MEDIUM),
            (1024, LengthBand.MEDIUM),
            (1025, LengthBand.LONG),
            (2048, LengthBand.LONG),
            (2049, LengthBand.TRUNCATED),
            (40_000, LengthBand.TRUNCATED),
        ],
    )
    def test_the_boundaries_are_the_context_limits(
        self, residues: int, expected: LengthBand
    ) -> None:
        assert length_band_for(residues) is expected

    def test_the_label_agrees_with_the_cut(self) -> None:
        """A protein of exactly 512 residues belongs to the band labelled <=512."""
        assert length_band_for(512).value == "<=512"

    @pytest.mark.parametrize("residues", [0, -1])
    def test_an_impossible_length_is_refused(self, residues: int) -> None:
        with pytest.raises(ValueError, match="cannot have"):
            length_band_for(residues)


class TestHomologyBandsAndTheirLabels:
    @pytest.mark.parametrize(
        ("identity", "expected"),
        [
            (0.0, HomologyBand.TWILIGHT),
            (30.0, HomologyBand.TWILIGHT),
            (30.1, HomologyBand.DISTANT),
            (60.0, HomologyBand.DISTANT),
            (60.1, HomologyBand.CLOSE),
            (90.0, HomologyBand.CLOSE),
            (90.1, HomologyBand.NEAR_IDENTICAL),
            (100.0, HomologyBand.NEAR_IDENTICAL),
        ],
    )
    def test_the_bands_are_half_open(self, identity: float, expected: HomologyBand) -> None:
        assert homology_band_for(identity) is expected

    def test_every_label_agrees_with_its_own_cut(self) -> None:
        """A mislabelled boundary mis-titles a column in every table."""
        assert homology_band_for(30.0).value == "<=30"
        assert homology_band_for(90.0).value == "60-90"
        assert homology_band_for(90.1).value == ">90"

    def test_no_donor_is_a_band_not_a_missing_value(self) -> None:
        """Folding it into the lowest band hides retrieval failures in the model."""
        assert homology_band_for(None) is HomologyBand.NONE
        assert homology_band_for(0.0) is not HomologyBand.NONE

    @pytest.mark.parametrize("identity", [-0.1, 100.1, 1000.0])
    def test_a_non_percentage_is_refused(self, identity: float) -> None:
        with pytest.raises(ValueError, match="percentage"):
            homology_band_for(identity)


class TestPlacingAnObservation:
    def test_it_lands_on_all_four_axes(self) -> None:
        s = stratum_for(
            category="NK",
            aspect="P",
            residues=3000,
            best_identity=25.0,
            donor_is_experimental=True,
        )
        assert s.category is Category.NO_KNOWLEDGE
        assert s.aspect is Aspect.BIOLOGICAL_PROCESS
        assert s.length is LengthBand.TRUNCATED
        assert s.homology is HomologyBand.TWILIGHT
        assert s.donor_evidence is DonorEvidence.EXPERIMENTAL

    def test_typed_axes_are_accepted_as_well_as_codes(self) -> None:
        by_code = stratum_for(
            category="LK", aspect="F", residues=100, best_identity=None,
            donor_is_experimental=None,
        )
        by_enum = stratum_for(
            category=Category.LIMITED_KNOWLEDGE, aspect=Aspect.MOLECULAR_FUNCTION, residues=100,
            best_identity=None, donor_is_experimental=None,
        )
        assert by_code == by_enum

    def test_a_donor_verdict_without_a_donor_is_refused(self) -> None:
        """Silently resolving the contradiction would invent a population."""
        with pytest.raises(ValueError, match="disagree about whether a donor exists"):
            stratum_for(
                category="NK", aspect="P", residues=100, best_identity=None,
                donor_is_experimental=True,
            )

    def test_a_donor_without_a_verdict_is_refused_too(self) -> None:
        with pytest.raises(ValueError, match="disagree about whether a donor exists"):
            stratum_for(
                category="NK", aspect="P", residues=100, best_identity=55.0,
                donor_is_experimental=None,
            )

    def test_the_string_form_names_every_axis(self) -> None:
        s = stratum_for(
            category="PK", aspect="C", residues=100, best_identity=95.0,
            donor_is_experimental=False,
        )
        assert str(s) == "PK/C/<=512/>90/other"


class TestTheGridIsDerivedNotEnumerated:
    def test_no_stratum_repeats(self) -> None:
        assert len(set(all_strata())) == len(all_strata())

    def test_the_no_donor_band_carries_only_the_no_donor_evidence(self) -> None:
        """Crossing it with an evidence verdict would invent empty strata."""
        for s in all_strata():
            if s.homology is HomologyBand.NONE:
                assert s.donor_evidence is DonorEvidence.NONE
            else:
                assert s.donor_evidence is not DonorEvidence.NONE

    def test_every_axis_value_appears(self) -> None:
        grid = all_strata()
        assert {s.category for s in grid} == set(Category)
        assert {s.aspect for s in grid} == set(Aspect)
        assert {s.length for s in grid} == set(LengthBand)
        assert {s.homology for s in grid} == set(HomologyBand)

    def test_the_size_is_what_the_axes_imply(self) -> None:
        """3 categories x 3 aspects x 4 lengths x (1 + 4x2) donor states."""
        assert len(all_strata()) == 3 * 3 * 4 * 9

    def test_a_placed_observation_is_in_the_grid(self) -> None:
        s = stratum_for(
            category="LK", aspect="F", residues=700, best_identity=45.0,
            donor_is_experimental=False,
        )
        assert s in set(all_strata())

    def test_report_order_is_stable(self) -> None:
        sample = list(all_strata()[:10])
        assert report_order(list(reversed(sample))) == tuple(sample)


class TestPoolingIsRefusedRatherThanDiscouraged:
    def _stratum(self, category: str = "NK") -> Stratum:
        return stratum_for(
            category=category, aspect="P", residues=100, best_identity=50.0,
            donor_is_experimental=True,
        )

    def test_an_empty_report_raises(self) -> None:
        with pytest.raises(UnstratifiedResultError, match="no stratum was reported"):
            assert_stratified({}, context="the headline")

    def test_a_report_naming_a_stratum_passes(self) -> None:
        assert_stratified({self._stratum(): 0.5}, context="the headline")

    def test_combining_without_populations_raises(self) -> None:
        """The population sizes are what make it a mean of the population."""
        with pytest.raises(UnstratifiedResultError, match="no population size"):
            pooled_mean({self._stratum(): 0.5}, {}, context="the headline")

    def test_the_mean_is_weighted_by_population(self) -> None:
        big, small = self._stratum("NK"), self._stratum("PK")
        got = pooled_mean({big: 0.2, small: 0.8}, {big: 900, small: 100}, context="c")
        assert got == pytest.approx(0.26)

    def test_it_differs_from_the_unweighted_mean(self) -> None:
        """The whole point: the unweighted mean promotes the smallest stratum."""
        big, small = self._stratum("NK"), self._stratum("PK")
        weighted = pooled_mean({big: 0.2, small: 0.8}, {big: 900, small: 100}, context="c")
        assert weighted != pytest.approx((0.2 + 0.8) / 2)

    def test_an_empty_population_raises_rather_than_dividing_by_zero(self) -> None:
        s = self._stratum()
        with pytest.raises(UnstratifiedResultError, match="no observations"):
            pooled_mean({s: 0.5}, {s: 0}, context="c")
