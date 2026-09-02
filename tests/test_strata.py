"""The six axes, and the refusal to report a number that names no stratum."""

from __future__ import annotations

import pytest

from protea.core.domain.aspect import Aspect
from protea.core.domain.category import Category
from protea.core.strata import (
    DonorEvidence,
    HomologyBand,
    LengthBand,
    Neighbourhood,
    PropagationBand,
    Stratum,
    TaxonomyBand,
    UnstratifiedResultError,
    all_strata,
    assert_stratified,
    homology_band_for,
    length_band_for,
    pooled_mean,
    propagation_band_for,
    report_order,
    reportable_strata,
    stratum_for,
    taxonomy_band_for,
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
            neighbourhood=Neighbourhood(best_identity=25.0, donor_is_experimental=True),
        )
        assert s.category is Category.NO_KNOWLEDGE
        assert s.aspect is Aspect.BIOLOGICAL_PROCESS
        assert s.length is LengthBand.TRUNCATED
        assert s.homology is HomologyBand.TWILIGHT
        assert s.donor_evidence is DonorEvidence.EXPERIMENTAL

    def test_typed_axes_are_accepted_as_well_as_codes(self) -> None:
        by_code = stratum_for(
            category="LK",
            aspect="F",
            residues=100,
            neighbourhood=Neighbourhood(best_identity=None, donor_is_experimental=None),
        )
        by_enum = stratum_for(
            category=Category.LIMITED_KNOWLEDGE,
            aspect=Aspect.MOLECULAR_FUNCTION,
            residues=100,
            neighbourhood=Neighbourhood(best_identity=None, donor_is_experimental=None),
        )
        assert by_code == by_enum

    def test_a_donor_verdict_without_a_donor_is_refused(self) -> None:
        """Silently resolving the contradiction would invent a population."""
        with pytest.raises(ValueError, match="disagree about whether a donor exists"):
            stratum_for(
            category="NK",
            aspect="P",
            residues=100,
            neighbourhood=Neighbourhood(best_identity=None, donor_is_experimental=True),
        )

    def test_a_donor_without_a_verdict_is_refused_too(self) -> None:
        with pytest.raises(ValueError, match="disagree about whether a donor exists"):
            stratum_for(
            category="NK",
            aspect="P",
            residues=100,
            neighbourhood=Neighbourhood(best_identity=55.0, donor_is_experimental=None),
        )

    def test_the_string_form_names_every_axis(self) -> None:
        s = stratum_for(
            category="PK",
            aspect="C",
            residues=100,
            neighbourhood=Neighbourhood(best_identity=95.0, donor_is_experimental=False),
        )
        assert str(s) == "PK/C/<=512/>90/other/none/none"


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
        assert {s.taxonomy for s in grid} == set(TaxonomyBand)
        assert {s.propagation for s in grid} == set(PropagationBand)

    def test_the_size_is_what_the_axes_imply(self) -> None:
        """3 categories x 3 aspects x 4 lengths x (1 + 4x2) donor states
        x 6 taxonomy bands x 5 propagation bands.

        The number is large on purpose and is not a reporting frame. See
        ``reportable_strata``: of the 1,080 combinations the five
        protein-level axes admit, 77 were populated on prot_t5 at K=30
        before the campaign wipe of 2026-08-27.
        """
        assert len(all_strata()) == 3 * 3 * 4 * 9 * 6 * 5

    def test_the_protein_level_axes_admit_one_thousand_and_eighty(self) -> None:
        """The count the docstring quotes, held to the grid that produces it.

        The five protein-level axes are not freely crossed: homology and donor
        evidence are coupled, so their pair contributes 9 states and not 15,
        and the free product of the enums (1,800) is an overcount. The number
        that belongs in prose is 1,080, and it has been wrong in this docstring
        before, so it is pinned here rather than left to be re-derived by hand.
        """
        grid = all_strata()
        protein_level = {
            (s.length, s.homology, s.donor_evidence, s.taxonomy, s.propagation) for s in grid
        }
        category_aspect = {(s.category, s.aspect) for s in grid}

        assert len(protein_level) == 1080
        assert len(category_aspect) == 9
        # The grid is the full rectangle of the two halves, which is why the
        # 9,720 total already implies the 1,080 and cannot disagree with it.
        assert len(grid) == 9720
        assert len(grid) == len(category_aspect) * len(protein_level)

    def test_the_docstring_quotes_the_count_the_grid_produces(self) -> None:
        """The defect this guards against was prose, so the prose is asserted.

        ``all_strata`` was correct while its docstring claimed the five
        protein-level axes admit 1,920 combinations, a number that is neither
        the coupled count nor the free product, and that contradicted the same
        docstring's own 9,720. Nothing failed, because no code reads a
        docstring. This reads it.
        """
        doc = all_strata.__doc__ or ""

        assert "1,080" in doc
        assert "1,920" not in doc
        # The 77 is a pre-wipe observation and must not read as a current one.
        assert "2026-08-27" in doc

    def test_a_placed_observation_is_in_the_grid(self) -> None:
        s = stratum_for(
            category="LK",
            aspect="F",
            residues=700,
            neighbourhood=Neighbourhood(best_identity=45.0, donor_is_experimental=False),
        )
        assert s in set(all_strata())

    def test_report_order_is_stable(self) -> None:
        sample = list(all_strata()[:10])
        assert report_order(list(reversed(sample))) == tuple(sample)


class TestPoolingIsRefusedRatherThanDiscouraged:
    def _stratum(self, category: str = "NK") -> Stratum:
        return stratum_for(
            category=category,
            aspect="P",
            residues=100,
            neighbourhood=Neighbourhood(best_identity=50.0, donor_is_experimental=True),
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


class TestTaxonomyIsResolvedOverNonSelfDonors:
    """The stored relation is usable as-is; the reduction rule is the choice."""

    def test_the_five_common_relations_map_to_their_own_band(self) -> None:
        for relation in ("same", "close", "intermediate", "distant", "root-only"):
            assert taxonomy_band_for(relation).value == relation

    def test_lineal_relations_join_the_band_that_claims_least(self) -> None:
        """ancestor and descendant are 0.04 percent of donor rows each.

        Too few to carry a band, and DISTANT would overstate the separation.
        """
        assert taxonomy_band_for("ancestor") is TaxonomyBand.INTERMEDIATE
        assert taxonomy_band_for("descendant") is TaxonomyBand.INTERMEDIATE

    def test_no_donor_is_a_band_and_not_a_missing_value(self) -> None:
        assert taxonomy_band_for(None) is TaxonomyBand.NONE

    def test_an_unknown_relation_raises_rather_than_defaulting(self) -> None:
        with pytest.raises(ValueError, match="unknown taxonomic relation"):
            taxonomy_band_for("sibling")


class TestPropagationSeparatesTheZeroMassFirst:
    """53.5 percent of proteins have a gap of exactly zero, so it is a band."""

    def test_a_gap_of_zero_is_its_own_band(self) -> None:
        assert propagation_band_for(0.05, 0.05) is PropagationBand.ZERO

    def test_the_positive_tail_is_cut_at_the_fixed_boundaries(self) -> None:
        assert propagation_band_for(0.05, 0.07) is PropagationBand.NEAR
        assert propagation_band_for(0.05, 0.15) is PropagationBand.MID
        assert propagation_band_for(0.05, 0.30) is PropagationBand.FAR

    def test_no_experimental_donor_is_censored_and_not_infinite(self) -> None:
        """Right-censored at K. Folding it into FAR would invent a distance."""
        assert propagation_band_for(0.05, None) is PropagationBand.NONE
        assert propagation_band_for(None, None) is PropagationBand.NONE

    def test_a_negative_gap_means_two_different_donor_sets(self) -> None:
        with pytest.raises(ValueError, match="different donor sets"):
            propagation_band_for(0.20, 0.05)


class TestReportableStrataNamesWhatItWithholds:
    def _stratum(self, category: str = "NK") -> Stratum:
        return stratum_for(
            category=category,
            aspect="F",
            residues=400,
            neighbourhood=Neighbourhood(best_identity=55.0, donor_is_experimental=True, taxonomic_relation="close", nearest_any=0.05, nearest_experimental=0.12),
        )

    def test_thin_strata_are_returned_as_withheld_rather_than_dropped(self) -> None:
        big, small = self._stratum("NK"), self._stratum("LK")
        reportable, withheld = reportable_strata({big: 120, small: 4}, min_population=30)
        assert reportable == (big,)
        assert withheld == (small,)

    def test_both_halves_are_in_canonical_order(self) -> None:
        a, b = self._stratum("NK"), self._stratum("PK")
        reportable, _ = reportable_strata({b: 50, a: 50}, min_population=1)
        assert reportable == report_order([a, b])

    def test_a_floor_below_one_is_refused(self) -> None:
        with pytest.raises(ValueError, match="at least 1"):
            reportable_strata({}, min_population=0)
