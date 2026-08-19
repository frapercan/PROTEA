"""Pooling per-protein scores into strata, and reading the neighbourhood.

The two halves the strata vocabulary needed and did not have: something that
says which cell a protein is in, and something that pools the cell.

The pooling is micro, and that is the point of most of these tests. `f_micro_w`
sums tp / pred / n_gt over the population and then divides; the mean of each
protein's F is a macro average, and the two differ whenever proteins carry
different numbers of terms, which they always do.
"""

from __future__ import annotations

import pytest

from protea.core.operations._run_cafa_strata import Cell, micro_cells, project
from protea.core.strata import (
    Aspect,
    Category,
    DonorEvidence,
    HomologyBand,
    LengthBand,
    Neighbourhood,
    PropagationBand,
    TaxonomyBand,
    stratum_for,
    taxonomy_band_for,
)


def _row(acc: str, tp: float, pred: float, n_gt: float, band: str = "a") -> dict:
    return {"protein_accession": acc, "tp_w": tp, "pred_w": pred, "n_gt_w": n_gt, "band": band}


class TestPoolingIsMicroNotMacro:
    def test_one_cell_sums_before_dividing(self) -> None:
        rows = [_row("A", 1.0, 2.0, 1.0), _row("B", 3.0, 3.0, 6.0)]
        cell = micro_cells(rows, lambda r: r["band"])["a"]
        # precision 4/5, recall 4/7 -> F = 2*.8*.5714/(1.3714)
        assert cell.precision_w == pytest.approx(0.8)
        assert cell.recall_w == pytest.approx(4 / 7)
        assert cell.f_micro_w == pytest.approx(2 * 0.8 * (4 / 7) / (0.8 + 4 / 7))

    def test_it_differs_from_the_mean_of_per_protein_f(self) -> None:
        """The failure this guards: publishing a macro average as f_micro_w.

        One protein with many terms and one with few is enough to separate them.
        """
        rows = [_row("A", 1.0, 1.0, 1.0), _row("B", 1.0, 10.0, 10.0)]
        micro = micro_cells(rows, lambda r: "a")["a"].f_micro_w
        per_protein = [1.0, 2 * 0.1 * 0.1 / 0.2]
        macro = sum(per_protein) / len(per_protein)
        assert micro != pytest.approx(macro)

    def test_population_travels_with_the_score(self) -> None:
        cells = micro_cells([_row("A", 1, 1, 1), _row("B", 1, 1, 1)], lambda r: r["band"])
        assert cells["a"].n_proteins == 2

    def test_rows_group_into_separate_cells(self) -> None:
        rows = [_row("A", 1, 1, 1, "a"), _row("B", 0, 4, 1, "b")]
        cells = micro_cells(rows, lambda r: r["band"])
        assert set(cells) == {"a", "b"}
        assert cells["a"].f_micro_w == pytest.approx(1.0)
        assert cells["b"].f_micro_w == pytest.approx(0.0)

    def test_a_none_key_drops_the_row(self) -> None:
        """A protein whose stratum could not be resolved is excluded, not
        pooled into some default cell where it would move a number."""
        cells = micro_cells([_row("A", 1, 1, 1)], lambda r: None)
        assert cells == {}

    def test_zero_denominators_do_not_raise(self) -> None:
        cell = micro_cells([_row("A", 0.0, 0.0, 0.0)], lambda r: "a")["a"]
        assert cell == Cell(1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)


class TestProjection:
    def _stratum(self):
        return stratum_for(
            category=Category.NO_KNOWLEDGE,
            aspect=Aspect.MOLECULAR_FUNCTION,
            residues=300,
            neighbourhood=Neighbourhood(
                best_identity=45.0, donor_is_experimental=True, taxonomic_relation="close"
            ),
        )

    def test_it_keeps_only_the_named_axes_in_order(self) -> None:
        assert project(self._stratum(), ["length", "homology"]) == (
            LengthBand.SHORT,
            HomologyBand.DISTANT,
        )

    def test_an_unknown_axis_is_an_error_not_a_missing_column(self) -> None:
        with pytest.raises(ValueError, match="unknown stratum axes"):
            project(self._stratum(), ["length", "homolgy"])


class TestTheVocabularyCoversWhatThePipelineEmits:
    """feature_engineering.compute_taxonomy can emit ten relation strings.

    A vocabulary covering only the ones seen so far fails on the first run that
    meets the rest, which is what happened here: 'unrelated' reached the
    resolver from a live prediction set and stopped the evaluation.
    """

    EMITTED = (
        "same", "close", "intermediate", "distant", "root-only",
        "ancestor", "descendant", "child", "parent", "unrelated",
    )

    @pytest.mark.parametrize("relation", EMITTED)
    def test_every_emitted_relation_has_a_band(self, relation: str) -> None:
        assert isinstance(taxonomy_band_for(relation), TaxonomyBand)

    def test_unrelated_is_absence_not_distance(self) -> None:
        """compute_taxonomy returns it when a taxon is MISSING, before any
        lineage is compared. Filing it under a distance band would assert a
        separation nobody measured."""
        assert taxonomy_band_for("unrelated") is TaxonomyBand.NONE

    def test_lineal_relations_share_the_weaker_band(self) -> None:
        for relation in ("ancestor", "descendant", "child", "parent"):
            assert taxonomy_band_for(relation) is TaxonomyBand.INTERMEDIATE

    def test_a_genuinely_unknown_relation_still_raises(self) -> None:
        with pytest.raises(ValueError, match="unknown taxonomic relation"):
            taxonomy_band_for("sibling")


class TestTheStratumRefusesContradictions:
    def test_no_donor_with_an_evidence_verdict_is_refused(self) -> None:
        with pytest.raises(ValueError, match="disagree about whether a donor exists"):
            stratum_for(
                category=Category.NO_KNOWLEDGE,
                aspect=Aspect.MOLECULAR_FUNCTION,
                residues=100,
                neighbourhood=Neighbourhood(best_identity=None, donor_is_experimental=True),
            )

    def test_no_donor_places_the_protein_in_the_none_bands(self) -> None:
        s = stratum_for(
            category=Category.NO_KNOWLEDGE,
            aspect=Aspect.MOLECULAR_FUNCTION,
            residues=100,
            neighbourhood=Neighbourhood(best_identity=None, donor_is_experimental=None),
        )
        assert s.homology is HomologyBand.NONE
        assert s.donor_evidence is DonorEvidence.NONE
        assert s.propagation is PropagationBand.NONE


class TestIdentityIsAPercentage:
    """go_prediction stores identity as a FRACTION and the bands take a
    PERCENTAGE. Passing the fraction raises nothing and files the whole
    population under the lowest band, so the mistake is silent."""

    def test_a_fraction_would_land_everything_in_twilight(self) -> None:
        assert HomologyBand.TWILIGHT is stratum_for(
            category=Category.NO_KNOWLEDGE, aspect=Aspect.MOLECULAR_FUNCTION, residues=100,
            neighbourhood=Neighbourhood(best_identity=0.95, donor_is_experimental=True),
        ).homology

    def test_the_percentage_lands_where_it_should(self) -> None:
        assert HomologyBand.NEAR_IDENTICAL is stratum_for(
            category=Category.NO_KNOWLEDGE, aspect=Aspect.MOLECULAR_FUNCTION, residues=100,
            neighbourhood=Neighbourhood(best_identity=95.0, donor_is_experimental=True),
        ).homology

    def test_out_of_range_is_refused(self) -> None:
        with pytest.raises(ValueError, match="not a percentage"):
            stratum_for(
                category=Category.NO_KNOWLEDGE, aspect=Aspect.MOLECULAR_FUNCTION, residues=100,
                neighbourhood=Neighbourhood(best_identity=140.0, donor_is_experimental=True),
            )
