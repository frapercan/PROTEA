"""The rule that stops the corpus from grading the method on what it already knew.

The case these tests exist for is the withdraw-and-restore: an annotation present
early, removed, and returned. The retired pairwise rule counts it as new, so the
method is scored on predicting something already in its training corpus. The
corpus contracts by roughly thirty percent twice in the release history this
campaign spans, so the case is ordinary rather than exotic.
"""

from __future__ import annotations

import pytest

from protea.core.first_appearance import (
    NotEnoughHistoryError,
    RestorationReport,
    first_appearance,
    pairwise_additions,
    restoration_report,
)


def snap(**proteins: dict[str, set[str]]) -> dict[str, dict[str, set[str]]]:
    """Build one cut: ``snap(P1={"P": {"GO:1"}})``."""
    return dict(proteins)


class TestTheWithdrawAndRestoreCase:
    """The whole reason the pairwise difference was retired."""

    def test_a_restored_annotation_is_not_ground_truth(self) -> None:
        history = [
            snap(P1={"P": {"GO:1"}}),  # present
            snap(P1={"P": set()}),  # withdrawn
            snap(P1={"P": {"GO:1"}}),  # restored
        ]
        assert first_appearance(history) == {}

    def test_the_retired_rule_would_have_counted_it(self) -> None:
        """Stated explicitly, so the difference is not taken on trust."""
        withdrawn = snap(P1={"P": set()})
        restored = snap(P1={"P": {"GO:1"}})
        assert pairwise_additions(withdrawn, restored) == {"P1": {"P": {"GO:1"}}}

    def test_a_genuinely_new_annotation_is_ground_truth(self) -> None:
        history = [
            snap(P1={"P": {"GO:1"}}),
            snap(P1={"P": {"GO:1"}}),
            snap(P1={"P": {"GO:1", "GO:2"}}),
        ]
        assert first_appearance(history) == {"P1": {"P": {"GO:2"}}}

    def test_presence_at_any_earlier_cut_disqualifies_it(self) -> None:
        """Not just the cut immediately before: that is the entire change."""
        history = [
            snap(P1={"P": {"GO:9"}}),
            snap(P1={"P": set()}),
            snap(P1={"P": set()}),
            snap(P1={"P": set()}),
            snap(P1={"P": {"GO:9"}}),
        ]
        assert first_appearance(history) == {}


class TestTheRuleNeedsHistoryAndSaysSo:
    @pytest.mark.parametrize("cuts", [0, 1])
    def test_too_little_history_raises(self, cuts: int) -> None:
        """A silent fallback to the pairwise rule is the failure to prevent."""
        with pytest.raises(NotEnoughHistoryError, match="at least two cuts"):
            first_appearance([snap()] * cuts)

    def test_the_error_explains_the_consequence(self) -> None:
        with pytest.raises(NotEnoughHistoryError, match="every annotation looks new"):
            first_appearance([snap(P1={"P": {"GO:1"}})])

    def test_the_restoration_report_needs_history_too(self) -> None:
        with pytest.raises(NotEnoughHistoryError):
            restoration_report([snap()])

    def test_two_cuts_are_enough_and_agree_with_the_pairwise_rule(self) -> None:
        """With no earlier history there is nothing for the rules to disagree on."""
        history = [snap(P1={"P": {"GO:1"}}), snap(P1={"P": {"GO:1", "GO:2"}})]
        assert first_appearance(history) == pairwise_additions(*history)


class TestSeparationBetweenProteinsAndNamespaces:
    def test_another_protein_having_the_term_does_not_disqualify_it(self) -> None:
        """First appearance is per annotation, not per term."""
        history = [
            snap(P1={"P": {"GO:1"}}),
            snap(P1={"P": {"GO:1"}}, P2={"P": {"GO:1"}}),
        ]
        assert first_appearance(history) == {"P2": {"P": {"GO:1"}}}

    def test_the_same_term_in_another_namespace_is_separate(self) -> None:
        history = [
            snap(P1={"P": {"GO:1"}}),
            snap(P1={"P": {"GO:1"}, "F": {"GO:1"}}),
        ]
        assert first_appearance(history) == {"P1": {"F": {"GO:1"}}}

    def test_a_protein_that_disappears_contributes_nothing(self) -> None:
        history = [snap(P1={"P": {"GO:1"}}), snap()]
        assert first_appearance(history) == {}

    def test_a_protein_appearing_for_the_first_time_contributes_everything(self) -> None:
        history = [snap(), snap(P9={"C": {"GO:7", "GO:8"}})]
        assert first_appearance(history) == {"P9": {"C": {"GO:7", "GO:8"}}}


class TestTheRestorationRateIsMeasuredNotAsserted:
    def _history(self) -> list[dict[str, dict[str, set[str]]]]:
        return [
            snap(P1={"P": {"GO:1"}}),
            snap(P1={"P": set()}),
            # One restoration and one genuine first appearance.
            snap(P1={"P": {"GO:1", "GO:2"}}),
        ]

    def test_it_counts_the_apparent_additions(self) -> None:
        assert restoration_report(self._history()).apparent_additions == 2

    def test_it_separates_the_genuine_from_the_restored(self) -> None:
        report = restoration_report(self._history())
        assert report.genuine_first_appearances == 1
        assert report.restored == 1

    def test_the_rate_is_the_share_that_was_seen_before(self) -> None:
        assert restoration_report(self._history()).restoration_rate == pytest.approx(0.5)

    def test_the_parts_account_for_the_whole(self) -> None:
        report = restoration_report(self._history())
        assert report.genuine_first_appearances + report.restored == report.apparent_additions

    def test_it_agrees_with_the_ground_truth_builder(self) -> None:
        """The two must not be able to drift apart."""
        history = self._history()
        truth = first_appearance(history)
        counted = sum(len(terms) for by_ns in truth.values() for terms in by_ns.values())
        assert counted == restoration_report(history).genuine_first_appearances

    def test_no_growth_means_no_rate_rather_than_a_division_by_zero(self) -> None:
        history = [snap(P1={"P": {"GO:1"}}), snap(P1={"P": {"GO:1"}})]
        assert restoration_report(history).restoration_rate == 0.0


class TestTheRateIsReportedPerNamespace:
    """A single global figure misleads exactly where the series crosses a contraction."""

    def _uneven(self) -> list[dict[str, dict[str, set[str]]]]:
        return [
            snap(P1={"P": {"GO:1"}, "F": {"GO:5"}}),
            snap(P1={"P": set(), "F": set()}),
            # P restores its term; F gains a genuinely new one.
            snap(P1={"P": {"GO:1"}, "F": {"GO:6"}}),
        ]

    def test_one_namespace_is_all_restoration(self) -> None:
        assert restoration_report(self._uneven()).rate_for_namespace("P") == pytest.approx(1.0)

    def test_the_other_is_none(self) -> None:
        assert restoration_report(self._uneven()).rate_for_namespace("F") == pytest.approx(0.0)

    def test_the_global_rate_hides_both(self) -> None:
        """The point of reporting per namespace rather than in total."""
        report = restoration_report(self._uneven())
        assert report.restoration_rate == pytest.approx(0.5)

    def test_an_unseen_namespace_has_no_rate(self) -> None:
        assert restoration_report(self._uneven()).rate_for_namespace("C") == 0.0


class TestTheReportIsEmittable:
    def test_it_flattens_for_a_job_event(self) -> None:
        report = RestorationReport(
            apparent_additions=100,
            genuine_first_appearances=90,
            restored=10,
            restored_by_namespace={"P": 10},
            apparent_by_namespace={"P": 100},
        )
        emitted = report.as_dict()
        assert emitted["restoration_rate"] == pytest.approx(0.1)
        assert emitted["restored_by_namespace"] == {"P": 10}

    def test_the_emitted_copy_does_not_alias_the_report(self) -> None:
        report = restoration_report([snap(), snap(P1={"P": {"GO:1"}})])
        emitted = report.as_dict()
        assert isinstance(emitted["restored_by_namespace"], dict)
        emitted["restored_by_namespace"]["P"] = 999  # type: ignore[index]
        assert report.restored_by_namespace.get("P", 0) != 999
