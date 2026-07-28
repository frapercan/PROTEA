"""The join guards, and the one join they were written for.

Every assertion here corresponds to a way a join has produced output of the
expected shape while holding values that belonged to different rows.
"""

from __future__ import annotations

import pytest

from protea.core.feature_enricher import _closest_leaf_per_term
from protea.core.row_alignment import (
    RowAlignmentError,
    assert_row_count_preserved,
    assert_unique_key,
    lookup_by,
)


class TestAUniqueKeyPassesQuietly:
    def test_it_does_not_raise(self) -> None:
        rows = [{"id": "a"}, {"id": "b"}, {"id": "c"}]
        assert_unique_key(rows, lambda r: r["id"], context="ctx")

    def test_an_empty_input_is_unique(self) -> None:
        assert_unique_key([], lambda r: r, context="ctx")

    def test_the_lookup_holds_every_row(self) -> None:
        rows = [{"id": "a"}, {"id": "b"}]
        assert lookup_by(rows, lambda r: r["id"], context="ctx") == {
            "a": {"id": "a"},
            "b": {"id": "b"},
        }

    def test_a_generator_is_consumed_once_and_still_checked(self) -> None:
        """The helper materialises, so a caller may pass a generator safely."""
        rows = ({"id": i} for i in range(3))
        assert len(lookup_by(rows, lambda r: r["id"], context="ctx")) == 3


class TestADuplicateKeyIsRefused:
    def test_it_raises_rather_than_overwriting(self) -> None:
        rows = [{"id": "a", "v": 1}, {"id": "a", "v": 2}]
        with pytest.raises(RowAlignmentError):
            lookup_by(rows, lambda r: r["id"], context="ctx")

    def test_the_error_names_the_join(self) -> None:
        """Knowing some join broke is not actionable; knowing which one is."""
        rows = [{"id": "a"}, {"id": "a"}]
        with pytest.raises(RowAlignmentError, match="folding scores into results"):
            lookup_by(rows, lambda r: r["id"], context="folding scores into results")

    def test_the_error_quantifies_the_damage(self) -> None:
        """8.3% was the number that made the original defect legible."""
        rows = [{"id": "a"}, {"id": "a"}, {"id": "b"}, {"id": "c"}]
        with pytest.raises(RowAlignmentError, match=r"1 of 4 rows \(25\.0%"):
            lookup_by(rows, lambda r: r["id"], context="ctx")

    def test_the_error_names_the_offending_keys(self) -> None:
        rows = [{"id": "dup"}, {"id": "dup"}, {"id": "ok"}]
        with pytest.raises(RowAlignmentError, match="'dup' x2"):
            assert_unique_key(rows, lambda r: r["id"], context="ctx")

    def test_many_duplicates_are_truncated_not_dumped(self) -> None:
        rows = [{"id": f"k{i}"} for i in range(20)] * 2
        with pytest.raises(RowAlignmentError, match="and 15 more"):
            assert_unique_key(rows, lambda r: r["id"], context="ctx")


class TestRowCountPreservation:
    def test_an_equal_count_passes(self) -> None:
        assert_row_count_preserved([1, 2, 3], [4, 5, 6], context="ctx")

    def test_counts_may_be_given_as_integers(self) -> None:
        assert_row_count_preserved(10, 10, context="ctx")

    def test_a_shrinking_join_raises(self) -> None:
        with pytest.raises(RowAlignmentError, match="lost rows"):
            assert_row_count_preserved(100, 92, context="ctx")

    def test_a_fanning_join_raises_too(self) -> None:
        """Growth double counts every later aggregate, so it is not benign."""
        with pytest.raises(RowAlignmentError, match="grew rows"):
            assert_row_count_preserved(100, 137, context="ctx")

    def test_the_error_quantifies_the_drift(self) -> None:
        with pytest.raises(RowAlignmentError, match=r"8 rows, 8\.0%"):
            assert_row_count_preserved(100, 92, context="ctx")

    def test_an_empty_before_does_not_divide_by_zero(self) -> None:
        with pytest.raises(RowAlignmentError, match="grew rows"):
            assert_row_count_preserved(0, 3, context="ctx")


class TestTheClosestLeafSurvivesRatherThanTheLastOne:
    """The join this work was written for.

    Indexing a group's leaves by term with a plain comprehension keeps
    whichever record arrives last, so which row survives depends on the order
    predictions were produced in and can differ under a different batch size.
    """

    def test_unique_terms_are_unchanged(self) -> None:
        recs = [
            {"go_id": "GO:1", "distance": 0.4},
            {"go_id": "GO:2", "distance": 0.6},
        ]
        assert _closest_leaf_per_term(recs) == {"GO:1": recs[0], "GO:2": recs[1]}

    def test_the_closest_wins_when_it_comes_first(self) -> None:
        near = {"go_id": "GO:1", "distance": 0.1}
        far = {"go_id": "GO:1", "distance": 0.9}
        assert _closest_leaf_per_term([near, far])["GO:1"] is near

    def test_the_closest_wins_when_it_comes_last(self) -> None:
        """The case the old comprehension got right only by luck of ordering."""
        far = {"go_id": "GO:1", "distance": 0.9}
        near = {"go_id": "GO:1", "distance": 0.1}
        assert _closest_leaf_per_term([far, near])["GO:1"] is near

    def test_the_result_does_not_depend_on_input_order(self) -> None:
        recs = [
            {"go_id": "GO:1", "distance": 0.5},
            {"go_id": "GO:1", "distance": 0.2},
            {"go_id": "GO:2", "distance": 0.7},
        ]
        assert _closest_leaf_per_term(recs) == _closest_leaf_per_term(list(reversed(recs)))

    def test_a_missing_distance_does_not_crash_the_index(self) -> None:
        recs = [{"go_id": "GO:1"}, {"go_id": "GO:1", "distance": 0.2}]
        assert _closest_leaf_per_term(recs)["GO:1"]["distance"] == 0.2

    def test_no_row_count_is_invented(self) -> None:
        recs = [{"go_id": "GO:1", "distance": d} for d in (0.1, 0.2, 0.3)]
        assert len(_closest_leaf_per_term(recs)) == 1
