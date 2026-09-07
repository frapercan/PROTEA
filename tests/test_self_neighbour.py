"""A protein is not its own neighbour, when the retriever is told so.

Why the exclusion exists, measured read-only against the live store on
2026-08-28:

    K       candidate rows    with a donor other than self    survives
    1              263,152                          13,205        5.0%
    10             752,786                         499,979       66.4%

    query proteins whose only neighbour at K=1 is themselves: 11,472 of 14,032

At depth one the nearest neighbour is the query itself for 95 per cent of rows,
and four proteins in five have nobody else. A depth sweep over that pool measures
how much self-retrieval each cut contains, not what depth costs, and the shallow
arms win because they barely predict.

What changed on 2026-09-07: the accession-based pair this module used to
implement is gone, because dropping by accession while the method drops by
sequence is what broke every ``exclude_self_neighbour`` arm of axis C. These now
pin that the naive pair cannot come back by import, and that what the module
hands out is the sequence-aware pair both retrieval paths use.
"""

from __future__ import annotations

import pytest

from protea.core.operations.predict_go_terms import _self_neighbour


class TestTheModuleHandsOutTheSequenceAwarePair:
    def test_it_exports_the_margin_and_the_drop(self) -> None:
        assert callable(_self_neighbour.extra_neighbours_for)
        assert callable(_self_neighbour.without_own_sequence)

    def test_they_are_the_methods_own_functions_not_a_second_copy(self) -> None:
        """Two implementations of one rule is how the paths diverged before."""
        from protea_method import _self_by_sequence

        assert _self_neighbour.extra_neighbours_for is _self_by_sequence.extra_neighbours_for
        assert _self_neighbour.without_own_sequence is _self_by_sequence.without_own_sequence


class TestTheAccessionBasedPairIsGone:
    @pytest.mark.parametrize("name", ["search_k_for", "without_self"])
    def test_it_cannot_be_imported_from_here(self, name: str) -> None:
        """Callable and unmarked is how the unified path kept it for months."""
        assert not hasattr(_self_neighbour, name)

    @pytest.mark.parametrize("name", ["search_k_for", "without_self"])
    def test_nothing_in_the_tree_calls_it(self, name: str) -> None:
        """The grep the migration should have run. A second retrieval path
        added later must not find an accession-based exclusion to import."""
        import pathlib

        hits = [
            f"{path}:{i}"
            for path in pathlib.Path("protea").rglob("*.py")
            for i, line in enumerate(path.read_text().splitlines(), 1)
            if name in line
        ]
        assert hits == [], f"{name} is still referenced: {hits}"
