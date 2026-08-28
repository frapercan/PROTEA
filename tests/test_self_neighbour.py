"""A protein is not its own neighbour, when the retriever is told so.

Why this exists, measured read-only against the live store on 2026-08-28:

    K       candidate rows    with a donor other than self    survives
    1              263,152                          13,205        5.0%
    10             752,786                         499,979       66.4%

    query proteins whose only neighbour at K=1 is themselves: 11,472 of 14,032

At depth one the nearest neighbour is the query itself for 95 per cent of rows,
and four proteins in five have nobody else. A depth sweep over that pool measures
how much self-retrieval each cut contains, not what depth costs, and the shallow
arms win because they barely predict.
"""

from __future__ import annotations

import pytest

from protea.core.operations.predict_go_terms._self_neighbour import (
    search_k_for,
    without_self,
)


class TestTheSearchAsksForOneMore:
    def test_it_asks_for_one_extra_only_when_excluding(self):
        assert search_k_for(10, exclude_self=False) == 10
        assert search_k_for(10, exclude_self=True) == 11

    def test_asking_for_exactly_k_would_make_the_depth_depend_on_the_corpus(self):
        """The reason for k+1, stated as the thing that would otherwise happen.

        Filter after asking for k and a protein present in its own donor corpus
        keeps k-1 neighbours while one absent from it keeps k, so the same
        payload means two depths depending on who is in the bank.
        """
        neighbours = [[("SELF", 0.0), ("A", 0.1), ("B", 0.2)], [("A", 0.1), ("B", 0.2), ("C", 0.3)]]
        kept = without_self(neighbours, ["SELF", "OTHER"], k=3, exclude_self=True)
        assert [len(x) for x in kept] == [2, 3]
        # With k+1 asked for, the first list would have arrived with four and
        # both would end at three. The asymmetry above is exactly what the
        # extra slot removes.


class TestDroppingTheSelfHit:
    def test_the_query_is_removed_from_its_own_list(self):
        neighbours = [[("Q", 0.0), ("A", 0.1), ("B", 0.2)]]
        assert without_self(neighbours, ["Q"], k=2, exclude_self=True) == [[("A", 0.1), ("B", 0.2)]]

    def test_a_protein_that_did_not_retrieve_itself_keeps_all_k(self):
        """Trimming happens after the drop, never before.

        Otherwise the extra slot the search asked for on one protein's behalf
        would cost another protein a real donor.
        """
        neighbours = [[("A", 0.1), ("B", 0.2), ("C", 0.3)]]
        assert without_self(neighbours, ["Q"], k=3, exclude_self=True) == neighbours

    def test_nothing_is_touched_when_the_flag_is_off(self):
        neighbours = [[("Q", 0.0), ("A", 0.1)]]
        assert without_self(neighbours, ["Q"], k=2, exclude_self=False) is neighbours

    def test_a_length_mismatch_raises_rather_than_dropping_the_wrong_protein(self):
        """The pairing is positional, so drift corrupts the candidate set silently.

        Dropping 'self' by position against the wrong accession removes a real
        donor from another protein and leaves a set that still looks well formed.
        """
        with pytest.raises(ValueError, match="positionally paired"):
            without_self([[("A", 0.1)], [("B", 0.2)]], ["ONLY_ONE"], k=1, exclude_self=True)

    def test_every_neighbour_being_self_leaves_an_empty_neighbourhood(self):
        """Which is the honest answer, and the state 81.8 per cent of proteins were in."""
        assert without_self([[("Q", 0.0)]], ["Q"], k=5, exclude_self=True) == [[]]
