"""Pairings that are positional must be checked, not trusted.

Two places in the pipeline zip a list of identities against a list of computed
results and rely on position to say which belongs to which. Both were lenient.
Leniency does not truncate a short result, it renames it: the identities keep
coming while the results run out, so each identity is handed the result of a
later one and the mismatch is never reported.

This is not hypothetical. The torch KNN backend halved its query chunk on an
out-of-memory error and processed only half of it, returning fewer neighbour
lists than queries. Every query after that point was scored against another
protein's neighbours, and the run finished clean with one model looking simply
worse than the others.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace

import numpy as np
import pytest

from protea.core.operations._compute_embeddings_helpers import serialize_inferred_chunks
from protea.core.operations._predict_go_terms_adapter import (
    AdapterInputs,
    _build_pair_features_from_aspect_neighbors,
    _build_pair_features_from_neighbors,
)


def _inputs(accessions: list[str]) -> AdapterInputs:
    """An AdapterInputs that asks for pair features and needs no database.

    ``compute_alignments`` is on so the walk is not skipped, but the sequence
    maps are empty, so each pair resolves to an empty feature dict. What is
    under test is which pairs are visited, not what is computed for them.
    """
    return AdapterInputs(
        p=SimpleNamespace(compute_alignments=True, compute_taxonomy=False),
        valid_accessions=accessions,
        query_embeddings=np.zeros((len(accessions), 4), dtype=np.float32),
        ref_data={},
        annotations={},
        go_id_map={},
        go_aspect_map={},
        prediction_set_id=uuid.uuid4(),
        ref_sequences={},
        query_sequences={},
        ref_tax_ids={},
        query_tax_ids={},
    )


class TestNeighboursAreAttributedToTheQueryThatFoundThem:
    def test_a_neighbour_list_per_query_is_paired_in_order(self) -> None:
        inputs = _inputs(["Q1", "Q2"])
        pairs = _build_pair_features_from_neighbors(
            inputs, [[("R1", 0.1)], [("R2", 0.2)]]
        )
        assert set(pairs) == {("Q1", "R1"), ("Q2", "R2")}

    def test_fewer_neighbour_lists_than_queries_raises(self) -> None:
        """The search answered two of three queries. That must not pass."""
        inputs = _inputs(["Q1", "Q2", "Q3"])
        with pytest.raises(ValueError):
            _build_pair_features_from_neighbors(
                inputs, [[("R1", 0.1)], [("R2", 0.2)]]
            )

    def test_more_neighbour_lists_than_queries_raises_too(self) -> None:
        """The reverse misalignment is just as wrong and just as quiet."""
        inputs = _inputs(["Q1"])
        with pytest.raises(ValueError):
            _build_pair_features_from_neighbors(
                inputs, [[("R1", 0.1)], [("R2", 0.2)]]
            )

    def test_a_short_aspect_bank_raises_rather_than_being_absorbed(self) -> None:
        """One aspect returning short is invisible if the others came back whole."""
        inputs = _inputs(["Q1", "Q2"])
        with pytest.raises(ValueError):
            _build_pair_features_from_aspect_neighbors(
                inputs,
                {
                    "P": [[("R1", 0.1)], [("R2", 0.2)]],
                    "F": [[("R3", 0.3)]],
                },
            )


class TestAnEmbeddingIsStoredUnderTheSequenceItCameFrom:
    @staticmethod
    def _chunk(dim: int = 4) -> SimpleNamespace:
        return SimpleNamespace(
            chunk_index_s=0, chunk_index_e=1, vector=np.zeros(dim, dtype=np.float32)
        )

    def test_each_sequence_keeps_its_own_chunks(self) -> None:
        seqs = [SimpleNamespace(id=uuid.uuid4()) for _ in range(2)]
        rows = serialize_inferred_chunks(seqs, [[self._chunk()], [self._chunk()]])
        assert [r["sequence_id"] for r in rows] == [s.id for s in seqs]

    def test_fewer_chunk_lists_than_sequences_raises(self) -> None:
        """Otherwise one sequence's vector is written under another's id."""
        seqs = [SimpleNamespace(id=uuid.uuid4()) for _ in range(3)]
        with pytest.raises(ValueError):
            serialize_inferred_chunks(seqs, [[self._chunk()], [self._chunk()]])
