"""The reference pool comes in two shapes, and the sequence map has to read both.

``ref_data_by_aspect`` is one name over two structures. With
``aspect_separated_knn`` on it is keyed by aspect, each value holding its own
``accessions``; with it off it IS that inner dict. Reading it as if it were
always the first shape blocked the aspect-separation arm of axis C on
2026-09-07: every batch died on ``SequenceIdentityMissingError`` -- "1 of 200
neighbours have no sequence identity" -- against a store where zero donors are
unmappable. The neighbour was real; the map had been built over a different
population.

These pin both shapes, and pin that a partial read is caught rather than
silently accepted, because the guard that caught it is the only reason the arm
failed loudly instead of scoring against a pool it had not mapped.
"""

from __future__ import annotations

import pytest

from protea.core.operations.predict_go_terms._aspect_helpers import _pool_accessions


class TestBothShapes:
    def test_the_per_aspect_shape(self):
        """aspect_separated_knn=True: keyed by aspect, accessions inside each."""
        ref = {
            "P": {"accessions": ["A1", "A2"], "embeddings_f32": object()},
            "F": {"accessions": ["A2", "A3"], "embeddings_f32": object()},
            "C": {"accessions": ["A4"], "embeddings_f32": object()},
        }
        assert _pool_accessions(ref) == {"A1", "A2", "A3", "A4"}

    def test_the_flat_shape(self):
        """aspect_separated_knn=False: the inner dict, unwrapped."""
        ref = {"accessions": ["A1", "A2", "A3"], "embeddings_f32": object()}
        assert _pool_accessions(ref) == {"A1", "A2", "A3"}

    def test_the_flat_shape_was_the_one_that_broke(self):
        """Read as per-aspect, a flat pool yields nothing usable -- which is how
        a map came out short enough to lose 1 neighbour in 200."""
        flat = {"accessions": ["A1", "A2", "A3"], "embeddings_f32": object()}
        with pytest.raises((TypeError, IndexError, KeyError)):
            {a for d in flat.values() for a in d["accessions"]}  # the old expression
        assert _pool_accessions(flat) == {"A1", "A2", "A3"}

    def test_an_aspect_with_no_donors_contributes_nothing(self):
        ref = {"P": {"accessions": ["A1"]}, "F": {"accessions": []}}
        assert _pool_accessions(ref) == {"A1"}

    def test_an_empty_pool_is_an_empty_set_not_an_error(self):
        """Nothing mapped is a different statement from a partial map, and the
        caller distinguishes them; this must not raise on the way there."""
        assert _pool_accessions({}) == set()
        assert _pool_accessions({"accessions": []}) == set()


class TestTheShapesAreNotConfusable:
    def test_an_aspect_literally_named_accessions_would_be_ambiguous(self):
        """Documenting the one input this cannot disambiguate. GO aspects are
        P, F and C, so it cannot occur -- but the sniff is a stopgap for one
        name meaning two structures, and this says where its limit is."""
        pathological = {"accessions": {"accessions": ["A1"]}}
        assert _pool_accessions(pathological) == {"accessions"}
