"""Unit tests for the helpers extracted out of ``build_predictions_for_protein``.

The full ``build_predictions_for_protein`` flow needs a DB to exercise
the SQL join + ordering, so it is covered end-to-end via the router
test suite. The recently-extracted ``_prediction_row_to_dict`` helper is
pure (takes two ORM rows, returns a dict) so we pin its column contract
in isolation here. Future GOPrediction columns added without updating
this helper will fail the count assertion.
"""
from __future__ import annotations

from unittest.mock import MagicMock

from protea.services._embeddings_predictions_helpers import (
    _prediction_row_to_dict,
)


def _make_pred() -> MagicMock:
    pred = MagicMock()
    pred.distance = 0.123456
    pred.ref_protein_accession = "Q12345"
    pred.qualifier = None
    pred.evidence_code = "EXP"
    pred.identity_nw = 0.5
    pred.similarity_nw = 0.6
    pred.alignment_score_nw = 100
    pred.gaps_pct_nw = 0.05
    pred.alignment_length_nw = 200
    pred.identity_sw = 0.7
    pred.similarity_sw = 0.8
    pred.alignment_score_sw = 110
    pred.gaps_pct_sw = 0.04
    pred.alignment_length_sw = 180
    pred.length_query = 250
    pred.length_ref = 240
    pred.query_taxonomy_id = 9606
    pred.ref_taxonomy_id = 10090
    pred.taxonomic_lca = 1437010
    pred.taxonomic_distance = 12
    pred.taxonomic_common_ancestors = 5
    pred.taxonomic_relation = "siblings"
    pred.vote_count = 3
    pred.k_position = 1
    pred.go_term_frequency = 0.02
    pred.ref_annotation_density = 0.4
    pred.neighbor_distance_std = 0.05
    return pred


def _make_gt() -> MagicMock:
    gt = MagicMock()
    gt.go_id = "GO:0008150"
    gt.name = "biological process"
    gt.aspect = "P"
    return gt


def test_returns_expected_column_set() -> None:
    """The helper must keep the documented payload shape stable."""
    out = _prediction_row_to_dict(_make_pred(), _make_gt())
    assert set(out.keys()) == {
        "go_id",
        "name",
        "aspect",
        "distance",
        "ref_protein_accession",
        "qualifier",
        "evidence_code",
        "identity_nw",
        "similarity_nw",
        "alignment_score_nw",
        "gaps_pct_nw",
        "alignment_length_nw",
        "identity_sw",
        "similarity_sw",
        "alignment_score_sw",
        "gaps_pct_sw",
        "alignment_length_sw",
        "length_query",
        "length_ref",
        "query_taxonomy_id",
        "ref_taxonomy_id",
        "taxonomic_lca",
        "taxonomic_distance",
        "taxonomic_common_ancestors",
        "taxonomic_relation",
        "vote_count",
        "k_position",
        "go_term_frequency",
        "ref_annotation_density",
        "neighbor_distance_std",
    }


def test_distance_is_rounded_to_4dp() -> None:
    out = _prediction_row_to_dict(_make_pred(), _make_gt())
    # ``round(0.123456, 4)`` is ``0.1235``.
    assert out["distance"] == 0.1235


def test_go_term_fields_are_lifted_from_gt() -> None:
    out = _prediction_row_to_dict(_make_pred(), _make_gt())
    assert out["go_id"] == "GO:0008150"
    assert out["name"] == "biological process"
    assert out["aspect"] == "P"
