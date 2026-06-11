"""Unit tests for the helpers extracted out of ``iter_scored_predictions``.

The streaming generator itself opens a session and yields a header plus
per-row TSV bytes; covering it directly requires a full DB. The two
helpers extracted in this slice (``_pred_score_inputs`` and
``_scored_tsv_row``) are pure functions that the generator now composes,
so unit-testing them in isolation is enough to pin the wire shape.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from protea.services._scoring_streaming_helpers import (
    _format_optional,
    _pred_score_inputs,
    _scored_tsv_row,
)


def _make_pred(**overrides):
    """Build a ``GOPrediction``-shaped MagicMock with sensible defaults."""
    pred = MagicMock()
    pred.protein_accession = "P12345"
    pred.distance = 0.5
    pred.identity_nw = 0.3
    pred.identity_sw = 0.4
    pred.evidence_code = "EXP"
    pred.taxonomic_distance = 100
    pred.neighbor_vote_fraction = 0.6
    pred.qualifier = None
    pred.ref_protein_accession = "Q67890"
    for k, v in overrides.items():
        setattr(pred, k, v)
    return pred


# ---------------------------------------------------------------------------
# _pred_score_inputs
# ---------------------------------------------------------------------------


class TestPredScoreInputs:
    def test_lifts_signal_fields(self) -> None:
        pred = _make_pred()
        out = _pred_score_inputs(pred)
        assert set(out.keys()) == {
            "distance",
            "identity_nw",
            "identity_sw",
            "evidence_code",
            "taxonomic_distance",
            "neighbor_vote_fraction",
            # A-SCORE rich axes
            "alignment_length_nw",
            "gaps_pct_nw",
            "alignment_length_sw",
            "gaps_pct_sw",
            "length_query",
            "ref_annotation_density",
            "anc2vec_neighbor_cos",
            "anc2vec_neighbor_maxcos",
            "go_term_frequency",
        }

    def test_passes_through_none_signals(self) -> None:
        pred = _make_pred(identity_nw=None, taxonomic_distance=None)
        out = _pred_score_inputs(pred)
        assert out["identity_nw"] is None
        assert out["taxonomic_distance"] is None
        assert out["distance"] == 0.5  # other fields untouched


# ---------------------------------------------------------------------------
# _scored_tsv_row
# ---------------------------------------------------------------------------


class TestScoredTsvRow:
    def test_renders_full_row_in_column_order(self) -> None:
        pred = _make_pred()
        row = _scored_tsv_row(pred, go_id="GO:0008150", score=0.875)

        assert row.endswith(b"\n")
        cells = row.decode().rstrip("\n").split("\t")
        # _SCORED_TSV_COLUMNS has exactly 11 columns.
        assert len(cells) == 11
        assert cells[0] == "P12345"
        assert cells[1] == "GO:0008150"
        assert cells[2] == "0.875"
        assert cells[3] == "0.5"  # distance
        assert cells[4] == "Q67890"  # ref_protein_accession
        assert cells[5] == "EXP"  # evidence_code
        assert cells[6] == ""  # qualifier (None)

    def test_optional_fields_render_empty_when_none(self) -> None:
        pred = _make_pred(
            distance=None,
            identity_nw=None,
            identity_sw=None,
            taxonomic_distance=None,
            neighbor_vote_fraction=None,
            ref_protein_accession=None,
            evidence_code=None,
        )
        cells = _scored_tsv_row(pred, go_id="GO:0001", score=0.0).decode().rstrip("\n").split("\t")
        # Every optional column should be the empty string, not "None".
        assert cells[3] == ""  # distance
        assert cells[4] == ""  # ref_protein_accession
        assert cells[5] == ""  # evidence_code
        assert cells[7] == ""  # identity_nw
        assert cells[10] == ""  # neighbor_vote_fraction


# ---------------------------------------------------------------------------
# _format_optional (was already there, kept under test for parity)
# ---------------------------------------------------------------------------


class TestFormatOptional:
    def test_none_renders_empty_string(self) -> None:
        assert _format_optional(None) == ""

    def test_value_stringified(self) -> None:
        assert _format_optional(0.5) == "0.5"
        assert _format_optional("EXP") == "EXP"
        assert _format_optional(0) == "0"
