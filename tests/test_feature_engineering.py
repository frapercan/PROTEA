"""Unit tests for protea.core.feature_engineering.

Parasail and ete3 results are mocked so no external dependencies are needed.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from protea.core.feature_engineering import (
    _classify_relation,
    _normalize_tax_id,
    _parse_alignment,
    compute_alignment,
    compute_nw,
    compute_sw,
    compute_taxonomy,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fake_traceback(query: str, ref: str, comp: str) -> MagicMock:
    tb = MagicMock()
    tb.query = query
    tb.ref = ref
    tb.comp = comp
    return tb


def _fake_result(query: str, ref: str, comp: str, score: float = 42.0) -> MagicMock:
    r = MagicMock()
    r.traceback = _fake_traceback(query, ref, comp)
    r.score = score
    return r


# ---------------------------------------------------------------------------
# _parse_alignment
# ---------------------------------------------------------------------------

class TestParseAlignment:
    def test_nw_basic_metrics(self) -> None:
        # 3 matches, 0 gaps, aln_len=3
        result = _fake_result("ACE", "ACE", "|||")
        out = _parse_alignment(result, "ACE", "ACE", suffix="nw")
        assert out["identity_nw"] == pytest.approx(1.0)
        assert out["alignment_length_nw"] == 3.0
        assert out["gaps_pct_nw"] == pytest.approx(0.0)
        assert out["length_query"] == 3
        assert out["length_ref"] == 3

    def test_sw_suffix_excludes_lengths(self) -> None:
        result = _fake_result("ACE", "ACE", "|||")
        out = _parse_alignment(result, "ACE", "ACE", suffix="sw")
        assert "identity_sw" in out
        assert "length_query" not in out
        assert "length_ref" not in out

    def test_gaps_counted_correctly(self) -> None:
        # query has one gap, ref has none
        result = _fake_result("A-E", "ACE", "|.|")
        out = _parse_alignment(result, "AE", "ACE", suffix="nw")
        assert out["gaps_pct_nw"] == pytest.approx(1 / 3)

    def test_similarity_from_comp_line(self) -> None:
        # comp line: "|" = identical, ":" = similar, "." = mismatch
        # "|.:" → 2 similar chars ("|" and ":"), 1 mismatch (".")
        result = _fake_result("ACE", "ADE", "|.:")
        out = _parse_alignment(result, "ACE", "ADE", suffix="nw")
        # similarity counts "|" and ":" → 2 out of 3
        assert out["similarity_nw"] == pytest.approx(2 / 3)

    def test_zero_length_alignment(self) -> None:
        result = _fake_result("", "", "", score=0.0)
        out = _parse_alignment(result, "", "", suffix="nw")
        assert out["identity_nw"] == 0.0
        assert out["alignment_length_nw"] == 0.0
        assert out["alignment_score_nw"] == 0.0

    def test_score_stored(self) -> None:
        result = _fake_result("AC", "AC", "||", score=55.0)
        out = _parse_alignment(result, "AC", "AC", suffix="sw")
        assert out["alignment_score_sw"] == pytest.approx(55.0)


# ---------------------------------------------------------------------------
# compute_nw / compute_sw / compute_alignment
# ---------------------------------------------------------------------------

class TestComputeNW:
    def test_calls_parasail_nw_and_returns_dict(self) -> None:
        fake_res = _fake_result("ACDEF", "ACDEF", "|||||", score=100.0)
        with patch("protea.core.feature_engineering._PARASAIL_AVAILABLE", True), \
             patch("protea.core.feature_engineering.parasail") as mock_p:
            mock_p.nw_trace_striped_32.return_value = fake_res
            mock_p.blosum62 = object()
            out = compute_nw("ACDEF", "ACDEF")
        assert "identity_nw" in out
        assert "length_query" in out
        mock_p.nw_trace_striped_32.assert_called_once()

    def test_raises_when_parasail_unavailable(self) -> None:
        with patch("protea.core.feature_engineering._PARASAIL_AVAILABLE", False):
            with pytest.raises(RuntimeError, match="parasail"):
                compute_nw("ACDEF", "ACDEF")


class TestComputeSW:
    def test_calls_parasail_sw_and_returns_dict(self) -> None:
        fake_res = _fake_result("ACDEF", "ACDEF", "|||||", score=80.0)
        with patch("protea.core.feature_engineering._PARASAIL_AVAILABLE", True), \
             patch("protea.core.feature_engineering.parasail") as mock_p:
            mock_p.sw_trace_striped_32.return_value = fake_res
            mock_p.blosum62 = object()
            out = compute_sw("ACDEF", "ACDEF")
        assert "identity_sw" in out
        assert "length_query" not in out

    def test_raises_when_parasail_unavailable(self) -> None:
        with patch("protea.core.feature_engineering._PARASAIL_AVAILABLE", False):
            with pytest.raises(RuntimeError, match="parasail"):
                compute_sw("A", "A")


class TestComputeAlignment:
    def test_merges_nw_and_sw(self) -> None:
        fake_res = _fake_result("AC", "AC", "||", score=10.0)
        with patch("protea.core.feature_engineering._PARASAIL_AVAILABLE", True), \
             patch("protea.core.feature_engineering.parasail") as mock_p:
            mock_p.nw_trace_striped_32.return_value = fake_res
            mock_p.sw_trace_striped_32.return_value = fake_res
            mock_p.blosum62 = object()
            out = compute_alignment("AC", "AC")
        assert "identity_nw" in out
        assert "identity_sw" in out


# ---------------------------------------------------------------------------
# _normalize_tax_id
# ---------------------------------------------------------------------------

class TestNormalizeTaxId:
    def test_int_passthrough(self) -> None:
        assert _normalize_tax_id(9606) == 9606

    def test_string_digit(self) -> None:
        assert _normalize_tax_id("9606") == 9606

    def test_string_with_whitespace(self) -> None:
        assert _normalize_tax_id("  9606  ") == 9606

    def test_none_returns_none(self) -> None:
        assert _normalize_tax_id(None) is None

    def test_non_numeric_string_returns_none(self) -> None:
        assert _normalize_tax_id("human") is None

    def test_empty_string_returns_none(self) -> None:
        assert _normalize_tax_id("") is None


# ---------------------------------------------------------------------------
# compute_taxonomy
# ---------------------------------------------------------------------------

class TestComputeTaxonomy:
    def test_none_inputs_return_unrelated(self) -> None:
        out = compute_taxonomy(None, None)
        assert out["taxonomic_relation"] == "unrelated"
        assert out["taxonomic_lca"] is None

    def test_same_id_returns_same(self) -> None:
        out = compute_taxonomy(9606, 9606)
        assert out["taxonomic_relation"] == "same"
        assert out["taxonomic_distance"] == 0
        assert out["taxonomic_lca"] == 9606

    def test_invalid_id_returns_unrelated(self) -> None:
        out = compute_taxonomy("notanid", 9606)
        assert out["taxonomic_relation"] == "unrelated"

    def test_lineage_exception_returns_unrelated(self) -> None:
        with patch("protea.core.feature_engineering._ETE3_AVAILABLE", True), \
             patch("protea.core.feature_engineering._cached_lineage", side_effect=Exception("db error")):
            out = compute_taxonomy(9606, 10090)
        assert out["taxonomic_relation"] == "unrelated"

    def test_common_ancestors_computed(self) -> None:
        # lin1: [1, 131567, 2759, 9606]
        # lin2: [1, 131567, 2759, 10090]
        # LCA = 2759 (common ancestor at deepest level in lin1)
        lin1 = [1, 131567, 2759, 9606]
        lin2 = [1, 131567, 2759, 10090]
        with patch("protea.core.feature_engineering._cached_lineage", side_effect=[lin1, lin2]):
            out = compute_taxonomy(9606, 10090)
        assert out["taxonomic_lca"] == 2759
        assert out["taxonomic_common_ancestors"] == 3  # 1, 131567, 2759
        assert out["taxonomic_distance"] is not None

    def test_distance_calculation(self) -> None:
        # lin1: [1, 10, 100, 9606] → lca=100 at index 2
        # lin2: [1, 10, 100, 10090] → lca=100 at index 2
        # distance = (4-2) + (4-2) = 4
        lin1 = [1, 10, 100, 9606]
        lin2 = [1, 10, 100, 10090]
        with patch("protea.core.feature_engineering._cached_lineage", side_effect=[lin1, lin2]):
            out = compute_taxonomy(9606, 10090)
        assert out["taxonomic_distance"] == 4


# ---------------------------------------------------------------------------
# _classify_relation
# ---------------------------------------------------------------------------

class TestClassifyRelation:
    def test_same(self) -> None:
        assert _classify_relation(9606, 9606, 1, 9606, [1, 9606], [1, 9606]) == "same"

    def test_ancestor(self) -> None:
        # t1=1 is in lin2
        assert _classify_relation(1, 9606, 2, 1, [1], [1, 9606]) == "ancestor"

    def test_descendant(self) -> None:
        # t2=1 is in lin1
        assert _classify_relation(9606, 1, 2, 1, [1, 9606], [1]) == "descendant"

    def test_root_only(self) -> None:
        # only root (id=1) in common
        assert _classify_relation(100, 200, 1, 1, [1, 100], [1, 200]) == "root-only"

    def test_distant(self) -> None:
        assert _classify_relation(100, 200, 2, 50, [1, 50, 100], [1, 50, 200]) == "distant"

    def test_intermediate(self) -> None:
        lin1 = list(range(20)) + [100]
        lin2 = list(range(20)) + [200]
        # common_count = 20 ancestors, lca = 19
        assert _classify_relation(100, 200, 10, 19, lin1, lin2) == "intermediate"

    def test_close(self) -> None:
        lin1 = list(range(40)) + [100]
        lin2 = list(range(40)) + [200]
        assert _classify_relation(100, 200, 20, 39, lin1, lin2) == "close"
