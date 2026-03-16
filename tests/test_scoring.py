"""Tests for protea.core.scoring and related evidence weight resolution."""
import pytest
from unittest.mock import MagicMock

from protea.core.scoring import compute_score, evidence_weight, score_predictions
from protea.infrastructure.orm.models.embedding.scoring_config import (
    DEFAULT_EVIDENCE_WEIGHT_FALLBACK,
    DEFAULT_EVIDENCE_WEIGHTS,
    FORMULA_EVIDENCE_WEIGHTED,
    FORMULA_LINEAR,
    ScoringConfig,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _config(
    weights: dict,
    formula: str = FORMULA_LINEAR,
    evidence_weights: dict | None = None,
) -> ScoringConfig:
    cfg = MagicMock(spec=ScoringConfig)
    cfg.weights = weights
    cfg.formula = formula
    cfg.evidence_weights = evidence_weights
    return cfg


# ---------------------------------------------------------------------------
# evidence_weight
# ---------------------------------------------------------------------------

class TestEvidenceWeight:
    def test_none_code_returns_fallback(self):
        assert evidence_weight(None) == DEFAULT_EVIDENCE_WEIGHT_FALLBACK

    def test_empty_string_returns_fallback(self):
        assert evidence_weight("") == DEFAULT_EVIDENCE_WEIGHT_FALLBACK

    def test_known_experimental_code(self):
        assert evidence_weight("IDA") == 1.0

    def test_known_electronic_code(self):
        assert evidence_weight("IEA") == 0.3

    def test_known_computational_code(self):
        assert evidence_weight("IBA") == 0.7

    def test_unknown_code_returns_fallback(self):
        assert evidence_weight("UNKNOWN") == DEFAULT_EVIDENCE_WEIGHT_FALLBACK

    def test_override_takes_precedence(self):
        assert evidence_weight("IEA", overrides={"IEA": 0.0}) == 0.0

    def test_partial_override_fallback_to_default(self):
        # IDA not in overrides → falls back to DEFAULT_EVIDENCE_WEIGHTS
        assert evidence_weight("IDA", overrides={"IEA": 0.0}) == 1.0

    def test_eco_id_normalized(self):
        # ECO:0000501 maps to IEA
        from protea.core.evidence_codes import ECO_TO_CODE
        eco_ids = [eco for eco, go in ECO_TO_CODE.items() if go == "IEA"]
        if eco_ids:
            assert evidence_weight(eco_ids[0]) == pytest.approx(0.3)

    def test_override_with_none_overrides_arg(self):
        # overrides=None should not crash
        assert evidence_weight("IDA", overrides=None) == 1.0


# ---------------------------------------------------------------------------
# compute_score — pure embedding
# ---------------------------------------------------------------------------

class TestComputeScoreEmbeddingOnly:
    def setup_method(self):
        self.cfg = _config({"embedding_similarity": 1.0})

    def test_zero_distance_gives_one(self):
        score = compute_score({"distance": 0.0}, self.cfg)
        assert score == pytest.approx(1.0)

    def test_max_distance_gives_zero(self):
        score = compute_score({"distance": 2.0}, self.cfg)
        assert score == pytest.approx(0.0)

    def test_mid_distance(self):
        score = compute_score({"distance": 1.0}, self.cfg)
        assert score == pytest.approx(0.5)

    def test_no_signals_returns_zero(self):
        cfg = _config({})
        score = compute_score({"distance": 0.5}, cfg)
        assert score == 0.0

    def test_missing_distance_ignored(self):
        score = compute_score({}, self.cfg)
        assert score == 0.0

    def test_score_rounded_to_6_decimals(self):
        score = compute_score({"distance": 0.3333333}, self.cfg)
        assert len(str(score).split(".")[-1]) <= 6


# ---------------------------------------------------------------------------
# compute_score — multi-signal
# ---------------------------------------------------------------------------

class TestComputeScoreMultiSignal:
    def test_nw_identity_contributes(self):
        cfg = _config({"embedding_similarity": 0.5, "identity_nw": 0.5})
        pred = {"distance": 0.0, "identity_nw": 0.5}
        score = compute_score(pred, cfg)
        # embedding=1.0*0.5, nw=0.5*0.5 → (0.5+0.25)/1.0 = 0.75
        assert score == pytest.approx(0.75)

    def test_sw_identity_contributes(self):
        cfg = _config({"embedding_similarity": 0.5, "identity_sw": 0.5})
        pred = {"distance": 0.0, "identity_sw": 1.0}
        score = compute_score(pred, cfg)
        # embedding=1.0*0.5, sw=1.0*0.5 → (0.5+0.5)/1.0 = 1.0
        assert score == pytest.approx(1.0)

    def test_none_signal_excluded_from_denominator(self):
        cfg = _config({"embedding_similarity": 1.0, "identity_nw": 1.0})
        # identity_nw is None → only embedding_similarity contributes
        pred = {"distance": 0.0, "identity_nw": None}
        score = compute_score(pred, cfg)
        assert score == pytest.approx(1.0)

    def test_taxonomic_proximity_zero_distance(self):
        cfg = _config({"taxonomic_proximity": 1.0})
        pred = {"taxonomic_distance": 0.0}
        score = compute_score(pred, cfg)
        assert score == pytest.approx(1.0)

    def test_taxonomic_proximity_large_distance(self):
        cfg = _config({"taxonomic_proximity": 1.0})
        pred = {"taxonomic_distance": 999.0}
        score = compute_score(pred, cfg)
        assert 0.0 < score < 0.01

    def test_evidence_weight_signal(self):
        cfg = _config({"evidence_weight": 1.0})
        score_exp = compute_score({"evidence_code": "IDA"}, cfg)
        score_iea = compute_score({"evidence_code": "IEA"}, cfg)
        assert score_exp > score_iea

    def test_signal_clamped_to_zero(self):
        cfg = _config({"identity_nw": 1.0})
        # negative value should be clamped to 0
        score = compute_score({"identity_nw": -0.5}, cfg)
        assert score == pytest.approx(0.0)

    def test_signal_clamped_to_one(self):
        cfg = _config({"identity_nw": 1.0})
        score = compute_score({"identity_nw": 1.5}, cfg)
        assert score == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# compute_score — evidence_weighted formula
# ---------------------------------------------------------------------------

class TestComputeScoreEvidenceWeighted:
    def test_iea_downgrades_score(self):
        cfg_linear = _config({"embedding_similarity": 1.0}, formula=FORMULA_LINEAR)
        cfg_evw = _config({"embedding_similarity": 1.0}, formula=FORMULA_EVIDENCE_WEIGHTED)
        pred = {"distance": 0.0, "evidence_code": "IEA"}
        score_linear = compute_score(pred, cfg_linear)
        score_evw = compute_score(pred, cfg_evw)
        assert score_evw < score_linear

    def test_experimental_code_not_penalized(self):
        cfg = _config({"embedding_similarity": 1.0}, formula=FORMULA_EVIDENCE_WEIGHTED)
        pred = {"distance": 0.0, "evidence_code": "IDA"}
        score = compute_score(pred, cfg)
        assert score == pytest.approx(1.0)

    def test_custom_evidence_override_applied(self):
        cfg = _config(
            {"embedding_similarity": 1.0},
            formula=FORMULA_EVIDENCE_WEIGHTED,
            evidence_weights={"IEA": 0.0},
        )
        pred = {"distance": 0.0, "evidence_code": "IEA"}
        score = compute_score(pred, cfg)
        assert score == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# score_predictions
# ---------------------------------------------------------------------------

class TestScorePredictions:
    def setup_method(self):
        self.cfg = _config({"embedding_similarity": 1.0})

    def test_returns_list_with_score_key(self):
        preds = [{"distance": 0.5}, {"distance": 0.2}]
        result = score_predictions(preds, self.cfg)
        assert all("score" in r for r in result)

    def test_sorted_descending(self):
        preds = [{"distance": 1.0}, {"distance": 0.1}, {"distance": 0.5}]
        result = score_predictions(preds, self.cfg)
        scores = [r["score"] for r in result]
        assert scores == sorted(scores, reverse=True)

    def test_original_list_not_modified(self):
        preds = [{"distance": 0.5}]
        score_predictions(preds, self.cfg)
        assert "score" not in preds[0]

    def test_empty_list(self):
        assert score_predictions([], self.cfg) == []
