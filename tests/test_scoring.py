"""Tests for protea.core.scoring and related evidence weight resolution."""

from unittest.mock import MagicMock

import pytest

from protea.core.scoring import compute_score, evidence_weight, score_predictions
from protea.infrastructure.orm.models.embedding.scoring_config import (
    DEFAULT_EVIDENCE_WEIGHT_FALLBACK,
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
    params: dict | None = None,
) -> ScoringConfig:
    cfg = MagicMock(spec=ScoringConfig)
    cfg.weights = weights
    cfg.formula = formula
    cfg.evidence_weights = evidence_weights
    cfg.params = params
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
        assert evidence_weight("IEA") == 0.8

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
            assert evidence_weight(eco_ids[0]) == pytest.approx(0.8)

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

    def test_vote_fraction_signal(self):
        cfg = _config({"neighbor_vote_fraction": 1.0})
        score = compute_score({"neighbor_vote_fraction": 0.7}, cfg)
        assert score == pytest.approx(0.7)

    def test_vote_fraction_unanimous(self):
        cfg = _config({"neighbor_vote_fraction": 1.0})
        score = compute_score({"neighbor_vote_fraction": 1.0}, cfg)
        assert score == pytest.approx(1.0)

    def test_vote_fraction_combined_with_embedding(self):
        cfg = _config({"embedding_similarity": 0.5, "neighbor_vote_fraction": 0.5})
        # embedding similarity = 1.0 (distance=0), vote_fraction = 0.6
        # → (0.5*1.0 + 0.5*0.6)/1.0 = 0.8
        pred = {"distance": 0.0, "neighbor_vote_fraction": 0.6}
        score = compute_score(pred, cfg)
        assert score == pytest.approx(0.8)

    def test_vote_fraction_none_excluded(self):
        cfg = _config({"embedding_similarity": 0.5, "neighbor_vote_fraction": 0.5})
        # vote_fraction is None → only embedding_similarity contributes,
        # denominator collapses so score = 1.0
        score = compute_score({"distance": 0.0, "neighbor_vote_fraction": None}, cfg)
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


class TestBackwardCompatGolden:
    """Frozen composite scores for the legacy 6-signal scorer. A config that
    does not mention the new rich signals / params must reproduce these exactly
    (the A-SCORE knobs are strictly additive and default-off)."""

    _PRED = {
        "distance": 0.4,
        "identity_nw": 0.8,
        "identity_sw": 0.9,
        "evidence_code": "IEA",
        "taxonomic_distance": 2.0,
        "neighbor_vote_fraction": 0.5,
        # rich columns present on the row but unweighted -> must not move score
        "alignment_length_sw": 120.0,
        "gaps_pct_sw": 0.1,
        "length_query": 200,
        "ref_annotation_density": 7,
        "anc2vec_neighbor_cos": 0.6,
        "anc2vec_neighbor_maxcos": 0.8,
        "go_term_frequency": 5000,
    }

    def test_linear_multi_signal_golden(self):
        cfg = _config(
            {
                "embedding_similarity": 0.5,
                "identity_nw": 0.3,
                "evidence_weight": 0.2,
                "taxonomic_proximity": 0.1,
                "neighbor_vote_fraction": 0.4,
            },
            evidence_weights={"IEA": 0.3},
        )
        # embedding=0.8, id_nw=0.8, ev=0.3, tax_prox=1/3, vote=0.5
        # num = .5*.8 + .3*.8 + .2*.3 + .1*(1/3) + .4*.5 = .4+.24+.06+.033333+.2
        # den = 1.5 -> 0.933333/1.5 = 0.622222
        assert compute_score(self._PRED, cfg) == pytest.approx(0.622222)

    def test_evidence_weighted_golden(self):
        cfg = _config(
            {"embedding_similarity": 1.0, "evidence_weight": 0.0},
            formula=FORMULA_EVIDENCE_WEIGHTED,
            evidence_weights={"IEA": 0.5},
        )
        # base = 0.8 (embedding only), * ev_w(IEA=0.5) = 0.4
        assert compute_score(self._PRED, cfg) == pytest.approx(0.4)

    def test_rich_columns_present_but_unweighted_is_noop(self):
        # Same pred WITH vs WITHOUT the rich columns, legacy weights only.
        cfg = _config({"embedding_similarity": 1.0})
        legacy = {k: self._PRED[k] for k in ("distance",)}
        assert compute_score(self._PRED, cfg) == compute_score(legacy, cfg)


class TestRichSignals:
    def test_coverage_local_alignment(self):
        cfg = _config({"coverage": 1.0})
        # ungapped = 120 * (1 - 0.1) = 108; / 200 = 0.54
        pred = {"alignment_length_sw": 120.0, "gaps_pct_sw": 0.1, "length_query": 200}
        assert compute_score(pred, cfg) == pytest.approx(0.54)

    def test_coverage_falls_back_to_nw(self):
        cfg = _config({"coverage": 1.0})
        pred = {"alignment_length_nw": 200.0, "gaps_pct_nw": 0.0, "length_query": 200}
        assert compute_score(pred, cfg) == pytest.approx(1.0)

    def test_coverage_drops_out_without_length_query(self):
        cfg = _config({"coverage": 1.0, "embedding_similarity": 1.0})
        # coverage None -> only embedding contributes -> score 1.0
        pred = {"distance": 0.0, "alignment_length_sw": 50.0, "gaps_pct_sw": 0.0}
        assert compute_score(pred, cfg) == pytest.approx(1.0)

    def test_coverage_clamped_to_one(self):
        cfg = _config({"coverage": 1.0})
        pred = {"alignment_length_sw": 400.0, "gaps_pct_sw": 0.0, "length_query": 200}
        assert compute_score(pred, cfg) == pytest.approx(1.0)

    def test_ref_density_squash(self):
        cfg = _config({"ref_annotation_density": 1.0})
        # 1/(1+3) = 0.25
        assert compute_score({"ref_annotation_density": 3}, cfg) == pytest.approx(0.25)

    def test_ref_density_zero_is_one(self):
        cfg = _config({"ref_annotation_density": 1.0})
        assert compute_score({"ref_annotation_density": 0}, cfg) == pytest.approx(1.0)

    def test_ref_density_drops_out_when_none(self):
        cfg = _config({"ref_annotation_density": 1.0, "embedding_similarity": 1.0})
        assert compute_score({"distance": 0.0}, cfg) == pytest.approx(1.0)

    def test_anc2vec_cos_mapped_to_unit(self):
        cfg = _config({"anc2vec_neighbor_cos": 1.0})
        # (0.6 + 1)/2 = 0.8
        assert compute_score({"anc2vec_neighbor_cos": 0.6}, cfg) == pytest.approx(0.8)

    def test_anc2vec_negative_cos_clamped(self):
        cfg = _config({"anc2vec_neighbor_cos": 1.0})
        # (-1 + 1)/2 = 0.0
        assert compute_score({"anc2vec_neighbor_cos": -1.0}, cfg) == pytest.approx(0.0)

    def test_anc2vec_maxcos_signal(self):
        cfg = _config({"anc2vec_neighbor_maxcos": 1.0})
        assert compute_score({"anc2vec_neighbor_maxcos": 0.0}, cfg) == pytest.approx(0.5)

    def test_anc2vec_drops_out_when_none(self):
        cfg = _config({"anc2vec_neighbor_cos": 1.0, "embedding_similarity": 1.0})
        assert compute_score({"distance": 0.0}, cfg) == pytest.approx(1.0)


class TestIAPrior:
    _BASE = {"distance": 0.0}  # embedding_similarity = 1.0 by default config

    def test_disabled_by_default_is_noop(self):
        cfg = _config({"embedding_similarity": 1.0}, params=None)
        assert compute_score({"distance": 0.0, "go_term_frequency": 9999}, cfg) == pytest.approx(
            1.0
        )

    def test_disabled_block_is_noop(self):
        cfg = _config(
            {"embedding_similarity": 1.0},
            params={"ia_prior": {"enabled": False, "gamma": 1.0}},
        )
        assert compute_score({"distance": 0.0, "go_term_frequency": 9999}, cfg) == pytest.approx(
            1.0
        )

    def test_frequency_source_downweights_common_terms(self):
        import math

        cfg = _config(
            {"embedding_similarity": 1.0},
            params={"ia_prior": {"enabled": True, "gamma": 1.0, "source": "frequency"}},
        )
        prior = 1.0 / (1.0 + math.log1p(100.0))
        assert compute_score({"distance": 0.0, "go_term_frequency": 100}, cfg) == pytest.approx(
            round(prior, 6)
        )

    def test_gamma_zero_is_noop_even_when_enabled(self):
        cfg = _config(
            {"embedding_similarity": 1.0},
            params={"ia_prior": {"enabled": True, "gamma": 0.0, "source": "frequency"}},
        )
        assert compute_score({"distance": 0.0, "go_term_frequency": 9999}, cfg) == pytest.approx(
            1.0
        )

    def test_missing_frequency_field_no_penalty(self):
        cfg = _config(
            {"embedding_similarity": 1.0},
            params={"ia_prior": {"enabled": True, "gamma": 1.0, "source": "frequency"}},
        )
        assert compute_score({"distance": 0.0}, cfg) == pytest.approx(1.0)

    def test_ia_source_uses_term_ia(self):
        cfg = _config(
            {"embedding_similarity": 1.0},
            params={"ia_prior": {"enabled": True, "gamma": 1.0, "source": "ia"}},
        )
        # base 1.0 * prior(term_ia=0.3) = 0.3
        assert compute_score({"distance": 0.0, "term_ia": 0.3}, cfg) == pytest.approx(0.3)

    def test_ia_source_gamma_sharpens(self):
        cfg = _config(
            {"embedding_similarity": 1.0},
            params={"ia_prior": {"enabled": True, "gamma": 2.0, "source": "ia"}},
        )
        # 1.0 * 0.5 ** 2 = 0.25
        assert compute_score({"distance": 0.0, "term_ia": 0.5}, cfg) == pytest.approx(0.25)


class TestCalibrationHook:
    def test_disabled_is_noop(self):
        from protea.core.scoring import apply_calibration

        assert apply_calibration(0.7, calibration=None) == 0.7
        assert apply_calibration(0.7, calibration={"enabled": False}) == 0.7

    def test_enabled_stub_passes_through(self):
        from protea.core.scoring import apply_calibration

        # Stub: enabled but no tables -> still a pass-through (A-SCORE.2 fills it).
        assert apply_calibration(0.7, aspect="F", ia_bin=2, calibration={"enabled": True}) == 0.7

    def test_compute_score_with_calibration_flag_is_noop(self):
        cfg = _config(
            {"embedding_similarity": 1.0},
            params={"calibration": {"enabled": True}},
        )
        assert compute_score({"distance": 0.0}, cfg) == pytest.approx(1.0)


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
