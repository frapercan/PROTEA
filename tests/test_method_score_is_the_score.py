"""A method that scores its own candidates, scored by the platform.

Every score PROTEA publishes is derived at evaluation time from the retrieval
columns of ``go_prediction``, by a ``ScoringConfig``'s weighted average or by
a re-ranker booster. That is the right design for neighbour transfer plus
features and it cannot express a method that emits its own probability: there
is no donor to measure a distance to and no feature to weight.

The arithmetic that makes the gap tempting is in this module's second test.
``_resolve_signal_values`` turns a distance into ``embedding_similarity =
1 - d / 2``, so writing ``distance = 2 * (1 - p)`` makes the method's own
``p`` come back out of the default config exactly. It works, and it records a
cosine distance to a neighbour that does not exist, which is the overload this
project has already paid for twice (the native snapshot fields, and the
``frame`` column that could not hold a frame). So the number gets its own
column and its own formula, and the formula reads nothing else.

WHAT THESE PIN. That the method's number survives to the four decimals the
TSV is written at; that the weights are not consulted; that a row with no
method score RAISES instead of scoring zero, because a prediction set
declaring its own scores and having none is a provenance error and a cell of
zeros looks like a method performing badly; and that the vectorised path and
the scalar path still agree, which is the invariant
``_vectorized_scores`` promises in its own docstring and the one a new branch
is most likely to break.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from protea.core.operations._run_cafa_artifacts import (
    _BASE_SCORE_COLS,
    _vectorized_scores,
    _write_scored_base,
)
from protea.core.scoring import (
    FORMULA_METHOD_SCORE,
    MethodScoreMissing,
    compute_score,
)
from protea.infrastructure.orm.models.embedding.scoring_config import VALID_FORMULAS

#: Real scores from the lab's deployable recipe dump, the first rows of
#: ``deployable-recipe-ens8.tsv``, plus the two boundaries.
LAB_SCORES = (0.632965147, 0.626153886, 0.470000000, 1.0, 0.0)


class _MethodConfig:
    """Minimal stand-in, with weights that MUST be ignored."""

    formula = FORMULA_METHOD_SCORE
    #: Deliberately non-empty and deliberately wrong: if the formula consulted
    #: the weights at all, the distances below would move the answer.
    weights = {"embedding_similarity": 1.0, "identity_nw": 1.0}
    evidence_weights = None
    params = None


class _LinearConfig:
    formula = "linear"
    weights = {"embedding_similarity": 1.0}
    evidence_weights = None
    params = None


def _row(**over: object) -> dict:
    row = {
        "protein_accession": "P00001",
        "go_id": "GO:0000001",
        "distance": None,
        "method_score": 0.632965147,
        "evidence_code": "EXP",
        "taxonomic_distance": None,
        "neighbor_vote_fraction": None,
        "identity_nw": 0.9,
        "identity_sw": None,
    }
    row.update(over)
    return row


def _frame(scores: list[float | None], distances: list[float | None] | None = None):
    n = len(scores)
    return pd.DataFrame(
        {
            "protein_accession": [f"P{i:05d}" for i in range(n)],
            "go_id": ["GO:0000001"] * n,
            "method_score": scores,
            "distance": distances if distances is not None else [None] * n,
            "evidence_code": ["EXP"] * n,
            "taxonomic_distance": [None] * n,
            "neighbor_vote_fraction": [None] * n,
            "identity_nw": [0.9] * n,
            "identity_sw": [None] * n,
            "alignment_length_nw": [None] * n,
            "gaps_pct_nw": [None] * n,
            "alignment_length_sw": [None] * n,
            "gaps_pct_sw": [None] * n,
            "length_query": [None] * n,
            "ref_annotation_density": [None] * n,
            "anc2vec_neighbor_cos": [None] * n,
            "anc2vec_neighbor_maxcos": [None] * n,
            "go_term_frequency": [None] * n,
        }
    )


class TestTheFormulaReturnsTheMethodsNumber:
    def test_the_formula_is_part_of_the_vocabulary(self) -> None:
        assert FORMULA_METHOD_SCORE in VALID_FORMULAS

    @pytest.mark.parametrize("score", LAB_SCORES)
    def test_the_score_is_the_method_score(self, score: float) -> None:
        got = compute_score(_row(method_score=score), _MethodConfig())
        assert got == round(score, 6)

    def test_the_weights_are_not_consulted(self) -> None:
        # Same row twice, one distance apart. Under `linear` the distance
        # decides; under `method_score` it cannot move the answer.
        near = _row(distance=0.0)
        far = _row(distance=2.0)
        assert compute_score(near, _MethodConfig()) == compute_score(far, _MethodConfig())
        assert compute_score(near, _LinearConfig()) != compute_score(far, _LinearConfig())

    def test_the_other_formulas_ignore_the_column(self) -> None:
        # The regression that matters for every stored result: a row carrying
        # a method score must score under `linear` exactly as it did before
        # the column existed, which is 1 - d/2.
        got = compute_score(_row(distance=0.5, method_score=0.99), _LinearConfig())
        assert got == round(1.0 - 0.5 / 2.0, 6)


class TestItRefusesRatherThanScoringZero:
    def test_a_row_without_a_method_score_raises(self) -> None:
        with pytest.raises(MethodScoreMissing, match="has none"):
            compute_score(_row(method_score=None), _MethodConfig())

    def test_the_raise_names_the_row(self) -> None:
        with pytest.raises(MethodScoreMissing) as caught:
            compute_score(
                _row(method_score=None, protein_accession="Q9XYZ1", go_id="GO:0005515"),
                _MethodConfig(),
            )
        assert "Q9XYZ1" in str(caught.value)
        assert "GO:0005515" in str(caught.value)

    def test_the_vectorised_path_refuses_a_frame_with_any_gap(self) -> None:
        with pytest.raises(MethodScoreMissing, match="1 of 3"):
            _vectorized_scores(_frame([0.5, None, 0.7]), _MethodConfig())

    def test_the_vectorised_path_refuses_a_frame_cached_before_the_column(self) -> None:
        # The memo key names the prediction set and the depth cuts, not the
        # schema, so a parquet written before the column exists comes back
        # without it. That is a stale cache, not a method with no scores.
        stale = _frame([0.5, 0.6]).drop(columns=["method_score"])
        with pytest.raises(MethodScoreMissing, match="rebuilt"):
            _vectorized_scores(stale, _MethodConfig())

    def test_a_method_row_under_the_default_config_scores_zero(self) -> None:
        # Why the formula has to exist. A method row has no distance, so
        # `linear` finds no signal with a weight, the denominator is zero and
        # the score is 0.0. Scoring a method prediction set with the default
        # config does not fail: it publishes a cell of zeros.
        assert compute_score(_row(distance=None), _LinearConfig()) == 0.0


class TestThePathsAgreeAndTheNumberSurvivesTheWrite:
    def test_the_vectorised_path_equals_the_scalar_one(self) -> None:
        scores = list(LAB_SCORES)
        frame = _frame(scores)
        vector = _vectorized_scores(frame, _MethodConfig())
        scalar = [compute_score(_row(method_score=s), _MethodConfig()) for s in scores]
        # The vectorised path returns the UNROUNDED array and the caller
        # rounds, so compare after the same rounding the writer applies.
        assert [round(float(v), 6) for v in vector] == scalar

    def test_the_vectorised_path_is_exact_and_not_merely_close(self) -> None:
        scores = list(LAB_SCORES)
        got = _vectorized_scores(_frame(scores), _MethodConfig())
        assert np.array_equal(got, np.asarray(scores, dtype=np.float64))

    def test_the_written_tsv_carries_the_method_score_at_four_decimals(
        self, tmp_path: Path
    ) -> None:
        scores = list(LAB_SCORES)
        out = tmp_path / "preds.tsv"
        _write_scored_base(_frame(scores), _MethodConfig(), str(out))
        written = [line.split("\t")[2] for line in out.read_text().splitlines()]
        assert written == [f"{s:.4f}" for s in scores]

    def test_four_decimals_is_the_only_rounding_the_number_meets(self) -> None:
        # round(x, 6) then %.4f must equal %.4f of the original, or the
        # platform would publish a different number from the one the method
        # produced. Checked on the real scores and on a dense grid, because
        # the failure would be a value sitting on a fourth-decimal boundary.
        grid = [i / 20000 for i in range(20001)]
        for value in (*LAB_SCORES, *grid):
            assert f"{round(value, 6):.4f}" == f"{value:.4f}", value


class TestTheFrameCarriesTheColumn:
    def test_the_base_frame_selects_the_method_score(self) -> None:
        # Selected always, like donor_count: a frame whose schema depended on
        # which formula was going to read it would make the memo key wrong in
        # a second way.
        assert "method_score" in _BASE_SCORE_COLS

    def test_the_columnar_select_names_it(self) -> None:
        from protea.core.operations._run_cafa_artifacts import _base_select

        class _Ctx:
            pred_set_id = "11111111-1111-1111-1111-111111111111"
            delta_proteins = {"P00001"}
            max_distance = None
            max_k_position = None
            max_sequence_rank = None

        sql = str(_base_select(_Ctx()))
        assert "go_prediction.method_score" in sql
