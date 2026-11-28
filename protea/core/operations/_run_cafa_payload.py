"""The payload of ``run_cafa_evaluation``, and nothing else.

Out of ``run_cafa_evaluation.py`` because that file reached its 800-line budget
and the payload, each field with the prose that says what it decides, was a
third of it. Re-exported from ``run_cafa_evaluation`` so every existing import
keeps working.

Every field names the frame a result is measured in, or labels the result. None
names how a method's candidates become scores: the evaluator scores what a
method stored and nothing else (the closure's question 6, 2026-11-20), and the
fields that used to ask it to score, rerank, cut, graft or propagate are
refused with where that work happens now (:data:`RETIRED_FIELDS`). None
chooses the terms of interest or the population either: both are the
window's (decided on 2026-11-20), and the fields that chose them are refused
the same way.
"""

from __future__ import annotations

from typing import Any

from pydantic import Field, field_validator, model_validator

from protea.core.contracts.operation import ProteaPayload

_SCORING = (
    "score the candidates with score_prediction_set, which takes the scoring config "
    "and the depth cut, and evaluate the scored set"
)
_RERANKER = (
    "apply the reranker when predicting (predict_go_terms with reranker_model_id or "
    "the per-category bindings); its score is stored as the method's score"
)
_GRAFT = (
    "the InterPro BP graft runs when predicting, on top of a reranker "
    "(serve.interpro_bp_graft), and the prediction set's record says so"
)
_SOFTPROP = (
    "the platform no longer propagates scores; a method that propagates its scores "
    "stores them propagated"
)
_WINDOW_POPULATION = (
    "the population is the window's: every target counts, an unpredicted one as a "
    "failure, with coverage beside it, as LAFA scores; the result records it as "
    "results.population"
)
_WINDOW_TERMS = (
    "the terms of interest are the window's: the evaluator scores over the terms its "
    "gain rule names (SCORED_TERMS, protea/core/_truth_rules.py) and records the rule, "
    "the count and the file's sha256 on the result; scoring a window over other terms "
    "would be measuring another window"
)

#: The fields the evaluator no longer takes, each with where its work happens
#: now. A request that carries one with a value is refused with the pointer; one
#: that carries it empty (None, False, an empty mapping) has asked for nothing,
#: and is accepted, so a caller built before need not change to keep working.
RETIRED_FIELDS: dict[str, str] = {
    "scoring_config_id": _SCORING,
    "max_distance": _SCORING,
    "max_k_position": _SCORING,
    "max_sequence_rank": _SCORING,
    "reranker_id_nk": _RERANKER,
    "reranker_id_lk": _RERANKER,
    "reranker_id_pk": _RERANKER,
    "rerankers": _RERANKER,
    "interpro_graft": _GRAFT,
    "interpro_protein2ipr_file": _GRAFT,
    "interpro_ipr2go_file": _GRAFT,
    "interpro_graft_weight": _GRAFT,
    "softprop": _SOFTPROP,
    "toi_file": _WINDOW_TERMS,
    "toi_rule": _WINDOW_TERMS,
    "restrict_gt_to_predicted": _WINDOW_POPULATION,
}

_EMPTY: tuple[Any, ...] = (None, False, "", {}, [])


class RunCafaEvaluationPayload(ProteaPayload, frozen=True):
    """The evaluation request. See ``max_sequence_rank`` on counting depth."""

    evaluation_set_id: str
    #: Permits the single pass on the holdout; see split_registry.HOLDOUT_WAIVER.
    holdout_waiver: str | None = None
    #: Permits a band BEYOND the spent holdout, which begins at or after it and
    #: so cannot be a second look at it. Refused by name for a window that
    #: straddles the band. See split_registry.PROSPECTIVE_DECLARATION.
    prospective_declaration: str | None = None
    prediction_set_id: str

    @model_validator(mode="before")
    @classmethod
    def _the_evaluator_does_no_method_work(cls, data: Any) -> Any:
        """Refuse a retired field that asks for something, with where it is done now."""
        if not isinstance(data, dict):
            return data
        asked = sorted(
            key for key, value in data.items() if key in RETIRED_FIELDS and value not in _EMPTY
        )
        if asked:
            raise ValueError(
                "; ".join(f"{key}: {RETIRED_FIELDS[key]}" for key in asked)
                + ". The evaluator scores what a method stored, over the terms and the "
                "population its window names, and nothing else (docs: architecture/scoring)."
            )
        return {key: value for key, value in data.items() if key not in RETIRED_FIELDS}

    ia_file: str | None = Field(
        default=None,
        description=(
            "Path to an Information Accretion (IA) TSV file (two columns: go_id, ia_value). "
            "When provided, cafaeval weights each GO term by its IC so that rare, specific "
            "terms contribute more to the score than common, easy-to-predict terms. "
            "Without this file cafaeval assigns uniform weight (IC=1) to every term, which "
            "inflates Fmax because high-frequency terms dominate the metric. "
            "For CAFA6 evaluations use the IA_cafa6.tsv file supplied with the benchmark."
        ),
    )
    information_accretion_set_id: str | None = Field(
        default=None,
        description=(
            "Id of an InformationAccretionSet produced by the "
            "compute_information_accretion operation. Preferred over ia_file: the "
            "row pins all three axes IA actually depends on (ontology snapshot, "
            "annotation corpus, evidence regime) as foreign keys, the artifact is "
            "fetched from the shared object store so any machine resolves the same "
            "bytes, and the stored sha256 is verified after download. A bare path "
            "records none of that. Mutually exclusive with ia_file."
        ),
    )
    th_step: float = Field(
        default=0.01,
        gt=0.0,
        le=1.0,
        description=(
            "Score-threshold grid step passed to cafaeval. cafaeval sweeps tau over "
            "np.arange(th_step, 1, th_step) and reports the metric at its best tau. "
            "A finer grid (smaller th_step) optimises over more candidate thresholds "
            "and so reports a slightly higher Fmax / f_micro_w, which breaks numeric "
            "parity with LAFA. LAFA uses cafaeval's default 0.01; keep this at 0.01 "
            "for LAFA-comparable runs. See docs/EVAL_LAFA_PARITY.md."
        ),
    )
    score_reading: str = Field(
        default="raw",
        description=(
            "How the written scores are read before cafaeval thresholds them "
            "(ADR-D55). 'raw' (the default, and every run before it) keeps them as "
            "computed. 'percentile_rank' replaces each score by its percentile rank "
            "among the rows its prediction file scores, tied scores at the average "
            "of their ranks, so the uniform grid falls on the arm's own quantiles "
            "instead of on a scale the bank's anisotropy compressed: in the full "
            "length ankh bank two unrelated proteins already score 0.975, which "
            "leaves the 0.01 grid two steps. The reading is monotone, so it keeps "
            "the arm's ordering; what moves is where the grid falls, and with it any "
            "metric at a fixed threshold, which becomes relative to the arm. Part of "
            "the frame seal, so results read two ways never seal alike. LAFA reads "
            "raw scores; keep 'raw' for LAFA-comparable runs."
        ),
    )
    max_terms: int | None = Field(
        default=None,
        ge=1,
        description=(
            "Optional cap on the number of predicted terms kept per protein per "
            "namespace (highest-score first). None (the default, matching LAFA) "
            "keeps every predicted term. A cap only changes the score when a "
            "prediction exceeds it for some protein/namespace; PROTEA-KNN style "
            "predictions never do, so this is normally inert. Set it only to "
            "reproduce a legacy capped run. See docs/EVAL_LAFA_PARITY.md."
        ),
    )
    band: str | None = Field(
        default=None,
        description=(
            "Declared evaluation band / cutoff (e.g. 'v226', 'v227', or any "
            "vNNN-bearing dataset name). When set, the phantom-gap guard "
            "(protea.core.band_registry) rejects the run if the resolved pivot "
            "ontology snapshot or the resolved IA artifact come from a band "
            "other than this one: a cross-band snapshot/IA inflates a fake "
            "PROTEA-vs-LAFA gap. A band-declared cell may NEVER fall back to "
            "uniform IC=1. Leave None for ad-hoc (unbanded) episodes. See "
            "docs/IA_PROVENANCE_v227.md and docs/EVAL_LAFA_PARITY.md."
        ),
    )
    frame: str | None = Field(
        default=None,
        description=(
            "Scoring frame this result lives in, stamped onto the EvaluationResult "
            "so /benchmark and /evaluation are self-describing (slice "
            "F-METHOD-EVAL-SURFACE). 'lafa' = the parity-locked LAFA frame "
            "(leaderboard-comparable); 'internal' = the lab / full-GT frame. None "
            "leaves the row unstamped (UI shows an 'unknown' chip)."
        ),
    )
    temporal_window: str | None = Field(
        default=None,
        description=(
            "Rolling-origin window label stamped onto the EvaluationResult, e.g. "
            "'SELECT_220_227' (selection window) or 'FINAL_227_230' (report-once "
            "test window). Free text so new windows need no schema change."
        ),
    )
    leakage_role: str | None = Field(
        default=None,
        description=(
            "ADR D40 leakage-hygiene role stamped onto the EvaluationResult: "
            "'select' (feeds model/threshold selection), 'test' (report-once "
            "held-out measurement), or 'probe' (exploratory, must not feed "
            "selection). When None it is derived from the EvaluationSet "
            "window_role ('training', 'validation' and 'valid' -> 'select'; "
            "'test' -> 'test')."
        ),
    )
    arms_enabled: dict[str, bool] | None = Field(
        default=None,
        description=(
            "Method-arm composition flag dict stamped onto the EvaluationResult, "
            "e.g. {'knn': true, 'reranker': true, 'interpro_graft': false}, as the "
            "caller labels the method. When None the result carries none: the "
            "evaluator runs no arm, and what the method ran is in its prediction "
            "set's record."
        ),
    )
    window_role: str | None = Field(
        default=None,
        description=(
            "Rolling-origin protocol window binding ('valid' or 'test', ADR D40). "
            "When set, stamped onto the EvaluationSet if it does not already carry "
            "one, so re-staged sets become self-describing without a separate "
            "generate_evaluation_set rebind."
        ),
    )

    @field_validator("evaluation_set_id", "prediction_set_id", mode="before")
    @classmethod
    def must_be_non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()

    @field_validator("score_reading")
    @classmethod
    def _score_reading_vocab(cls, v: str) -> str:
        from protea.core.operations._run_cafa_scale import SCORE_READINGS

        if v not in SCORE_READINGS:
            raise ValueError(f"score_reading must be one of {SCORE_READINGS}")
        return v

    @field_validator("frame")
    @classmethod
    def _frame_vocab(cls, v: str | None) -> str | None:
        if v is not None and v not in ("lafa", "internal"):
            raise ValueError("frame must be 'lafa', 'internal', or None")
        return v

    @field_validator("leakage_role")
    @classmethod
    def _leakage_role_vocab(cls, v: str | None) -> str | None:
        if v is not None and v not in ("select", "test", "probe"):
            raise ValueError("leakage_role must be 'select', 'test', 'probe', or None")
        return v

    @field_validator("window_role")
    @classmethod
    def _window_role_vocab(cls, v: str | None) -> str | None:
        if v is not None and v not in ("valid", "test"):
            raise ValueError("window_role must be 'valid', 'test', or None")
        return v
