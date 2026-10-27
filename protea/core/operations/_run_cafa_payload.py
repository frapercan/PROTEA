"""The payload of ``run_cafa_evaluation``, and nothing else.

Out of ``run_cafa_evaluation.py`` because that file reached its 800-line budget
and the payload is 254 of them: twenty-six fields, each with the prose that says
what it decides. The budget exists to force exactly this split, and the holdout
guard that pushed the file over it is not the change that should also carry a
refactor.

Re-exported from ``run_cafa_evaluation`` so every existing import keeps working.
"""

from __future__ import annotations

from pydantic import Field, field_validator, model_validator

from protea.core.contracts.operation import ProteaPayload
from protea.core.operations._depth_unit_guard import (
    SEQUENCE_DEPTH_DESCRIPTION,
    assert_one_depth_unit,
)


class RunCafaEvaluationPayload(ProteaPayload, frozen=True):
    """The evaluation request. See ``max_sequence_rank`` on counting depth."""


    evaluation_set_id: str
    #: Permits the single pass on the holdout; see split_registry.HOLDOUT_WAIVER.
    holdout_waiver: str | None = None
    prediction_set_id: str
    max_distance: float | None = Field(default=None, ge=0.0, le=2.0)
    max_k_position: int | None = Field(
        default=None,
        ge=1,
        description=(
            "Score only the first N neighbours of each query. Candidates are "
            "written to a fixed depth and carry the rank they were retrieved at, "
            "so every smaller depth is a truncation of a list already stored and "
            "needs no new retrieval pass. Null scores every stored neighbour."
        ),
    )
    max_sequence_rank: int | None = Field(
        default=None, ge=1, description=SEQUENCE_DEPTH_DESCRIPTION
    )
    @model_validator(mode="after")
    def _a_depth_is_counted_in_one_unit(self) -> RunCafaEvaluationPayload:
        """See ``assert_one_depth_unit``; both null is the whole neighbourhood."""
        assert_one_depth_unit(self.max_k_position, self.max_sequence_rank)
        return self

    scoring_config_id: str | None = Field(default=None)
    reranker_id_nk: str | None = Field(default=None)
    reranker_id_lk: str | None = Field(default=None)
    reranker_id_pk: str | None = Field(default=None)
    rerankers: dict[str, dict[str, str]] | None = Field(
        default=None,
        description=(
            "Nested mapping of category → aspect → reranker_model_id. "
            'E.g. {"nk": {"bpo": "uuid", "mfo": "uuid"}, "lk": {...}}. '
            "Overrides the flat reranker_id_* fields when present."
        ),
    )
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
    restrict_gt_to_predicted: bool = Field(
        default=True,
        description=(
            "Standard CAFA practice: drop ground-truth proteins not present in the "
            "PredictionSet before evaluation, so coverage / Fmax measure performance "
            "on the actually-predicted cohort. Disable only when the eval set is "
            "guaranteed to be a subset of the predicted query set (e.g. a re-eval "
            "of a frozen lab dump where this filter has already been applied)."
        ),
    )
    softprop: bool = Field(
        default=False,
        description=(
            "Apply averaged soft Pmin/Pmax GO-DAG propagation (ProtBoost 4.5) to the "
            "prediction frame per protein right before cafaeval, as a scorer-agnostic "
            "post-processing step. Off by default so existing evals are bit-identical."
        ),
    )
    interpro_graft: bool = Field(
        default=False,
        description=(
            "Apply the InterPro2GO BP-only graft to the prediction frame per protein "
            "right before cafaeval, as a scorer-agnostic post-reranker arm. For BP terms "
            "the score becomes max(base, interpro_graded) (naive max) or a noisy-OR blend "
            "when interpro_graft_weight is set; BP terms InterPro adds are grafted as new "
            "candidates, while MF/CC terms stay untouched. Reproduces the offline "
            "naivemax_bponly champion. The board's own 9-cell mean f_micro_w is "
            "0.38844 for the pre-graft arm and 0.40765 for the grafted one; the "
            "0.4063 this description used to quote was the offline projection of "
            "the second, not a board figure. See _run_cafa_interpro_graft. "
            "Off by default so existing evals are bit-identical. Requires per-setting "
            "reranker predictions (same seam as softprop)."
        ),
    )
    interpro_protein2ipr_file: str | None = Field(
        default=None,
        description=(
            "Path to a JSON map of accession -> list of InterPro (IPR) accessions for the "
            "evaluation proteins. Required when interpro_graft is on; the arm is skipped "
            "with a warning (never crashes) when missing."
        ),
    )
    interpro_ipr2go_file: str | None = Field(
        default=None,
        description=(
            "Path to a JSON map of InterPro (IPR) accession -> list of propagated, "
            "namespaced GO ids (the offline ipr2go_prop.json). Required when "
            "interpro_graft is on; the arm is skipped with a warning when missing."
        ),
    )
    interpro_graft_weight: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description=(
            "Optional per-aspect weight for the InterPro BP graft. None (default) uses "
            "the parameter-free naive max max(base, graded); a value w uses the noisy-OR "
            "blend 1 - (1 - base)(1 - w*graded) on BP terms."
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
    toi_file: str | None = Field(
        default=None,
        description=(
            "Path to an explicit terms-of-interest (TOI) file (one GO id per line) "
            "passed to cafaeval's -toi flag. TOI restricts which terms count toward "
            "precision/recall. When None, PROTEA derives the TOI from every GO term "
            "in the pivot ontology snapshot. LAFA passes a release-specific "
            "groundtruth_terms_of_interest.txt that is a subset of the full ontology, "
            "so for strict LAFA parity pass that exact file here. See "
            "docs/EVAL_LAFA_PARITY.md."
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
            "window_role ('valid'->'select', 'test'->'test')."
        ),
    )
    arms_enabled: dict[str, bool] | None = Field(
        default=None,
        description=(
            "Method-arm composition flag dict stamped onto the EvaluationResult, "
            "e.g. {'knn': true, 'reranker': true, 'mlp_tower': false, "
            "'interpro': false, 'interpro_graft': false}. When None it is derived "
            "from the run (knn always on; reranker on when a reranker model is "
            "supplied; interpro_graft on when the payload opts into the InterPro "
            "BP graft and the run has rerankers)."
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
