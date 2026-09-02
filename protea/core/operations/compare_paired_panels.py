"""Is the difference between two evaluation results real, in each of the nine panels.

**The failure this prevents.** This project already had an interval producer,
``scripts/bootstrap_fmax_ci.py``. It bootstraps the mean of per-protein Fmax,
where every protein picks its own best threshold. The campaign reports
``f_micro_w``: an information-accretion weighted micro F, a ratio of sums pooled
over the whole panel at ONE shared threshold, that threshold chosen to maximise
the panel metric. Three differences, each of which alone breaks the comparison.
A per-protein oracle threshold is a quantity no deployable system reaches, since
at serving time the ground truth is unknown, and it does not inflate two arms
equally: the arm whose score distribution varies more across proteins gains
more, so a paired difference under the oracle statistic is not the paired
difference under the reported metric. A mean over proteins and a ratio of sums
weight a two-term protein and a two-hundred-term protein completely differently.
And the unweighted variant answers a different question, in a project whose own
record holds that reweighting moves ten resolvable winners. Promoting that
script would have carried the wrong estimator onto the trunk under the right
name.

**Why an operation and not a script.** A procedure outside the platform is a
capability that dies with the disk. This one produces the interval every
difference in the thesis has to carry, it will be re-run after every rung and
after every re-evaluation, and its inputs, its seed and its refusals have to be
recoverable from a job row rather than from somebody's shell history. It is
read-only, like ``audit_evaluation_frames``: no rows, no artefacts, nothing
rewritten.

**What it does.** Per panel, category by aspect, nine of them: a protein-level
paired bootstrap of the panel's ratio of sums, at an operating point the caller
declares, a bias-corrected and accelerated interval where the acceleration is
computable and a named percentile fallback where it is not, and a minimum
detectable effect read off the bootstrap distribution so that a null can be
told from an unanswerable question. The reading needs one number from the
caller: ``effect_of_interest``, the smallest difference worth detecting. A null
is read against that and never against the difference the run itself observed,
which would be the procedure grading its own resolution.

**The operating point is declared, not assumed.** ``reselected_per_resample``
is the default and is what the estimator argument above is about; ``fixed``
reads the estimator at a threshold the caller declares, which is what a
campaign wants when its primary metric is read at a fixed tau precisely to
remove an undeclared max-over-tau selection. That second reading was for a long
time inexpressible here, because the word was a literal in the emitted output
and the payload had no field for it, so the decision to fix the threshold read
as free when it was not. ``_paired_panels_bootstrap`` argues why both are
correct procedures for different published quantities, the payload below argues
why the vocabulary is closed, and the result records which one ran.

**What it does not do.** It does not report a mean over the nine panels or an
interval for one. The nine panels are nine populations, since an aspect scores
only the proteins that gained in that aspect, so a mean over them is a different
estimator needing a joint resample over the union with per-panel membership
tracked. Asking for one is a refusal naming this paragraph. It does not offer
two independent marginal bootstraps in place of the paired one. It does not
compute an unweighted number and label it ``f_micro_w``. And it does not
estimate anything from the single-threshold per-protein artefact: it refuses and
names the producer it needs, because a refusal that states its precondition is
the correct behaviour and a fixed-threshold interval is an interval for a
different statistic.
"""

from __future__ import annotations

import contextlib
from typing import Annotated, Any, Literal

from pydantic import ConfigDict, Field, field_validator, model_validator
from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, Operation, OperationResult, ProteaPayload
from protea.core.operations._paired_panels_artifact import (
    SETTINGS,
    GridMeta,
    PanelComparabilityError,
    ThresholdGridUnavailableError,
    assert_comparable,
)
from protea.core.operations._paired_panels_panel import (
    PanelConfig,
    Side,
    load_side,
    panel_result,
    resolve_tau_index,
    tally,
)
from protea.core.utils import contract_payload

#: The nine panels, category by aspect, in canonical report order. There is no
#: tenth entry and there is no aggregate over them.
ALL_PANELS: tuple[str, ...] = (
    "NK:MFO",
    "NK:BPO",
    "NK:CCO",
    "LK:MFO",
    "LK:BPO",
    "LK:CCO",
    "PK:MFO",
    "PK:BPO",
    "PK:CCO",
)

#: Markers that must agree for two evaluation results to be comparable.
#: ``prediction_set_id`` and ``scoring_config_id`` are expected to differ: that
#: is what is being compared.
_FRAME_MARKERS: tuple[str, ...] = (
    "evaluation_set_id",
    "frame",
    "temporal_window",
    "leakage_role",
)

_PROVENANCE_SQL = text(
    "SELECT id::text AS id,"
    "       evaluation_set_id::text AS evaluation_set_id,"
    "       prediction_set_id::text AS prediction_set_id,"
    "       scoring_config_id::text AS scoring_config_id,"
    "       reranker_model_id::text AS reranker_model_id,"
    "       frame, temporal_window, leakage_role, results"
    "  FROM evaluation_result WHERE id = :id"
)


class ComparePairedPanelsPayload(ProteaPayload, frozen=True):
    """Which two results are being compared, over which panels, at what precision.

    ``weighting`` is a closed vocabulary and not a bool, and there is no
    ``auto``: asking for the weighted estimator against an artefact that carries
    no weighted components is a refusal, never a silent fallback. That is the
    requirement expressed in the type rather than in a comment somebody has to
    read.

    ``operating_point`` is a closed vocabulary for the same argument. Two
    semantics give two intervals from the same bytes and neither is recoverable
    from the numbers afterwards, so the choice belongs on the job row beside
    the seed. There is no ``auto`` here either, for a sharper version of the
    reason: the only thing an ``auto`` could do is take the argmax, which is
    not a fallback but the other semantics, silently. Hence the two refusals
    below, which are one rule read in both directions: a declared threshold
    exactly when something reads it.

    Unknown keys are refused. ``{"artifact_root": ...}`` is a plausible typo of
    ``artifacts_root``, and without this it validates, resolves to no local
    root, sends the operation to the object store, finds nothing there and
    returns a successful job full of nulls.
    """

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    evaluation_result_id: Annotated[str, Field(description="system A, the arm under test")]
    baseline_evaluation_result_id: Annotated[str, Field(description="system B, the baseline")]
    panels: Annotated[
        list[str],
        Field(default_factory=lambda: list(ALL_PANELS), description="category:aspect keys"),
    ]
    n_resamples: Annotated[int, Field(default=2000, ge=1000)]
    seed: Annotated[int, Field(default=0, ge=0)]
    confidence: Annotated[float, Field(default=0.95, gt=0.5, lt=1.0)]
    power: Annotated[float, Field(default=0.80, gt=0.5, lt=1.0)]
    weighting: Literal["ia_weighted", "unweighted"] = "ia_weighted"
    operating_point: Literal["reselected_per_resample", "fixed"] = "reselected_per_resample"
    declared_tau: Annotated[
        float | None,
        Field(
            default=None,
            gt=0.0,
            lt=1.0,
            description=(
                "the threshold a fixed operating point is read at, required by "
                "operating_point='fixed' and refused otherwise. It must be one the "
                "artefact's grid carries: a tau off the grid is refused and never snapped "
                "to its neighbour, because a comparison reported at a threshold it was not "
                "evaluated at is not recoverable from its own result."
            ),
        ),
    ]
    min_population: Annotated[int, Field(default=30, ge=1)]
    interval_method: Literal["bca", "percentile"] = "bca"
    effect_of_interest: Annotated[
        float | None,
        Field(
            default=None,
            gt=0.0,
            lt=1.0,
            description=(
                "the smallest difference worth detecting, on the estimator's own scale. A "
                "null is read against this and never against the observed difference: "
                "without it a panel whose interval covers zero is reported as null_unread, "
                "because nothing in the data says which effects matter."
            ),
        ),
    ]
    population_rule: Literal["intersect", "require_identical"] = "intersect"
    min_jaccard: Annotated[float, Field(default=0.95, gt=0.0, le=1.0)]
    allow_frame_mismatch: bool = False
    artifacts_root: str | None = None
    baseline_artifacts_root: str | None = None

    @field_validator("evaluation_result_id", "baseline_evaluation_result_id")
    @classmethod
    def _non_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("an evaluation result id is required; there is nothing to compare")
        return value.strip()

    @field_validator("panels")
    @classmethod
    def _known_panels(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("omit panels to take all nine; an empty list compares nothing")
        duplicated = sorted({key for key in value if value.count(key) > 1})
        if duplicated:
            raise ValueError(
                f"panels {duplicated} are listed more than once; each panel would be "
                "resampled twice and reported once, and the progress count would say 1/2 "
                "for one panel"
            )
        unknown = [key for key in value if key not in ALL_PANELS]
        if unknown:
            raise ValueError(
                f"unknown panels {unknown}; the nine are {list(ALL_PANELS)}. There is no "
                "aggregate over them: the nine panels are nine populations, so a mean over "
                "them is a different estimator needing a joint resample over the union with "
                "per-panel membership tracked, which this operation does not compute."
            )
        return value

    @model_validator(mode="after")
    def _coherent(self) -> ComparePairedPanelsPayload:
        if self.evaluation_result_id == self.baseline_evaluation_result_id:
            raise ValueError(
                "a result compared against itself yields a zero delta and a zero-width "
                "interval, which reads as a decisive null"
            )
        if bool(self.artifacts_root) != bool(self.baseline_artifacts_root):
            raise ValueError(
                "give both artifacts_root and baseline_artifacts_root or neither; one local "
                "and one remote is two different resolutions of where the bytes are"
            )
        if self.operating_point == "fixed" and self.declared_tau is None:
            raise ValueError(
                "operating_point='fixed' needs declared_tau: the threshold has to come from "
                "the caller. The only other place it could come from is the argmax of the "
                "panel curve, and taking that would reinstate the max-over-tau selection a "
                "fixed operating point exists to remove, under the name that says it was "
                "removed. Declare the tau the campaign reads its primary metric at."
            )
        if self.operating_point != "fixed" and self.declared_tau is not None:
            raise ValueError(
                f"declared_tau={self.declared_tau} was given with "
                f"operating_point={self.operating_point!r}, which re-selects the threshold "
                "inside every resample and reads nothing at the declared one. Recording a "
                "threshold no arithmetic used tells a later reader that a decision was taken "
                "when it was not. Set operating_point='fixed', or drop declared_tau."
            )
        return self

    @property
    def alpha(self) -> float:
        return 1.0 - self.confidence

    @property
    def variant(self) -> str:
        return "weighted" if self.weighting == "ia_weighted" else "unweighted"

    @property
    def estimator(self) -> str:
        """The metric's own name. ``f_micro_w`` never labels an unweighted number."""
        return "f_micro_w" if self.weighting == "ia_weighted" else "f_micro"


def _provenance(session: Session, result_id: str) -> dict[str, Any]:
    row = session.execute(_PROVENANCE_SQL, {"id": result_id}).mappings().first()
    if row is None:
        raise ValueError(
            f"evaluation result {result_id} is not in evaluation_result; an interval on a "
            "number whose provenance cannot be read is not attributable to anything"
        )
    return dict(row)


def _frame_gate(
    session: Session, p: ComparePairedPanelsPayload, emit: EmitFn
) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    """Refuse before any bytes are read when the two frames are not one frame.

    Two SELECTs and nothing else. They open a read transaction that stays open
    for the rest of the run, which on a large comparison is minutes of idle in
    transaction on the shared database; the operation does not close it because
    it does not own the session's lifecycle, and a rollback here would discard
    whatever the worker has pending on the same session, including the job row.
    Whoever gives this operation a session on a busy database should give it one
    of its own.
    """
    a = _provenance(session, p.evaluation_result_id)
    b = _provenance(session, p.baseline_evaluation_result_id)
    # Presence first, equality second, which is the same rule the artefact gate
    # applies and for the same reason: an absent marker compares equal to another
    # absent marker, so two rows that declare nothing would pass a gate whose
    # whole purpose is to establish that they declare the same thing. Those three
    # columns are nullable with no server default, so a pair of unstamped rows is
    # the common case rather than the exotic one, and the campaign that this
    # operation exists to serve was invalidated by exactly that: numbers compared
    # across frames because nothing recorded which frame each was measured in.
    absent = [
        key
        for key in _FRAME_MARKERS
        if a.get(key) in (None, "") or b.get(key) in (None, "")
    ]
    if absent:
        raise PanelComparabilityError(
            f"the two evaluation results do not both declare {absent}, so they cannot be "
            "shown to share a frame. An unstamped marker is not a matching one. Stamp the "
            "rows with seal_evaluation_frames, or audit what is unstamped with "
            "audit_evaluation_frames, and run this again. This refusal is not waivable by "
            "allow_frame_mismatch, which waives a DISAGREEMENT and not an absence."
        )
    mismatch = [key for key in _FRAME_MARKERS if a.get(key) != b.get(key)]
    if mismatch and not p.allow_frame_mismatch:
        raise PanelComparabilityError(
            f"the two evaluation results disagree on {mismatch}. The same reranker reads "
            "0.3433 under one frame and 0.117 under another and both are correct, so a "
            "difference across frames is not a difference between systems. Set "
            "allow_frame_mismatch to record the mismatch and continue anyway."
        )
    emit(
        "compare_paired_panels.frame",
        "frames agree" if not mismatch else f"frame mismatch permitted on {mismatch}",
        {
            "a": {key: a.get(key) for key in _FRAME_MARKERS},
            "b": {key: b.get(key) for key in _FRAME_MARKERS},
            "mismatch": mismatch,
        },
        "warning" if mismatch else "info",
    )
    return a, b, mismatch


def _grid_block(meta: GridMeta | None) -> dict[str, Any] | None:
    if meta is None:
        return None
    return {
        "th_step": meta.th_step,
        "n_thresholds": meta.n_tau,
        "min": float(meta.tau_grid.min()),
        "max": float(meta.tau_grid.max()),
    }


def _system_block(side: Side) -> dict[str, Any]:
    """Everything needed to trace the number back, not everything to print.

    The scalar frame keys are safe to report once because ``assert_settings_agree``
    has already refused a side whose settings disagree on them. ``producer_git_sha``
    is not in that set (one setting re-run by a later build is a real and
    harmless state) so it is reported per setting rather than by whichever file
    happened to load first.
    """
    meta = side.meta()
    block: dict[str, Any] = {key: side.provenance.get(key) for key in _FRAME_MARKERS}
    block["evaluation_result_id"] = side.result_id
    for key in ("prediction_set_id", "scoring_config_id", "reranker_model_id"):
        block[key] = side.provenance.get(key)
    for key in ("ontology_snapshot_id", "information_accretion_set_id"):
        block[key] = None if meta is None else meta.values.get(key)
    block["producer_git_sha"] = {
        setting: grid.meta.values.get("producer_git_sha") for setting, grid in side.grids.items()
    }
    return block


def _emit_pairing(panel: dict[str, Any], key: str, p: ComparePairedPanelsPayload, emit: EmitFn) -> None:
    emit(
        "compare_paired_panels.pairing",
        None,
        {
            "panel": key,
            **{k: panel.get(k) for k in ("n_paired", "n_only_a", "n_only_b", "population_rule")},
        },
        "info",
    )
    if panel["status"] == "refused":
        emit(
            "compare_paired_panels.panel_refused",
            f"{key}: {panel['message']}",
            {"panel": key, "reason": panel["reason"], **{
                k: panel.get(k) for k in ("n_paired", "n_only_a", "n_only_b", "jaccard")
            }},
            "error",
        )
        return
    if panel["status"] == "empty":
        emit(
            "compare_paired_panels.panel_absent",
            f"{key}: no artefact for this panel on one or both sides, so it was not "
            "computed; it is reported as absent and never as a null",
            {"panel": key, "reason": panel["reason"]},
            "warning",
        )
        return
    if not panel["reportable"]:
        emit(
            "compare_paired_panels.withheld",
            f"{key} holds {panel['n_paired']} paired proteins, below the floor of "
            f"{p.min_population}; computed and flagged, never dropped",
            {"panel": key, "n_paired": panel["n_paired"], "min_population": p.min_population},
            "warning",
        )
    if panel.get("arm_silent"):
        emit(
            "compare_paired_panels.arm_silent",
            f"{key}: arm {panel['arm_silent']} predicts nothing on this panel and scores "
            "zero; the difference against it is a real difference, so it is named rather "
            "than dropped",
            {"panel": key, "arm": panel["arm_silent"]},
            "info",
        )
    if panel.get("interval_fallback_reason"):
        emit(
            "compare_paired_panels.interval_fallback",
            None,
            {"panel": key, "reason": panel["interval_fallback_reason"]},
            "warning",
        )


def _emit_panel(
    panel: dict[str, Any],
    key: str,
    p: ComparePairedPanelsPayload,
    cfg: PanelConfig,
    emit: EmitFn,
) -> None:
    """What the panel measured, at the level it belongs.

    What a reader should CONCLUDE from it is emitted separately, because the
    two answer different questions and are filtered at different levels: this
    one is always info, and a verdict about power is a warning.

    ``operating_point`` sits beside ``tau_a`` and ``tau_b`` because the three
    are one fact: two taus that may differ are each an argmax, two equal ones
    are the declared threshold. It comes off the config, the object the
    resampler was handed, so this event reports what ran and not what was asked.
    """
    _emit_pairing(panel, key, p, emit)
    reported = (
        "delta",
        "ci_low",
        "ci_high",
        "interval_method",
        "minimum_detectable_effect",
        "resolves",
        "status",
        "verdict",
    )
    emit(
        "compare_paired_panels.panel",
        None,
        {
            "panel": key,
            **{k: panel.get(k) for k in reported},
            "operating_point": cfg.operating_point,
            "tau_a": (panel["a"] or {}).get("tau"),
            "tau_b": (panel["b"] or {}).get("tau"),
            "tau_a_switched_fraction": panel["diagnostics"].get("tau_a_switched_fraction"),
        },
        "info",
    )
    _emit_verdict(panel, key, emit)


def _emit_verdict(panel: dict[str, Any], key: str, emit: EmitFn) -> None:
    """Which of the three readings of an interval covering zero this one is.

    The distinction is the operation's reason to exist. An interval covering
    zero can mean the two systems are the same down to a size worth caring
    about, or that the comparison could never have resolved anything, or that
    nobody declared what size was worth caring about so the question is
    unanswered. Those are different conclusions and only the first is evidence.

    The first is info and the other two are warnings, so a reader filtering the
    job event log on level sees the ones that need an action.
    """
    mde, effect = panel["minimum_detectable_effect"], panel.get("effect_of_interest")
    if panel["status"] == "underpowered" and mde is not None:
        emit(
            "compare_paired_panels.underpowered",
            f"{key} could not have resolved an effect smaller than {mde:.4f} on "
            f"{panel['n_paired']} proteins, and the effect of interest is {effect}; the "
            "interval covering zero here is about power, not about the two systems being "
            "the same",
            {
                "panel": key,
                "minimum_detectable_effect": mde,
                "effect_of_interest": effect,
                "n_paired": panel["n_paired"],
            },
            "warning",
        )
    elif panel["status"] == "null_unread" and mde is not None:
        emit(
            "compare_paired_panels.null_unread",
            f"{key} covers zero and could have resolved {mde:.4f}, but no effect_of_interest "
            "was declared, so nothing here says whether that is evidence of sameness or "
            "evidence of nothing. Declare the smallest difference worth detecting and re-run; "
            "this operation will not pick the campaign's standard for it",
            {"panel": key, "minimum_detectable_effect": mde},
            "warning",
        )
    elif panel["status"] == "null_with_power":
        emit(
            "compare_paired_panels.null_with_power",
            f"{key} covers zero and could have resolved {mde:.4f}, which is at or below the "
            f"declared effect of interest {effect}; this null is evidence of sameness down "
            "to that size, and is not an unanswered question",
            {"panel": key, "minimum_detectable_effect": mde, "effect_of_interest": effect},
            "info",
        )


def _refuse_if_nothing_was_comparable(
    sides: tuple[Side, Side], panels: dict[str, dict[str, Any]]
) -> None:
    """A run in which every requested panel was absent is not a result.

    Each side loaded something, otherwise ``load_side`` would already have
    refused, and yet no requested panel exists on both: the two results were
    written for different settings, or the aspect asked for is in neither file.
    Returning nine nulls from that is the quiet-success shape, indistinguishable
    in the job list from a comparison that ran.
    """
    if not panels or any(panel["status"] != "empty" for panel in panels.values()):
        return
    raise ThresholdGridUnavailableError(
        f"none of the requested panels {sorted(panels)} exists on both sides. "
        f"{sides[0].result_id} carries settings {sorted(sides[0].grids)} and "
        f"{sides[1].result_id} carries {sorted(sides[1].grids)}; nothing was compared, so "
        "every panel would report a null and the job would succeed. Ask for panels both "
        "results were evaluated on."
    )


class ComparePairedPanelsOperation(Operation):
    """Paired interval on the campaign's own estimator, one per panel."""

    name = "compare_paired_panels"
    description = (
        "Read-only paired bootstrap of f_micro_w between two evaluation results, one "
        "interval per category-by-aspect panel, at a declared operating point (the "
        "threshold re-selected inside every resample by default, or held at a declared "
        "tau) and a minimum detectable effect read against the declared effect of "
        "interest so a null can be read. Writes nothing."
    )
    payload_model = ComparePairedPanelsPayload

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        a = str(payload.get("evaluation_result_id", "?"))[:8]
        b = str(payload.get("baseline_evaluation_result_id", "?"))[:8]
        panels = payload.get("panels") or list(ALL_PANELS)
        return (
            f"{a} against {b} on {len(panels)} panels, "
            f"{payload.get('n_resamples', 2000)} resamples, seed {payload.get('seed', 0)}"
        )

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = ComparePairedPanelsPayload.model_validate(contract_payload(payload))
        self._emit_start(p, emit)
        prov_a, prov_b, mismatch = _frame_gate(session, p, emit)
        wanted = {key.split(":")[0] for key in p.panels}
        settings = tuple(s for s in SETTINGS if s in wanted)
        with contextlib.ExitStack() as stack:
            sides = (
                load_side(
                    p.evaluation_result_id, p.artifacts_root, prov_a, stack, (settings, p.variant)
                ),
                load_side(
                    p.baseline_evaluation_result_id,
                    p.baseline_artifacts_root,
                    prov_b,
                    stack,
                    (settings, p.variant),
                ),
            )
            artifact_mismatch = self._grid_events(sides, p, emit)
            # After the comparability gate, deliberately: only past it is one
            # grid known to be declared by both sides, so one column index can
            # serve both arms. Resolved before it, a grid disagreement would
            # surface as an off-grid tau, the right refusal under a wrong name.
            cfg = self._config(sides, p)
            panels = self._panels(sides, p, cfg, emit)
            _refuse_if_nothing_was_comparable(sides, panels)
            result = self._result(sides, p, cfg, panels, mismatch, artifact_mismatch)
        emit("compare_paired_panels.verdict", None, result["verdict"], "info")
        return OperationResult(
            result=result, progress_current=len(panels), progress_total=len(p.panels)
        )

    @staticmethod
    def _emit_start(p: ComparePairedPanelsPayload, emit: EmitFn) -> None:
        """What was asked for, before a single byte is read.

        The payload's operating point, not the config's: this fires before the
        artefacts exist to resolve a tau against, so it records the request and
        the panel events record what ran. It used to record the literal
        ``reselected_per_resample``, true only because nothing else was possible.
        """
        emit(
            "compare_paired_panels.start",
            f"{p.estimator} on {len(p.panels)} panels, {p.n_resamples} resamples",
            {
                "evaluation_result_id": p.evaluation_result_id,
                "baseline_evaluation_result_id": p.baseline_evaluation_result_id,
                "estimator": p.estimator,
                "weighting": p.weighting,
                "operating_point": p.operating_point,
                "declared_tau": p.declared_tau,
                "n_resamples": p.n_resamples,
                "seed": p.seed,
                "confidence": p.confidence,
                "power": p.power,
                "panels": list(p.panels),
            },
            "info",
        )

    @staticmethod
    def _grid_events(
        sides: tuple[Side, Side], p: ComparePairedPanelsPayload, emit: EmitFn
    ) -> dict[str, list[str]]:
        """Both files must declare one grid and one comparability set.

        Narrated only because it is gated: every check named here raises on
        failure, so the event records what passed rather than a claim nobody
        acted on. What a permitted mismatch returns is written into the result
        and emitted at warning level: a check whose answer is discarded is a
        check nobody performed, and "the two artefacts disagree on something we
        allowed" is exactly the fact a reader of the result needs.
        """
        shared = sorted(set(sides[0].grids) & set(sides[1].grids))
        artifact_mismatch: dict[str, list[str]] = {}
        for setting in shared:
            waived = assert_comparable(
                sides[0].grids[setting].meta,
                sides[1].grids[setting].meta,
                allow_mismatch=p.allow_frame_mismatch,
            )
            if waived:
                artifact_mismatch[setting] = waived
                emit(
                    "compare_paired_panels.artifact_mismatch",
                    f"setting {setting}: the two artefacts disagree on {waived}, permitted by "
                    "allow_frame_mismatch and recorded in the result",
                    {"setting": setting, "keys": waived},
                    "warning",
                )
        meta = sides[0].meta()
        emit(
            "compare_paired_panels.grid",
            None,
            {
                "settings": shared,
                "th_step": None if meta is None else meta.th_step,
                "n_thresholds": None if meta is None else meta.n_tau,
                "variants": [] if meta is None else list(meta.variants),
                "variant_read": p.variant,
            },
            "info",
        )
        emit(
            "compare_paired_panels.invariants",
            None,
            {
                "rows": {s: sum(a.n for a in sides[0].grids[s].panels.values()) for s in shared},
                "checked": [
                    "grid width equals the declared thresholds, per row",
                    "the explicit grid agrees with th_step",
                    "mass columns are non-increasing in the threshold",
                    "true positives never exceed predicted or ground-truth mass",
                    "every row carries ground truth, so the rows are the scored population",
                    "the pooled curve varies along the grid",
                    "(protein_accession, namespace) is unique",
                ],
            },
            "info",
        )
        return artifact_mismatch

    @staticmethod
    def _config(sides: tuple[Side, Side], p: ComparePairedPanelsPayload) -> PanelConfig:
        """The payload's request, resolved against the grid the artefacts declare.

        Every emission below reads the operating point off this object and not
        off the payload: what the resampler was handed is what a reader of a
        stored comparison needs, and what was asked for is not.
        """
        return PanelConfig(
            alpha=p.alpha,
            power=p.power,
            n_resamples=p.n_resamples,
            seed=p.seed,
            min_population=p.min_population,
            force_percentile=p.interval_method == "percentile",
            effect_of_interest=p.effect_of_interest,
            tau_index=resolve_tau_index(sides, p.operating_point, p.declared_tau),
        )

    @staticmethod
    def _panels(
        sides: tuple[Side, Side],
        p: ComparePairedPanelsPayload,
        cfg: PanelConfig,
        emit: EmitFn,
    ) -> dict[str, dict[str, Any]]:
        rule = (p.population_rule, p.min_jaccard)
        out: dict[str, dict[str, Any]] = {}
        for key in p.panels:
            panel = panel_result(sides, key, cfg, ALL_PANELS.index(key), rule)
            _emit_panel(panel, key, p, cfg, emit)
            out[key] = panel
        return out

    @staticmethod
    def _result(
        sides: tuple[Side, Side],
        p: ComparePairedPanelsPayload,
        cfg: PanelConfig,
        panels: dict[str, dict[str, Any]],
        mismatch: list[str],
        artifact_mismatch: dict[str, list[str]],
    ) -> dict[str, Any]:
        """Everything a later reader needs to know what these numbers are.

        ``operating_point`` is read off the config for the same reason
        ``estimator`` is derived rather than typed: it has to be the semantics
        the arithmetic used, not the ones the payload hoped for.
        """
        return {
            "estimator": p.estimator,
            "weighting": p.weighting,
            "operating_point": cfg.operating_point,
            "declared_tau": p.declared_tau,
            "sampling_unit": "protein",
            "n_resamples": p.n_resamples,
            "seed": p.seed,
            "confidence": p.confidence,
            "power": p.power,
            "min_population": p.min_population,
            "population_rule": p.population_rule,
            "interval_method_requested": p.interval_method,
            "effect_of_interest": p.effect_of_interest,
            "tau_grid": _grid_block(sides[0].meta()),
            "systems": {"a": _system_block(sides[0]), "b": _system_block(sides[1])},
            "frame_mismatch": mismatch,
            "artifact_mismatch": artifact_mismatch,
            "panels": panels,
            # Three different facts, three lists. "Withheld" once held all of
            # them, so nine missing files read as nine underpowered cells.
            "withheld": [
                k
                for k, v in panels.items()
                if not v["reportable"] and v["status"] not in ("empty", "refused")
            ],
            "absent": [k for k, v in panels.items() if v["status"] == "empty"],
            "refused": [k for k, v in panels.items() if v["status"] == "refused"],
            "verdict": tally(panels),
        }
