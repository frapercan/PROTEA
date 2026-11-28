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

**The comparison declares its axis.** Two evaluation results sit behind two
prediction sets, and a delta between them is a delta in whatever those two
methods differ in. ``method_axis`` names the field the comparison claims to be
varying, and ``_paired_panels_method`` refuses unless every other field of the
method agrees, naming the ones that do not with both their values. The empty
default is the claim that the method was held still. This is the upstream half
of what ``_frame_gate`` does downstream, and it is the half that was missing
when two arms believed to differ in the donor bank alone also differed in
ancestor expansion, aspect-separated retrieval, the code revision and the
ontology snapshot, and nine deltas were published off them.

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
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, Operation, OperationResult
from protea.core.operations._paired_panels_artifact import (
    SETTINGS,
    GridMeta,
    PanelComparabilityError,
    ThresholdGridUnavailableError,
    assert_comparable,
)
from protea.core.operations._paired_panels_events import _emit_panel
from protea.core.operations._paired_panels_method import _method_gate
from protea.core.operations._paired_panels_panel import (
    PanelConfig,
    Side,
    finish_panel,
    load_side,
    panel_result,
    prepare_panel,
    resolve_tau_index,
    tally,
)
from protea.core.operations._paired_panels_payload import (
    ALL_PANELS,
    ComparePairedPanelsPayload,
)
from protea.core.operations._paired_panels_simultaneous import (
    declared_tau_indices,
    simultaneous_block,
    simultaneous_resamples,
)
from protea.core.operations._paired_panels_stratum import (
    Restriction,
    stratum_label,
    stratum_population,
)
from protea.core.utils import contract_payload

__all__ = ["ALL_PANELS", "ComparePairedPanelsOperation", "ComparePairedPanelsPayload"]

#: Which closed vocabulary each Stratum axis draws from. Written here rather
#: than inferred, so a seventh axis added to Stratum fails this operation's
#: tests instead of silently accepting any string for itself.
#: Markers that must agree for two evaluation results to be comparable.
#: ``prediction_set_id`` and ``scoring_config_id`` are expected to differ: that
#: is what is being compared.
#:
#: THE SEAL, NOT THE LABEL. This gate used to compare ``frame`` and
#: ``temporal_window``, which are strings a harness writes. ``frame_digest`` is
#: computed from the material a frame is actually made of -- the evaluation set,
#: the pivot snapshot, the accretion set, the window and the two evaluation caps
#: -- so it is the only column that can say two results were measured against
#: the same thing.
#:
#: Reading the label had both failure modes at once, and both were live on this
#: campaign. It PASSED results that are not comparable: four distinct digests
#: shared two labels, so two rows both stamped ``internal`` / ``SELECT_220_227``
#: could sit in different frames and compare anyway. And it REFUSED results that
#: are: 110 of the campaign's 153 evaluations carried a digest and no label, so
#: every substrate and depth comparison was rejected while the three arms that
#: happened to be labelled went through. Which axes could be declared with this
#: bootstrap was decided by that, and nobody chose it.
#:
#: ``leakage_role`` stays a marker of its own because it is not part of the
#: digest; ``evaluation_set_id`` is kept although the digest covers it, so a
#: mismatch names the set rather than only two opaque hashes.
_FRAME_MARKERS: tuple[str, ...] = (
    "evaluation_set_id",
    "frame_digest",
    "leakage_role",
)

_PROVENANCE_SQL = text(
    "SELECT id::text AS id,"
    "       evaluation_set_id::text AS evaluation_set_id,"
    "       prediction_set_id::text AS prediction_set_id,"
    "       scoring_config_id::text AS scoring_config_id,"
    "       reranker_model_id::text AS reranker_model_id,"
    "       frame, temporal_window, frame_digest, leakage_role, results"
    "  FROM evaluation_result WHERE id = :id"
)


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


def _threshold_loader(session: Session) -> Any:
    """Read a declared threshold's weights from the store, verified, once per id."""
    from protea.core.operating_point import load_declared_threshold
    from protea.core.operations._operating_point_reads import operating_point_store

    def load(weights_id: str) -> Any:
        return load_declared_threshold(session, weights_id, operating_point_store())

    return load


def _rung_of(payload: dict[str, Any]) -> str:
    """The declared rung and role, as the summary's prefix; nothing when none is declared."""
    declared = payload.get("ladder_declaration")
    if not isinstance(declared, dict) or not declared.get("rung"):
        return ""
    label = f" {declared['label']}" if declared.get("label") else ""
    return f"rung {declared['rung']}, {declared.get('role')}{label}: "


def _declared_thresholds(p: ComparePairedPanelsPayload) -> dict[str, Any]:
    """The declared threshold ids, recorded only when the comparison was read at them."""
    if p.declared_threshold_id is None:
        return {}
    return {
        "declared_threshold_id": p.declared_threshold_id,
        "baseline_declared_threshold_id": p.baseline_declared_threshold_id,
    }


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
        # The axis is in the one-line summary because that is the line a reader
        # scanning a job list sees. "31dd3eb8 against 0e076cb3" says which rows
        # were compared and not what the comparison was OF, which is the sentence
        # the nine invalid deltas were filed under.
        #
        # PRESENCE FIRST, which is the rule ``_frame_gate`` applies to its
        # markers and ``method_seal.MUST_BE_RECORDED`` to its revisions. This
        # line is rendered over the STORED payload of every job the list holds,
        # including every job that ran before this field existed: job 96e1942d,
        # the one the nine invalid deltas were published from, carries no
        # ``method_axis`` key at all. Reading that absence as the empty axis
        # would print "one method held still" across exactly the comparison in
        # which four method fields moved, which is this operation asserting the
        # false claim it exists to refuse, about itself, on the surface a reader
        # scans. An absent declaration is not a declaration that nothing varied.
        axis = payload.get("method_axis")
        if axis is None:
            varying = "no method axis declared"
        elif not isinstance(axis, list):
            # ``POST /jobs`` stores the payload blob unvalidated and the model
            # only sees it on dequeue, so a bare string is reachable here, and
            # joining one renders it one character at a time as though
            # seventeen fields had varied.
            varying = f"method axis {axis!r} is not a list of fields"
        elif axis:
            varying = "varying " + ", ".join(str(field) for field in axis)
        else:
            varying = "one method held still"
        return (
            f"{_rung_of(payload)}{a} against {b}, {varying}, on {len(panels)} panels, "
            f"{payload.get('n_resamples', 2000)} resamples, seed {payload.get('seed', 0)}"
        )

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = ComparePairedPanelsPayload.model_validate(contract_payload(payload))
        self._emit_start(p, emit)
        prov_a, prov_b, mismatch = _frame_gate(session, p, emit)
        # After the frame gate and before a single byte of artefact is read.
        # The two gates ask different questions -- were these numbers MEASURED
        # the same way, and were they PRODUCED by methods that differ in one
        # named way -- and the second is the one the bank comparison needed.
        axis = _method_gate(session, p, (prov_a, prov_b), emit)
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
            cfg = self._config(sides, p, declared_tau_indices(p, sides, _threshold_loader(session)))
            keep = (
                stratum_population(session, p.restrict_to_stratum, prov_b, emit)
                if p.restrict_to_stratum
                else None
            )
            panels, family = self._panels(sides, p, cfg, emit, keep=keep)
            _refuse_if_nothing_was_comparable(sides, panels)
            result = self._result(sides, p, cfg, panels, mismatch, artifact_mismatch)
            # Recorded beside the number, not only emitted. A delta whose stored
            # result does not say which field it is a delta IN is the shape the
            # nine invalid deltas had, and an event is not in the result.
            result["method_axis"] = list(axis)
            result.update(simultaneous_block(family))
            result.update(_declared_thresholds(p))
            if p.ladder_declaration is not None:
                result["ladder_declaration"] = p.ladder_declaration.record()
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
                **_declared_thresholds(p),
                **(
                    {"interval_family": p.interval_family}
                    if p.interval_family != "per_panel"
                    else {}
                ),
                **(
                    {"ladder_declaration": p.ladder_declaration.record()}
                    if p.ladder_declaration is not None
                    else {}
                ),
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
    def _config(
        sides: tuple[Side, Side],
        p: ComparePairedPanelsPayload,
        declared: dict[str, tuple[int, int]] | None = None,
    ) -> PanelConfig:
        """The payload's request, resolved against the grid the artefacts declare.

        Every emission below reads the operating point off this object and not
        off the payload: what the resampler was handed is what a reader of a
        stored comparison needs, and what was asked for is not. ``declared``
        holds each panel's two declared columns when each arm is read at its own
        declared threshold.
        """
        one_tau = p.operating_point != "fixed" or p.declared_tau is not None
        return PanelConfig(
            alpha=p.alpha,
            power=p.power,
            n_resamples=p.n_resamples,
            seed=p.seed,
            min_population=p.min_population,
            force_percentile=p.interval_method == "percentile",
            effect_of_interest=p.effect_of_interest,
            tau_index=(
                resolve_tau_index(sides, p.operating_point, p.declared_tau) if one_tau else None
            ),
            declared=declared,
            family=p.interval_family,
        )

    @staticmethod
    def _panels(
        sides: tuple[Side, Side],
        p: ComparePairedPanelsPayload,
        cfg: PanelConfig,
        emit: EmitFn,
        keep: frozenset[str] | None = None,
    ) -> tuple[dict[str, dict[str, Any]], dict[str, Any] | None]:
        """Every panel's answer, and the family's record when they share one stream.

        The reported name carries the restriction, so a stratum's delta and the
        whole panel's delta cannot be filed under one key and later read as two
        measurements of the same thing.
        """
        rule = (p.population_rule, p.min_jaccard)
        reported = {key: stratum_label(key, p.restrict_to_stratum) for key in p.panels}

        def narrow(key: str) -> Restriction | None:
            return Restriction(keep, reported[key]) if keep is not None else None

        out: dict[str, dict[str, Any]] = {}
        if cfg.family != "simultaneous":
            for key in p.panels:
                panel = panel_result(sides, key, cfg, ALL_PANELS.index(key), rule, narrow(key))
                _emit_panel(panel, reported[key], p, cfg, emit)
                out[reported[key]] = panel
            return out, None
        steps = {key: prepare_panel(sides, key, cfg, rule, narrow(key)) for key in p.panels}
        prepared = {k: v for k, v in steps.items() if not isinstance(v, dict)}
        resampled, family = simultaneous_resamples(prepared, cfg)
        for key, step in steps.items():
            panel = step if isinstance(step, dict) else finish_panel(step, cfg, resampled[key])
            _emit_panel(panel, reported[key], p, cfg, emit)
            out[reported[key]] = panel
        return out, family

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
            "restrict_to_stratum": p.restrict_to_stratum,
            # The window axes come from the window's placement, the retrieval axes
            # from the baseline's neighbours (_paired_panels_stratum).
            "stratum_membership_from": (
                {"window_axes": "window_placement", "retrieval_axes": "baseline"}
                if p.restrict_to_stratum
                else None
            ),
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
