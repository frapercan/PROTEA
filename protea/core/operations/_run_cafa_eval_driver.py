"""Per-setting cafaeval driver extracted from
``RunCafaEvaluationOperation.execute``.

Wraps the per-setting (NK / LK / PK) loop that:
  1. reads the one prediction file every setting shares, the method's
     stored scores (``_method_score_predictions``),
  2. scores it with :func:`protea.core.operating_point.grid.evaluate_setting`,
     the call every path to the per-protein grid shares (a fit's out-of-fold
     scores reach the same grid through ``evaluate_to_grid``),
  3. writes the full cafaeval artifact tree per setting,
  4. emits the ``setting_done`` / ``setting_failed`` audit events.

The helper takes the operation's ``emit`` callback as a parameter
to keep the audit trail unchanged.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from protea.core.contracts.operation import EmitFn

# The frame literals and the cafaeval call live in the grid module, which a fit
# evaluates through too; they are re-exported here, where they were first named.
from protea.core.operating_point.grid import (  # noqa: F401
    CAFAEVAL_NO_ORPHANS,
    CAFAEVAL_NORM,
    CAFAEVAL_PROP,
    SETTINGS,
    EvaluatorInputs,
    evaluate_setting,
    invoke_cafaeval,
)
from protea.core.operating_point.grid import (
    default_signals_where_possible as _default_signals_where_possible,  # noqa: F401
)
from protea.core.operating_point.grid import (
    new_per_protein_sink as _new_per_protein_sink,  # noqa: F401
)
from protea.core.operations._run_cafa_scale import annotate_maxima


class SettingEvaluationError(RuntimeError):
    """Raised when one or more NK/LK/PK settings produced no metrics.

    Before this existed, a setting that raised was recorded as a warning and
    contributed ``{}`` to the result, so a run where every setting failed
    still reported success and persisted an ``EvaluationResult`` whose three
    cells were empty. An evaluation with no metrics in it is not a result, and
    a job that says it succeeded is the one place the absence cannot be seen.
    """


@dataclass(frozen=True)
class CafaEvalRunContext:
    """Bundle of per-run inputs consumed by :func:`evaluate_all_settings`.

    Groups the per-call inputs (artifact paths, the ground truth, the frame
    markers) so the entry-point signature stays under flake8-bugbear's
    parameter ceiling. The predictions are the method's stored scores, one
    file every setting reads: nothing here scores, reranks, cuts or filters.
    """

    artifacts_root: Path
    obo_path: str
    nk_path: str
    lk_path: str
    pk_path: str
    pk_known_path: str
    ia_path: str | None
    toi_path: str
    shared_pred_dir: str
    # Identity of the frame the numbers live in. None of the three is a
    # property of the cafaeval call, so none of them can be derived at the
    # write point; they are threaded down from the operation that resolved
    # them. Default None so an existing caller still builds a context, and the
    # grid artefact then refuses to be written rather than stamping a
    # substitute: the consumer's comparability gate is on presence first, so an
    # unstamped file compares equal to another unstamped file and two runs
    # under different frames would be published as a method difference.
    ontology_snapshot_id: str | None = None
    evaluation_set_id: str | None = None
    #: Which information-accretion table, as a UUID when the run named a set,
    #: a content hash when it passed a bare file or the snapshot's URL, and the
    #: literal "null" only when there was no IA table at all, in which case
    #: there is no weighted variant either.
    information_accretion_frame: str | None = None
    th_step: float = 0.01
    #: How the prediction file is read before thresholding (ADR-D55), applied
    #: once, to the shared file, at staging.
    score_reading: str = "raw"
    max_terms: int | None = None

    def evaluator_inputs(self) -> EvaluatorInputs:
        """The staged files as the grid module takes them: every setting's truth and the known terms."""
        return EvaluatorInputs(
            obo_path=self.obo_path,
            ground_truth={"NK": self.nk_path, "LK": self.lk_path, "PK": self.pk_path},
            pk_known_path=self.pk_known_path,
            toi_path=self.toi_path,
            ia_path=self.ia_path,
            th_step=self.th_step,
            max_terms=self.max_terms,
        )


def _invoke_cafaeval_signal_safe(
    *,
    ctx: CafaEvalRunContext,
    pred_dir: str,
    gt_file: str,
    known_file: str | None,
) -> tuple[Any, Any, Any]:
    """One cafaeval call on ``ctx``'s frame: :func:`~protea.core.operating_point.grid.invoke_cafaeval`."""
    return invoke_cafaeval(
        ctx.evaluator_inputs(), pred_dir=pred_dir, gt_file=gt_file, known_file=known_file
    )


def _persist_setting_artifacts(
    ctx: CafaEvalRunContext,
    setting: str,
    df: Any,
    dfs_best: Any,
) -> None:
    """Write PR curves + dfs_best artefacts to ``ctx.artifacts_root/<setting>/``."""
    if df is None:
        return
    from cafaeval.evaluation import write_results as _write_results

    setting_dir = ctx.artifacts_root / setting
    setting_dir.mkdir(exist_ok=True)
    _write_results(df, dfs_best, str(setting_dir))


def _persist_per_protein(
    ctx: CafaEvalRunContext,
    setting: str,
    sink: Any,
    result: dict[str, Any],
    emit: EmitFn,
) -> None:
    """Write the per-protein scores beside the setting's other artefacts.

    Never fatal. A run whose aggregate is sound should not be discarded because
    the extra table could not be written, and the failure is emitted rather than
    swallowed so an absent file is distinguishable from one nobody asked for.
    """
    # ``result`` is keyed by the short CAFA code (BPO/MFO/CCO); the sink reports
    # cafaeval's long namespace ("biological_process"). Invert the project's own
    # mapping rather than writing a second one, so the two cannot drift.
    from protea.core.operations._run_cafa_helpers import _NS_LABELS
    from protea.core.operations._run_cafa_per_protein import rows_from_sink

    long_for_short = {short: long for long, short in _NS_LABELS.items()}
    tau_by_ns = {
        long_for_short[ns]: float(v["tau"])
        for ns, v in (result or {}).items()
        if ns in long_for_short and isinstance(v, dict) and v.get("tau") is not None
    }
    try:
        rows = rows_from_sink(sink, th_step=ctx.th_step, tau_by_ns=tau_by_ns)
        if not rows:
            emit(
                "run_cafa_evaluation.per_protein_empty",
                None,
                {"setting": setting, "namespaces": sorted(tau_by_ns)},
                "warning",
            )
            return
        import pandas as pd

        setting_dir = ctx.artifacts_root / setting
        setting_dir.mkdir(parents=True, exist_ok=True)
        out = setting_dir / "per_protein.parquet"
        pd.DataFrame(rows).to_parquet(out, index=False)
        emit(
            "run_cafa_evaluation.per_protein_written",
            None,
            {"setting": setting, "rows": len(rows), "path": str(out)},
            "info",
        )
    except Exception as exc:
        emit(
            "run_cafa_evaluation.per_protein_failed",
            None,
            {"setting": setting, "error": str(exc)},
            "warning",
        )


def _emit_grid_drops(setting: str, dropped: list[dict[str, str]], emit: EmitFn) -> None:
    """Name every namespace left out of the grid file, one event each.

    The consumer reads an absent namespace as a null panel on a successful job,
    so a drop nobody reported is a result with nine nulls and no complaint.

    At error level, not warning. A drop means a panel this evaluation was asked
    for cannot be compared with an interval, and the operator's next sight of it
    is the consumer refusing and naming this producer. ``code`` travels beside
    the prose so the landing calibration can count refusals over a re-run
    without grepping messages.
    """
    for entry in dropped:
        emit(
            "run_cafa_evaluation.per_protein_grid_namespace_dropped",
            None,
            {"setting": setting, **entry},
            "error",
        )


def _emit_grid_written(setting: str, artifact: Any, out: Any, emit: EmitFn) -> None:
    """What was written, so an absent panel downstream can be told from a refused one."""
    emit(
        "run_cafa_evaluation.per_protein_grid_written",
        None,
        {
            "setting": setting,
            "rows": len(artifact.rows),
            "variants": list(artifact.variants),
            "n_tau": artifact.n_tau,
            "path": str(out),
        },
        "info",
    )


def _grid_frame(ctx: CafaEvalRunContext, setting: str) -> Any:
    """The footer's frame, built from the run's own values and nothing else.

    The three cafaeval literals come from the constants the call site uses, so
    the stamp cannot claim a normalisation the run did not use. The three
    identities come off the context, where they may be None; ``GridFrame.stamp``
    is what refuses, so the refusal names every missing key at once instead of
    the first one.
    """
    from protea.core.operations._run_cafa_per_protein import GridFrame
    from protea.core.parquet_export import resolve_protea_git_sha

    return GridFrame(
        setting=setting,
        th_step=ctx.th_step,
        max_terms=ctx.max_terms,
        normalization=CAFAEVAL_NORM,
        prop=CAFAEVAL_PROP,
        no_orphans=CAFAEVAL_NO_ORPHANS,
        ontology_snapshot_id=ctx.ontology_snapshot_id,
        evaluation_set_id=ctx.evaluation_set_id,
        information_accretion_frame=ctx.information_accretion_frame,
        producer_git_sha=resolve_protea_git_sha(),
    )


def _record_cell_populations(result: dict[str, Any], grid: Any) -> None:
    """Put each cell's population in each variant beside its metrics, in place.

    ``n_proteins`` is cafaeval's ``n``, the proteins with a prediction at the
    reported threshold, so it moves with the threshold. These do not: they are
    the cell's size, the denominator of its coverage (``cell_populations``).
    Recorded only for the namespaces the result reports, from the grid that was
    written, so a cell with no grid carries no population rather than a guessed one.
    """
    from protea.core.operations._run_cafa_grid_artifact import cell_populations
    from protea.core.operations._run_cafa_helpers import _NS_LABELS

    for namespace, counts in cell_populations(grid).items():
        label = _NS_LABELS.get(namespace)
        if label is not None and label in result:
            result[label].update(counts)


def _persist_per_protein_grid(
    ctx: CafaEvalRunContext,
    setting: str,
    sink: Any,
    emit: EmitFn,
) -> Any:
    """Write the whole-threshold-curve artefact BESIDE the single-tau one.

    A new file, never a widened old one. ``stratify_evaluation`` reads
    ``per_protein.parquet`` and would break under a changed schema, and an old
    file and a new file sharing one name is the detection problem made
    permanently unsolvable. Both are written; the legacy one keeps its exact
    content, and ``_upload_artifacts`` walks the directory, so the second file
    reaches the store under the key the consumer probes with no change to the
    upload path.

    Never fatal, for the same reason its sibling is not: a run whose aggregate
    is sound should not be discarded because an extra table could not be
    written. Every refusal is emitted at ERROR, because an absent artefact
    reaches the consumer as a refusal whose own remedy text is "re-run
    run_cafa_evaluation", which the operator has already done: re-running
    reproduces the same silence unless the reason was loud the first time.
    """
    from protea.core.operations._run_cafa_per_protein import (
        GRID_FILENAME,
        grid_rows_from_sink,
        write_grid_parquet,
    )

    try:
        artifact = grid_rows_from_sink(sink)
        _emit_grid_drops(setting, artifact.dropped, emit)
        setting_dir = ctx.artifacts_root / setting
        setting_dir.mkdir(parents=True, exist_ok=True)
        out = write_grid_parquet(
            setting_dir / GRID_FILENAME,
            artifact,
            _grid_frame(ctx, setting).stamp(artifact.variants),
        )
        _emit_grid_written(setting, artifact, out, emit)
        return artifact
    except Exception as exc:
        emit(
            "run_cafa_evaluation.per_protein_grid_failed",
            None,
            {
                "setting": setting,
                "error": str(exc),
                "code": getattr(exc, "code", exc.__class__.__name__),
            },
            "error",
        )
        return None


def _run_cafaeval_for_setting(
    *,
    setting: str,
    pred_dir: str,
    ctx: CafaEvalRunContext,
    emit: EmitFn,
) -> dict[str, Any]:
    """Run cafaeval for one setting under signal-safe handlers.

    The setting's truth and exclusion files come from ``ctx``. Returns the
    parsed per-namespace metrics dict, or ``{}`` when the setting failed. The
    caller is responsible for refusing an empty one: see
    :func:`evaluate_all_settings`. The event is still emitted here so the
    audit trail names every setting that failed, not only the first.
    """
    emit("run_cafa_evaluation.evaluating", None, {"setting": setting}, "info")
    try:
        evaluation = evaluate_setting(ctx.evaluator_inputs(), setting, pred_dir)
        df, dfs_best, sink = evaluation.curve, evaluation.best, evaluation.sink
        result = evaluation.metrics
        censored = annotate_maxima(df, result)
        if censored:
            emit(
                "run_cafa_evaluation.censored_maximum",
                None,
                {"setting": setting, "edges": censored, "score_reading": ctx.score_reading},
                "warning",
            )
        _persist_setting_artifacts(ctx, setting, df, dfs_best)
        _persist_per_protein(ctx, setting, sink, result, emit)
        grid = _persist_per_protein_grid(ctx, setting, sink, emit)
        if grid is not None:
            _record_cell_populations(result, grid)
        emit(
            "run_cafa_evaluation.setting_done",
            None,
            {"setting": setting, "namespaces": list(result.keys())},
            "info",
        )
        return result
    except Exception as exc:
        emit(
            "run_cafa_evaluation.setting_failed",
            None,
            {"setting": setting, "error": str(exc)},
            "warning",
        )
        return {}


def evaluate_all_settings(
    *,
    ctx: CafaEvalRunContext,
    emit: EmitFn,
) -> dict[str, dict[str, Any]]:
    """Drive the per-setting NK / LK / PK cafaeval loop.

    Every setting reads the shared prediction file, invokes cafaeval, parses
    results, persists the full cafaeval artifact tree, and emits per-setting
    audit events. Returns ``{setting → namespace metrics dict}``.

    Raises :class:`SettingEvaluationError` when any setting produced no
    metrics, after all three have run and emitted their events.
    """
    results: dict[str, dict[str, Any]] = {}
    for setting in SETTINGS:
        results[setting] = _run_cafaeval_for_setting(
            setting=setting,
            pred_dir=ctx.shared_pred_dir,
            ctx=ctx,
            emit=emit,
        )

    # Every setting runs before this check so the audit trail names all the
    # ones that failed. Then the job fails, because an EvaluationResult with
    # an empty cell is worse than no row at all: it is indistinguishable from
    # a configuration that genuinely scored nothing, and a caller comparing
    # arms would read the absence as a measurement.
    empty = [setting for setting, metrics in results.items() if not metrics]
    if empty:
        raise SettingEvaluationError(
            f"settings produced no metrics: {', '.join(empty)}. "
            "The run_cafa_evaluation.setting_failed events carry the reason."
        )
    return results
