"""Per-setting cafaeval driver extracted from
``RunCafaEvaluationOperation.execute``.

Wraps the per-setting (NK / LK / PK) loop that:
  1. writes per-setting prediction TSVs (when a reranker applies),
  2. invokes ``cafaeval.evaluation.cafa_eval`` with signal-safe
     handlers (default SIGTERM/SIGINT during the fork pool),
  3. parses the dfs_best result,
  4. writes the full cafaeval artifact tree per setting,
  5. emits the ``setting_done`` / ``setting_failed`` audit events.

The helper takes the operation's ``emit`` callback as a parameter
to keep the audit trail unchanged.
"""

from __future__ import annotations

import os
import signal
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn
from protea.core.evaluation import EvaluationData
from protea.core.operations import _run_cafa_artifacts as _artifacts
from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig

# The frame literals, named once. They are arguments to the cafaeval call AND
# facts the grid artefact stamps, and a stamp thirty lines from the call it
# describes is the classic shape of a value that drifts from the thing it
# claims. One constant each means the two cannot disagree.
CAFAEVAL_PROP = "fill"
CAFAEVAL_NORM = "cafa"
CAFAEVAL_NO_ORPHANS = True


@dataclass(frozen=True)
class CafaEvalRunContext:
    """Bundle of per-run inputs consumed by :func:`evaluate_all_settings`.

    Groups the 16 per-call inputs (artifact paths, reranker bundles,
    delta cohort, scoring snapshot) so the entry-point signature stays
    under flake8-bugbear's parameter ceiling.
    """

    pred_set_id: uuid.UUID
    delta_proteins: set[str]
    max_distance: float | None
    artifacts_root: Path
    has_rerankers: bool
    reranker_models: dict[str, dict[str, dict[str, Any]]]
    scoring_config_snapshot: ScoringConfig | None
    data: EvaluationData
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
    max_terms: int | None = None
    max_k_position: int | None = None
    softprop: bool = False
    interpro_graft: bool = False
    interpro_protein2ipr_file: str | None = None
    interpro_ipr2go_file: str | None = None
    interpro_graft_weight: float | None = None


def _write_setting_predictions(
    session: Session,
    *,
    setting: str,
    ctx: CafaEvalRunContext,
) -> str:
    """Write the per-setting predictions TSV when a reranker applies.

    Returns the prediction directory the cafaeval call should consume.
    For settings without a reranker the caller falls back to the
    pre-written shared `predictions/` dir.
    """
    pred_dir = os.path.join(str(ctx.artifacts_root), f"predictions_{setting}")
    os.makedirs(pred_dir, exist_ok=True)
    pred_path = os.path.join(pred_dir, "predictions.tsv")
    rr_aspect_map = ctx.reranker_models.get(setting, {})
    # anc2vec_query_known_* is only meaningful when the query has
    # pre-cutoff annotations (LK / PK). NK proteins have nothing
    # "known", so training/serving parity requires leaving the
    # features at their predict-time NaN / 0.
    setting_known = ctx.data.known if setting in ("LK", "PK") else None
    write_ctx = _artifacts.WritePredictionsContext(
        pred_set_id=ctx.pred_set_id,
        delta_proteins=ctx.delta_proteins,
        max_distance=ctx.max_distance,
        max_k_position=ctx.max_k_position,
        path=pred_path,
    )
    if "" in rr_aspect_map:
        bundle = rr_aspect_map[""]
        _artifacts.write_predictions(
            session,
            write_ctx,
            scoring_config=ctx.scoring_config_snapshot,
            reranker_bundle=bundle,
            known_gos=setting_known,
        )
    else:
        _artifacts.write_predictions_per_aspect(
            session,
            write_ctx,
            aspect_models=rr_aspect_map,
            known_gos=setting_known,
        )
    return pred_dir


def _new_per_protein_sink() -> Any:
    """Build the sink, failing loudly if the installed cafaeval cannot make one.

    Imported through a function rather than inline because the only caller sits
    inside ``_run_cafaeval_for_setting``'s broad ``except Exception``. An
    ImportError raised there would be swallowed and reported as "this setting
    produced no results", so a wrong dependency version would look exactly like
    an evaluation that legitimately scored nothing. This raises a message that
    names the actual problem instead.
    """
    try:
        from cafaeval.evaluation import PerProteinSink
    except ImportError as exc:  # pragma: no cover - depends on the installed pin
        raise RuntimeError(
            "the installed cafaeval cannot emit per-protein scores; the pin in "
            "pyproject expects a build with PerProteinSink. Reinstall the "
            "dependency rather than reading this run's empty settings as a result."
        ) from exc
    return PerProteinSink()


def _invoke_cafaeval_signal_safe(
    *,
    ctx: CafaEvalRunContext,
    pred_dir: str,
    gt_file: str,
    known_file: str | None,
) -> tuple[Any, Any, Any]:
    """Run ``cafa_eval`` with default SIGTERM/SIGINT handlers restored.

    Our ``_handle_stop`` handler only sets a flag without calling
    ``sys.exit()``, so forked pool children would ignore SIGTERM and
    ``pool.terminate()`` / ``pool.join()`` would block forever. Reset
    to ``SIG_DFL`` for the duration of the call.
    """
    from cafaeval.evaluation import cafa_eval

    # Collected on every run rather than behind a flag. The cost is one array
    # per namespace held for the length of the call, and the alternative is a
    # setting that is off exactly when someone wants to stratify a result that
    # has already been computed.
    sink = _new_per_protein_sink()

    old_sigterm = signal.signal(signal.SIGTERM, signal.SIG_DFL)
    old_sigint = signal.signal(signal.SIGINT, signal.SIG_DFL)
    try:
        df, dfs_best = cafa_eval(
            ctx.obo_path,
            pred_dir,
            gt_file,
            ia=ctx.ia_path,
            exclude=known_file,
            prop=CAFAEVAL_PROP,
            norm=CAFAEVAL_NORM,
            no_orphans=CAFAEVAL_NO_ORPHANS,
            toi_file=ctx.toi_path,
            max_terms=ctx.max_terms,
            th_step=ctx.th_step,
            n_cpu=1,
            per_protein_sink=sink,
        )
        return df, dfs_best, sink
    finally:
        signal.signal(signal.SIGTERM, old_sigterm)
        signal.signal(signal.SIGINT, old_sigint)


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


def _persist_per_protein_grid(
    ctx: CafaEvalRunContext,
    setting: str,
    sink: Any,
    emit: EmitFn,
) -> None:
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


def _run_cafaeval_for_setting(
    *,
    setting: str,
    pred_dir: str,
    gt_file: str,
    known_file: str | None,
    ctx: CafaEvalRunContext,
    emit: EmitFn,
) -> dict[str, Any]:
    """Run cafaeval for one setting under signal-safe handlers.

    Returns the parsed per-namespace metrics dict (empty on failure).
    """
    emit("run_cafa_evaluation.evaluating", None, {"setting": setting}, "info")
    try:
        df, dfs_best, sink = _invoke_cafaeval_signal_safe(
            ctx=ctx, pred_dir=pred_dir, gt_file=gt_file, known_file=known_file
        )
        result = _artifacts.parse_results(dfs_best)
        _persist_setting_artifacts(ctx, setting, df, dfs_best)
        _persist_per_protein(ctx, setting, sink, result, emit)
        _persist_per_protein_grid(ctx, setting, sink, emit)
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


def _resolve_setting_pred_dir(
    session: Session,
    *,
    setting: str,
    ctx: CafaEvalRunContext,
    emit: EmitFn,
) -> str:
    """Return the prediction dir for one setting, applying the scoring arms.

    With per-setting rerankers, writes a FRESH per-setting dir and applies the
    softprop / InterPro-graft arms in place (idempotency: the shared dir is
    reused across settings, so a noisy-OR arm would double-apply there). Without
    rerankers, reuses ``ctx.shared_pred_dir`` and emits a skip per requested arm.
    """
    if not ctx.has_rerankers:
        for enabled, arm in ((ctx.softprop, "softprop"), (ctx.interpro_graft, "interpro_graft")):
            if enabled:
                emit(
                    f"run_cafa_evaluation.{arm}_skipped",
                    None,
                    {"reason": f"{arm} requires per-setting reranker predictions"},
                    "warning",
                )
        return ctx.shared_pred_dir
    pred_dir = _write_setting_predictions(session, setting=setting, ctx=ctx)
    if ctx.softprop:
        from protea.core.operations._run_cafa_softprop import apply_softprop

        apply_softprop(pred_dir, ctx.obo_path, emit)
    if ctx.interpro_graft:
        from protea.core.operations._run_cafa_interpro_graft import apply_interpro_graft

        apply_interpro_graft(
            pred_dir,
            ctx.obo_path,
            ctx.interpro_protein2ipr_file,
            ctx.interpro_ipr2go_file,
            ctx.interpro_graft_weight,
            emit,
        )
    return pred_dir


def evaluate_all_settings(
    session: Session,
    *,
    ctx: CafaEvalRunContext,
    emit: EmitFn,
) -> dict[str, dict[str, Any]]:
    """Drive the per-setting NK / LK / PK cafaeval loop.

    Writes per-setting predictions when a reranker applies (otherwise
    reuses ``ctx.shared_pred_dir``), invokes cafaeval, parses results,
    persists the full cafaeval artifact tree, and emits per-setting
    audit events. Returns ``{setting → namespace metrics dict}``.
    """
    results: dict[str, dict[str, Any]] = {}
    for setting, gt_file, known_file in [
        ("NK", ctx.nk_path, None),
        ("LK", ctx.lk_path, None),
        ("PK", ctx.pk_path, ctx.pk_known_path),
    ]:
        pred_dir = _resolve_setting_pred_dir(session, setting=setting, ctx=ctx, emit=emit)
        results[setting] = _run_cafaeval_for_setting(
            setting=setting,
            pred_dir=pred_dir,
            gt_file=gt_file,
            known_file=known_file,
            ctx=ctx,
            emit=emit,
        )
    return results
