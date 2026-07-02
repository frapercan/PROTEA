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
    th_step: float = 0.01
    max_terms: int | None = None
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


def _invoke_cafaeval_signal_safe(
    *,
    ctx: CafaEvalRunContext,
    pred_dir: str,
    gt_file: str,
    known_file: str | None,
) -> tuple[Any, Any]:
    """Run ``cafa_eval`` with default SIGTERM/SIGINT handlers restored.

    Our ``_handle_stop`` handler only sets a flag without calling
    ``sys.exit()``, so forked pool children would ignore SIGTERM and
    ``pool.terminate()`` / ``pool.join()`` would block forever. Reset
    to ``SIG_DFL`` for the duration of the call.
    """
    from cafaeval.evaluation import cafa_eval

    old_sigterm = signal.signal(signal.SIGTERM, signal.SIG_DFL)
    old_sigint = signal.signal(signal.SIGINT, signal.SIG_DFL)
    try:
        return cafa_eval(
            ctx.obo_path,
            pred_dir,
            gt_file,
            ia=ctx.ia_path,
            exclude=known_file,
            prop="fill",
            norm="cafa",
            no_orphans=True,
            toi_file=ctx.toi_path,
            max_terms=ctx.max_terms,
            th_step=ctx.th_step,
            n_cpu=1,
        )
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
        df, dfs_best = _invoke_cafaeval_signal_safe(
            ctx=ctx, pred_dir=pred_dir, gt_file=gt_file, known_file=known_file
        )
        result = _artifacts.parse_results(dfs_best)
        _persist_setting_artifacts(ctx, setting, df, dfs_best)
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
