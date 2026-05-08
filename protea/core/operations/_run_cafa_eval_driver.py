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
            reranker_model_str=bundle["model"],
            reranker_cat_codes=bundle.get("cat_codes"),
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
    Persists the full PR-curve + dfs_best artifacts to
    ``ctx.artifacts_root/<setting>/`` so the upload loop in the
    orchestrator picks them up.
    """
    from cafaeval.evaluation import cafa_eval

    emit("run_cafa_evaluation.evaluating", None, {"setting": setting}, "info")
    try:
        # Reset SIGTERM/SIGINT to defaults before cafaeval forks pool workers.
        # Our `_handle_stop` handler only sets a flag without calling
        # sys.exit(), so forked children would ignore SIGTERM from
        # pool.terminate() and pool.join() would block forever.
        old_sigterm = signal.signal(signal.SIGTERM, signal.SIG_DFL)
        old_sigint = signal.signal(signal.SIGINT, signal.SIG_DFL)
        try:
            df, dfs_best = cafa_eval(
                ctx.obo_path,
                pred_dir,
                gt_file,
                ia=ctx.ia_path,
                exclude=known_file,
                prop="fill",
                norm="cafa",
                no_orphans=True,
                toi_file=ctx.toi_path,
                max_terms=500,
                th_step=0.001,
                n_cpu=1,
            )
        finally:
            signal.signal(signal.SIGTERM, old_sigterm)
            signal.signal(signal.SIGINT, old_sigint)

        result = _artifacts.parse_results(dfs_best)

        # Persist full cafaeval output (PR curves + best metrics per metric type)
        if df is not None:
            from cafaeval.evaluation import write_results as _write_results

            setting_dir = ctx.artifacts_root / setting
            setting_dir.mkdir(exist_ok=True)
            _write_results(df, dfs_best, str(setting_dir))

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
        if ctx.has_rerankers:
            pred_dir = _write_setting_predictions(
                session,
                setting=setting,
                ctx=ctx,
            )
        else:
            pred_dir = ctx.shared_pred_dir
        results[setting] = _run_cafaeval_for_setting(
            setting=setting,
            pred_dir=pred_dir,
            gt_file=gt_file,
            known_file=known_file,
            ctx=ctx,
            emit=emit,
        )
    return results
