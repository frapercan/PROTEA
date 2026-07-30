"""Batch re-score evaluation operation (A-SCORE eval optimisation, win #1).

The A-SCORE ablation / HPO re-runs CAFA evaluation for the SAME prediction set
under many ``ScoringConfig`` variants (only the score column differs). Running a
separate :class:`~protea.core.operations.run_cafa_evaluation.RunCafaEvaluationOperation`
per config re-loads the prediction feature matrix, the ground truth, the OBO,
and the IA file every time → N x redundant heavy work.

This operation collapses that into **one load, many cheap re-scores**:

1. Load the EvaluationSet + ground truth ONCE (the GT is reused from
   ``EvaluationSet.groundtruth_uri`` (win #2, already materialised), restrict
   it to the predicted cohort once, write the GT / TOI files once.
2. Download the OBO + IA file ONCE.
3. Fetch the column-pruned, deduped prediction base frame ONCE (win #3:
   ``_base_select`` selects only the ~17 scoring columns, never the ~60-column
   ORM row), holding it in memory for the whole batch.
4. Per ``scoring_config``: ``score = _vectorized_scores(base, cfg)`` (cheap
   numpy), write the predictions TSV, run cafaeval's tau-sweep over NK/LK/PK,
   and persist ONE ``EvaluationResult``.

Every per-config result is byte-identical to the equivalent standalone
``run_cafa_evaluation`` run (no reranker path), because the per-config score +
cafaeval invocation reuse the exact same helpers
(:func:`protea.core.operations._run_cafa_artifacts._write_scored_base`,
:func:`protea.core.operations._run_cafa_eval_driver._run_cafaeval_for_setting`).

The legacy per-config disk cache
(:mod:`protea.core.operations._pred_base_cache`) still lets SEPARATE jobs share
the base frame across process boundaries; this op additionally avoids the
parquet round-trip and the GT/OBO/IA re-staging within a single batch job.
"""

from __future__ import annotations

import os
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pydantic import Field, field_validator
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.operations import _run_cafa_artifacts as _artifacts
from protea.core.operations import _run_cafa_data_helpers as _data
from protea.core.operations._run_cafa_eval_driver import _run_cafaeval_for_setting
from protea.core.operations.run_cafa_evaluation import (
    RunCafaEvaluationOperation,
    RunCafaEvaluationPayload,
)
from protea.infrastructure.orm.models.annotation.evaluation_result import EvaluationResult
from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store


class BatchRescoreEvaluationPayload(ProteaPayload, frozen=True):
    """One prediction set + one eval set + a LIST of scoring configs.

    Shares the cafaeval knobs (``ia_file`` / ``band`` / ``toi_file`` /
    ``th_step`` / ``max_terms`` / ``max_distance`` / ``restrict_gt_to_predicted``
    / provenance markers) across every config in the batch; only the
    ``scoring_config_ids`` vary per result. The reranker fields of
    ``run_cafa_evaluation`` are intentionally absent: the batch op is the
    no-reranker score-ablation fast path.
    """

    evaluation_set_id: str
    prediction_set_id: str
    scoring_config_ids: list[str] = Field(min_length=1)
    max_distance: float | None = Field(default=None, ge=0.0, le=2.0)
    ia_file: str | None = Field(default=None)
    #: Preferred over ``ia_file``: resolves the IA table through the artifact
    #: store and verifies its recorded sha256, so every config in the batch is
    #: scored against a table whose corpus and evidence regime are pinned
    #: (ADR-D46). Mutually exclusive with ``ia_file``.
    information_accretion_set_id: str | None = Field(default=None)
    restrict_gt_to_predicted: bool = Field(default=True)
    th_step: float = Field(default=0.01, gt=0.0, le=1.0)
    max_terms: int | None = Field(default=None, ge=1)
    band: str | None = Field(default=None)
    toi_file: str | None = Field(default=None)
    frame: str | None = Field(default=None)
    temporal_window: str | None = Field(default=None)
    leakage_role: str | None = Field(default=None)
    window_role: str | None = Field(default=None)
    # LEAN internal train/valid split for A-SCORE.2 score HPO. ``protein_folds
    # <= 1`` (default) evaluates the full cohort; otherwise the GT cohort is
    # deterministically partitioned by ``blake2b(seed:protein) % folds`` and only
    # ``protein_fold`` is kept. No new EvaluationSet rows, no homology clustering.
    protein_folds: int = Field(default=1, ge=1, le=20)
    protein_fold: int = Field(default=0, ge=0)
    protein_seed: int = Field(default=0, ge=0)

    @field_validator("protein_fold")
    @classmethod
    def _fold_in_range(cls, v: int, info: Any) -> int:
        folds = (info.data or {}).get("protein_folds", 1)
        if folds and v >= folds:
            raise ValueError(f"protein_fold {v} must be < protein_folds {folds}")
        return v

    @field_validator("evaluation_set_id", "prediction_set_id", mode="before")
    @classmethod
    def must_be_non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()

    @field_validator("scoring_config_ids")
    @classmethod
    def _non_empty_ids(cls, v: list[str]) -> list[str]:
        cleaned = [s.strip() for s in v if isinstance(s, str) and s.strip()]
        if not cleaned:
            raise ValueError("scoring_config_ids must contain at least one non-empty id")
        return cleaned


@dataclass(frozen=True)
class _StageCtx:
    """Inputs for the once-per-batch shared staging, bundled under the §3 ceiling.

    Carries the loaded ``inputs`` + derived single-eval ``base_payload`` plus the
    optional staging knobs: ``needs_term_ia`` triggers the IA-file -> ``term_ia``
    injection (only when a config uses ``ia_prior source="ia"``), and
    ``fold_spec`` is the ``(folds, fold, seed)`` internal train/valid split
    (``folds<=1`` = full cohort).
    """

    base_payload: RunCafaEvaluationPayload
    inputs: Any
    needs_term_ia: bool = False
    fold_spec: tuple[int, int, int] = (1, 0, 0)


@dataclass(frozen=True)
class _BatchRun:
    """Per-batch shared state, fixed once before the per-config loop.

    Bundles the validated payload, loaded eval inputs, once-staged shared
    artifacts (``staged``), and the artifact store so the per-config worker
    signature stays under the §3 6-param ceiling.
    """

    p: BatchRescoreEvaluationPayload
    inputs: Any
    staged: dict[str, Any]
    artifact_store: Any


class BatchRescoreEvaluationOperation:
    """Re-score one prediction set under N scoring configs in a single load.

    Delegates inputs/staging/provenance to ``RunCafaEvaluationOperation``'s
    static helpers so the per-config result is identical to a standalone eval.
    """

    name = "batch_rescore_evaluation"
    description = (
        "Re-score one PredictionSet against an EvaluationSet under a list of "
        "ScoringConfigs in a single load (one GT + base-frame fetch, N cheap "
        "re-scores). Equivalent to N run_cafa_evaluation jobs, much cheaper."
    )

    def __init__(self) -> None:
        # Reuse the single-eval operation's input-loading / staging / provenance
        # helpers verbatim so results stay byte-identical.
        self._single = RunCafaEvaluationOperation()

    def summarize_payload(self, payload: dict[str, Any], *, session: Session | None = None) -> str:
        n = len((payload or {}).get("scoring_config_ids") or [])
        return f"batch re-score: {n} scoring config(s)"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = BatchRescoreEvaluationPayload.model_validate(payload)
        # Build the equivalent single-eval payload (sans scoring_config_id) so the
        # shared input loader + stager run exactly as in run_cafa_evaluation.
        base_payload = self._single_payload(p)
        inputs = self._single._load_evaluation_inputs(session, base_payload, emit)
        self._single._stamp_window_role(inputs.eval_set, p.window_role, emit)

        configs = self._load_scoring_snapshots(session, p.scoring_config_ids)
        artifact_store = get_artifact_store(load_settings(Path(__file__).resolve().parents[3]))
        needs_term_ia = any(self._uses_ia_source(snap) for _, snap in configs)

        result_ids: list[str] = []
        with tempfile.TemporaryDirectory(prefix="protea_cafa_batch_") as tmpdir:
            stage_ctx = _StageCtx(
                base_payload=base_payload,
                inputs=inputs,
                needs_term_ia=needs_term_ia,
                fold_spec=(p.protein_folds, p.protein_fold, p.protein_seed),
            )
            staged = self._stage_shared_inputs(session, stage_ctx, Path(tmpdir), emit)
            # Release the DB connection before cafaeval forks its pool (same
            # rationale as run_cafa_evaluation's no-op commit), then run every
            # config against the in-memory shared base frame.
            session.commit()
            emit(
                "batch_rescore_evaluation.staged",
                None,
                {"configs": len(configs), "delta_proteins": len(staged["delta_proteins"])},
                "info",
            )
            run = _BatchRun(p=p, inputs=inputs, staged=staged, artifact_store=artifact_store)
            for idx, (cfg_id, snapshot) in enumerate(configs):
                result_id = self._evaluate_one_config(
                    session, run, cfg_id=cfg_id, snapshot=snapshot, emit=emit
                )
                result_ids.append(str(result_id))
                emit(
                    "batch_rescore_evaluation.config_done",
                    None,
                    {"index": idx + 1, "of": len(configs), "result_id": str(result_id)},
                    "info",
                )

        emit(
            "batch_rescore_evaluation.done",
            None,
            {"evaluation_result_ids": result_ids, "configs": len(configs)},
            "info",
        )
        return OperationResult(result={"evaluation_result_ids": result_ids})

    # ------------------------------------------------------------------
    # Shared (once-per-batch) staging
    # ------------------------------------------------------------------

    @staticmethod
    def _single_payload(p: BatchRescoreEvaluationPayload) -> RunCafaEvaluationPayload:
        """The run_cafa_evaluation payload shared by every config (no score id)."""
        return RunCafaEvaluationPayload(
            evaluation_set_id=p.evaluation_set_id,
            prediction_set_id=p.prediction_set_id,
            max_distance=p.max_distance,
            ia_file=p.ia_file,
            information_accretion_set_id=p.information_accretion_set_id,
            restrict_gt_to_predicted=p.restrict_gt_to_predicted,
            th_step=p.th_step,
            max_terms=p.max_terms,
            band=p.band,
            toi_file=p.toi_file,
            frame=p.frame,
            temporal_window=p.temporal_window,
            leakage_role=p.leakage_role,
            window_role=p.window_role,
        )

    @staticmethod
    def _uses_ia_source(snapshot: ScoringConfig | None) -> bool:
        """True if the config's ``ia_prior`` is enabled with ``source="ia"``.

        Only then does the batch need to attach the (cost-bearing) per-row
        ``term_ia`` column from the IA file; frequency-source and disabled
        priors read columns already present on the base frame.
        """
        from protea.infrastructure.orm.models.embedding.scoring_config import (
            IA_PRIOR_SOURCE_IA,
        )

        params = getattr(snapshot, "params", None)
        block = (params or {}).get("ia_prior") if isinstance(params, dict) else None
        if not isinstance(block, dict) or not block.get("enabled"):
            return False
        return block.get("source") == IA_PRIOR_SOURCE_IA

    def _stage_shared_inputs(
        self,
        session: Session,
        ctx: _StageCtx,
        tmpdir: Path,
        emit: EmitFn,
    ) -> dict[str, Any]:
        """Download OBO + IA, restrict GT, write GT/TOI, and fetch the base frame.

        Everything here is config-independent, so it runs ONCE per batch. The
        deduped base frame is held in memory and reused for every config. When
        ``ctx.needs_term_ia`` (any batched config uses the ``ia_prior``
        ``source="ia"`` lever), the IA file is also mapped onto the frame as a
        normalised ``term_ia`` column so the prior aligns exactly with the
        cafaeval ``f_micro_w`` IA weighting.
        """
        base_payload, inputs = ctx.base_payload, ctx.inputs
        obo_path = os.path.join(str(tmpdir), "go.obo")
        emit("batch_rescore_evaluation.downloading_obo", None, {"url": inputs.snapshot.obo_url},
             "info")
        _artifacts.download_obo(inputs.snapshot.obo_url, obo_path)
        ia_path = self._single._resolve_ia_file(
            str(tmpdir), inputs.snapshot, base_payload.ia_file, emit,
            session, base_payload.information_accretion_set_id,
        )
        self._single._enforce_band(base_payload.band, inputs.snapshot, base_payload.ia_file, emit)

        data = self._restrict_cohort(session, base_payload, inputs, ctx.fold_spec, emit)
        gt_root = tmpdir / "gt"
        gt_root.mkdir(parents=True, exist_ok=True)
        gt_paths = _data.write_ground_truth_files(gt_root, data)
        toi_path = self._single._resolve_toi_file(gt_root, base_payload, inputs.toi_go_ids, emit)

        delta_proteins = set(data.nk) | set(data.lk) | set(data.pk)
        base_frame = self._fetch_base_frame(
            session, inputs.pred_set_id, delta_proteins, base_payload.max_distance, emit
        )
        if ctx.needs_term_ia and ia_path:
            self._attach_term_ia(base_frame, ia_path, emit)
        return {
            "tmpdir": tmpdir,
            "obo_path": obo_path,
            "ia_path": ia_path,
            "toi_path": toi_path,
            "gt_paths": gt_paths,
            "delta_proteins": delta_proteins,
            "base_frame": base_frame,
        }

    @staticmethod
    def _restrict_cohort(
        session: Session,
        base_payload: RunCafaEvaluationPayload,
        inputs: Any,
        fold_spec: tuple[int, int, int],
        emit: EmitFn,
    ) -> Any:
        """Restrict the GT cohort to the predicted set, then the internal fold."""
        data = inputs.data
        if base_payload.restrict_gt_to_predicted:
            data = _data.restrict_data_to_predicted(
                session, prediction_set_id=inputs.pred_set_id, data=data, emit=emit
            )
        folds, fold, fold_seed = fold_spec
        if folds > 1:
            data = _data.restrict_data_to_protein_fold(
                data, folds=folds, fold=fold, seed=fold_seed, emit=emit
            )
        return data

    @staticmethod
    def _attach_term_ia(base_frame: Any, ia_path: str, emit: EmitFn) -> None:
        """Map the IA file onto the base frame as a normalised ``term_ia`` column."""
        ia_map = _artifacts.load_ia_map(ia_path)
        _artifacts.attach_term_ia(base_frame, ia_map)
        emit(
            "batch_rescore_evaluation.term_ia_attached",
            None,
            {"ia_terms": len(ia_map), "rows": int(len(base_frame))},
            "info",
        )

    @staticmethod
    def _fetch_base_frame(
        session: Session,
        pred_set_id: uuid.UUID,
        delta_proteins: set[str],
        max_distance: float | None,
        emit: EmitFn,
    ) -> Any:
        """Fetch + dedup the column-pruned base frame ONCE for the whole batch.

        Reuses the same disk cache as the single-eval path so the frame is
        shared across separate jobs too, then keeps it in memory for the batch.
        """
        ctx = _artifacts.WritePredictionsContext(
            pred_set_id=pred_set_id,
            delta_proteins=delta_proteins,
            max_distance=max_distance,
            path="",  # unused: we fetch the frame, not write a TSV
        )
        base = _artifacts._pred_base_cache.load_or_build_base(
            ctx.pred_set_id,
            ctx.max_distance,
            ctx.delta_proteins,
            count_fn=lambda: _artifacts._count_base_rows(session, ctx),
            build_fn=lambda: _artifacts._build_base_frame(session, ctx),
            emit=lambda event, fields: emit(event, None, fields, "info"),
        )
        emit(
            "batch_rescore_evaluation.base_frame_loaded",
            None,
            {"rows": int(len(base))},
            "info",
        )
        return base

    # ------------------------------------------------------------------
    # Per-config evaluation
    # ------------------------------------------------------------------

    def _evaluate_one_config(
        self,
        session: Session,
        run: _BatchRun,
        *,
        cfg_id: str | None,
        snapshot: ScoringConfig | None,
        emit: EmitFn,
    ) -> uuid.UUID:
        """Score the shared base frame with one config, run cafaeval, persist."""
        p, inputs, staged = run.p, run.inputs, run.staged
        result_id = uuid.uuid4()
        tmpdir: Path = staged["tmpdir"]
        run_dir = tmpdir / f"cfg_{result_id}"
        pred_dir = run_dir / "predictions"
        pred_dir.mkdir(parents=True, exist_ok=True)
        pred_path = str(pred_dir / "predictions.tsv")
        # Cheap numpy re-score of the already-loaded base frame (win #1).
        _artifacts._write_scored_base(staged["base_frame"], snapshot, pred_path)

        ctx = _BatchEvalContext(
            obo_path=staged["obo_path"],
            ia_path=staged["ia_path"],
            toi_path=staged["toi_path"],
            th_step=p.th_step,
            max_terms=p.max_terms,
            artifacts_root=run_dir,
        )
        results = self._run_settings(ctx, staged["gt_paths"], str(pred_dir), emit)
        uploaded = self._single._upload_artifacts(run.artifact_store, result_id, run_dir, emit)
        results["artifacts"] = {"keys": uploaded}

        base_payload = self._single_payload(p)
        frame, temporal_window, leakage_role, arms_enabled = self._single._build_eval_provenance(
            base_payload, inputs.eval_set, has_rerankers=False
        )
        eval_result = EvaluationResult(
            id=result_id,
            evaluation_set_id=inputs.eval_set_id,
            prediction_set_id=inputs.pred_set_id,
            scoring_config_id=uuid.UUID(cfg_id) if cfg_id else None,
            reranker_model_id=None,
            reranker_config=None,
            results=results,
            frame=frame,
            temporal_window=temporal_window,
            leakage_role=leakage_role,
            arms_enabled=arms_enabled,
        )
        session.add(eval_result)
        session.flush()
        return result_id

    @staticmethod
    def _run_settings(
        ctx: _BatchEvalContext,
        gt_paths: dict[str, str],
        pred_dir: str,
        emit: EmitFn,
    ) -> dict[str, Any]:
        """Run NK/LK/PK cafaeval over the shared predictions dir for one config.

        Mirrors ``evaluate_all_settings`` no-reranker branch: all three settings
        read the single ``pred_dir`` (only GT / exclude differs), so the result
        per setting is identical to a standalone eval.
        """
        run_ctx = ctx.as_run_context(pred_dir, gt_paths)
        results: dict[str, Any] = {}
        for setting, gt_file, known_file in [
            ("NK", gt_paths["nk"], None),
            ("LK", gt_paths["lk"], None),
            ("PK", gt_paths["pk"], gt_paths["pk_known"]),
        ]:
            results[setting] = _run_cafaeval_for_setting(
                setting=setting,
                pred_dir=pred_dir,
                gt_file=gt_file,
                known_file=known_file,
                ctx=run_ctx,
                emit=emit,
            )
        return results

    @staticmethod
    def _load_scoring_snapshots(
        session: Session, scoring_config_ids: list[str]
    ) -> list[tuple[str, ScoringConfig | None]]:
        """Materialise in-memory ScoringConfig snapshots before the no-op commit.

        Detached copies (formula / weights / params only) so cafaeval's forked
        workers never touch the session. A literal ``"none"`` / empty id maps to
        the distance fallback (snapshot ``None``), matching run_cafa_evaluation.
        """
        out: list[tuple[str, ScoringConfig | None]] = []
        for cfg_id in scoring_config_ids:
            if not cfg_id or cfg_id.lower() == "none":
                out.append((cfg_id, None))
                continue
            sc = session.get(ScoringConfig, uuid.UUID(cfg_id))
            if sc is None:
                raise ValueError(f"ScoringConfig {cfg_id} not found")
            out.append(
                (
                    cfg_id,
                    ScoringConfig(
                        formula=sc.formula,
                        weights=dict(sc.weights),
                        params=dict(sc.params) if sc.params else None,
                    ),
                )
            )
        return out


@dataclass(frozen=True)
class _BatchEvalContext:
    """Per-config cafaeval inputs, adapted to the driver's ``CafaEvalRunContext``.

    Holds only the config-independent paths (OBO / IA / TOI) plus the per-config
    artifacts root; ``as_run_context`` builds the
    :class:`~protea.core.operations._run_cafa_eval_driver.CafaEvalRunContext` the
    no-reranker driver needs (``has_rerankers=False`` so it never re-writes
    predictions).
    """

    obo_path: str
    ia_path: str | None
    toi_path: str
    th_step: float
    max_terms: int | None
    artifacts_root: Path

    def as_run_context(self, pred_dir: str, gt_paths: dict[str, str]) -> Any:
        from protea.core.evaluation import EvaluationData
        from protea.core.operations._run_cafa_eval_driver import CafaEvalRunContext

        return CafaEvalRunContext(
            pred_set_id=uuid.uuid4(),  # unused on the no-reranker path
            delta_proteins=set(),  # unused: predictions already written
            max_distance=None,
            artifacts_root=self.artifacts_root,
            has_rerankers=False,
            reranker_models={},
            scoring_config_snapshot=None,
            data=EvaluationData(nk={}, lk={}, pk={}, pk_known={}, known={}),
            obo_path=self.obo_path,
            nk_path=gt_paths["nk"],
            lk_path=gt_paths["lk"],
            pk_path=gt_paths["pk"],
            pk_known_path=gt_paths["pk_known"],
            ia_path=self.ia_path,
            toi_path=self.toi_path,
            shared_pred_dir=pred_dir,
            th_step=self.th_step,
            max_terms=self.max_terms,
        )
