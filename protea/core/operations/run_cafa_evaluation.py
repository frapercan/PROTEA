from __future__ import annotations

import os
import tempfile
import uuid
from pathlib import Path
from typing import Any

from pydantic import Field, field_validator
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.evaluation import load_evaluation_data_for_set
from protea.core.operations import _run_cafa_artifacts as _artifacts
from protea.core.operations import _run_cafa_data_helpers as _data
from protea.core.operations import _run_cafa_summary as _summary
from protea.core.operations._run_cafa_eval_driver import (
    CafaEvalRunContext,
    evaluate_all_settings,
)

# Re-exports for backwards compatibility with existing imports.
# Helpers live in ``_run_cafa_helpers`` so this file can stay close
# to the master-plan v3.2 §3 LOC ceiling (<800).
from protea.core.operations._run_cafa_helpers import (  # noqa: F401
    _NS_LABELS,
    _NS_SHORT,
    _NUMERIC_ORM_COLS,
    _patch_query_known_features,
    _record_from_pred,
    eval_artifact_key,
)
from protea.core.operations._run_cafa_reranker_loader import (
    load_reranker_models_for_payload,
)
from protea.core.operations._run_cafa_setup import (  # noqa: F401
    _emit_evaluation_setup_events,
    _EvalInputs,
    _load_terms_of_interest,
    _PipelineCtx,
)
from protea.infrastructure.orm.models.annotation.evaluation_result import EvaluationResult
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import ArtifactStore, get_artifact_store


class RunCafaEvaluationPayload(ProteaPayload, frozen=True):
    evaluation_set_id: str
    prediction_set_id: str
    max_distance: float | None = Field(default=None, ge=0.0, le=2.0)
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

    @field_validator("evaluation_set_id", "prediction_set_id", mode="before")
    @classmethod
    def must_be_non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()


class RunCafaEvaluationOperation:
    """Runs the CAFA evaluator against NK, LK and PK settings.

    Steps:

    1. Load ``EvaluationSet`` and ``PredictionSet`` from DB.
    2. Compute evaluation data (delta NK/LK + known terms) with full
       NOT propagation.
    3. Download the OBO file from the ontology snapshot URL.
    4. Resolve the Information Accretion (IA) file: if ``ia_file`` is set
       in the payload, use that path directly; otherwise, if the
       ``OntologySnapshot`` has an ``ia_url``, download it to a temporary
       file; if neither is set, cafaeval runs with uniform IC=1. IA weights
       make rare GO terms count more than common ones and are recommended
       for publishable evaluations.
    5. Write temp files: ground-truth NK/LK, known-terms, predictions
       (CAFA format).
    6. Call ``cafa_eval`` for each setting (NK, LK, PK).
    7. Parse per-namespace Fmax / precision / recall / coverage from
       results.
    8. Persist an ``EvaluationResult`` row with all metrics.
    """

    name = "run_cafa_evaluation"
    description = (
        "Run the CAFA evaluator (NK/LK/PK) against an EvaluationSet using a "
        "PredictionSet, optionally weighted by Information Accretion."
    )

    def summarize_payload(self, payload: dict[str, Any], *, session: Session | None = None) -> str:
        return _summary.summarize_payload(payload or {}, session)

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = RunCafaEvaluationPayload.model_validate(payload)
        inputs = self._load_evaluation_inputs(session, p, emit)
        scoring_snapshot = self._resolve_scoring_snapshot(session, p.scoring_config_id)
        reranker_models, reranker_config_snapshot = load_reranker_models_for_payload(
            session,
            rerankers_nested=p.rerankers,
            reranker_id_nk=p.reranker_id_nk,
            reranker_id_lk=p.reranker_id_lk,
            reranker_id_pk=p.reranker_id_pk,
            emit=emit,
        )
        # Pre-generate result_id so the artifact-store prefix matches the DB row.
        result_id = uuid.uuid4()
        artifact_store = get_artifact_store(load_settings(Path(__file__).resolve().parents[3]))
        ctx = _PipelineCtx(
            inputs=inputs,
            scoring_snapshot=scoring_snapshot,
            reranker_models=reranker_models,
            result_id=result_id,
            artifact_store=artifact_store,
        )
        results = self._run_evaluation_pipeline(session, p, ctx, emit)
        first_reranker_id, reranker_config_snapshot = self._finalize_reranker_config(
            reranker_config_snapshot, reranker_models, p
        )
        eval_result = EvaluationResult(
            id=result_id,
            evaluation_set_id=inputs.eval_set_id,
            prediction_set_id=inputs.pred_set_id,
            scoring_config_id=uuid.UUID(p.scoring_config_id) if p.scoring_config_id else None,
            reranker_model_id=first_reranker_id,
            reranker_config=reranker_config_snapshot,
            results=results,
        )
        session.add(eval_result)
        session.flush()
        emit(
            "run_cafa_evaluation.done",
            None,
            {
                "evaluation_result_id": str(result_id),
                "settings_evaluated": [k for k in results.keys() if k != "artifacts"],
                "artifacts_prefix": f"eval_artifacts/{result_id}/",
                "artifacts_count": len((results.get("artifacts") or {}).get("keys") or []),
            },
            "info",
        )
        return OperationResult(result={"evaluation_result_id": str(result_id), "results": results})

    @staticmethod
    def _load_evaluation_inputs(
        session: Session, p: RunCafaEvaluationPayload, emit: EmitFn
    ) -> _EvalInputs:
        """Validate eval/pred set FKs, compute the delta data, resolve the
        pivot snapshot + terms-of-interest, and emit the start / delta_done
        events that downstream listeners use to gate progress UI."""
        eval_set_id = uuid.UUID(p.evaluation_set_id)
        pred_set_id = uuid.UUID(p.prediction_set_id)
        eval_set = session.get(EvaluationSet, eval_set_id)
        if eval_set is None:
            raise ValueError(f"EvaluationSet {eval_set_id} not found")
        pred_set = session.get(PredictionSet, pred_set_id)
        if pred_set is None:
            raise ValueError(f"PredictionSet {pred_set_id} not found")

        emit("run_cafa_evaluation.computing_delta", None, {}, "info")
        data, pivot_snapshot_id = load_evaluation_data_for_set(session, eval_set)
        snapshot = session.get(OntologySnapshot, pivot_snapshot_id)
        if snapshot is None:
            raise ValueError(f"Pivot OntologySnapshot {pivot_snapshot_id} not found")
        inputs = _EvalInputs(
            eval_set_id=eval_set_id,
            pred_set_id=pred_set_id,
            eval_set=eval_set,
            pred_set=pred_set,
            data=data,
            snapshot=snapshot,
            pivot_snapshot_id=pivot_snapshot_id,
            toi_go_ids=_load_terms_of_interest(session, pivot_snapshot_id),
        )
        _emit_evaluation_setup_events(emit, inputs)
        if data.delta_proteins == 0:
            raise ValueError("No delta proteins found; cannot evaluate")
        return inputs

    @staticmethod
    def _resolve_scoring_snapshot(
        session: Session, scoring_config_id: str | None
    ) -> ScoringConfig | None:
        """Materialise an in-memory snapshot of the ScoringConfig before the
        no-op commit so cafaeval workers don't need a session."""
        if not scoring_config_id:
            return None
        sc = session.get(ScoringConfig, uuid.UUID(scoring_config_id))
        if sc is None:
            raise ValueError(f"ScoringConfig {scoring_config_id} not found")
        return ScoringConfig(formula=sc.formula, weights=dict(sc.weights))

    def _run_evaluation_pipeline(
        self,
        session: Session,
        p: RunCafaEvaluationPayload,
        ctx: _PipelineCtx,
        emit: EmitFn,
    ) -> dict[str, Any]:
        """Stage cafaeval inputs in a tempdir, run the per-setting evaluator,
        upload artifacts, return the merged results dict (with ``artifacts``)."""
        with tempfile.TemporaryDirectory(prefix="protea_cafa_") as tmpdir:
            artifacts_root = Path(tmpdir) / "artifacts"
            artifacts_root.mkdir(parents=True, exist_ok=True)
            run_data = self._stage_evaluator_inputs(session, p, ctx, artifacts_root, emit)
            # No-op commit: releases the DB connection back to the pool before
            # cafaeval forks worker processes via multiprocessing.Pool. Forked
            # children would otherwise inherit SQLAlchemy connection-pool
            # locks, causing an indefinite deadlock on first use. commit()
            # (vs session.close()) keeps ORM objects so BaseWorker can still
            # update job.status after execute().
            session.commit()
            results = evaluate_all_settings(session, ctx=run_data, emit=emit)
            uploaded_keys = self._upload_artifacts(
                ctx.artifact_store, ctx.result_id, artifacts_root, emit
            )
            results["artifacts"] = {"keys": uploaded_keys}
            return results

    def _stage_evaluator_inputs(
        self,
        session: Session,
        p: RunCafaEvaluationPayload,
        ctx: _PipelineCtx,
        artifacts_root: Path,
        emit: EmitFn,
    ) -> CafaEvalRunContext:
        """Download OBO + IA, restrict GT, write GT/TOI/predictions files, and
        bundle every cafaeval input into a ``CafaEvalRunContext``."""
        inputs = ctx.inputs
        tmpdir = str(artifacts_root.parent)
        emit("run_cafa_evaluation.downloading_obo", None, {"url": inputs.snapshot.obo_url}, "info")
        obo_path = os.path.join(tmpdir, "go.obo")
        _artifacts.download_obo(inputs.snapshot.obo_url, obo_path)
        ia_path = self._resolve_ia_file(tmpdir, inputs.snapshot, p.ia_file, emit)

        data = inputs.data
        if p.restrict_gt_to_predicted:
            data = _data.restrict_data_to_predicted(
                session, prediction_set_id=inputs.pred_set_id, data=data, emit=emit
            )
        gt_paths = _data.write_ground_truth_files(artifacts_root, data)
        toi_path = os.path.join(str(artifacts_root), "terms_of_interest.txt")
        _data.write_terms_of_interest(toi_path, inputs.toi_go_ids, emit=emit)

        delta_proteins = set(data.nk) | set(data.lk) | set(data.pk)
        emit(
            "run_cafa_evaluation.writing_predictions",
            None,
            {"delta_proteins": len(delta_proteins)},
            "info",
        )
        has_rerankers = bool(ctx.reranker_models)
        if not has_rerankers:
            self._write_shared_prediction_file(session, p, ctx, artifacts_root, delta_proteins)
        return CafaEvalRunContext(
            pred_set_id=inputs.pred_set_id,
            delta_proteins=delta_proteins,
            max_distance=p.max_distance,
            artifacts_root=artifacts_root,
            has_rerankers=has_rerankers,
            reranker_models=ctx.reranker_models,
            scoring_config_snapshot=ctx.scoring_snapshot,
            data=data,
            obo_path=obo_path,
            nk_path=gt_paths["nk"],
            lk_path=gt_paths["lk"],
            pk_path=gt_paths["pk"],
            pk_known_path=gt_paths["pk_known"],
            ia_path=ia_path,
            toi_path=toi_path,
            shared_pred_dir=os.path.join(str(artifacts_root), "predictions"),
        )

    @staticmethod
    def _write_shared_prediction_file(
        session: Session,
        p: RunCafaEvaluationPayload,
        ctx: _PipelineCtx,
        artifacts_root: Path,
        delta_proteins: set[str],
    ) -> None:
        """Write the single shared predictions.tsv (no-rerankers path)."""
        pred_dir = os.path.join(str(artifacts_root), "predictions")
        os.makedirs(pred_dir, exist_ok=True)
        _artifacts.write_predictions(
            session,
            _artifacts.WritePredictionsContext(
                pred_set_id=ctx.inputs.pred_set_id,
                delta_proteins=delta_proteins,
                max_distance=p.max_distance,
                path=os.path.join(pred_dir, "predictions.tsv"),
            ),
            scoring_config=ctx.scoring_snapshot,
        )

    @staticmethod
    def _resolve_ia_file(
        tmpdir: str, snapshot: OntologySnapshot, payload_ia_file: str | None, emit: EmitFn
    ) -> str | None:
        """Resolve the Information Accretion file for cafaeval.

        Priority: explicit ``payload.ia_file`` > snapshot ``ia_url`` (downloaded
        once) > ``None`` (cafaeval falls back to uniform IC=1, with a warning).
        """
        ia_path: str | None = payload_ia_file
        if ia_path is None and snapshot.ia_url:
            ia_path = os.path.join(tmpdir, "ia.tsv")
            emit("run_cafa_evaluation.downloading_ia", None, {"url": snapshot.ia_url}, "info")
            _artifacts.download_tsv(snapshot.ia_url, ia_path)
        if ia_path:
            emit("run_cafa_evaluation.ia_resolved", None, {"ia_path": ia_path}, "info")
            return ia_path
        emit(
            "run_cafa_evaluation.ia_missing",
            None,
            {
                "warning": "No IA file available; cafaeval will use uniform IC=1 for all "
                "GO terms. Set ia_url on the OntologySnapshot or pass ia_file "
                "in the payload for information-content-weighted metrics.",
            },
            "warning",
        )
        return None

    @staticmethod
    def _upload_artifacts(
        artifact_store: ArtifactStore,
        result_id: uuid.UUID,
        artifacts_root: Path,
        emit: EmitFn,
    ) -> list[str]:
        """Walk ``artifacts_root``, upload every file under the
        ``eval_artifacts/{result_id}/`` prefix, return the list of keys."""
        uploaded_keys: list[str] = []
        for path in sorted(artifacts_root.rglob("*")):
            if not path.is_file():
                continue
            relpath = path.relative_to(artifacts_root).as_posix()
            key = eval_artifact_key(result_id, relpath)
            artifact_store.put(key, path)
            uploaded_keys.append(key)
        emit(
            "run_cafa_evaluation.artifacts_uploaded",
            None,
            {"count": len(uploaded_keys), "prefix": f"eval_artifacts/{result_id}/"},
            "info",
        )
        return uploaded_keys

    @staticmethod
    def _finalize_reranker_config(
        reranker_config_snapshot: dict[str, Any] | None,
        reranker_models: dict[str, Any],
        p: RunCafaEvaluationPayload,
    ) -> tuple[uuid.UUID | None, dict[str, Any] | None]:
        """Pick the representative ``reranker_model_id`` for backwards-compat
        and synthesise the ``reranker_config`` snapshot from the flat
        per-category fields when the nested mapping is empty."""
        first_reranker_id: uuid.UUID | None = None
        if reranker_config_snapshot:
            for _cat_map in reranker_config_snapshot.values():
                for _rid_str in _cat_map.values():
                    first_reranker_id = uuid.UUID(_rid_str)
                    break
                if first_reranker_id:
                    break
            return first_reranker_id, reranker_config_snapshot
        if reranker_models:
            reranker_config_snapshot = {}
            for setting, field in [
                ("nk", p.reranker_id_nk),
                ("lk", p.reranker_id_lk),
                ("pk", p.reranker_id_pk),
            ]:
                if field:
                    reranker_config_snapshot[setting] = {"all": field}
                    if first_reranker_id is None:
                        first_reranker_id = uuid.UUID(field)
        return first_reranker_id, reranker_config_snapshot

    # Backwards-compat shims: tests patch and call these names. The bodies
    # live in ``_run_cafa_artifacts``; these instance methods delegate so
    # ``mock.patch.object(op, "_download_obo")`` and direct
    # ``self.op._download_obo(...)`` invocations continue to work.

    def _download_obo(self, url: str, dest: str) -> None:
        _artifacts.download_obo(url, dest)

    def _download_tsv(self, url: str, dest: str) -> None:
        _artifacts.download_tsv(url, dest)

    def _write_gt(self, annotations: dict[str, set[str]], path: str) -> None:
        _artifacts.write_gt(annotations, path)

    def _write_predictions(self, *args: Any, **kwargs: Any) -> None:
        _artifacts.write_predictions(*args, **kwargs)

    def _write_predictions_reranked(self, *args: Any, **kwargs: Any) -> None:
        _artifacts.write_predictions_reranked(*args, **kwargs)

    def _write_predictions_per_aspect(self, *args: Any, **kwargs: Any) -> None:
        _artifacts.write_predictions_per_aspect(*args, **kwargs)

    def _parse_results(self, dfs_best: dict) -> dict[str, Any]:
        return _artifacts.parse_results(dfs_best)
