from __future__ import annotations

import os
import signal
import tempfile
import uuid
from pathlib import Path
from typing import Any

from pydantic import Field, field_validator
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.evaluation import load_evaluation_data_for_set
from protea.core.operations import _run_cafa_artifacts as _artifacts

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
from protea.infrastructure.orm.models.annotation.evaluation_result import EvaluationResult
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store


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
        p = payload or {}
        bits: list[str] = []

        pred_id_raw = p.get("prediction_set_id")
        if pred_id_raw and session is not None:
            try:
                pred = session.get(PredictionSet, uuid.UUID(str(pred_id_raw)))
            except Exception:
                pred = None
            if pred is not None and pred.embedding_config_id is not None:
                from protea.infrastructure.orm.models.embedding.embedding_config import (
                    EmbeddingConfig,
                )

                cfg = session.get(EmbeddingConfig, pred.embedding_config_id)
                if cfg is not None:
                    label = cfg.display_name or cfg.model_name or str(cfg.id)[:8]
                    bits.append(label)
            if pred is None:
                bits.append(f"pred={str(pred_id_raw)[:8]}")
        elif pred_id_raw:
            bits.append(f"pred={str(pred_id_raw)[:8]}")

        eval_id_raw = p.get("evaluation_set_id")
        if eval_id_raw and session is not None:
            try:
                ev = session.get(EvaluationSet, uuid.UUID(str(eval_id_raw)))
            except Exception:
                ev = None
            if ev is not None:
                old_v = getattr(ev, "old_source_version", None) or "?"
                new_v = getattr(ev, "new_source_version", None) or "?"
                bits.append(f"eval={old_v}→{new_v}")
        elif eval_id_raw:
            bits.append(f"eval={str(eval_id_raw)[:8]}")

        scoring_id_raw = p.get("scoring_config_id")
        if scoring_id_raw and session is not None:
            try:
                sc = session.get(ScoringConfig, uuid.UUID(str(scoring_id_raw)))
            except Exception:
                sc = None
            if sc is not None:
                bits.append(f"scoring={sc.name}")
        elif scoring_id_raw:
            bits.append(f"scoring={str(scoring_id_raw)[:8]}")

        if p.get("reranker_model_id"):
            bits.append("+reranker")
        if p.get("max_distance") is not None:
            bits.append(f"max_d={p['max_distance']}")
        return " · ".join(bits)

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        from cafaeval.evaluation import cafa_eval

        p = RunCafaEvaluationPayload.model_validate(payload)

        eval_set_id = uuid.UUID(p.evaluation_set_id)
        pred_set_id = uuid.UUID(p.prediction_set_id)

        eval_set = session.get(EvaluationSet, eval_set_id)
        if eval_set is None:
            raise ValueError(f"EvaluationSet {eval_set_id} not found")

        pred_set = session.get(PredictionSet, pred_set_id)
        if pred_set is None:
            raise ValueError(f"PredictionSet {pred_set_id} not found")

        # ── 1. Compute evaluation data (dispatches same-snapshot vs reconciled) ─
        emit("run_cafa_evaluation.computing_delta", None, {}, "info")
        data, pivot_snapshot_id = load_evaluation_data_for_set(session, eval_set)
        snapshot = session.get(OntologySnapshot, pivot_snapshot_id)
        if snapshot is None:
            raise ValueError(f"Pivot OntologySnapshot {pivot_snapshot_id} not found")

        # Terms-of-interest (CAFA6 -toi): all GO terms in the pivot graph.
        # cafaeval restricts evaluation to this set so that terms not in the
        # frozen graph (e.g. new since t-1) are excluded from scoring.
        toi_go_ids: list[str] = [
            gid
            for (gid,) in session.query(GOTerm.go_id)
            .filter(GOTerm.ontology_snapshot_id == pivot_snapshot_id)
            .all()
        ]

        emit(
            "run_cafa_evaluation.start",
            None,
            {
                "evaluation_set_id": str(eval_set_id),
                "prediction_set_id": str(pred_set_id),
                "pivot_ontology_snapshot_id": str(pivot_snapshot_id),
                "mode": (eval_set.stats or {}).get("mode", "same_snapshot"),
                "obo_url": snapshot.obo_url,
            },
            "info",
        )
        emit(
            "run_cafa_evaluation.delta_done",
            None,
            {
                "nk_proteins": data.nk_proteins,
                "lk_proteins": data.lk_proteins,
                "pk_proteins": data.pk_proteins,
            },
            "info",
        )

        if data.delta_proteins == 0:
            raise ValueError("No delta proteins found — cannot evaluate")

        # Load and snapshot ScoringConfig before the no-op commit below
        scoring_config_snapshot: ScoringConfig | None = None
        if p.scoring_config_id:
            sc = session.get(ScoringConfig, uuid.UUID(p.scoring_config_id))
            if sc is None:
                raise ValueError(f"ScoringConfig {p.scoring_config_id} not found")
            scoring_config_snapshot = ScoringConfig(
                formula=sc.formula,
                weights=dict(sc.weights),
            )

        # Load per-category (and optionally per-aspect) reranker models before
        # session commit. Body lives in
        # ``_run_cafa_reranker_loader.load_reranker_models_for_payload``.
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

        project_root = Path(__file__).resolve().parents[3]
        artifact_store = get_artifact_store(load_settings(project_root))
        uploaded_keys: list[str] = []

        results: dict[str, Any] = {}
        with tempfile.TemporaryDirectory(prefix="protea_cafa_") as tmpdir:
            # Persistent staging dir for cafaeval outputs (uploaded to MinIO at the
            # end of each setting). Lives inside tmpdir so it vanishes on exit.
            artifacts_root = Path(tmpdir) / "artifacts"
            artifacts_root.mkdir(parents=True, exist_ok=True)
            # Download OBO into temp dir (large file, not persisted)
            emit("run_cafa_evaluation.downloading_obo", None, {"url": snapshot.obo_url}, "info")
            obo_path = os.path.join(tmpdir, "go.obo")
            _artifacts.download_obo(snapshot.obo_url, obo_path)

            # Resolve IA file: explicit payload path > snapshot ia_url > None (uniform IC).
            # Priority: an explicit ia_file in the payload overrides the snapshot URL so
            # that one-off experiments can use a custom IA without touching the snapshot.
            # When ia_file is absent but the snapshot carries an ia_url, the file is
            # downloaded once into tmpdir and used for all three settings (NK/LK/PK).
            ia_path: str | None = p.ia_file
            if ia_path is None and snapshot.ia_url:
                ia_path = os.path.join(tmpdir, "ia.tsv")
                emit("run_cafa_evaluation.downloading_ia", None, {"url": snapshot.ia_url}, "info")
                _artifacts.download_tsv(snapshot.ia_url, ia_path)
            if ia_path:
                emit("run_cafa_evaluation.ia_resolved", None, {"ia_path": ia_path}, "info")
            else:
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

            # Restrict GT to the actually-predicted protein cohort. Without this,
            # delta proteins outside the PredictionSet's query coverage hurt
            # Fmax / coverage despite the booster being unable to score them.
            if p.restrict_gt_to_predicted:
                from sqlalchemy import distinct, select

                from protea.infrastructure.orm.models.embedding.go_prediction import (
                    GOPrediction as _GP,
                )

                predicted_set: set[str] = set(
                    session.execute(
                        select(distinct(_GP.protein_accession)).where(
                            _GP.prediction_set_id == pred_set_id
                        )
                    )
                    .scalars()
                    .all()
                )
                _orig_counts = (len(data.nk), len(data.lk), len(data.pk))
                data = type(data)(
                    nk={k: v for k, v in data.nk.items() if k in predicted_set},
                    lk={k: v for k, v in data.lk.items() if k in predicted_set},
                    pk={k: v for k, v in data.pk.items() if k in predicted_set},
                    pk_known={k: v for k, v in data.pk_known.items() if k in predicted_set},
                    known={k: v for k, v in data.known.items() if k in predicted_set},
                )
                emit(
                    "run_cafa_evaluation.gt_restricted_to_predicted",
                    None,
                    {
                        "predicted_proteins": len(predicted_set),
                        "nk_before": _orig_counts[0],
                        "nk_after": len(data.nk),
                        "lk_before": _orig_counts[1],
                        "lk_after": len(data.lk),
                        "pk_before": _orig_counts[2],
                        "pk_after": len(data.pk),
                    },
                    "info",
                )

            # Write ground truth files into the staging artifacts root.
            gt_dir = str(artifacts_root)
            nk_path = os.path.join(gt_dir, "gt_NK.tsv")
            lk_path = os.path.join(gt_dir, "gt_LK.tsv")
            pk_path = os.path.join(gt_dir, "gt_PK.tsv")
            known_path = os.path.join(gt_dir, "known_terms.tsv")
            pk_known_path = os.path.join(gt_dir, "pk_known_terms.tsv")

            _artifacts.write_gt(data.nk, nk_path)
            _artifacts.write_gt(data.lk, lk_path)
            _artifacts.write_gt(data.pk, pk_path)
            _artifacts.write_gt(data.known, known_path)
            _artifacts.write_gt(data.pk_known, pk_known_path)

            # Write terms-of-interest file (CAFA6 -toi flag).
            toi_path = os.path.join(gt_dir, "terms_of_interest.txt")
            with open(toi_path, "w") as f:
                for go_id in sorted(toi_go_ids):
                    f.write(f"{go_id}\n")
            emit(
                "run_cafa_evaluation.toi_written",
                None,
                {"toi_terms": len(toi_go_ids), "path": toi_path},
                "info",
            )

            delta_proteins = set(data.nk) | set(data.lk) | set(data.pk)
            emit(
                "run_cafa_evaluation.writing_predictions",
                None,
                {"delta_proteins": len(delta_proteins)},
                "info",
            )

            # If any reranker is set, write per-setting prediction files;
            # otherwise write a single shared file.
            has_rerankers = bool(reranker_models)
            if not has_rerankers:
                pred_dir = os.path.join(gt_dir, "predictions")
                os.makedirs(pred_dir, exist_ok=True)
                pred_path = os.path.join(pred_dir, "predictions.tsv")
                _artifacts.write_predictions(
                    session,
                    pred_set_id,
                    delta_proteins,
                    p.max_distance,
                    pred_path,
                    scoring_config_snapshot,
                )

            # No-op commit: releases the DB connection back to the pool before
            # cafaeval forks worker processes via multiprocessing.Pool.  Forked
            # children would otherwise inherit SQLAlchemy connection-pool locks
            # held by other threads, causing an indefinite deadlock on first use.
            # Unlike session.close(), commit() keeps all ORM objects in the
            # session so BaseWorker can still update job.status after execute().
            session.commit()

            # Run evaluator for each setting
            for setting, gt_file, known_file in [
                ("NK", nk_path, None),
                ("LK", lk_path, None),
                ("PK", pk_path, pk_known_path),
            ]:
                # Write per-setting predictions if this setting has a reranker
                if has_rerankers:
                    pred_dir = os.path.join(gt_dir, f"predictions_{setting}")
                    os.makedirs(pred_dir, exist_ok=True)
                    pred_path = os.path.join(pred_dir, "predictions.tsv")
                    rr_aspect_map = reranker_models.get(setting, {})
                    # anc2vec_query_known_* is only meaningful when the query
                    # has pre-cutoff annotations (LK / PK).  NK proteins have
                    # nothing "known", so training/serving parity requires
                    # leaving the features at their predict-time NaN / 0.
                    setting_known = data.known if setting in ("LK", "PK") else None
                    if "" in rr_aspect_map:
                        # Single model for all aspects (legacy flat field)
                        bundle = rr_aspect_map[""]
                        _artifacts.write_predictions(
                            session,
                            pred_set_id,
                            delta_proteins,
                            p.max_distance,
                            pred_path,
                            scoring_config_snapshot,
                            reranker_model_str=bundle["model"],
                            reranker_cat_codes=bundle.get("cat_codes"),
                            known_gos=setting_known,
                        )
                    else:
                        # Per-aspect models
                        _artifacts.write_predictions_per_aspect(
                            session,
                            pred_set_id,
                            delta_proteins,
                            p.max_distance,
                            pred_path,
                            rr_aspect_map,  # bundle dicts now
                            known_gos=setting_known,
                        )
                emit("run_cafa_evaluation.evaluating", None, {"setting": setting}, "info")
                try:
                    # Reset SIGTERM/SIGINT to defaults before cafaeval forks pool
                    # workers.  Our _handle_stop handler only sets a flag without
                    # calling sys.exit(), so forked children would ignore SIGTERM
                    # from pool.terminate() and pool.join() would block forever.
                    _old_sigterm = signal.signal(signal.SIGTERM, signal.SIG_DFL)
                    _old_sigint = signal.signal(signal.SIGINT, signal.SIG_DFL)
                    try:
                        df, dfs_best = cafa_eval(
                            obo_path,
                            pred_dir,
                            gt_file,
                            ia=ia_path,
                            exclude=known_file,
                            prop="fill",
                            norm="cafa",
                            no_orphans=True,
                            toi_file=toi_path,
                            max_terms=500,
                            th_step=0.001,
                            n_cpu=1,
                        )
                    finally:
                        signal.signal(signal.SIGTERM, _old_sigterm)
                        signal.signal(signal.SIGINT, _old_sigint)

                    results[setting] = _artifacts.parse_results(dfs_best)

                    # Persist full cafaeval output (PR curves + best metrics per metric type)
                    if df is not None:
                        from cafaeval.evaluation import write_results as _write_results

                        setting_dir = artifacts_root / setting
                        setting_dir.mkdir(exist_ok=True)
                        _write_results(df, dfs_best, str(setting_dir))

                    emit(
                        "run_cafa_evaluation.setting_done",
                        None,
                        {
                            "setting": setting,
                            "namespaces": list(results[setting].keys()),
                        },
                        "info",
                    )
                except Exception as exc:
                    emit(
                        "run_cafa_evaluation.setting_failed",
                        None,
                        {
                            "setting": setting,
                            "error": str(exc),
                        },
                        "warning",
                    )
                    results[setting] = {}

            # ── 2b. Upload all staged artifacts to the artifact store ────────
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

        results["artifacts"] = {"keys": uploaded_keys}

        # ── 3. Persist EvaluationResult ───────────────────────────────────────
        # For backwards compat, pick a single representative reranker_model_id
        first_reranker_id: uuid.UUID | None = None
        if reranker_config_snapshot:
            for _cat_map in reranker_config_snapshot.values():
                for _rid_str in _cat_map.values():
                    first_reranker_id = uuid.UUID(_rid_str)
                    break
                if first_reranker_id:
                    break
        elif reranker_models:
            # Flat per-category fields: build config snapshot and pick first ID
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

        eval_result = EvaluationResult(
            id=result_id,
            evaluation_set_id=eval_set_id,
            prediction_set_id=pred_set_id,
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
                "artifacts_count": len(uploaded_keys),
            },
            "info",
        )
        return OperationResult(
            result={
                "evaluation_result_id": str(result_id),
                "results": results,
            }
        )

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
