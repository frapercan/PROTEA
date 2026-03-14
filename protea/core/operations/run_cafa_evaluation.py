from __future__ import annotations

import os
import signal
import tempfile
import uuid
from pathlib import Path
from typing import Any

import requests
from pydantic import Field, field_validator
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.evaluation import compute_evaluation_data
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.evaluation_result import EvaluationResult
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet

# Namespace labels used by cafaeval OBO parser
_NS_LABELS = {
    "biological_process": "BPO",
    "molecular_function": "MFO",
    "cellular_component": "CCO",
}
_NS_SHORT = {"BPO", "MFO", "CCO"}


class RunCafaEvaluationPayload(ProteaPayload, frozen=True):
    evaluation_set_id: str
    prediction_set_id: str
    max_distance: float | None = Field(default=None, ge=0.0, le=2.0)
    artifacts_dir: str | None = Field(default=None)

    @field_validator("evaluation_set_id", "prediction_set_id", mode="before")
    @classmethod
    def must_be_non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()


class RunCafaEvaluationOperation:
    """Runs the CAFA evaluator against NK, LK and PK settings.

    Steps:
      1. Load EvaluationSet and PredictionSet from DB.
      2. Compute evaluation data (delta NK/LK + known-terms) with full NOT propagation.
      3. Download the OBO file from the ontology snapshot URL.
      4. Write temp files: ground-truth NK/LK, known-terms, predictions (CAFA format).
      5. Call ``cafa_eval`` for each setting (NK, LK, PK).
      6. Parse per-namespace Fmax / precision / recall / coverage from results.
      7. Persist an EvaluationResult row with all metrics.
    """

    name = "run_cafa_evaluation"

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

        ann_old = session.get(AnnotationSet, eval_set.old_annotation_set_id)
        snapshot = session.get(OntologySnapshot, ann_old.ontology_snapshot_id)

        emit("run_cafa_evaluation.start", None, {
            "evaluation_set_id": str(eval_set_id),
            "prediction_set_id": str(pred_set_id),
            "obo_url": snapshot.obo_url,
        }, "info")

        # ── 1. Compute evaluation data ────────────────────────────────────────
        emit("run_cafa_evaluation.computing_delta", None, {}, "info")
        data = compute_evaluation_data(
            session,
            eval_set.old_annotation_set_id,
            eval_set.new_annotation_set_id,
            ann_old.ontology_snapshot_id,
        )
        emit("run_cafa_evaluation.delta_done", None, {
            "nk_proteins": data.nk_proteins,
            "lk_proteins": data.lk_proteins,
        }, "info")

        if data.delta_proteins == 0:
            raise ValueError("No delta proteins found — cannot evaluate")

        # Pre-generate result_id so the artifact directory name matches the DB row.
        result_id = uuid.uuid4()

        # ── 2. Prepare artifact directory (persistent) + temp dir for OBO ─────
        artifacts_root = (
            Path(p.artifacts_dir) / str(result_id)
            if p.artifacts_dir
            else None
        )
        if artifacts_root is not None:
            artifacts_root.mkdir(parents=True, exist_ok=True)

        results: dict[str, Any] = {}
        with tempfile.TemporaryDirectory(prefix="protea_cafa_") as tmpdir:

            # Download OBO into temp dir (large file, not persisted)
            emit("run_cafa_evaluation.downloading_obo", None, {"url": snapshot.obo_url}, "info")
            obo_path = os.path.join(tmpdir, "go.obo")
            self._download_obo(snapshot.obo_url, obo_path)

            # Write ground truth files
            gt_dir = str(artifacts_root) if artifacts_root else tmpdir
            nk_path = os.path.join(gt_dir, "gt_NK.tsv")
            lk_path = os.path.join(gt_dir, "gt_LK.tsv")
            known_path = os.path.join(gt_dir, "known_terms.tsv")

            self._write_gt(data.nk, nk_path)
            self._write_gt(data.lk, lk_path)
            self._write_gt(data.known, known_path)

            # Write predictions (CAFA format) filtered to delta proteins
            pred_dir = os.path.join(gt_dir, "predictions")
            os.makedirs(pred_dir, exist_ok=True)
            pred_path = os.path.join(pred_dir, "predictions.tsv")
            delta_proteins = set(data.nk) | set(data.lk)
            emit("run_cafa_evaluation.writing_predictions", None, {
                "delta_proteins": len(delta_proteins),
            }, "info")
            self._write_predictions(session, pred_set_id, delta_proteins, p.max_distance, pred_path)

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
                ("PK", lk_path, known_path),
            ]:
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
                            obo_path, pred_dir, gt_file,
                            exclude=known_file,
                            prop="max",
                            norm="cafa",
                            n_cpu=1,
                        )
                    finally:
                        signal.signal(signal.SIGTERM, _old_sigterm)
                        signal.signal(signal.SIGINT, _old_sigint)

                    results[setting] = self._parse_results(dfs_best)

                    # Persist full cafaeval output (PR curves + best metrics per metric type)
                    if artifacts_root is not None and df is not None:
                        from cafaeval.evaluation import write_results as _write_results
                        setting_dir = artifacts_root / setting
                        setting_dir.mkdir(exist_ok=True)
                        _write_results(df, dfs_best, str(setting_dir))

                    emit("run_cafa_evaluation.setting_done", None, {
                        "setting": setting,
                        "namespaces": list(results[setting].keys()),
                    }, "info")
                except Exception as exc:
                    emit("run_cafa_evaluation.setting_failed", None, {
                        "setting": setting,
                        "error": str(exc),
                    }, "warning")
                    results[setting] = {}

        # ── 3. Persist EvaluationResult ───────────────────────────────────────
        eval_result = EvaluationResult(
            id=result_id,
            evaluation_set_id=eval_set_id,
            prediction_set_id=pred_set_id,
            results=results,
        )
        session.add(eval_result)
        session.flush()

        emit("run_cafa_evaluation.done", None, {
            "evaluation_result_id": str(result_id),
            "settings_evaluated": list(results.keys()),
            "artifacts_dir": str(artifacts_root) if artifacts_root else None,
        }, "info")
        return OperationResult(result={
            "evaluation_result_id": str(result_id),
            "results": results,
        })

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _download_obo(self, url: str, dest: str) -> None:
        """Download OBO file to dest, decompressing gzip if needed."""
        import gzip
        resp = requests.get(url, stream=True, timeout=300)
        resp.raise_for_status()
        if url.endswith(".gz"):
            with open(dest, "wb") as f:
                f.write(gzip.decompress(resp.content))
        else:
            with open(dest, "w", encoding="utf-8") as f:
                f.write(resp.text)

    def _write_gt(self, annotations: dict[str, set[str]], path: str) -> None:
        """Write {protein: {go_id}} to a 2-column TSV (no header)."""
        with open(path, "w") as f:
            for protein in sorted(annotations):
                for go_id in sorted(annotations[protein]):
                    f.write(f"{protein}\t{go_id}\n")

    def _write_predictions(
        self,
        session: Session,
        pred_set_id: uuid.UUID,
        delta_proteins: set[str],
        max_distance: float | None,
        path: str,
    ) -> None:
        """Write CAFA-format predictions (protein\\tgo_id\\tscore) for delta proteins."""
        q = (
            session.query(GOPrediction, GOTerm)
            .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
            .filter(GOPrediction.prediction_set_id == pred_set_id)
            .filter(GOPrediction.protein_accession.in_(delta_proteins))
        )
        if max_distance is not None:
            q = q.filter(GOPrediction.distance <= max_distance)
        q = q.order_by(GOPrediction.protein_accession, GOTerm.go_id, GOPrediction.distance)

        seen: set[tuple[str, str]] = set()
        with open(path, "w") as f:
            for pred, gt in q.yield_per(1000):
                key = (pred.protein_accession, gt.go_id)
                if key in seen:
                    continue
                seen.add(key)
                score = max(0.0, 1.0 - pred.distance)
                f.write(f"{pred.protein_accession}\t{gt.go_id}\t{score:.4f}\n")

    def _parse_results(self, dfs_best: dict) -> dict[str, Any]:
        """Extract per-namespace Fmax metrics from cafaeval dfs_best."""
        ns_results: dict[str, Any] = {}

        df_f = dfs_best.get("f")
        if df_f is None or df_f.empty:
            return ns_results

        df_f = df_f.reset_index()
        for _, row in df_f.iterrows():
            ns_long = str(row.get("ns", ""))
            ns = _NS_LABELS.get(ns_long)
            if ns is None:
                continue
            ns_results[ns] = {
                "fmax": round(float(row.get("f", 0)), 4),
                "precision": round(float(row.get("pr", 0)), 4),
                "recall": round(float(row.get("rc", 0)), 4),
                "tau": round(float(row.get("tau", 0)), 4),
                "coverage": round(float(row.get("cov_max", row.get("cov", 0))), 4),
                "n_proteins": int(row.get("n", 0)) if "n" in row else None,
            }

        return ns_results
