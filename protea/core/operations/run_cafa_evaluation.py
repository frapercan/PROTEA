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
from protea.core.scoring import compute_score
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.evaluation_result import EvaluationResult
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel as RerankerModelORM
from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig

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
    scoring_config_id: str | None = Field(default=None)
    reranker_id_nk: str | None = Field(default=None)
    reranker_id_lk: str | None = Field(default=None)
    reranker_id_pk: str | None = Field(default=None)
    rerankers: dict[str, dict[str, str]] | None = Field(
        default=None,
        description=(
            "Nested mapping of category → aspect → reranker_model_id. "
            "E.g. {\"nk\": {\"bpo\": \"uuid\", \"mfo\": \"uuid\"}, \"lk\": {...}}. "
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
      4. Resolve the Information Accretion (IA) file:
           - If ``ia_file`` is set in the payload, use that path directly.
           - Otherwise, if the OntologySnapshot has an ``ia_url``, download it to
             a temporary file and pass it to cafaeval.
           - If neither is set, cafaeval runs with uniform IC=1 for all terms.
         IA weights make rare, specific GO terms count more than common ones and
         are strongly recommended for publishable evaluations.  Each CAFA benchmark
         ships its own IA file (e.g. ``IA_cafa6.tsv``); store its URL in the
         corresponding OntologySnapshot so future evaluations pick it up
         automatically without touching the job payload.
      5. Write temp files: ground-truth NK/LK, known-terms, predictions (CAFA format).
      6. Call ``cafa_eval`` for each setting (NK, LK, PK).
      7. Parse per-namespace Fmax / precision / recall / coverage from results.
      8. Persist an EvaluationResult row with all metrics.
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

        emit(
            "run_cafa_evaluation.start",
            None,
            {
                "evaluation_set_id": str(eval_set_id),
                "prediction_set_id": str(pred_set_id),
                "obo_url": snapshot.obo_url,
            },
            "info",
        )

        # ── 1. Compute evaluation data ────────────────────────────────────────
        emit("run_cafa_evaluation.computing_delta", None, {}, "info")
        data = compute_evaluation_data(
            session,
            eval_set.old_annotation_set_id,
            eval_set.new_annotation_set_id,
            ann_old.ontology_snapshot_id,
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

        # Load per-category (and optionally per-aspect) reranker models before session commit.
        # reranker_models: setting → aspect → model_data  (aspect="" means single model for all aspects)
        reranker_models: dict[str, dict[str, str]] = {}
        reranker_config_snapshot: dict[str, dict[str, str]] | None = None  # for persisting in EvaluationResult

        if p.rerankers:
            # New nested mapping: {"nk": {"bpo": "uuid", "mfo": "uuid", ...}, ...}
            reranker_config_snapshot = {}
            _aspect_map = {"bpo": "P", "mfo": "F", "cco": "C"}
            for cat_key, aspect_map in p.rerankers.items():
                setting = cat_key.upper()
                reranker_models[setting] = {}
                reranker_config_snapshot[cat_key] = {}
                for aspect_key, rid_str in aspect_map.items():
                    rid = uuid.UUID(rid_str)
                    rm = session.get(RerankerModelORM, rid)
                    if rm is None:
                        raise ValueError(f"RerankerModel {rid_str} not found")
                    aspect_char = _aspect_map.get(aspect_key, aspect_key)
                    reranker_models[setting][aspect_char] = rm.model_data
                    reranker_config_snapshot[cat_key][aspect_key] = rid_str
                    emit("run_cafa_evaluation.reranker_loaded", None, {
                        "setting": setting, "aspect": aspect_key,
                        "reranker_id": str(rid), "name": rm.name,
                    }, "info")
        else:
            # Legacy flat fields: one model per category (all aspects)
            for setting, field in [("NK", p.reranker_id_nk), ("LK", p.reranker_id_lk), ("PK", p.reranker_id_pk)]:
                if field:
                    rid = uuid.UUID(field)
                    rm = session.get(RerankerModelORM, rid)
                    if rm is None:
                        raise ValueError(f"RerankerModel {field} not found")
                    reranker_models[setting] = {"": rm.model_data}  # "" = all aspects
                    emit("run_cafa_evaluation.reranker_loaded", None, {"setting": setting, "reranker_id": str(rid), "name": rm.name}, "info")

        # Pre-generate result_id so the artifact directory name matches the DB row.
        result_id = uuid.uuid4()

        # ── 2. Prepare artifact directory (persistent) + temp dir for OBO ─────
        artifacts_root = Path(p.artifacts_dir) / str(result_id) if p.artifacts_dir else None
        if artifacts_root is not None:
            artifacts_root.mkdir(parents=True, exist_ok=True)

        results: dict[str, Any] = {}
        with tempfile.TemporaryDirectory(prefix="protea_cafa_") as tmpdir:
            # Download OBO into temp dir (large file, not persisted)
            emit("run_cafa_evaluation.downloading_obo", None, {"url": snapshot.obo_url}, "info")
            obo_path = os.path.join(tmpdir, "go.obo")
            self._download_obo(snapshot.obo_url, obo_path)

            # Resolve IA file: explicit payload path > snapshot ia_url > None (uniform IC).
            # Priority: an explicit ia_file in the payload overrides the snapshot URL so
            # that one-off experiments can use a custom IA without touching the snapshot.
            # When ia_file is absent but the snapshot carries an ia_url, the file is
            # downloaded once into tmpdir and used for all three settings (NK/LK/PK).
            ia_path: str | None = p.ia_file
            if ia_path is None and snapshot.ia_url:
                ia_path = os.path.join(tmpdir, "ia.tsv")
                emit("run_cafa_evaluation.downloading_ia", None, {"url": snapshot.ia_url}, "info")
                self._download_tsv(snapshot.ia_url, ia_path)
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

            # Write ground truth files
            gt_dir = str(artifacts_root) if artifacts_root else tmpdir
            nk_path = os.path.join(gt_dir, "gt_NK.tsv")
            lk_path = os.path.join(gt_dir, "gt_LK.tsv")
            pk_path = os.path.join(gt_dir, "gt_PK.tsv")
            known_path = os.path.join(gt_dir, "known_terms.tsv")
            pk_known_path = os.path.join(gt_dir, "pk_known_terms.tsv")

            self._write_gt(data.nk, nk_path)
            self._write_gt(data.lk, lk_path)
            self._write_gt(data.pk, pk_path)
            self._write_gt(data.known, known_path)
            self._write_gt(data.pk_known, pk_known_path)

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
                self._write_predictions(
                    session, pred_set_id, delta_proteins, p.max_distance,
                    pred_path, scoring_config_snapshot,
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
                    if "" in rr_aspect_map:
                        # Single model for all aspects (legacy flat field)
                        self._write_predictions(
                            session, pred_set_id, delta_proteins, p.max_distance,
                            pred_path, scoring_config_snapshot,
                            reranker_model_str=rr_aspect_map[""],
                        )
                    else:
                        # Per-aspect models
                        self._write_predictions_per_aspect(
                            session, pred_set_id, delta_proteins, p.max_distance,
                            pred_path, rr_aspect_map,
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
            for setting, field in [("nk", p.reranker_id_nk), ("lk", p.reranker_id_lk), ("pk", p.reranker_id_pk)]:
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
                "settings_evaluated": list(results.keys()),
                "artifacts_dir": str(artifacts_root) if artifacts_root else None,
            },
            "info",
        )
        return OperationResult(
            result={
                "evaluation_result_id": str(result_id),
                "results": results,
            }
        )

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

    def _download_tsv(self, url: str, dest: str) -> None:
        """Copy or download a plain-text TSV file (gzip-transparent) to dest.

        Accepts both HTTP(S) URLs and local filesystem paths (absolute or
        ``file://`` scheme).  Local paths are resolved without any network
        request, which is useful during development when the IA file lives
        inside the repository (``data/benchmarks/IA_cafa6.tsv``) and
        ``ia_url`` is set to its absolute path.  Once the file is pushed to
        GitHub the URL can be switched to the raw.githubusercontent.com
        address and the same code path handles it transparently.
        """
        import gzip as _gzip
        import shutil

        # Resolve local paths (absolute or file:// scheme) without HTTP.
        local_path: str | None = None
        if url.startswith("file://"):
            local_path = url[len("file://") :]
        elif url.startswith("/"):
            local_path = url

        if local_path is not None:
            if url.endswith(".gz"):
                with _gzip.open(local_path, "rb") as src, open(dest, "wb") as f:
                    shutil.copyfileobj(src, f)
            else:
                shutil.copy2(local_path, dest)
            return

        resp = requests.get(url, stream=True, timeout=300)
        resp.raise_for_status()
        if url.endswith(".gz"):
            with open(dest, "wb") as f:
                f.write(_gzip.decompress(resp.content))
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
        scoring_config: ScoringConfig | None = None,
        reranker_model_str: str | None = None,
    ) -> None:
        """Write CAFA-format predictions (protein\\tgo_id\\tscore) for delta proteins.

        Scoring priority:
          1. If ``reranker_model_str`` is provided, apply the LightGBM model to
             all predictions and use re-ranker probabilities as scores.
          2. If a ``ScoringConfig`` is provided, compute scores via ``compute_score()``.
          3. Otherwise fall back to ``1 - cosine_distance / 2``.
        """
        if reranker_model_str is not None:
            self._write_predictions_reranked(
                session, pred_set_id, delta_proteins, max_distance, path, reranker_model_str,
            )
            return

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
                if scoring_config is not None:
                    pred_dict = {
                        "distance": pred.distance,
                        "identity_nw": pred.identity_nw,
                        "identity_sw": pred.identity_sw,
                        "evidence_code": pred.evidence_code,
                        "taxonomic_distance": pred.taxonomic_distance,
                    }
                    score = compute_score(pred_dict, scoring_config)
                else:
                    score = max(0.0, 1.0 - (pred.distance or 0.0) / 2.0)
                f.write(f"{pred.protein_accession}\t{gt.go_id}\t{score:.4f}\n")

    def _write_predictions_reranked(
        self,
        session: Session,
        pred_set_id: uuid.UUID,
        delta_proteins: set[str],
        max_distance: float | None,
        path: str,
        reranker_model_str: str,
    ) -> None:
        """Write CAFA-format predictions using LightGBM re-ranker scores."""
        import pandas as pd

        from protea.core.reranker import model_from_string, predict as reranker_predict

        q = (
            session.query(GOPrediction, GOTerm.go_id)
            .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
            .filter(GOPrediction.prediction_set_id == pred_set_id)
            .filter(GOPrediction.protein_accession.in_(delta_proteins))
        )
        if max_distance is not None:
            q = q.filter(GOPrediction.distance <= max_distance)

        records: list[dict[str, Any]] = []
        for pred, go_id in q.yield_per(5000):
            records.append({
                "protein_accession": pred.protein_accession,
                "go_id": go_id,
                "distance": pred.distance,
                "qualifier": pred.qualifier or "",
                "evidence_code": pred.evidence_code or "",
                "identity_nw": pred.identity_nw,
                "similarity_nw": pred.similarity_nw,
                "alignment_score_nw": pred.alignment_score_nw,
                "gaps_pct_nw": pred.gaps_pct_nw,
                "alignment_length_nw": pred.alignment_length_nw,
                "identity_sw": pred.identity_sw,
                "similarity_sw": pred.similarity_sw,
                "alignment_score_sw": pred.alignment_score_sw,
                "gaps_pct_sw": pred.gaps_pct_sw,
                "alignment_length_sw": pred.alignment_length_sw,
                "length_query": pred.length_query,
                "length_ref": pred.length_ref,
                "query_taxonomy_id": pred.query_taxonomy_id,
                "ref_taxonomy_id": pred.ref_taxonomy_id,
                "taxonomic_lca": pred.taxonomic_lca,
                "taxonomic_distance": pred.taxonomic_distance,
                "taxonomic_common_ancestors": pred.taxonomic_common_ancestors,
                "taxonomic_relation": pred.taxonomic_relation or "",
                "vote_count": pred.vote_count,
                "k_position": pred.k_position,
                "go_term_frequency": pred.go_term_frequency,
                "ref_annotation_density": pred.ref_annotation_density,
                "neighbor_distance_std": pred.neighbor_distance_std,
            })

        if not records:
            with open(path, "w") as f:
                pass
            return

        df = pd.DataFrame(records)
        model = model_from_string(reranker_model_str)
        scores = reranker_predict(model, df)

        # Deduplicate: keep highest score per (protein, go_id)
        df["score"] = scores
        df = df.sort_values("score", ascending=False).drop_duplicates(
            subset=["protein_accession", "go_id"], keep="first",
        )

        with open(path, "w") as f:
            for _, row in df.iterrows():
                f.write(f"{row['protein_accession']}\t{row['go_id']}\t{row['score']:.4f}\n")

    def _write_predictions_per_aspect(
        self,
        session: Session,
        pred_set_id: uuid.UUID,
        delta_proteins: set[str],
        max_distance: float | None,
        path: str,
        aspect_models: dict[str, str],
    ) -> None:
        """Write CAFA-format predictions applying per-aspect LightGBM models.

        ``aspect_models`` maps GO aspect char (P/F/C) to model_data strings.
        Predictions whose aspect has no model fall back to ``1 - distance/2``.
        """
        import pandas as pd

        from protea.core.reranker import model_from_string, predict as reranker_predict

        q = (
            session.query(GOPrediction, GOTerm.go_id, GOTerm.aspect)
            .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
            .filter(GOPrediction.prediction_set_id == pred_set_id)
            .filter(GOPrediction.protein_accession.in_(delta_proteins))
        )
        if max_distance is not None:
            q = q.filter(GOPrediction.distance <= max_distance)

        records: list[dict[str, Any]] = []
        for pred, go_id, aspect in q.yield_per(5000):
            records.append({
                "protein_accession": pred.protein_accession,
                "go_id": go_id,
                "aspect": aspect or "",
                "distance": pred.distance,
                "qualifier": pred.qualifier or "",
                "evidence_code": pred.evidence_code or "",
                "identity_nw": pred.identity_nw,
                "similarity_nw": pred.similarity_nw,
                "alignment_score_nw": pred.alignment_score_nw,
                "gaps_pct_nw": pred.gaps_pct_nw,
                "alignment_length_nw": pred.alignment_length_nw,
                "identity_sw": pred.identity_sw,
                "similarity_sw": pred.similarity_sw,
                "alignment_score_sw": pred.alignment_score_sw,
                "gaps_pct_sw": pred.gaps_pct_sw,
                "alignment_length_sw": pred.alignment_length_sw,
                "length_query": pred.length_query,
                "length_ref": pred.length_ref,
                "query_taxonomy_id": pred.query_taxonomy_id,
                "ref_taxonomy_id": pred.ref_taxonomy_id,
                "taxonomic_lca": pred.taxonomic_lca,
                "taxonomic_distance": pred.taxonomic_distance,
                "taxonomic_common_ancestors": pred.taxonomic_common_ancestors,
                "taxonomic_relation": pred.taxonomic_relation or "",
                "vote_count": pred.vote_count,
                "k_position": pred.k_position,
                "go_term_frequency": pred.go_term_frequency,
                "ref_annotation_density": pred.ref_annotation_density,
                "neighbor_distance_std": pred.neighbor_distance_std,
            })

        if not records:
            with open(path, "w") as f:
                pass
            return

        df = pd.DataFrame(records)

        # Score each aspect group with its corresponding model
        df["score"] = 0.0
        for aspect_char, model_str in aspect_models.items():
            mask = df["aspect"] == aspect_char
            if not mask.any():
                continue
            model = model_from_string(model_str)
            df.loc[mask, "score"] = reranker_predict(model, df.loc[mask])

        # Fallback for aspects without a model
        modeled_aspects = set(aspect_models.keys())
        fallback_mask = ~df["aspect"].isin(modeled_aspects)
        if fallback_mask.any():
            df.loc[fallback_mask, "score"] = df.loc[fallback_mask, "distance"].apply(
                lambda d: max(0.0, 1.0 - (d or 0.0) / 2.0)
            )

        # Deduplicate: keep highest score per (protein, go_id)
        df = df.sort_values("score", ascending=False).drop_duplicates(
            subset=["protein_accession", "go_id"], keep="first",
        )

        with open(path, "w") as f:
            for _, row in df.iterrows():
                f.write(f"{row['protein_accession']}\t{row['go_id']}\t{row['score']:.4f}\n")

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
