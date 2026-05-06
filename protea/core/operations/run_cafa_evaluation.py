from __future__ import annotations

import os
import signal
import tempfile
import uuid
from pathlib import Path
from typing import Any

import numpy as np
import requests
from pydantic import Field, field_validator
from sqlalchemy.orm import Session

from protea.core.anc2vec_embeddings import get_index as get_anc2vec_index
from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.domain.aspect import ASPECT_CAFA_CODES, Aspect
from protea.core.evaluation import load_evaluation_data_for_set
from protea.core.reranker import load_reranker
from protea.core.scoring import compute_score
from protea.infrastructure.orm.models.annotation.evaluation_result import EvaluationResult
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.reranker_model import (
    RerankerModel as RerankerModelORM,
)
from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig
from protea.infrastructure.settings import load_settings
from protea.infrastructure.settings import load_settings as _load_settings_for_reranker
from protea.infrastructure.storage import get_artifact_store
from protea.infrastructure.storage import get_artifact_store as _get_store_for_reranker


def eval_artifact_key(result_id: uuid.UUID, relpath: str) -> str:
    """Canonical MinIO/artifact-store key for a cafaeval output file."""
    return f"eval_artifacts/{result_id}/{relpath.lstrip('/')}"

# Namespace labels used by cafaeval OBO parser. The full names come from
# the obo file; we map them to PROTEA's canonical CAFA codes.
_NS_LABELS: dict[str, str] = {
    "biological_process": Aspect.BIOLOGICAL_PROCESS.cafa,
    "molecular_function": Aspect.MOLECULAR_FUNCTION.cafa,
    "cellular_component": Aspect.CELLULAR_COMPONENT.cafa,
}
_NS_SHORT: set[str] = set(ASPECT_CAFA_CODES)


# Feature columns read straight off the GOPrediction ORM into the reranker
# DataFrame. Kept local (not imported from reranker.py) so this module does
# not pay the LightGBM import cost when no reranker is configured.
_NUMERIC_ORM_COLS: tuple[str, ...] = (
    "distance",
    "identity_nw",
    "similarity_nw",
    "alignment_score_nw",
    "gaps_pct_nw",
    "alignment_length_nw",
    "identity_sw",
    "similarity_sw",
    "alignment_score_sw",
    "gaps_pct_sw",
    "alignment_length_sw",
    "length_query",
    "length_ref",
    "taxonomic_distance",
    "taxonomic_common_ancestors",
    "vote_count",
    "k_position",
    "go_term_frequency",
    "ref_annotation_density",
    "neighbor_distance_std",
    "neighbor_vote_fraction",
    "neighbor_min_distance",
    "neighbor_mean_distance",
    "anc2vec_neighbor_cos",
    "anc2vec_neighbor_maxcos",
    "anc2vec_has_emb",
    "anc2vec_query_known_cos",
    "anc2vec_query_known_maxcos",
    "anc2vec_query_known_count",
    "tax_voters_same_frac",
    "tax_voters_close_frac",
    "tax_voters_mean_common_ancestors",
    *(f"emb_pca_query_{i}" for i in range(16)),
)


def _record_from_pred(
    pred: GOPrediction,
    go_id: str,
    aspect: str | None = None,
) -> dict[str, Any]:
    """Extract a reranker-ready record from a GOPrediction ORM instance.

    ``aspect`` is only needed when the caller routes by aspect (per-aspect
    models). For category-level reranking pass ``None``.
    """
    record: dict[str, Any] = {
        "protein_accession": pred.protein_accession,
        "go_id": go_id,
        "aspect": aspect or "",
        "qualifier": pred.qualifier or "",
        "evidence_code": pred.evidence_code or "",
        "taxonomic_relation": pred.taxonomic_relation or "",
    }
    for col in _NUMERIC_ORM_COLS:
        record[col] = getattr(pred, col, None)
    return record


def _patch_query_known_features(
    df: Any,
    known_gos: dict[str, set[str]],
) -> None:
    """Overwrite ``anc2vec_query_known_*`` in-place from eval-time known GOs.

    At predict time these columns are stored as NaN / 0 because the query's
    pre-cutoff annotation set is a property of the evaluation split, not the
    prediction set.  This helper repairs them for LK / PK evaluation so the
    reranker sees the same query-profile features it was trained on.

    - ``anc2vec_query_known_cos`` : cosine between the candidate's Anc2Vec
      vector and the L2-normalized centroid of the query's known-GO vectors.
    - ``anc2vec_query_known_maxcos`` : max cosine vs any individual known-GO
      vector.
    - ``anc2vec_query_known_count`` : raw size of the known-GO set (before
      filtering by Anc2Vec coverage).  Stays informative even when the
      intersection with the Anc2Vec vocab is empty.

    Rows whose candidate term has no Anc2Vec vector keep cos/maxcos as NaN
    (matches the predict-time convention and the training code path).
    """
    import pandas as pd

    if df.empty or not known_gos:
        return

    anc_idx = get_anc2vec_index()

    unique_proteins = df["protein_accession"].unique().tolist()
    candidate_go_ids = df["go_id"].unique().tolist()

    all_go_ids: set[str] = set(candidate_go_ids)
    for q_acc in unique_proteins:
        all_go_ids.update(known_gos.get(q_acc, ()))
    go_list = sorted(all_go_ids)
    if not go_list:
        return
    idx_of_go: dict[str, int] = {g: i for i, g in enumerate(go_list)}
    emb = anc_idx.batch(go_list)
    raw_norms = np.linalg.norm(emb, axis=1)
    has_emb_mask = raw_norms > 0.0
    safe_norms = np.where(has_emb_mask, raw_norms, 1.0)[:, None]
    all_norm = (emb / safe_norms).astype(np.float32)
    all_norm[~has_emb_mask] = 0.0

    cos_col = np.full(len(df), np.nan, dtype=np.float32)
    maxcos_col = np.full(len(df), np.nan, dtype=np.float32)
    count_col = np.zeros(len(df), dtype=np.float32)

    protein_groups = df.groupby("protein_accession", sort=False).indices

    for q_acc, row_indices in protein_groups.items():
        known = known_gos.get(q_acc, set())
        count_col[row_indices] = float(len(known))
        if not known:
            continue
        known_rows = [
            idx_of_go[g]
            for g in known
            if g in idx_of_go and has_emb_mask[idx_of_go[g]]
        ]
        if not known_rows:
            continue
        kmat = all_norm[known_rows]
        centroid = kmat.mean(axis=0)
        cn = float(np.linalg.norm(centroid))
        centroid_unit = (centroid / cn).astype(np.float32) if cn > 0.0 else None

        for ridx in row_indices:
            go_id = df["go_id"].iat[ridx]
            cand_i = idx_of_go.get(go_id)
            if cand_i is None or not has_emb_mask[cand_i]:
                continue
            cand_vec = all_norm[cand_i]
            if centroid_unit is not None:
                cos_col[ridx] = float(cand_vec @ centroid_unit)
            maxcos_col[ridx] = float((kmat @ cand_vec).max())

    df["anc2vec_query_known_cos"] = pd.Series(cos_col, index=df.index).replace(
        {np.nan: pd.NA}
    )
    df["anc2vec_query_known_maxcos"] = pd.Series(maxcos_col, index=df.index).replace(
        {np.nan: pd.NA}
    )
    df["anc2vec_query_known_count"] = count_col


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

        # Load per-category (and optionally per-aspect) reranker models before session commit.
        # reranker_models: setting → aspect → {"model": model_str, "cat_codes": dict|None}
        # aspect="" means single model for all aspects (legacy flat field).
        reranker_models: dict[str, dict[str, dict[str, Any]]] = {}
        reranker_config_snapshot: dict[str, dict[str, str]] | None = (
            None  # for persisting in EvaluationResult
        )

        def _resolve_model_bundle(rm: RerankerModelORM) -> dict[str, Any]:
            """Return ``{"model": str, "cat_codes": dict|None}`` from either the
            legacy inline blob or the ``artifact_uri`` cache. Boosters trained
            by the lab and imported via ``/reranker-models/import`` only set
            ``artifact_uri`` and leave ``model_data`` NULL, so the operation
            must transparently support both paths.

            ``cat_codes`` (if present in the imported run.json under
            ``__categorical_codes__``) is the lab's per-column sorted-unique
            string vocabulary, used at predict time to reproduce the encoding
            seen during training. Without it, ``reranker_predict`` falls back
            to ``pd.factorize`` over the inference batch — which silently
            produces the wrong codes for per-aspect inference and tanks the
            LK / PK fmax. See ``protea.core.reranker.predict``.
            """
            if rm.model_data:
                model_str = rm.model_data
            elif rm.artifact_uri:
                project_root = Path(__file__).resolve().parents[3]
                store = _get_store_for_reranker(_load_settings_for_reranker(project_root))
                booster = load_reranker(
                    rm.artifact_uri,
                    feature_schema_sha=rm.feature_schema_sha or rm.name,
                    store=store,
                )
                model_str = booster.model_to_string()
            else:
                raise ValueError(
                    f"RerankerModel {rm.id} has no booster — both ``model_data`` "
                    f"(legacy inline) and ``artifact_uri`` (artifact-store path) are NULL."
                )
            cat_codes = (rm.metrics or {}).get("__categorical_codes__")
            return {"model": model_str, "cat_codes": cat_codes}

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
                    reranker_models[setting][aspect_char] = _resolve_model_bundle(rm)
                    reranker_config_snapshot[cat_key][aspect_key] = rid_str
                    emit(
                        "run_cafa_evaluation.reranker_loaded",
                        None,
                        {
                            "setting": setting,
                            "aspect": aspect_key,
                            "reranker_id": str(rid),
                            "name": rm.name,
                        },
                        "info",
                    )
        else:
            # Legacy flat fields: one model per category (all aspects)
            for setting, field in [
                ("NK", p.reranker_id_nk),
                ("LK", p.reranker_id_lk),
                ("PK", p.reranker_id_pk),
            ]:
                if field:
                    rid = uuid.UUID(field)
                    rm = session.get(RerankerModelORM, rid)
                    if rm is None:
                        raise ValueError(f"RerankerModel {field} not found")
                    reranker_models[setting] = {"": _resolve_model_bundle(rm)}  # "" = all aspects
                    emit(
                        "run_cafa_evaluation.reranker_loaded",
                        None,
                        {"setting": setting, "reranker_id": str(rid), "name": rm.name},
                        "info",
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
                        select(distinct(_GP.protein_accession))
                        .where(_GP.prediction_set_id == pred_set_id)
                    ).scalars().all()
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
                        "nk_before": _orig_counts[0], "nk_after": len(data.nk),
                        "lk_before": _orig_counts[1], "lk_after": len(data.lk),
                        "pk_before": _orig_counts[2], "pk_after": len(data.pk),
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

            self._write_gt(data.nk, nk_path)
            self._write_gt(data.lk, lk_path)
            self._write_gt(data.pk, pk_path)
            self._write_gt(data.known, known_path)
            self._write_gt(data.pk_known, pk_known_path)

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
                self._write_predictions(
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
                        self._write_predictions(
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
                        self._write_predictions_per_aspect(
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

                    results[setting] = self._parse_results(dfs_best)

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
        reranker_cat_codes: dict[str, list[str]] | None = None,
        known_gos: dict[str, set[str]] | None = None,
    ) -> None:
        """Write CAFA-format predictions (protein\\tgo_id\\tscore) for delta proteins.

        Scoring priority:
          1. If ``reranker_model_str`` is provided, apply the LightGBM model to
             all predictions and use re-ranker probabilities as scores.
          2. If a ``ScoringConfig`` is provided, compute scores via ``compute_score()``.
          3. Otherwise fall back to ``1 - cosine_distance / 2``.

        ``known_gos`` carries the query's pre-cutoff annotations (LK / PK
        settings) and is used to override ``anc2vec_query_known_*`` before the
        reranker sees the DataFrame. For NK it must stay ``None``.
        """
        if reranker_model_str is not None:
            self._write_predictions_reranked(
                session,
                pred_set_id,
                delta_proteins,
                max_distance,
                path,
                reranker_model_str,
                reranker_cat_codes=reranker_cat_codes,
                known_gos=known_gos,
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
                        "neighbor_vote_fraction": pred.neighbor_vote_fraction,
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
        reranker_cat_codes: dict[str, list[str]] | None = None,
        known_gos: dict[str, set[str]] | None = None,
    ) -> None:
        """Write CAFA-format predictions using LightGBM re-ranker scores."""
        import pandas as pd

        from protea.core.reranker import model_from_string
        from protea.core.reranker import predict as reranker_predict

        q = (
            session.query(GOPrediction, GOTerm.go_id, GOTerm.aspect)
            .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
            .filter(GOPrediction.prediction_set_id == pred_set_id)
            .filter(GOPrediction.protein_accession.in_(delta_proteins))
        )
        if max_distance is not None:
            q = q.filter(GOPrediction.distance <= max_distance)

        records: list[dict[str, Any]] = [
            _record_from_pred(pred, go_id, aspect=aspect)
            for pred, go_id, aspect in q.yield_per(5000)
        ]

        if not records:
            with open(path, "w") as f:
                pass
            return

        df = pd.DataFrame(records)
        if known_gos:
            _patch_query_known_features(df, known_gos)
        model = model_from_string(reranker_model_str)
        scores = reranker_predict(model, df, categorical_codes=reranker_cat_codes)

        # Deduplicate: keep highest score per (protein, go_id)
        df["score"] = scores
        df = df.sort_values("score", ascending=False).drop_duplicates(
            subset=["protein_accession", "go_id"],
            keep="first",
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
        aspect_models: dict[str, dict[str, Any]],
        known_gos: dict[str, set[str]] | None = None,
    ) -> None:
        """Write CAFA-format predictions applying per-aspect LightGBM models.

        ``aspect_models`` maps GO aspect char (P/F/C) to ``{"model": str,
        "cat_codes": dict|None}`` bundles. Predictions whose aspect has no
        model fall back to ``1 - distance/2``.

        ``known_gos`` carries the query's pre-cutoff annotations (LK / PK
        settings) so the per-aspect model sees the same
        ``anc2vec_query_known_*`` features it was trained with. Must be
        ``None`` for NK.
        """
        import pandas as pd

        from protea.core.reranker import model_from_string
        from protea.core.reranker import predict as reranker_predict

        q = (
            session.query(GOPrediction, GOTerm.go_id, GOTerm.aspect)
            .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
            .filter(GOPrediction.prediction_set_id == pred_set_id)
            .filter(GOPrediction.protein_accession.in_(delta_proteins))
        )
        if max_distance is not None:
            q = q.filter(GOPrediction.distance <= max_distance)

        records: list[dict[str, Any]] = [
            _record_from_pred(pred, go_id, aspect) for pred, go_id, aspect in q.yield_per(5000)
        ]

        if not records:
            with open(path, "w") as f:
                pass
            return

        df = pd.DataFrame(records)
        if known_gos:
            _patch_query_known_features(df, known_gos)

        # Score each aspect group with its corresponding model
        df["score"] = 0.0
        for aspect_char, bundle in aspect_models.items():
            mask = df["aspect"] == aspect_char
            if not mask.any():
                continue
            model = model_from_string(bundle["model"])
            df.loc[mask, "score"] = reranker_predict(
                model, df.loc[mask], categorical_codes=bundle.get("cat_codes"),
            )

        # Fallback for aspects without a model
        modeled_aspects = set(aspect_models.keys())
        fallback_mask = ~df["aspect"].isin(modeled_aspects)
        if fallback_mask.any():
            df.loc[fallback_mask, "score"] = df.loc[fallback_mask, "distance"].apply(
                lambda d: max(0.0, 1.0 - (d or 0.0) / 2.0)
            )

        # Deduplicate: keep highest score per (protein, go_id)
        df = df.sort_values("score", ascending=False).drop_duplicates(
            subset=["protein_accession", "go_id"],
            keep="first",
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
