"""Showcase endpoint — aggregates platform stats and best evaluation results."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy import func
from sqlalchemy.orm import Session, sessionmaker

from protea.api.deps import get_session_factory
from protea.infrastructure.orm.models.annotation.evaluation_result import EvaluationResult
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.sequence.sequence import Sequence
from protea.infrastructure.session import session_scope

router = APIRouter(prefix="/showcase", tags=["showcase"])


def _derive_method(
    scoring_config_id: Any, reranker_model_id: Any
) -> tuple[str, str]:
    """Return (method_key, human_label) from nullable FK columns."""
    if reranker_model_id is not None:
        return "knn_reranker", "KNN + Re-ranker"
    if scoring_config_id is not None:
        return "knn_scored", "KNN + Scoring"
    return "knn_baseline", "KNN (embedding distance)"


# Method display order
_METHOD_ORDER = ["knn_baseline", "knn_scored", "knn_reranker"]
_ASPECTS = ["BPO", "MFO", "CCO"]


@router.get("", summary="Platform showcase data")
def get_showcase(
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Aggregate stats, best evaluation metrics, and method comparison for the
    landing page.  Returns a single JSON object so the frontend needs only one
    fetch on mount."""

    with session_scope(factory) as session:
        # ── Protein stats (mirrors /proteins/stats but lighter) ──────────
        total_proteins = session.query(func.count(Protein.accession)).scalar() or 0
        canonical_proteins = (
            session.query(func.count(Protein.accession))
            .filter(Protein.is_canonical.is_(True))
            .scalar()
            or 0
        )

        # ── Counts ───────────────────────────────────────────────────────
        total_sequences = session.query(func.count(Sequence.id)).scalar() or 0
        total_embeddings = session.query(func.count(SequenceEmbedding.id)).scalar() or 0
        total_prediction_sets = session.query(func.count(PredictionSet.id)).scalar() or 0
        total_predictions = session.query(func.count(GOPrediction.id)).scalar() or 0
        total_rerankers = session.query(func.count(RerankerModel.id)).scalar() or 0

        # ── Evaluation results ───────────────────────────────────────────
        eval_rows = session.query(EvaluationResult).all()
        total_evaluations = len(eval_rows)

        # Group by category → method, track best fmax per aspect
        _CATEGORIES = ["NK", "LK", "PK"]
        # best_fmax[category][aspect] = {fmax, method, ...}
        best_fmax: dict[str, dict[str, dict[str, Any]]] = {}
        # method_best[category][method_key] = {label, BPO: {fmax}, ...}
        method_best: dict[str, dict[str, dict[str, Any]]] = {}

        for er in eval_rows:
            method_key, method_label = _derive_method(
                er.scoring_config_id, er.reranker_model_id
            )
            results = er.results or {}

            for cat in _CATEGORIES:
                cat_data = results.get(cat, {})
                if not cat_data:
                    continue

                if cat not in method_best:
                    method_best[cat] = {}
                if method_key not in method_best[cat]:
                    method_best[cat][method_key] = {
                        "label": method_label,
                        **{a: {"fmax": None} for a in _ASPECTS},
                    }

                for aspect in _ASPECTS:
                    aspect_data = cat_data.get(aspect, {})
                    fmax = aspect_data.get("fmax")
                    if fmax is None:
                        continue

                    # Update method-level best for this category
                    cur = method_best[cat][method_key][aspect].get("fmax")
                    if cur is None or fmax > cur:
                        method_best[cat][method_key][aspect] = {"fmax": round(fmax, 4)}

                    # Update global best for this category
                    if cat not in best_fmax:
                        best_fmax[cat] = {}
                    if aspect not in best_fmax[cat] or fmax > best_fmax[cat][aspect]["fmax"]:
                        best_fmax[cat][aspect] = {
                            "fmax": round(fmax, 4),
                            "method": method_key,
                            "method_label": method_label,
                            "evaluation_result_id": str(er.id),
                        }

        # Build ordered method_comparison per category
        method_comparison: dict[str, list[dict[str, Any]]] = {}
        for cat in _CATEGORIES:
            cat_methods = method_best.get(cat, {})
            cat_list: list[dict[str, Any]] = []
            for mk in _METHOD_ORDER:
                if mk in cat_methods:
                    entry: dict[str, Any] = {
                        "method": mk,
                        "label": cat_methods[mk]["label"],
                    }
                    for aspect in _ASPECTS:
                        entry[aspect] = cat_methods[mk][aspect]
                    cat_list.append(entry)
            if cat_list:
                method_comparison[cat] = cat_list

        # Pipeline stages
        pipeline_stages = [
            {"name": "sequences", "count": total_sequences, "href": "/proteins"},
            {"name": "embeddings", "count": total_embeddings, "href": "/embeddings"},
            {"name": "predictions", "count": total_predictions, "href": "/functional-annotation"},
            {"name": "reranker_models", "count": total_rerankers, "href": "/reranker"},
            {"name": "evaluations", "count": total_evaluations, "href": "/evaluation"},
        ]

        return {
            "protein_stats": {
                "total": total_proteins,
                "canonical": canonical_proteins,
            },
            "best_fmax": best_fmax if best_fmax else {},
            "method_comparison": method_comparison,
            "counts": {
                "proteins": total_proteins,
                "sequences": total_sequences,
                "embeddings": total_embeddings,
                "prediction_sets": total_prediction_sets,
                "predictions": total_predictions,
                "reranker_models": total_rerankers,
                "evaluations": total_evaluations,
            },
            "pipeline_stages": pipeline_stages,
        }
