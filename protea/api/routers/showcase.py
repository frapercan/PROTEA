"""Showcase endpoint: aggregates platform stats and the single best evaluation
result with full embedding attribution.

Unlike :mod:`protea.api.routers.benchmark`, which exposes the full per-model
per-stage matrix, this module is deliberately minimal: it returns **one**
"spotlight" result that the Home page can use for its hero card, plus the
pipeline stage counts.

Background
----------
The previous implementation collapsed every evaluation into three method
buckets (``knn_baseline`` / ``knn_scored`` / ``knn_reranker``) and took the
maximum Fmax across *all* embeddings in each bucket.  That hid which concrete
embedding won a given cell, and silently dropped losing embeddings from the
UI entirely.  With the introduction of the 8-model benchmark, that collapse
is actively misleading; so this endpoint now returns a single named winner
and a link to ``/benchmark`` for the full matrix.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session, sessionmaker

from protea.api.deps import get_session_factory
from protea.api.stages import stage_of
from protea.core.domain.aspect import ASPECT_CAFA_CODES as _ASPECTS
from protea.infrastructure.orm.models.annotation.evaluation_result import EvaluationResult
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel
from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.session import session_scope

router = APIRouter(prefix="/showcase", tags=["showcase"])

_CATEGORIES = ("NK", "LK", "PK")


def _approx_count(session: Session, table: str) -> int:
    """Fast approximate row count via pg_class.reltuples. Accurate enough for
    a UI spotlight, and O(1) instead of a full table scan on 40M+ row tables."""
    n = session.execute(
        text("SELECT reltuples::bigint FROM pg_class WHERE relname = :t"),
        {"t": table},
    ).scalar()
    return int(n) if n is not None and n >= 0 else 0


def _avg_fmax(results: dict[str, Any]) -> float | None:
    """Mean Fmax across the 9 (category × aspect) cells, ignoring missing ones.

    Returns ``None`` if the ``results`` blob is empty or has no Fmax values
    so a malformed or partial evaluation does not pretend to be
    "the best".
    """
    values: list[float] = []
    for cat in _CATEGORIES:
        cat_data = results.get(cat) or {}
        for asp in _ASPECTS:
            cell = cat_data.get(asp) or {}
            fmax = cell.get("fmax")
            if fmax is not None:
                values.append(float(fmax))
    if not values:
        return None
    return sum(values) / len(values)


def _load_showcase_counts(session: Session) -> dict[str, int]:
    """Pipeline stage row counts shown on the showcase page.

    Large tables (``protein`` / ``sequence`` / ``sequence_embedding`` /
    ``go_prediction``) use ``pg_class.reltuples`` because exact
    ``COUNT(*)`` on ``go_prediction`` takes 20-30 s. Small tables keep
    exact counts.
    """
    return {
        "proteins": _approx_count(session, "protein"),
        "canonical_proteins": (
            session.scalar(
                select(func.count(Protein.accession)).where(Protein.is_canonical.is_(True))
            )
            or 0
        ),
        "sequences": _approx_count(session, "sequence"),
        "embeddings": _approx_count(session, "sequence_embedding"),
        "prediction_sets": session.scalar(select(func.count(PredictionSet.id))) or 0,
        "predictions": _approx_count(session, "go_prediction"),
        "reranker_models": session.scalar(select(func.count(RerankerModel.id))) or 0,
    }


def _pick_best_evaluation(session: Session) -> tuple[dict[str, Any] | None, int]:
    """Return the single best ``EvaluationResult`` by mean Fmax + total count."""
    rows = session.execute(
        select(EvaluationResult, EmbeddingConfig, ScoringConfig.name)
        .join(PredictionSet, PredictionSet.id == EvaluationResult.prediction_set_id)
        .join(EmbeddingConfig, EmbeddingConfig.id == PredictionSet.embedding_config_id)
        .outerjoin(ScoringConfig, ScoringConfig.id == EvaluationResult.scoring_config_id)
    ).all()
    best: dict[str, Any] | None = None
    best_score: float = -1.0
    for er, cfg, scoring_name in rows:
        score = _avg_fmax(er.results or {})
        if score is None or score <= best_score:
            continue
        best_score = score
        best = {
            "evaluation_result_id": str(er.id),
            "evaluation_set_id": str(er.evaluation_set_id),
            "stage": stage_of(er, scoring_name),
            "avg_fmax": round(score, 4),
            "embedding": {
                "id": str(cfg.id),
                "model_name": cfg.model_name,
                "model_backend": cfg.model_backend,
                "display_name": cfg.display_name or cfg.model_name,
                "family": cfg.family or cfg.model_backend,
                "param_count": cfg.param_count,
            },
            "per_cell": _flatten_cells(er.results or {}),
        }
    return best, len(rows)


@router.get("", summary="Platform showcase data")
def get_showcase(
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Aggregate pipeline stage counts and return the single best evaluation
    result (by mean Fmax across the 9 cells) along with the embedding that
    produced it.

    Empty-state contract: ``best`` is ``None`` when no ``EvaluationResult``
    exists; ``pipeline_stages`` always returns the same five entries with
    ``count = 0`` for unpopulated stages; ``counts`` always returns the
    same keys.
    """
    with session_scope(factory) as session:
        counts = _load_showcase_counts(session)
        best, total_evaluations = _pick_best_evaluation(session)
        pipeline_stages = [
            {"name": "sequences", "count": int(counts["sequences"]), "href": "/proteins"},
            {"name": "embeddings", "count": int(counts["embeddings"]), "href": "/embeddings"},
            {
                "name": "predictions",
                "count": int(counts["predictions"]),
                "href": "/functional-annotation",
            },
            {
                "name": "reranker_models",
                "count": int(counts["reranker_models"]),
                "href": "/reranker",
            },
            {"name": "evaluations", "count": total_evaluations, "href": "/benchmark"},
        ]
        return {
            "protein_stats": {
                "total": int(counts["proteins"]),
                "canonical": int(counts["canonical_proteins"]),
            },
            "best": best,
            "counts": {
                "proteins": int(counts["proteins"]),
                "sequences": int(counts["sequences"]),
                "embeddings": int(counts["embeddings"]),
                "prediction_sets": int(counts["prediction_sets"]),
                "predictions": int(counts["predictions"]),
                "reranker_models": int(counts["reranker_models"]),
                "evaluations": total_evaluations,
            },
            "pipeline_stages": pipeline_stages,
        }


def _flatten_cells(results: dict[str, Any]) -> list[dict[str, Any]]:
    """Serialise the nested (category → aspect) Fmax blob as a flat list.

    Kept on the showcase response so the Home page can render a compact
    "per-tier breakdown" tile without a second fetch.  Only cells with a
    non-null ``fmax`` are included.
    """
    out: list[dict[str, Any]] = []
    for cat in _CATEGORIES:
        cat_data = results.get(cat) or {}
        for asp in _ASPECTS:
            cell = cat_data.get(asp) or {}
            fmax = cell.get("fmax")
            if fmax is None:
                continue
            out.append(
                {
                    "category": cat,
                    "aspect": asp,
                    "fmax": round(float(fmax), 4),
                    "precision": (
                        round(float(cell["precision"]), 4)
                        if cell.get("precision") is not None
                        else None
                    ),
                    "recall": (
                        round(float(cell["recall"]), 4) if cell.get("recall") is not None else None
                    ),
                }
            )
    return out
