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
from protea.api.metrics import PRIMARY_METRIC, per_task_aggregate, primary_cell_score
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


def _avg_primary(results: dict[str, Any]) -> tuple[float | None, str]:
    """Mean IA-weighted primary metric across the 9 cells + the driving metric.

    Ranks evaluations by the LAFA-comparable ``f_micro_w`` (FIX-METRIC-IA),
    falling back per-cell to the unweighted ``fmax`` for legacy cells. Returns
    ``(mean, metric)`` where ``metric`` is ``f_micro_w`` when every contributing
    cell carried a real IA-weighted score, else ``fmax`` so the home spotlight
    can flag a not-IA-weighted result honestly. ``(None, ...)`` for a blob with
    no scored cells so a partial evaluation never pretends to be "the best".
    """
    values: list[float] = []
    all_weighted = True
    for cat in _CATEGORIES:
        cat_data = results.get(cat) or {}
        for asp in _ASPECTS:
            cell = cat_data.get(asp) or {}
            if cell.get("fmax") is None and cell.get(PRIMARY_METRIC) is None:
                continue
            score, metric = primary_cell_score(cell)
            values.append(score)
            if metric != PRIMARY_METRIC:
                all_weighted = False
    if not values:
        return None, PRIMARY_METRIC
    return sum(values) / len(values), PRIMARY_METRIC if all_weighted else "fmax"


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


def _pick_best_evaluation(
    session: Session,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]], int]:
    """Return ``(best, per_task, total)``.

    ``best`` is the single best ``EvaluationResult`` ranked by mean IA-weighted
    primary metric (``f_micro_w``, fmax fallback). It is the best-CELL spotlight
    and must be labelled as such by the front-end, never as the headline.

    ``per_task`` is the honest per-(category, aspect) summary the home page
    headlines: mean of the primary metric across every model plus a 95% CI
    half-width, so the dashboard leads with a calibrated central tendency
    rather than the winner's-curse maximum (FIX-METRIC-IA).
    """
    rows = session.execute(
        select(EvaluationResult, EmbeddingConfig, ScoringConfig.name)
        .join(PredictionSet, PredictionSet.id == EvaluationResult.prediction_set_id)
        .join(EmbeddingConfig, EmbeddingConfig.id == PredictionSet.embedding_config_id)
        .outerjoin(ScoringConfig, ScoringConfig.id == EvaluationResult.scoring_config_id)
    ).all()
    best: dict[str, Any] | None = None
    best_score: float = -1.0
    grouped: dict[tuple[str, str], list[float]] = {}
    for er, cfg, scoring_name in rows:
        results = er.results or {}
        _collect_primary_cells(results, grouped)
        score, metric = _avg_primary(results)
        if score is None or score <= best_score:
            continue
        best_score = score
        best = {
            "evaluation_result_id": str(er.id),
            "evaluation_set_id": str(er.evaluation_set_id),
            "stage": stage_of(er, scoring_name),
            "avg_primary": round(score, 4),
            "primary_metric": metric,
            "embedding": {
                "id": str(cfg.id),
                "model_name": cfg.model_name,
                "model_backend": cfg.model_backend,
                "display_name": cfg.display_name or cfg.model_name,
                "family": cfg.family or cfg.model_backend,
                "param_count": cfg.param_count,
            },
            "per_cell": _flatten_cells(results),
        }
    return best, _build_per_task(grouped), len(rows)


def _collect_primary_cells(
    results: dict[str, Any], grouped: dict[tuple[str, str], list[float]]
) -> None:
    """Fold one evaluation's per-cell primary scores into the per-task buckets."""
    for cat in _CATEGORIES:
        cat_data = results.get(cat) or {}
        for asp in _ASPECTS:
            cell = cat_data.get(asp) or {}
            if cell.get("fmax") is None and cell.get(PRIMARY_METRIC) is None:
                continue
            score, _ = primary_cell_score(cell)
            grouped.setdefault((cat, asp), []).append(score)


def _build_per_task(grouped: dict[tuple[str, str], list[float]]) -> list[dict[str, Any]]:
    """Per-task mean + 95% CI across all models, in canonical cell order."""
    out: list[dict[str, Any]] = []
    for cat in _CATEGORIES:
        for asp in _ASPECTS:
            agg = per_task_aggregate(grouped.get((cat, asp), []))
            if agg is None:
                continue
            out.append(
                {
                    "category": cat,
                    "aspect": asp,
                    "metric": PRIMARY_METRIC,
                    "mean": agg["mean"],
                    "ci95": agg["ci95"],
                    "max": agg["max"],
                    "min": agg["min"],
                    "n_models": agg["n"],
                }
            )
    return out


@router.get("", summary="Platform showcase data")
def get_showcase(
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Aggregate pipeline stage counts and return the IA-weighted ``per_task``
    summary (the home headline) plus the single best-cell evaluation result
    ranked by mean ``f_micro_w`` (fmax fallback), along with the embedding that
    produced it.

    Empty-state contract: ``best`` is ``None`` and ``per_task`` is ``[]`` when
    no ``EvaluationResult`` exists; ``pipeline_stages`` always returns the same
    five entries with ``count = 0`` for unpopulated stages; ``counts`` always
    returns the same keys.
    """
    with session_scope(factory) as session:
        counts = _load_showcase_counts(session)
        best, per_task, total_evaluations = _pick_best_evaluation(session)
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
            "primary_metric": PRIMARY_METRIC,
            "per_task": per_task,
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
    """Serialise the nested (category → aspect) blob as a flat list.

    Kept on the showcase response so the Home page can render a compact
    "per-tier breakdown" tile without a second fetch. Each cell carries the
    IA-weighted ``primary`` (``f_micro_w`` with fmax fallback) plus the
    ``primary_metric`` flag so the front-end can mark legacy non-IA cells.
    Only cells with a scored value are included.
    """
    out: list[dict[str, Any]] = []
    for cat in _CATEGORIES:
        cat_data = results.get(cat) or {}
        for asp in _ASPECTS:
            cell = cat_data.get(asp) or {}
            if cell.get("fmax") is None and cell.get(PRIMARY_METRIC) is None:
                continue
            primary, primary_metric = primary_cell_score(cell)
            out.append(
                {
                    "category": cat,
                    "aspect": asp,
                    "primary": round(primary, 4),
                    "primary_metric": primary_metric,
                    "f_micro_w": (
                        round(float(cell[PRIMARY_METRIC]), 4)
                        if cell.get(PRIMARY_METRIC) is not None
                        else None
                    ),
                    "fmax": (
                        round(float(cell["fmax"]), 4) if cell.get("fmax") is not None else None
                    ),
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
