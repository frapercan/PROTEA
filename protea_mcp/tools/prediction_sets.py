"""Tools de lectura sobre prediction_sets."""

from __future__ import annotations

from typing import Optional
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import joinedload

from protea.infrastructure.orm.models import (
    EmbeddingConfig,
    GOPrediction,
    GOTerm,
    PredictionSet,
)
from protea_mcp.db import get_session_factory, session_scope
from protea_mcp.schemas import PredictionSetSummary
from protea_mcp.server import mcp


def _summary(ps: PredictionSet, n: int) -> PredictionSetSummary:
    return PredictionSetSummary(
        id=ps.id,
        model_name=ps.embedding_config.model_name if ps.embedding_config else "",
        limit_per_entry=ps.limit_per_entry,
        distance_threshold=ps.distance_threshold,
        annotation_set_id=ps.annotation_set_id,
        ontology_snapshot_id=ps.ontology_snapshot_id,
        created_at=ps.created_at,
        n_predictions=n,
    )


@mcp.tool()
def list_prediction_sets(
    limit: int = 50,
    model_name: Optional[str] = None,
    limit_per_entry: Optional[int] = None,
) -> list[dict]:
    """Lista prediction_sets con totales de predicciones."""
    with session_scope(get_session_factory()) as s:
        counts = dict(
            s.execute(
                select(GOPrediction.prediction_set_id, func.count(GOPrediction.id)).group_by(
                    GOPrediction.prediction_set_id
                )
            ).all()
        )
        q = (
            select(PredictionSet)
            .options(joinedload(PredictionSet.embedding_config))
            .order_by(PredictionSet.created_at.desc())
            .limit(limit)
        )
        if limit_per_entry is not None:
            q = q.where(PredictionSet.limit_per_entry == limit_per_entry)
        if model_name is not None:
            q = q.join(EmbeddingConfig).where(EmbeddingConfig.model_name == model_name)
        rows = s.execute(q).unique().scalars().all()
        return [_summary(ps, counts.get(ps.id, 0)).model_dump(mode="json") for ps in rows]


@mcp.tool()
def get_prediction_set(prediction_set_id: UUID) -> dict:
    """Detalle completo de un prediction_set."""
    with session_scope(get_session_factory()) as s:
        ps = s.execute(
            select(PredictionSet)
            .options(
                joinedload(PredictionSet.embedding_config),
                joinedload(PredictionSet.annotation_set),
                joinedload(PredictionSet.ontology_snapshot),
                joinedload(PredictionSet.query_set),
            )
            .where(PredictionSet.id == prediction_set_id)
        ).scalar_one_or_none()
        if ps is None:
            return {"error": "not found", "id": str(prediction_set_id)}
        n = s.scalar(
            select(func.count(GOPrediction.id)).where(
                GOPrediction.prediction_set_id == prediction_set_id
            )
        )
        return {
            "id": str(ps.id),
            "embedding_config": {
                "id": str(ps.embedding_config.id),
                "model_name": ps.embedding_config.model_name,
                "backend": ps.embedding_config.model_backend,
                "pooling": ps.embedding_config.pooling,
                "layer_indices": ps.embedding_config.layer_indices,
                "layer_agg": ps.embedding_config.layer_agg,
            },
            "annotation_set_id": str(ps.annotation_set_id),
            "ontology_snapshot_id": str(ps.ontology_snapshot_id),
            "query_set_id": str(ps.query_set_id) if ps.query_set_id else None,
            "limit_per_entry": ps.limit_per_entry,
            "distance_threshold": ps.distance_threshold,
            "meta": ps.meta,
            "created_at": ps.created_at.isoformat(),
            "n_predictions": n or 0,
        }


@mcp.tool()
def compare_prediction_sets(ids: list[UUID]) -> dict:
    """Tabla comparativa básica: params + conteo de predicciones por set."""
    out: dict[str, dict] = {}
    with session_scope(get_session_factory()) as s:
        for pid in ids:
            ps = s.execute(
                select(PredictionSet)
                .options(joinedload(PredictionSet.embedding_config))
                .where(PredictionSet.id == pid)
            ).scalar_one_or_none()
            if ps is None:
                out[str(pid)] = {"error": "not found"}
                continue
            n = s.scalar(
                select(func.count(GOPrediction.id)).where(GOPrediction.prediction_set_id == pid)
            )
            out[str(pid)] = {
                "model": ps.embedding_config.model_name if ps.embedding_config else None,
                "k": ps.limit_per_entry,
                "distance_threshold": ps.distance_threshold,
                "n_predictions": n or 0,
                "created_at": ps.created_at.isoformat(),
            }
    return out


@mcp.tool()
def top_predictions(
    prediction_set_id: UUID,
    protein_accession: str,
    k: int = 10,
) -> list[dict]:
    """Top-K GO terms predichos para una proteína en un set, ordenados por distance asc."""
    with session_scope(get_session_factory()) as s:
        rows = s.execute(
            select(GOPrediction, GOTerm)
            .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
            .where(
                GOPrediction.prediction_set_id == prediction_set_id,
                GOPrediction.protein_accession == protein_accession,
            )
            .order_by(GOPrediction.distance.asc())
            .limit(k)
        ).all()
        return [
            {
                "go_id": term.go_id,
                "name": getattr(term, "name", None),
                "ontology": getattr(term, "ontology", None),
                "distance": pred.distance,
                "ref_protein": pred.ref_protein_accession,
                "evidence_code": pred.evidence_code,
                "vote_count": pred.vote_count,
                "k_position": pred.k_position,
            }
            for pred, term in rows
        ]
