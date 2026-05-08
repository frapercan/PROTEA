"""Admin / analytics helpers extracted from ``embeddings_service``.

These pure functions own the heavy DB work for admin operations
(cascade-delete an :class:`EmbeddingConfig` and all dependent rows)
and read-only analytics (per-aspect GO-term distribution for a
PredictionSet). The service layer keeps the existence check that
maps to ``EntityNotFoundError`` and delegates the body here, so this
module has zero coupling to the domain-exception classes.
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding


def cascade_delete_embedding_config(
    session: Session,
    config: EmbeddingConfig,
) -> dict[str, Any]:
    """Cascade-delete the given :class:`EmbeddingConfig` and all linked rows.

    Bulk-deletes the dependent ``GOPrediction`` (via PredictionSet),
    ``PredictionSet``, and ``SequenceEmbedding`` rows; then the
    config itself. Returns a summary dict with the deletion counts.

    The ORM-level ``ondelete`` cascade would handle this on
    ``session.delete(c)`` alone, but we bulk-delete explicitly here
    so the response reports per-table counts the UI surfaces.

    The caller is responsible for verifying the config exists before
    calling (the existence check belongs in the service layer next
    to the ``EntityNotFoundError`` definition).
    """
    config_id = config.id

    pred_set_ids = [
        row[0]
        for row in session.query(PredictionSet.id)
        .filter(PredictionSet.embedding_config_id == config_id)
        .all()
    ]
    deleted_predictions = 0
    if pred_set_ids:
        deleted_predictions = (
            session.query(GOPrediction)
            .filter(GOPrediction.prediction_set_id.in_(pred_set_ids))
            .delete(synchronize_session=False)
        )

    deleted_prediction_sets = (
        session.query(PredictionSet)
        .filter(PredictionSet.embedding_config_id == config_id)
        .delete(synchronize_session=False)
    )

    deleted_embeddings = (
        session.query(SequenceEmbedding)
        .filter(SequenceEmbedding.embedding_config_id == config_id)
        .delete(synchronize_session=False)
    )

    session.delete(config)

    return {
        "deleted": str(config_id),
        "embeddings_deleted": deleted_embeddings,
        "prediction_sets_deleted": deleted_prediction_sets,
        "predictions_deleted": deleted_predictions,
    }


def compute_go_term_distribution(
    session: Session,
    *,
    prediction_set_id: uuid.UUID,
    limit: int = 50,
) -> dict[str, Any]:
    """Return the most-frequent GO terms predicted in a set + per-aspect totals.

    The caller is responsible for verifying the PredictionSet exists
    before calling (the existence check stays in the service layer).
    """
    rows = (
        session.query(
            GOTerm.go_id,
            GOTerm.name,
            GOTerm.aspect,
            func.count(GOPrediction.id).label("count"),
        )
        .join(GOPrediction, GOPrediction.go_term_id == GOTerm.id)
        .filter(GOPrediction.prediction_set_id == prediction_set_id)
        .group_by(GOTerm.go_id, GOTerm.name, GOTerm.aspect)
        .order_by(func.count(GOPrediction.id).desc())
        .limit(limit)
        .all()
    )

    by_aspect: dict[str, list[dict[str, Any]]] = {"F": [], "P": [], "C": [], "other": []}
    for go_id, name, aspect, count in rows:
        entry = {"go_id": go_id, "name": name, "count": count}
        by_aspect.get(aspect or "other", by_aspect["other"]).append(entry)

    aspect_counts = (
        session.query(GOTerm.aspect, func.count(GOPrediction.id))
        .join(GOPrediction, GOPrediction.go_term_id == GOTerm.id)
        .filter(GOPrediction.prediction_set_id == prediction_set_id)
        .group_by(GOTerm.aspect)
        .all()
    )

    return {
        "by_aspect": by_aspect,
        "aspect_totals": {asp or "other": cnt for asp, cnt in aspect_counts},
        "top_terms": [
            {"go_id": go_id, "name": name, "aspect": aspect, "count": count}
            for go_id, name, aspect, count in rows
        ],
    }
