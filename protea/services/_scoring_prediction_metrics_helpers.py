"""Prediction-metrics computation helpers for ``scoring_service``.

The orchestrator ``compute_prediction_metrics`` lives here split
into private helpers (validate / score / format) so neither it nor
any helper exceeds the §3 method-LOC ceiling, and ``scoring_service.py``
can shrink toward the file-LOC budget.

The function is re-exported from ``protea.services.scoring_service``
for backwards compatibility with router and CLI callers.
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy.orm import Session

from protea.core.evaluation import compute_evaluation_data
from protea.core.metrics import compute_cafa_metrics
from protea.core.scoring import compute_score
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig


def _validate_prediction_metrics_entities(
    session: Session,
    prediction_set_id: uuid.UUID,
    scoring_config_id: uuid.UUID,
) -> ScoringConfig:
    """Resolve PS + ScoringConfig, snapshot the config, validate signal coverage.

    Returns the snapshotted config (detached, safe past session close).
    Imports ``EntityNotFoundError``, ``snapshot_config`` and
    ``check_signal_coverage`` lazily to avoid the circular dep with
    the re-exporting ``scoring_service`` module.
    """
    from protea.services.scoring_service import (
        EntityNotFoundError,
        check_signal_coverage,
        snapshot_config,
    )

    if session.get(PredictionSet, prediction_set_id) is None:
        raise EntityNotFoundError("PredictionSet", prediction_set_id)
    config = session.get(ScoringConfig, scoring_config_id)
    if config is None:
        raise EntityNotFoundError("ScoringConfig", scoring_config_id)
    config_snap = snapshot_config(config)
    check_signal_coverage(session, prediction_set_id, config_snap)
    return config_snap


def _score_prediction_rows(
    session: Session,
    prediction_set_id: uuid.UUID,
    config_snap: ScoringConfig,
) -> list[dict[str, Any]]:
    """Materialise + score every GOPrediction row of the set.

    Builds the minimal pred_dict that ``compute_score`` requires
    (distance + alignment identities + evidence code + taxonomic
    distance + neighbor vote fraction) and tags each with the
    computed score.
    """
    rows = (
        session.query(GOPrediction, GOTerm.go_id)
        .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
        .filter(GOPrediction.prediction_set_id == prediction_set_id)
        .all()
    )

    scored: list[dict[str, Any]] = []
    for pred, go_id in rows:
        pred_dict: dict[str, Any] = {
            "protein_accession": pred.protein_accession,
            "go_id": go_id,
            "distance": pred.distance,
            "identity_nw": pred.identity_nw,
            "identity_sw": pred.identity_sw,
            "evidence_code": pred.evidence_code,
            "taxonomic_distance": pred.taxonomic_distance,
            "neighbor_vote_fraction": pred.neighbor_vote_fraction,
        }
        pred_dict["score"] = compute_score(pred_dict, config_snap)
        scored.append(pred_dict)
    return scored


def _format_prediction_metrics_response(
    prediction_set_id: uuid.UUID,
    scoring_config_id: uuid.UUID,
    config_name: str,
    metrics: Any,
) -> dict[str, Any]:
    """Shape the metrics payload for the API."""
    return {
        "prediction_set_id": str(prediction_set_id),
        "scoring_config_id": str(scoring_config_id),
        "scoring_config_name": config_name,
        **metrics.summary(),
        "curve": [
            {
                "threshold": p.threshold,
                "precision": p.precision,
                "recall": p.recall,
                "f1": p.f1,
            }
            for p in metrics.curve
        ],
    }


def compute_prediction_metrics(
    session: Session,
    *,
    prediction_set_id: uuid.UUID,
    scoring_config_id: uuid.UUID,
    old_annotation_set_id: uuid.UUID,
    new_annotation_set_id: uuid.UUID,
    ontology_snapshot_id: uuid.UUID,
    category: str,
) -> dict[str, Any]:
    """Compute CAFA Fmax and AUC-PR for a PredictionSet under a ScoringConfig.

    Decomposed into private helpers (``_validate_*``, ``_score_*``,
    ``_format_*``) so this orchestrator stays under the §3 method-LOC
    ceiling.

    Raises
    ------
    EntityNotFoundError
        Either ``PredictionSet`` or ``ScoringConfig`` does not exist.
    SignalCoverageError
        The config requires signals absent from the PredictionSet.
    """
    config_snap = _validate_prediction_metrics_entities(
        session, prediction_set_id, scoring_config_id
    )
    eval_data = compute_evaluation_data(
        session,
        old_annotation_set_id=old_annotation_set_id,
        new_annotation_set_id=new_annotation_set_id,
        ontology_snapshot_id=ontology_snapshot_id,
    )
    scored = _score_prediction_rows(session, prediction_set_id, config_snap)
    metrics = compute_cafa_metrics(scored, eval_data, category=category)
    return _format_prediction_metrics_response(
        prediction_set_id, scoring_config_id, config_snap.name, metrics
    )
