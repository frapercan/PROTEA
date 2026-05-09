"""Validation + signal-coverage helpers extracted from ``scoring_service``.

These pure functions own the heavy DB introspection (per-signal NULL
counts, temporal NK/LK/PK delta materialisation) for the scoring API.
They return raw results; the service-layer wrappers translate them
into domain exceptions (``SignalCoverageError``, ``EntityNotFoundError``)
so the helper module stays free of those types.
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.evaluation import compute_evaluation_data
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.scoring_config import (
    FORMULA_EVIDENCE_WEIGHTED,
    ScoringConfig,
)

_SIGNAL_TO_COLUMN: dict[str, str] = {
    "embedding_similarity": "distance",
    "identity_nw": "identity_nw",
    "identity_sw": "identity_sw",
    "evidence_weight": "evidence_code",
    "taxonomic_proximity": "taxonomic_distance",
    "neighbor_vote_fraction": "neighbor_vote_fraction",
}


def compute_missing_signals(
    session: Session,
    prediction_set_id: uuid.UUID,
    config_snap: ScoringConfig,
) -> list[str]:
    """Return human-readable strings for every required signal absent from the set.

    For each signal with a non-zero weight in ``config_snap.weights``
    (plus ``evidence_code`` when the formula is ``evidence_weighted``,
    where the multiplier is always applied), count how many rows in the
    PredictionSet have the backing column non-NULL. Empty list means
    full coverage. The service-layer wrapper raises
    ``SignalCoverageError`` when this list is non-empty.
    """
    weights = config_snap.weights or {}
    required: list[tuple[str, str]] = []
    for signal, col in _SIGNAL_TO_COLUMN.items():
        if float(weights.get(signal, 0.0)) > 0.0:
            required.append((signal, col))
    if getattr(config_snap, "formula", "linear") == FORMULA_EVIDENCE_WEIGHTED and not any(
        s == "evidence_weight" for s, _ in required
    ):
        required.append(("evidence_weight", "evidence_code"))
    if not required:
        return []

    cols_sql = ", ".join(f"COUNT({col}) AS cnt_{col}" for _, col in required)
    row = (
        session.execute(
            text(
                f"SELECT COUNT(*) AS total, {cols_sql} "  # noqa: S608 — col names hard-coded
                "FROM go_prediction WHERE prediction_set_id = :pid"
            ),
            {"pid": str(prediction_set_id)},
        )
        .mappings()
        .one()
    )
    total = int(row["total"] or 0)
    missing: list[str] = []
    for signal, col in required:
        cnt = int(row[f"cnt_{col}"] or 0)
        if total == 0 or cnt == 0:
            missing.append(f"{signal} (column '{col}': {cnt}/{total} rows)")
    return missing


def build_training_gt_pairs(
    session: Session,
    *,
    prediction_set: PredictionSet,
    evaluation_set: Any,
    category: str,
) -> set[tuple[str, str]]:
    """Compute the ``(protein, go_id)`` ground-truth pair set for training.

    The caller (service layer) is responsible for resolving the
    ``PredictionSet`` and ``EvaluationSet`` rows and raising
    ``EntityNotFoundError`` for missing ids. ``evaluation_set`` is
    typed as ``Any`` so the helper can stay free of the
    ``EvaluationSet`` ORM import (which lives at module level via
    lazy import in the service).
    """
    eval_data = compute_evaluation_data(
        session,
        old_annotation_set_id=evaluation_set.old_annotation_set_id,
        new_annotation_set_id=evaluation_set.new_annotation_set_id,
        ontology_snapshot_id=prediction_set.ontology_snapshot_id,
    )
    ground_truth: dict[str, set[str]] = getattr(eval_data, category)
    gt_pairs: set[tuple[str, str]] = set()
    for protein, go_ids in ground_truth.items():
        for go_id in go_ids:
            gt_pairs.add((protein, go_id))
    return gt_pairs
