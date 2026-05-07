"""Scoring service — business logic extracted from ``protea.api.routers.scoring``.

The router is a thin FastAPI translation layer. All non-trivial logic
(booster loading, signal-coverage validation, ORM ↔ response model
mapping, snapshot detachment for streaming responses) lives here so
the same primitives can be used from CLI tools, batch jobs, or other
HTTP endpoints without re-implementing them.

Exceptions raised by this module are domain-level and map to HTTP
status codes at the router boundary:

- :class:`BoosterUnavailableError` → ``409 Conflict`` (RerankerModel row
  exists but the backing booster is missing on both legacy and
  artifact-store paths).
- :class:`SignalCoverageError` → ``409 Conflict`` (ScoringConfig requires
  signals absent from the PredictionSet's GOPrediction rows).
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.reranker import load_reranker, model_from_string
from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel
from protea.infrastructure.orm.models.embedding.scoring_config import (
    FORMULA_EVIDENCE_WEIGHTED,
    ScoringConfig,
)
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store


class ScoringServiceError(Exception):
    """Base class for scoring-service domain errors."""


class BoosterUnavailableError(ScoringServiceError):
    """A RerankerModel row exists but no booster bytes are reachable.

    Both ``model_data`` (legacy inline blob) and ``artifact_uri``
    (artifact-store path) are NULL, so inference cannot proceed.
    """

    def __init__(self, reranker_id: uuid.UUID) -> None:
        self.reranker_id = reranker_id
        super().__init__(
            f"RerankerModel {reranker_id} has no booster — both "
            f"``model_data`` (legacy inline) and ``artifact_uri`` "
            f"(artifact-store path) are NULL."
        )


class SignalCoverageError(ScoringServiceError):
    """A ScoringConfig requires signals absent from a PredictionSet.

    ``missing`` is a list of human-readable strings, one per signal,
    suitable for direct inclusion in the response body.
    """

    def __init__(self, missing: list[str]) -> None:
        self.missing = missing
        super().__init__(
            "ScoringConfig requires signals absent from the PredictionSet: "
            + "; ".join(missing)
            + ". Re-predict with the corresponding compute_* flag enabled."
        )


class ScoringConfigResponse(BaseModel):
    """Serialised representation of a stored :class:`ScoringConfig`."""

    id: uuid.UUID
    name: str
    formula: str
    weights: dict[str, Any]
    evidence_weights: dict[str, Any] | None
    description: str | None
    created_at: Any


def to_response(c: ScoringConfig) -> ScoringConfigResponse:
    """Convert an ORM :class:`ScoringConfig` to its API response model."""
    return ScoringConfigResponse(
        id=c.id,
        name=c.name,
        formula=c.formula,
        weights=c.weights,
        evidence_weights=c.evidence_weights,
        description=c.description,
        created_at=c.created_at,
    )


def snapshot_config(c: ScoringConfig) -> ScoringConfig:
    """Detached :class:`ScoringConfig` copy safe to use after a session closes.

    The scoring endpoints close the DB session before streaming the
    response body. This helper captures all scoring-relevant fields
    into a plain ORM instance that does not require an open session.
    """
    return ScoringConfig(
        id=c.id,
        name=c.name,
        formula=c.formula,
        weights=c.weights,
        evidence_weights=c.evidence_weights,
        description=c.description,
    )


def load_booster(rm: RerankerModel) -> Any:
    """Load the LightGBM booster from either the legacy inline blob or
    the new ``artifact_uri`` path.

    Raises :class:`BoosterUnavailableError` when neither is available.
    """
    if rm.model_data:
        return model_from_string(rm.model_data)
    if rm.artifact_uri:
        # Settings are loaded relative to the PROTEA repo root, four levels
        # up from this service file (services / protea / repo).
        project_root = Path(__file__).resolve().parents[2]
        store = get_artifact_store(load_settings(project_root))
        return load_reranker(
            rm.artifact_uri,
            feature_schema_sha=rm.feature_schema_sha or rm.name,
            store=store,
        )
    raise BoosterUnavailableError(rm.id)


# Maps each scoring signal key to the GOPrediction column whose fill rate
# determines whether the signal is usable for a given PredictionSet.
_SIGNAL_TO_COLUMN: dict[str, str] = {
    "embedding_similarity": "distance",
    "identity_nw": "identity_nw",
    "identity_sw": "identity_sw",
    "evidence_weight": "evidence_code",
    "taxonomic_proximity": "taxonomic_distance",
    "neighbor_vote_fraction": "neighbor_vote_fraction",
}


def check_signal_coverage(
    session: Session,
    prediction_set_id: uuid.UUID,
    config_snap: ScoringConfig,
) -> None:
    """Fail fast when the config needs signals absent from the PredictionSet.

    For every signal with a non-zero weight in ``config_snap.weights``
    (plus ``evidence_code`` when the formula is ``evidence_weighted`` —
    the multiplier is always applied), count how many rows in the
    PredictionSet have the backing column non-NULL. Zero coverage is a
    configuration mismatch (typically a ``ScoringConfig`` that requires
    ``compute_alignments=True`` or ``compute_taxonomy=True`` applied to
    a PredictionSet computed without those flags). Raise
    :class:`SignalCoverageError` with the list of missing signals
    instead of silently producing a degraded score (``compute_score``
    drops NULL signals from both numerator and denominator).
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
        return

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
    if missing:
        raise SignalCoverageError(missing)


__all__ = [
    "BoosterUnavailableError",
    "ScoringConfigResponse",
    "ScoringServiceError",
    "SignalCoverageError",
    "check_signal_coverage",
    "load_booster",
    "snapshot_config",
    "to_response",
]
