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

from sqlalchemy.orm import Session

from protea.core.reranker import load_reranker, model_from_string
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel
from protea.infrastructure.orm.models.embedding.scoring_config import (
    ScoringConfig,
)
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store


class ScoringServiceError(Exception):
    """Base class for scoring-service domain errors."""


class EntityNotFoundError(ScoringServiceError):
    """Generic 404 — a referenced entity does not exist.

    Construct with the entity label (e.g. ``"PredictionSet"``) and
    the looked-up UUID; the message becomes ``"<entity> not found"``.
    Pickle-safe via ``__reduce__`` so the structured ``entity`` /
    ``entity_id`` attributes survive a round-trip without tripping
    flake8-bugbear's B042 rule about argument forwarding.
    """

    def __init__(self, entity: str, entity_id: uuid.UUID) -> None:  # noqa: B042
        super().__init__(f"{entity} not found")
        self.entity = entity
        self.entity_id = entity_id

    def __reduce__(self) -> tuple[type, tuple[str, uuid.UUID]]:
        return (self.__class__, (self.entity, self.entity_id))


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


# RerankerResponse / ScoringConfigCreate / ScoringConfigResponse / PRESET_CONFIGS
# canonically defined in _scoring_models; re-exported here so existing
# router/CLI imports keep working unchanged.
from protea.services._scoring_models import (  # noqa: E402,F401
    PRESET_CONFIGS,
    RerankerResponse,
    ScoringConfigCreate,
    ScoringConfigResponse,
    to_reranker_response,
    to_response,
)


def list_scoring_configs_data(session: Session) -> list[ScoringConfigResponse]:
    """All stored :class:`ScoringConfig` rows as response models, oldest first."""
    rows = session.query(ScoringConfig).order_by(ScoringConfig.created_at).all()
    return [to_response(c) for c in rows]


def create_scoring_config_data(
    session: Session,
    body: ScoringConfigCreate,
) -> ScoringConfigResponse:
    """Persist a new :class:`ScoringConfig` from a validated request body.

    Pydantic field validators on :class:`ScoringConfigCreate` already
    enforce ``formula`` membership and reject unknown signal keys, so
    this helper just maps the model to the ORM row.
    """
    config = ScoringConfig(
        name=body.name,
        formula=body.formula,
        weights=body.weights,
        evidence_weights=body.evidence_weights,
        description=body.description,
    )
    session.add(config)
    session.flush()
    return to_response(config)


def get_scoring_config_data(
    session: Session,
    config_id: uuid.UUID,
) -> ScoringConfigResponse:
    """Fetch a :class:`ScoringConfig` by UUID; raise :class:`EntityNotFoundError`."""
    config = session.get(ScoringConfig, config_id)
    if config is None:
        raise EntityNotFoundError("ScoringConfig", config_id)
    return to_response(config)


def delete_scoring_config_data(
    session: Session,
    config_id: uuid.UUID,
) -> None:
    """Delete a :class:`ScoringConfig` by UUID; raise :class:`EntityNotFoundError`."""
    config = session.get(ScoringConfig, config_id)
    if config is None:
        raise EntityNotFoundError("ScoringConfig", config_id)
    session.delete(config)


def create_preset_configs_data(session: Session) -> list[str]:
    """Insert built-in :data:`PRESET_CONFIGS` that are not already present.

    Idempotent — presets matched by ``name`` are skipped silently.
    Returns the list of preset names actually created (for the HTTP
    response body).
    """
    existing_names = {row[0] for row in session.query(ScoringConfig.name).all()}
    created: list[str] = []
    for preset in PRESET_CONFIGS:
        if preset["name"] in existing_names:
            continue
        session.add(ScoringConfig(**preset))
        created.append(preset["name"])
    return created


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
# _SIGNAL_TO_COLUMN + compute_missing_signals canonically defined in
# _scoring_validation_helpers; service wrapper raises SignalCoverageError.
from protea.services._scoring_validation_helpers import (  # noqa: E402
    compute_missing_signals,
)


def check_signal_coverage(
    session: Session,
    prediction_set_id: uuid.UUID,
    config_snap: ScoringConfig,
) -> None:
    """Fail fast when the config needs signals absent from the PredictionSet.

    Body lives in
    :func:`_scoring_validation_helpers.compute_missing_signals`; this
    wrapper raises :class:`SignalCoverageError` when that helper
    returns a non-empty list. Keeping the raise in the service module
    keeps the helper free of domain-exception coupling.
    """
    missing = compute_missing_signals(session, prediction_set_id, config_snap)
    if missing:
        raise SignalCoverageError(missing)


def validate_scoring_request(
    session: Session,
    prediction_set_id: uuid.UUID,
    scoring_config_id: uuid.UUID,
) -> ScoringConfig:
    """Validate ``(prediction_set, scoring_config)`` and return a detached snapshot.

    Raises
    ------
    EntityNotFoundError
        Either ``PredictionSet`` or ``ScoringConfig`` does not exist.
    SignalCoverageError
        The config requires signals absent from the PredictionSet.
    """
    if session.get(PredictionSet, prediction_set_id) is None:
        raise EntityNotFoundError("PredictionSet", prediction_set_id)
    config = session.get(ScoringConfig, scoring_config_id)
    if config is None:
        raise EntityNotFoundError("ScoringConfig", scoring_config_id)
    config_snap = snapshot_config(config)
    check_signal_coverage(session, prediction_set_id, config_snap)
    return config_snap


def prepare_training_data_request(
    session: Session,
    *,
    prediction_set_id: uuid.UUID,
    evaluation_set_id: uuid.UUID,
    category: str,
) -> set[tuple[str, str]]:
    """Validate the request and compute the ``(protein, go_id)`` ground-truth pair set.

    Body lives in
    :func:`_scoring_validation_helpers.build_training_gt_pairs`; this
    wrapper resolves the two ORM rows and raises
    :class:`EntityNotFoundError` for missing ids, keeping the helper
    free of domain-exception coupling.
    """
    from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
    from protea.services._scoring_validation_helpers import build_training_gt_pairs

    ps = session.get(PredictionSet, prediction_set_id)
    if ps is None:
        raise EntityNotFoundError("PredictionSet", prediction_set_id)
    es = session.get(EvaluationSet, evaluation_set_id)
    if es is None:
        raise EntityNotFoundError("EvaluationSet", evaluation_set_id)
    return build_training_gt_pairs(
        session,
        prediction_set=ps,
        evaluation_set=es,
        category=category,
    )


# score_predictions_with_reranker lives in _scoring_pipeline_helpers and
# is re-exported below so existing router/CLI imports keep working unchanged.
# compute_reranker_metrics_data lives in _scoring_metrics_helpers and is
# re-exported below so existing router/CLI imports keep working unchanged.
# Mock targets for the tests inside the moved orchestrator point at the
# helper module (e.g. protea.services._scoring_metrics_helpers.compute_cafa_metrics).
from protea.services._scoring_metrics_helpers import (  # noqa: E402,F401
    compute_reranker_metrics_data,
)
from protea.services._scoring_pipeline_helpers import (  # noqa: E402,F401
    score_predictions_with_reranker,
)

# compute_prediction_metrics lives in _scoring_prediction_metrics_helpers and
# is re-exported below so existing router/CLI imports keep working unchanged.
from protea.services._scoring_prediction_metrics_helpers import (  # noqa: E402,F401
    compute_prediction_metrics,
)

# iter_scored_predictions / iter_training_data / iter_reranked_predictions_tsv
# canonically defined in _scoring_streaming_helpers; re-exported here so
# existing router/CLI imports keep working unchanged.
from protea.services._scoring_streaming_helpers import (  # noqa: E402,F401
    iter_reranked_predictions_tsv,
    iter_scored_predictions,
    iter_training_data,
)

__all__ = [
    "PRESET_CONFIGS",
    "BoosterUnavailableError",
    "EntityNotFoundError",
    "RerankerResponse",
    "ScoringConfigCreate",
    "ScoringConfigResponse",
    "ScoringServiceError",
    "SignalCoverageError",
    "check_signal_coverage",
    "compute_prediction_metrics",
    "compute_reranker_metrics_data",
    "iter_reranked_predictions_tsv",
    "iter_scored_predictions",
    "iter_training_data",
    "load_booster",
    "prepare_training_data_request",
    "score_predictions_with_reranker",
    "snapshot_config",
    "to_reranker_response",
    "to_response",
    "validate_scoring_request",
]
