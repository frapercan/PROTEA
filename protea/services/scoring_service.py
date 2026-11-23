"""Scoring service: business logic extracted from ``protea.api.routers.scoring``.

The router is a thin FastAPI translation layer. The ScoringConfig CRUD, the
ORM to response model mapping and the reranker's labelled training export live
here, so the same primitives can be used from CLI tools, batch jobs, or other
HTTP endpoints without re-implementing them. Nothing here scores a set or
measures one: that is ``score_prediction_set`` and the evaluator.

:class:`EntityNotFoundError` maps to ``404 Not Found`` at the router boundary.
"""

from __future__ import annotations

import uuid

from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.scoring_config import (
    ScoringConfig,
)


class ScoringServiceError(Exception):
    """Base class for scoring-service domain errors."""


class EntityNotFoundError(ScoringServiceError):
    """Generic 404; a referenced entity does not exist.

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
        params=body.params,
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

    Idempotent: presets matched by ``name`` are skipped silently.
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

# compute_prediction_metrics lives in _scoring_prediction_metrics_helpers and
# is re-exported below so existing router/CLI imports keep working unchanged.

# iter_training_data is canonically defined in _scoring_streaming_helpers;
# re-exported here so existing router/CLI imports keep working unchanged.
from protea.services._scoring_streaming_helpers import (  # noqa: E402,F401
    iter_training_data,
)

__all__ = [
    "PRESET_CONFIGS",
    "EntityNotFoundError",
    "RerankerResponse",
    "ScoringConfigCreate",
    "ScoringConfigResponse",
    "ScoringServiceError",
    "iter_training_data",
    "prepare_training_data_request",
    "to_reranker_response",
    "to_response",
]
