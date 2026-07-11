"""Embeddings service: pure-logic helpers extracted from
``protea.api.routers.embeddings``.

Validation rules, ORM ↔ dict serialisers, and the predictions-TSV
streaming generator live here so non-router callers (CLI tools,
batch scripts) can reuse them without pulling FastAPI in.

The router translates the domain exceptions raised here to HTTP
responses:

- :class:`InvalidEmbeddingConfigError` → ``422 Unprocessable Entity``
  (validation errors carry a list of human-readable messages in
  ``.errors``).
- :class:`EntityNotFoundError` → ``404 Not Found`` (e.g. a
  ``PredictionSet`` UUID does not resolve).
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet

# VALID_* sets canonically defined in _embeddings_validation_helpers; re-exported
# here so existing callers and ``__all__`` consumers keep working.
from protea.services._embeddings_validation_helpers import (  # noqa: E402,F401
    VALID_BACKENDS,
    VALID_LAYER_AGG,
    VALID_POOLING,
)


class EmbeddingsServiceError(Exception):
    """Base class for embeddings-service domain errors."""


class EntityNotFoundError(EmbeddingsServiceError):
    """Generic 404; a referenced entity does not exist.

    Construct with the entity label (e.g. ``"PredictionSet"``) and
    the looked-up UUID; the message becomes ``"<entity> not found"``.
    Pickle-safe via ``__reduce__`` so the structured ``entity`` /
    ``entity_id`` attributes survive a round-trip without tripping
    flake8-bugbear B042.
    """

    def __init__(self, entity: str, entity_id: uuid.UUID) -> None:  # noqa: B042
        super().__init__(f"{entity} not found")
        self.entity = entity
        self.entity_id = entity_id

    def __reduce__(self) -> tuple[type, tuple[str, uuid.UUID]]:
        return (self.__class__, (self.entity, self.entity_id))


class InvalidEmbeddingConfigError(EmbeddingsServiceError):
    """Validation failure for an EmbeddingConfig request body.

    ``errors`` carries a list of human-readable messages, one per
    failed rule, suitable for inclusion in the HTTP 422 response
    body.
    """

    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        super().__init__("; ".join(errors) if errors else "invalid embedding config")


class InvalidUUIDFieldError(EmbeddingsServiceError):
    """Predict request body had a field that does not parse as UUID.

    Carries the offending field name in ``field``; the router
    translates this to ``422`` with detail
    ``"<field> must be a valid UUID"``.
    """

    def __init__(self, field: str) -> None:  # noqa: B042
        super().__init__(f"{field} must be a valid UUID")
        self.field = field

    def __reduce__(self) -> tuple[type, tuple[str]]:
        return (self.__class__, (self.field,))


_PREDICT_ENTITY_MAP: tuple[tuple[str, str, type], ...] = (
    ("embedding_config_id", "EmbeddingConfig", EmbeddingConfig),
    ("annotation_set_id", "AnnotationSet", AnnotationSet),
    ("ontology_snapshot_id", "OntologySnapshot", OntologySnapshot),
)


def validate_predict_request(
    session: Session, body: dict[str, Any]
) -> dict[str, uuid.UUID]:
    """Parse + validate the three required UUID fields of a predict request.

    Returns a dict mapping field name to its parsed :class:`uuid.UUID`.
    Raises :class:`InvalidUUIDFieldError` for parse failures (router →
    422) or :class:`EntityNotFoundError` if a referenced entity does
    not exist (router → 404). Field order is preserved so the first
    failure wins, matching the previous in-router behaviour.
    """
    parsed: dict[str, uuid.UUID] = {}
    for field, _, _ in _PREDICT_ENTITY_MAP:
        try:
            parsed[field] = uuid.UUID(str(body.get(field)))
        except (ValueError, AttributeError):
            raise InvalidUUIDFieldError(field) from None

    for field, label, model_cls in _PREDICT_ENTITY_MAP:
        if session.get(model_cls, parsed[field]) is None:
            raise EntityNotFoundError(label, parsed[field])

    return parsed


# validate_embedding_config_body lives in _embeddings_validation_helpers and
# is re-exported below so existing router/CLI imports keep working unchanged.
from protea.services._embeddings_validation_helpers import (  # noqa: E402,F401
    validate_embedding_config_body,
)


def config_to_dict(c: EmbeddingConfig, embedding_count: int | None = None) -> dict[str, Any]:
    """Serialise an :class:`EmbeddingConfig` ORM row to its API dict shape.

    The ``embedding_count`` field is only included when the caller
    has a number to report (the bare ``GET /configs/{id}`` endpoint
    does not).
    """
    out: dict[str, Any] = {
        "id": str(c.id),
        "model_name": c.model_name,
        "model_backend": c.model_backend,
        "layer_indices": c.layer_indices,
        "layer_agg": c.layer_agg,
        "pooling": c.pooling,
        "normalize_residues": c.normalize_residues,
        "normalize": c.normalize,
        "embedding_scale": c.embedding_scale,
        "max_length": c.max_length,
        "use_chunking": c.use_chunking,
        "chunk_size": c.chunk_size,
        "chunk_overlap": c.chunk_overlap,
        "description": c.description,
        "created_at": c.created_at.isoformat(),
    }
    if embedding_count is not None:
        out["embedding_count"] = embedding_count
    return out


# cascade_delete_embedding_config + compute_go_term_distribution own the
# heavy DB work; service-layer wrappers above keep the existence check.
from protea.services._embeddings_admin_helpers import (  # noqa: E402
    cascade_delete_embedding_config,
    compute_go_term_distribution,
)

# PREDICTIONS_TSV_COLUMNS / _format_float / _format_optional canonically
# defined in _embeddings_predictions_helpers; re-exported here so existing
# callers (and ``__all__`` consumers) keep working.
from protea.services._embeddings_predictions_helpers import (  # noqa: E402,F401
    PREDICTIONS_TSV_COLUMNS,
    _format_float,
    _format_optional,
    build_predictions_for_protein,
    iter_predictions_tsv,
)


def assert_prediction_set_exists(session: Session, prediction_set_id: uuid.UUID) -> None:
    """Raise :class:`EntityNotFoundError` if the PredictionSet UUID is unknown."""
    if session.get(PredictionSet, prediction_set_id) is None:
        raise EntityNotFoundError("PredictionSet", prediction_set_id)


def list_prediction_sets_data(session: Session) -> list[dict[str, Any]]:
    """Top 100 most-recent ``PredictionSet`` rows joined with their context.

    Returns a list of dicts each carrying the embedding-config name,
    annotation-set label, ontology version, plus the per-set
    ``prediction_count``. The per-set count comes from a single
    ``GROUP BY`` over GOPrediction (one index-only scan) rather than a
    correlated subquery; for ~10⁷-row tables Postgres' planner falls
    into a per-row index probe with the correlated form (~30s per
    outer row). The grouped form returns all 100 counts at once.
    """
    from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
    from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot

    rows = (
        session.query(
            PredictionSet,
            EmbeddingConfig,
            AnnotationSet,
            OntologySnapshot,
        )
        .join(EmbeddingConfig, PredictionSet.embedding_config_id == EmbeddingConfig.id)
        .join(AnnotationSet, PredictionSet.annotation_set_id == AnnotationSet.id)
        .join(OntologySnapshot, PredictionSet.ontology_snapshot_id == OntologySnapshot.id)
        .order_by(PredictionSet.created_at.desc())
        .limit(100)
        .all()
    )
    counts = {
        set_id: cnt
        for set_id, cnt in session.query(
            GOPrediction.prediction_set_id,
            func.count(GOPrediction.id),
        )
        .group_by(GOPrediction.prediction_set_id)
        .all()
    }
    return [
        {
            "id": str(ps.id),
            "embedding_config_id": str(ps.embedding_config_id),
            "embedding_config_name": ec.model_name,
            "annotation_set_id": str(ps.annotation_set_id),
            "annotation_set_label": (
                f"{ann.source} {ann.source_version}" if ann.source_version else ann.source
            ),
            "ontology_snapshot_id": str(ps.ontology_snapshot_id),
            "ontology_snapshot_version": snap.obo_version,
            "query_set_id": str(ps.query_set_id) if ps.query_set_id else None,
            "limit_per_entry": ps.limit_per_entry,
            "distance_threshold": ps.distance_threshold,
            "created_at": ps.created_at.isoformat(),
            "prediction_count": int(counts.get(ps.id, 0)),
        }
        for ps, ec, ann, snap in rows
    ]


def get_prediction_set_data(
    session: Session,
    prediction_set_id: uuid.UUID,
) -> dict[str, Any]:
    """Retrieve a prediction set with total + per-protein GO term counts.

    Raises :class:`EntityNotFoundError` when the UUID does not resolve.
    """
    ps = session.get(PredictionSet, prediction_set_id)
    if ps is None:
        raise EntityNotFoundError("PredictionSet", prediction_set_id)

    prediction_count = (
        session.query(func.count(GOPrediction.id))
        .filter(GOPrediction.prediction_set_id == prediction_set_id)
        .scalar()
    )

    per_protein = (
        session.query(GOPrediction.protein_accession, func.count(GOPrediction.id))
        .filter(GOPrediction.prediction_set_id == prediction_set_id)
        .group_by(GOPrediction.protein_accession)
        .all()
    )

    return {
        "id": str(ps.id),
        "embedding_config_id": str(ps.embedding_config_id),
        "annotation_set_id": str(ps.annotation_set_id),
        "ontology_snapshot_id": str(ps.ontology_snapshot_id),
        "query_set_id": str(ps.query_set_id) if ps.query_set_id else None,
        "limit_per_entry": ps.limit_per_entry,
        "distance_threshold": ps.distance_threshold,
        "created_at": ps.created_at.isoformat(),
        "prediction_count": prediction_count or 0,
        "per_protein_counts": {acc: cnt for acc, cnt in per_protein},
    }


# prepare_cafa_export + iter_predictions_cafa_tsv live in
# _embeddings_cafa_helpers and are re-exported below so existing
# router/CLI imports keep working unchanged.
from protea.services._embeddings_cafa_helpers import (  # noqa: E402,F401
    iter_predictions_cafa_tsv,
    prepare_cafa_export,
)


def delete_prediction_set_cascade(
    session: Session,
    prediction_set_id: uuid.UUID,
) -> dict[str, Any]:
    """Delete a :class:`PredictionSet` and all its :class:`GOPrediction` rows.

    Returns ``{"deleted": <id>, "predictions_deleted": <count>}``. Raises
    :class:`EntityNotFoundError` when the UUID does not resolve so the
    router can translate to 404.
    """
    ps = session.get(PredictionSet, prediction_set_id)
    if ps is None:
        raise EntityNotFoundError("PredictionSet", prediction_set_id)

    deleted_predictions = (
        session.query(GOPrediction)
        .filter(GOPrediction.prediction_set_id == prediction_set_id)
        .delete(synchronize_session=False)
    )
    session.delete(ps)
    return {"deleted": str(prediction_set_id), "predictions_deleted": deleted_predictions}


def delete_embedding_config_cascade(
    session: Session,
    config_id: uuid.UUID,
) -> dict[str, Any]:
    """Cascade-delete an :class:`EmbeddingConfig` and all linked rows.

    Raises :class:`EntityNotFoundError` when ``config_id`` does not
    resolve. Body lives in
    :func:`_embeddings_admin_helpers.cascade_delete_embedding_config`.
    """
    c = session.get(EmbeddingConfig, config_id)
    if c is None:
        raise EntityNotFoundError("EmbeddingConfig", config_id)
    return cascade_delete_embedding_config(session, c)


# list_proteins_in_prediction_set lives in _embeddings_proteins_helpers and
# is re-exported below so existing router/CLI imports keep working unchanged.
from protea.services._embeddings_proteins_helpers import (  # noqa: E402,F401
    list_proteins_in_prediction_set,
)


def get_predictions_for_protein(
    session: Session,
    *,
    prediction_set_id: uuid.UUID,
    accession: str,
) -> list[dict[str, Any]]:
    """Return all predicted GO terms for one protein, sorted by distance.

    Raises :class:`EntityNotFoundError` when the PredictionSet does
    not resolve. (No 404 for unknown accession; returns empty list,
    matching the legacy endpoint's behaviour.)
    """
    if session.get(PredictionSet, prediction_set_id) is None:
        raise EntityNotFoundError("PredictionSet", prediction_set_id)
    return build_predictions_for_protein(
        session,
        prediction_set_id=prediction_set_id,
        accession=accession,
    )


def get_go_term_distribution_data(
    session: Session,
    *,
    prediction_set_id: uuid.UUID,
    limit: int = 50,
) -> dict[str, Any]:
    """Return the most-frequent GO terms predicted in this set + per-aspect totals.

    Raises :class:`EntityNotFoundError` when the PredictionSet does
    not resolve. Body lives in
    :func:`_embeddings_admin_helpers.compute_go_term_distribution`.
    """
    if session.get(PredictionSet, prediction_set_id) is None:
        raise EntityNotFoundError("PredictionSet", prediction_set_id)
    return compute_go_term_distribution(
        session,
        prediction_set_id=prediction_set_id,
        limit=limit,
    )


__all__ = [
    "PREDICTIONS_TSV_COLUMNS",
    "VALID_BACKENDS",
    "VALID_LAYER_AGG",
    "VALID_POOLING",
    "EmbeddingsServiceError",
    "EntityNotFoundError",
    "InvalidEmbeddingConfigError",
    "InvalidUUIDFieldError",
    "assert_prediction_set_exists",
    "config_to_dict",
    "delete_embedding_config_cascade",
    "delete_prediction_set_cascade",
    "get_go_term_distribution_data",
    "get_prediction_set_data",
    "get_predictions_for_protein",
    "iter_predictions_cafa_tsv",
    "iter_predictions_tsv",
    "list_prediction_sets_data",
    "list_proteins_in_prediction_set",
    "prepare_cafa_export",
    "validate_embedding_config_body",
    "validate_predict_request",
]
