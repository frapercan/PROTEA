"""Embeddings service — pure-logic helpers extracted from
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

import csv
import io
import uuid
from collections.abc import Iterator
from typing import Any

from sqlalchemy import func
from sqlalchemy.orm import Session, sessionmaker

from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
from protea.infrastructure.session import session_scope

# Allowed values for the embedding config fields. Mirror the (de
# facto) public API contract; new backends/aggs/poolings are added by
# extending these sets.
VALID_BACKENDS: frozenset[str] = frozenset({"esm", "esm3c", "t5", "ankh", "auto"})
VALID_LAYER_AGG: frozenset[str] = frozenset({"mean", "last", "concat"})
VALID_POOLING: frozenset[str] = frozenset({"mean", "max", "cls", "mean_max"})


class EmbeddingsServiceError(Exception):
    """Base class for embeddings-service domain errors."""


class EntityNotFoundError(EmbeddingsServiceError):
    """Generic 404 — a referenced entity does not exist.

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


def validate_embedding_config_body(body: dict[str, Any]) -> dict[str, Any]:
    """Validate a request body for ``POST /embeddings/configs``.

    Returns the canonicalised dict (defaults filled in) on success.
    Raises :class:`InvalidEmbeddingConfigError` with the full list
    of failures otherwise; the router translates that to a 422 with
    the same shape it produced before extraction.

    The validation is duck-typed (manual ``isinstance`` checks)
    rather than Pydantic to preserve the exact response payload
    shape and message wording the existing tests assert on.
    """
    errors: list[str] = []

    model_name = body.get("model_name")
    if not isinstance(model_name, str) or not model_name.strip():
        errors.append("model_name must be a non-empty string")

    model_backend = body.get("model_backend")
    if model_backend not in VALID_BACKENDS:
        errors.append(f"model_backend must be one of {sorted(VALID_BACKENDS)}")

    layer_indices = body.get("layer_indices")
    if (
        not isinstance(layer_indices, list)
        or len(layer_indices) == 0
        or not all(isinstance(i, int) for i in layer_indices)
    ):
        errors.append("layer_indices must be a non-empty list of ints")

    layer_agg = body.get("layer_agg")
    if layer_agg not in VALID_LAYER_AGG:
        errors.append(f"layer_agg must be one of {sorted(VALID_LAYER_AGG)}")

    pooling = body.get("pooling")
    if pooling not in VALID_POOLING:
        errors.append(f"pooling must be one of {sorted(VALID_POOLING)}")

    normalize_residues = body.get("normalize_residues", False)
    if not isinstance(normalize_residues, bool):
        errors.append("normalize_residues must be a boolean")

    normalize = body.get("normalize", True)
    if not isinstance(normalize, bool):
        errors.append("normalize must be a boolean")

    max_length = body.get("max_length", 1022)
    if not isinstance(max_length, int) or max_length <= 0:
        errors.append("max_length must be a positive integer")

    use_chunking = body.get("use_chunking", False)
    if not isinstance(use_chunking, bool):
        errors.append("use_chunking must be a boolean")

    chunk_size = body.get("chunk_size", 512)
    if not isinstance(chunk_size, int) or chunk_size <= 0:
        errors.append("chunk_size must be a positive integer")

    chunk_overlap = body.get("chunk_overlap", 0)
    if not isinstance(chunk_overlap, int) or chunk_overlap < 0:
        errors.append("chunk_overlap must be a non-negative integer")

    description = body.get("description", None)
    if description is not None and not isinstance(description, str):
        errors.append("description must be a string or null")

    if (
        isinstance(chunk_size, int)
        and isinstance(chunk_overlap, int)
        and chunk_overlap >= chunk_size
    ):
        errors.append(
            f"chunk_overlap ({chunk_overlap}) must be strictly less "
            f"than chunk_size ({chunk_size})"
        )

    if errors:
        raise InvalidEmbeddingConfigError(errors)

    return {
        "model_name": model_name,
        "model_backend": model_backend,
        "layer_indices": layer_indices,
        "layer_agg": layer_agg,
        "pooling": pooling,
        "normalize_residues": normalize_residues,
        "normalize": normalize,
        "max_length": max_length,
        "use_chunking": use_chunking,
        "chunk_size": chunk_size,
        "chunk_overlap": chunk_overlap,
        "description": description,
    }


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


PREDICTIONS_TSV_COLUMNS: tuple[str, ...] = (
    "protein_accession",
    "go_id",
    "go_name",
    "go_aspect",
    "distance",
    "ref_protein_accession",
    "qualifier",
    "evidence_code",
    # NW alignment
    "identity_nw",
    "similarity_nw",
    "alignment_score_nw",
    "gaps_pct_nw",
    "alignment_length_nw",
    # SW alignment
    "identity_sw",
    "similarity_sw",
    "alignment_score_sw",
    "gaps_pct_sw",
    "alignment_length_sw",
    # Lengths
    "length_query",
    "length_ref",
    # Taxonomy
    "query_taxonomy_id",
    "ref_taxonomy_id",
    "taxonomic_lca",
    "taxonomic_distance",
    "taxonomic_common_ancestors",
    "taxonomic_relation",
    # Re-ranker features
    "vote_count",
    "k_position",
    "go_term_frequency",
    "ref_annotation_density",
    "neighbor_distance_std",
)


def _format_float(v: float | None) -> str:
    """Format a nullable float for TSV output (``""`` for ``None``)."""
    if v is None:
        return ""
    return f"{v:.6g}"


def _format_optional(v: Any) -> Any:
    """Render ``None`` as the empty string; otherwise pass through."""
    return "" if v is None else v


def assert_prediction_set_exists(session: Session, prediction_set_id: uuid.UUID) -> None:
    """Raise :class:`EntityNotFoundError` if the PredictionSet UUID is unknown."""
    if session.get(PredictionSet, prediction_set_id) is None:
        raise EntityNotFoundError("PredictionSet", prediction_set_id)


def iter_predictions_tsv(
    factory: Any,
    *,
    prediction_set_id: uuid.UUID,
    accession: str | None = None,
    aspect: str | None = None,
    max_distance: float | None = None,
) -> Any:
    """Yield TSV rows (as ``str``) of every GOPrediction in a set.

    Opens its own session inside the generator so the caller's
    existence-check session can close cleanly. The first yielded
    chunk is the header line; one row per ``(GOPrediction, GOTerm)``
    pair follows, ordered by ``(protein_accession, distance)``.

    Optional filters: ``accession`` (single query protein),
    ``aspect`` (``F`` / ``P`` / ``C``), ``max_distance``.
    """
    with session_scope(factory) as session:
        buf = io.StringIO()
        writer = csv.writer(buf, delimiter="\t", lineterminator="\n")
        writer.writerow(PREDICTIONS_TSV_COLUMNS)
        yield buf.getvalue()

        q = (
            session.query(GOPrediction, GOTerm)
            .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
            .filter(GOPrediction.prediction_set_id == prediction_set_id)
        )
        if accession:
            q = q.filter(GOPrediction.protein_accession == accession)
        if aspect:
            q = q.filter(GOTerm.aspect == aspect.upper())
        if max_distance is not None:
            q = q.filter(GOPrediction.distance <= max_distance)

        q = q.order_by(GOPrediction.protein_accession, GOPrediction.distance)

        for pred, gt in q.yield_per(1000):
            buf = io.StringIO()
            writer = csv.writer(buf, delimiter="\t", lineterminator="\n")
            writer.writerow(
                [
                    pred.protein_accession,
                    gt.go_id,
                    gt.name,
                    gt.aspect,
                    pred.distance,
                    pred.ref_protein_accession,
                    pred.qualifier or "",
                    pred.evidence_code or "",
                    _format_float(pred.identity_nw),
                    _format_float(pred.similarity_nw),
                    _format_float(pred.alignment_score_nw),
                    _format_float(pred.gaps_pct_nw),
                    _format_float(pred.alignment_length_nw),
                    _format_float(pred.identity_sw),
                    _format_float(pred.similarity_sw),
                    _format_float(pred.alignment_score_sw),
                    _format_float(pred.gaps_pct_sw),
                    _format_float(pred.alignment_length_sw),
                    _format_optional(pred.length_query),
                    _format_optional(pred.length_ref),
                    _format_optional(pred.query_taxonomy_id),
                    _format_optional(pred.ref_taxonomy_id),
                    _format_optional(pred.taxonomic_lca),
                    _format_optional(pred.taxonomic_distance),
                    _format_optional(pred.taxonomic_common_ancestors),
                    pred.taxonomic_relation or "",
                    _format_optional(pred.vote_count),
                    _format_optional(pred.k_position),
                    _format_optional(pred.go_term_frequency),
                    _format_optional(pred.ref_annotation_density),
                    _format_float(pred.neighbor_distance_std),
                ]
            )
            yield buf.getvalue()


def list_prediction_sets_data(session: Session) -> list[dict[str, Any]]:
    """Top 100 most-recent ``PredictionSet`` rows joined with their context.

    Returns a list of dicts each carrying the embedding-config name,
    annotation-set label, ontology version, plus the per-set
    ``prediction_count``. The per-set count comes from a single
    ``GROUP BY`` over GOPrediction (one index-only scan) rather than a
    correlated subquery — for ~10⁷-row tables Postgres' planner falls
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


def prepare_cafa_export(
    session: Session,
    *,
    prediction_set_id: uuid.UUID,
    eval_id: uuid.UUID | None,
) -> set[str] | None:
    """Preflight CAFA export: validate the PredictionSet exists and, if an
    ``EvaluationSet`` was supplied, compute the union of NK + LK delta
    proteins to restrict the export.

    Returns the delta-protein accession set when ``eval_id`` is provided
    (the streaming generator filters on it), otherwise ``None``.

    Raises :class:`EntityNotFoundError` for missing PredictionSet or
    EvaluationSet so the router can translate to 404.
    """
    from protea.core.evaluation import compute_evaluation_data
    from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
    from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet

    if session.get(PredictionSet, prediction_set_id) is None:
        raise EntityNotFoundError("PredictionSet", prediction_set_id)

    if eval_id is None:
        return None

    e = session.get(EvaluationSet, eval_id)
    if e is None:
        raise EntityNotFoundError("EvaluationSet", eval_id)
    ann_old = session.get(AnnotationSet, e.old_annotation_set_id)
    if ann_old is None:
        raise EntityNotFoundError("AnnotationSet", e.old_annotation_set_id)
    data = compute_evaluation_data(
        session,
        e.old_annotation_set_id,
        e.new_annotation_set_id,
        ann_old.ontology_snapshot_id,
    )
    return set(data.nk) | set(data.lk)


def iter_predictions_cafa_tsv(
    factory: sessionmaker[Session],
    *,
    prediction_set_id: uuid.UUID,
    aspect: str | None,
    max_distance: float | None,
    delta_proteins: set[str] | None,
) -> Iterator[str]:
    """Stream the CAFA-format prediction TSV.

    DB-level deduplication: a ``GROUP BY (protein_accession, go_term_id)``
    + ``MIN(distance)`` subquery keeps the best row per pair so the
    Python side never needs an unbounded ``seen`` set — true streaming.
    Score is ``max(0.0, 1.0 - distance)`` clamped to ``[0, 1]``.
    """
    with session_scope(factory) as session:
        min_dist_q = session.query(
            GOPrediction.protein_accession,
            GOPrediction.go_term_id,
            func.min(GOPrediction.distance).label("min_distance"),
        ).filter(GOPrediction.prediction_set_id == prediction_set_id)
        if max_distance is not None:
            min_dist_q = min_dist_q.filter(GOPrediction.distance <= max_distance)
        min_dist = min_dist_q.group_by(
            GOPrediction.protein_accession, GOPrediction.go_term_id
        ).subquery()

        q = session.query(
            min_dist.c.protein_accession, GOTerm.go_id, min_dist.c.min_distance
        ).join(GOTerm, min_dist.c.go_term_id == GOTerm.id)
        if aspect:
            q = q.filter(GOTerm.aspect == aspect.upper())
        if delta_proteins is not None:
            q = q.filter(min_dist.c.protein_accession.in_(delta_proteins))

        q = q.order_by(min_dist.c.protein_accession, GOTerm.go_id)

        for acc, go_id, dist in q.yield_per(1000):
            score = max(0.0, 1.0 - dist)
            yield f"{acc}\t{go_id}\t{score:.4f}\n"


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

    Bulk-deletes the dependent ``GOPrediction`` (via PredictionSet),
    ``PredictionSet``, and ``SequenceEmbedding`` rows; then the
    config itself. Returns a summary dict with the deletion counts.

    The ORM-level ``ondelete`` cascade would handle this on
    ``session.delete(c)`` alone, but we bulk-delete explicitly here
    so the response reports per-table counts the UI surfaces.

    Raises :class:`EntityNotFoundError` when ``config_id`` does not
    resolve.
    """
    c = session.get(EmbeddingConfig, config_id)
    if c is None:
        raise EntityNotFoundError("EmbeddingConfig", config_id)

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

    session.delete(c)

    return {
        "deleted": str(config_id),
        "embeddings_deleted": deleted_embeddings,
        "prediction_sets_deleted": deleted_prediction_sets,
        "predictions_deleted": deleted_predictions,
    }


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
    not resolve. (No 404 for unknown accession — returns empty list,
    matching the legacy endpoint's behaviour.)
    """
    if session.get(PredictionSet, prediction_set_id) is None:
        raise EntityNotFoundError("PredictionSet", prediction_set_id)

    rows = (
        session.query(GOPrediction, GOTerm)
        .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
        .filter(
            GOPrediction.prediction_set_id == prediction_set_id,
            GOPrediction.protein_accession == accession,
        )
        .order_by(GOPrediction.distance)
        .all()
    )

    return [
        {
            "go_id": gt.go_id,
            "name": gt.name,
            "aspect": gt.aspect,
            "distance": round(pred.distance, 4),
            "ref_protein_accession": pred.ref_protein_accession,
            "qualifier": pred.qualifier,
            "evidence_code": pred.evidence_code,
            "identity_nw": pred.identity_nw,
            "similarity_nw": pred.similarity_nw,
            "alignment_score_nw": pred.alignment_score_nw,
            "gaps_pct_nw": pred.gaps_pct_nw,
            "alignment_length_nw": pred.alignment_length_nw,
            "identity_sw": pred.identity_sw,
            "similarity_sw": pred.similarity_sw,
            "alignment_score_sw": pred.alignment_score_sw,
            "gaps_pct_sw": pred.gaps_pct_sw,
            "alignment_length_sw": pred.alignment_length_sw,
            "length_query": pred.length_query,
            "length_ref": pred.length_ref,
            "query_taxonomy_id": pred.query_taxonomy_id,
            "ref_taxonomy_id": pred.ref_taxonomy_id,
            "taxonomic_lca": pred.taxonomic_lca,
            "taxonomic_distance": pred.taxonomic_distance,
            "taxonomic_common_ancestors": pred.taxonomic_common_ancestors,
            "taxonomic_relation": pred.taxonomic_relation,
            "vote_count": pred.vote_count,
            "k_position": pred.k_position,
            "go_term_frequency": pred.go_term_frequency,
            "ref_annotation_density": pred.ref_annotation_density,
            "neighbor_distance_std": pred.neighbor_distance_std,
        }
        for pred, gt in rows
    ]


def get_go_term_distribution_data(
    session: Session,
    *,
    prediction_set_id: uuid.UUID,
    limit: int = 50,
) -> dict[str, Any]:
    """Return the most-frequent GO terms predicted in this set + per-aspect totals.

    Raises :class:`EntityNotFoundError` when the PredictionSet does
    not resolve.
    """
    if session.get(PredictionSet, prediction_set_id) is None:
        raise EntityNotFoundError("PredictionSet", prediction_set_id)

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
