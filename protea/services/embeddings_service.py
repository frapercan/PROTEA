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
from typing import Any

from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
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

    ``entity`` is a human-readable label (e.g. ``"PredictionSet"``)
    used in the error message; ``entity_id`` is the looked-up UUID.
    """

    def __init__(self, entity: str, entity_id: uuid.UUID) -> None:
        self.entity = entity
        self.entity_id = entity_id
        super().__init__(f"{entity} not found")


class InvalidEmbeddingConfigError(EmbeddingsServiceError):
    """Validation failure for an EmbeddingConfig request body.

    ``errors`` carries a list of human-readable messages, one per
    failed rule, suitable for inclusion in the HTTP 422 response
    body.
    """

    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        super().__init__("; ".join(errors) if errors else "invalid embedding config")


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


__all__ = [
    "PREDICTIONS_TSV_COLUMNS",
    "VALID_BACKENDS",
    "VALID_LAYER_AGG",
    "VALID_POOLING",
    "EmbeddingsServiceError",
    "EntityNotFoundError",
    "InvalidEmbeddingConfigError",
    "assert_prediction_set_exists",
    "config_to_dict",
    "iter_predictions_tsv",
    "validate_embedding_config_body",
]
