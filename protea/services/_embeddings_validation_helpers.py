"""Embedding-config request-body validation helpers for ``embeddings_service``.

The orchestrator ``validate_embedding_config_body`` lives here split
into per-field-group helpers so neither it nor any helper exceeds the
§3 method-LOC ceiling, and ``embeddings_service.py`` can shrink
toward the file-LOC budget.

The function is re-exported from ``protea.services.embeddings_service``
for backwards compatibility with router and CLI callers.

Validation is duck-typed (manual ``isinstance`` checks) rather than
Pydantic to preserve the exact response payload shape and message
wording the existing tests assert on.
"""

from __future__ import annotations

from typing import Any

# The three closed vocabularies live beside the columns they constrain and
# are imported rather than restated. There used to be two copies, this one
# and the model's, and they had already drifted by two entries: the tags an
# encoder operation writes were missing here for months while two merged
# operations wrote them on every run. ``tests/test_backend_registry.py``
# closes the general case by reading the operations themselves.
from protea.core._evaluation_leakage import FITTED_BACKENDS
from protea.infrastructure.orm.models.embedding.embedding_config import (
    VALID_BACKENDS,
    VALID_KINDS,
    VALID_LAYER_AGG,
    VALID_POOLING,
)


def _validate_model_fields(body: dict[str, Any], errors: list[str]) -> None:
    """Validate model_name, model_backend, layer_indices, layer_agg, pooling."""
    model_name = body.get("model_name")
    if not isinstance(model_name, str) or not model_name.strip():
        errors.append("model_name must be a non-empty string")

    if body.get("model_backend") not in VALID_BACKENDS:
        errors.append(f"model_backend must be one of {sorted(VALID_BACKENDS)}")

    layer_indices = body.get("layer_indices")
    if (
        not isinstance(layer_indices, list)
        or len(layer_indices) == 0
        or not all(isinstance(i, int) for i in layer_indices)
    ):
        errors.append("layer_indices must be a non-empty list of ints")

    if body.get("layer_agg") not in VALID_LAYER_AGG:
        errors.append(f"layer_agg must be one of {sorted(VALID_LAYER_AGG)}")

    if body.get("pooling") not in VALID_POOLING:
        errors.append(f"pooling must be one of {sorted(VALID_POOLING)}")


def _validate_boolean_flags(body: dict[str, Any], errors: list[str]) -> None:
    """Validate normalize_residues, normalize, use_chunking flags are bools."""
    if not isinstance(body.get("normalize_residues", False), bool):
        errors.append("normalize_residues must be a boolean")
    if not isinstance(body.get("normalize", True), bool):
        errors.append("normalize must be a boolean")
    if not isinstance(body.get("use_chunking", False), bool):
        errors.append("use_chunking must be a boolean")


def _validate_chunking_sizes(body: dict[str, Any], errors: list[str]) -> None:
    """Validate max_length / chunk_size / chunk_overlap with cross-field check.

    Requires ``chunk_overlap`` strictly less than ``chunk_size`` so the
    chunker makes forward progress on every step.
    """
    max_length = body.get("max_length", 1022)
    # None is the full-length configuration: no truncation applied. It is a
    # value, not a missing field, so it must pass where 0 and negatives do not.
    if max_length is not None and (not isinstance(max_length, int) or max_length <= 0):
        errors.append("max_length must be a positive integer, or null for full length")

    chunk_size = body.get("chunk_size", 512)
    if not isinstance(chunk_size, int) or chunk_size <= 0:
        errors.append("chunk_size must be a positive integer")

    chunk_overlap = body.get("chunk_overlap", 0)
    if not isinstance(chunk_overlap, int) or chunk_overlap < 0:
        errors.append("chunk_overlap must be a non-negative integer")

    if (
        isinstance(chunk_size, int)
        and isinstance(chunk_overlap, int)
        and chunk_overlap >= chunk_size
    ):
        errors.append(
            f"chunk_overlap ({chunk_overlap}) must be strictly less "
            f"than chunk_size ({chunk_size})"
        )


def _validate_embedding_scale(body: dict[str, Any], errors: list[str]) -> None:
    """Validate the optional ``embedding_scale`` divisor (default 1.0).

    A uniform per-config divisor lets high-magnitude PLM layers fit the fp16
    halfvec store; it must be a strictly positive number. ``bool`` is rejected
    explicitly because ``isinstance(True, int)`` is true in Python.
    """
    scale = body.get("embedding_scale", 1.0)
    if isinstance(scale, bool) or not isinstance(scale, (int, float)) or scale <= 0:
        errors.append("embedding_scale must be a positive number")


def _validate_description(body: dict[str, Any], errors: list[str]) -> None:
    description = body.get("description", None)
    if description is not None and not isinstance(description, str):
        errors.append("description must be a string or null")
    display_name = body.get("display_name", None)
    if display_name is not None and not isinstance(display_name, str):
        errors.append("display_name must be a string or null")
    family = body.get("family", None)
    if family is not None and not isinstance(family, str):
        errors.append("family must be a string or null")


def _validate_provenance(body: dict[str, Any], errors: list[str]) -> None:
    """The fields that say what this configuration descends from and saw.

    THE CUT IS WHY THIS EXISTS. ``trained_on_annotation_set_id`` is the only
    place a fitted encoding records which annotation release it saw, and
    ``protea.core._evaluation_leakage`` reads it to decide whether the
    encoding may be scored on a frame at all. The canonicaliser used to drop
    it, along with ``kind``, ``family`` and the parent config, so every
    configuration created over HTTP arrived claiming to be a pretrained
    backbone with nothing to declare. For a backend that is only ever
    produced by fitting, that is not a missing field: it is a row that
    ``check_training_cut`` will refuse later with "is fitted but declares no
    training release", after someone has spent a run on it.

    So the cut is required exactly when the backend says the artifact was
    fitted, and refused at creation rather than at scoring time.
    """
    kind = body.get("kind", "pretrained")
    if kind not in VALID_KINDS:
        errors.append(f"kind must be one of {sorted(VALID_KINDS)}")
    cut = body.get("trained_on_annotation_set_id")
    parent = body.get("derived_from_embedding_config_id")
    for name, value in (("trained_on_annotation_set_id", cut), ("derived_from_embedding_config_id", parent)):
        if value is not None and not _is_uuid(value):
            errors.append(f"{name} must be a uuid or null")
    fitted = body.get("model_backend") in FITTED_BACKENDS or kind == "learned"
    if fitted and cut is None:
        errors.append(
            "trained_on_annotation_set_id is required for a fitted encoding "
            f"(backend in {sorted(FITTED_BACKENDS)} or kind 'learned'): the "
            "leakage guard refuses to score one that declares no training "
            "release, so a row without it could never be used"
        )
    if not fitted and cut is not None:
        errors.append(
            "trained_on_annotation_set_id is for an encoding that was FITTED. A "
            "pretrained backbone saw none of our annotations and declaring a "
            "cut for it would be false"
        )


def _is_uuid(value: Any) -> bool:
    import uuid as _uuid

    try:
        _uuid.UUID(str(value))
    except (ValueError, AttributeError, TypeError):
        return False
    return True


def _canonicalise_config(body: dict[str, Any]) -> dict[str, Any]:
    """Build the validated, defaults-filled output dict.

    Only call after validation passed: bypasses defaults the
    individual field validators applied (the ``body.get(name, default)``
    fallbacks above) so the returned dict is the canonical shape.
    """
    return {
        "model_name": body.get("model_name"),
        "model_backend": body.get("model_backend"),
        "layer_indices": body.get("layer_indices"),
        "layer_agg": body.get("layer_agg"),
        "pooling": body.get("pooling"),
        "normalize_residues": body.get("normalize_residues", False),
        "normalize": body.get("normalize", True),
        "embedding_scale": float(body.get("embedding_scale", 1.0)),
        "max_length": body.get("max_length", 1022),
        "use_chunking": body.get("use_chunking", False),
        "chunk_size": body.get("chunk_size", 512),
        "chunk_overlap": body.get("chunk_overlap", 0),
        "description": body.get("description", None),
        "display_name": body.get("display_name", None),
        "family": body.get("family", None),
        "kind": body.get("kind", "pretrained"),
        "derived_from_embedding_config_id": body.get("derived_from_embedding_config_id"),
        "trained_on_annotation_set_id": body.get("trained_on_annotation_set_id"),
    }


def validate_embedding_config_body(body: dict[str, Any]) -> dict[str, Any]:
    """Validate a request body for ``POST /embeddings/configs``.

    Returns the canonicalised dict (defaults filled in) on success.
    Raises :class:`InvalidEmbeddingConfigError` (imported lazily to
    avoid the circular dep with ``embeddings_service``) with the full
    list of failures otherwise; the router translates that to a 422
    with the same shape it produced before extraction.

    Decomposed into per-field-group helpers so neither this orchestrator
    nor any helper breaches the 60-LOC method ceiling.
    """
    from protea.services.embeddings_service import InvalidEmbeddingConfigError

    errors: list[str] = []
    _validate_model_fields(body, errors)
    _validate_boolean_flags(body, errors)
    _validate_chunking_sizes(body, errors)
    _validate_embedding_scale(body, errors)
    _validate_description(body, errors)
    _validate_provenance(body, errors)

    if errors:
        raise InvalidEmbeddingConfigError(errors)
    return _canonicalise_config(body)
