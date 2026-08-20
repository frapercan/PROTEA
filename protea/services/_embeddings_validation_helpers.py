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

#: Every ``model_backend`` a config may carry.
#:
#: The five language-model backends and ``auto``, plus the two an encoder
#: operation writes. Those last two were missing for months while two merged
#: operations wrote them on every run, which is the failure mode this set has
#: by construction: nothing derives it, so it holds the values somebody
#: remembered rather than the values the code produces. ``tests/
#: test_backend_registry.py`` closes that by reading the operations.
#:
#: The two encoder tags are not interchangeable and the pairing is the point:
#:
#:   residue-sparse   select-then-pool, written by encode_residue_sparse
#:   learned-code     pool-then-select, written by apply_learned_encoder
#:
#: Each operation accepts only its own order and refuses the other by name, so
#: the tag is a fact about how a code was computed rather than a label anyone
#: chose. That is what lets a reader tell an ablation control from a serving
#: candidate by filtering instead of by reading prose.
VALID_BACKENDS: frozenset[str] = frozenset(
    {"esm", "esm3c", "t5", "ankh", "protst", "auto", "residue-sparse", "learned-code"}
)
VALID_LAYER_AGG: frozenset[str] = frozenset({"mean", "last", "concat"})
VALID_POOLING: frozenset[str] = frozenset({"mean", "max", "cls", "mean_max"})


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
    if not isinstance(max_length, int) or max_length <= 0:
        errors.append("max_length must be a positive integer")

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

    if errors:
        raise InvalidEmbeddingConfigError(errors)
    return _canonicalise_config(body)
