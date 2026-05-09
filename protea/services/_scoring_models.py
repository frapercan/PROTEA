"""Pydantic response models, ORM ↔ response serialisers, and preset
``ScoringConfig`` definitions extracted from ``scoring_service``.

The service module re-exports every public symbol so existing
router/CLI imports keep working unchanged.
"""

from __future__ import annotations

import uuid
from typing import Any

from pydantic import BaseModel, Field, field_validator

from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel
from protea.infrastructure.orm.models.embedding.scoring_config import (
    DEFAULT_EVIDENCE_WEIGHTS,
    DEFAULT_WEIGHTS,
    VALID_FORMULAS,
    ScoringConfig,
)


class RerankerResponse(BaseModel):
    """Serialised representation of a stored :class:`RerankerModel`."""

    id: uuid.UUID = Field(..., description="UUID primary key of the ``RerankerModel`` row.")
    name: str = Field(..., description="Unique human-readable name (also used as artifact key suffix).")
    prediction_set_id: uuid.UUID | None = Field(
        ...,
        description=(
            "UUID of the ``PredictionSet`` this booster was trained "
            "against; ``None`` when the booster is not bound to one."
        ),
    )
    evaluation_set_id: uuid.UUID | None = Field(
        ...,
        description=(
            "UUID of the ``EvaluationSet`` this booster was tuned "
            "against; ``None`` when not bound."
        ),
    )
    category: str = Field(
        ...,
        description="CAFA category (``NK`` / ``LK`` / ``PK``) the booster targets.",
    )
    aspect: str | None = Field(
        ...,
        description=(
            "GO aspect (``BPO`` / ``MFO`` / ``CCO``) when the booster "
            "is aspect-specific; ``None`` for cross-aspect models."
        ),
    )
    metrics: dict[str, Any] = Field(
        ...,
        description="Cached training/eval metrics (Fmax, AUPRC, etc.) keyed by metric name.",
    )
    feature_importance: dict[str, Any] = Field(
        ...,
        description="LightGBM feature-importance map keyed by feature name.",
    )
    created_at: Any = Field(
        ...,
        description="ISO 8601 timestamp of when the row was inserted.",
    )


def to_reranker_response(m: RerankerModel) -> RerankerResponse:
    """Convert an ORM :class:`RerankerModel` to its API response model."""
    return RerankerResponse(
        id=m.id,
        name=m.name,
        prediction_set_id=m.prediction_set_id,
        evaluation_set_id=m.evaluation_set_id,
        category=m.category,
        aspect=m.aspect,
        metrics=m.metrics,
        feature_importance=m.feature_importance,
        created_at=m.created_at,
    )


class ScoringConfigCreate(BaseModel):
    """Request body for ``POST /scoring/configs``.

    Lives in the service module so non-router callers (CLI tools,
    batch scripts) can reuse the validation rules without pulling
    FastAPI in.
    """

    name: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Unique human-readable name; doubles as the row's lookup key.",
    )
    formula: str = Field(
        "linear",
        description=(
            "Aggregation formula across signals: ``linear`` (weighted "
            "sum) or ``evidence_weighted`` (final multiply by evidence "
            "weight). Unknown values are rejected with 422."
        ),
    )
    weights: dict[str, float] = Field(
        default_factory=lambda: dict(DEFAULT_WEIGHTS),
        description=(
            "Per-signal weight map (``embedding_similarity``, "
            "``identity_nw``, ``identity_sw``, ``evidence_weight``, "
            "``taxonomic_proximity``, ``neighbor_vote_fraction``). "
            "Defaults to ``DEFAULT_WEIGHTS``; unknown keys are rejected "
            "with 422."
        ),
    )
    evidence_weights: dict[str, float] | None = Field(
        default=None,
        description=(
            "Per-GO-evidence-code quality overrides in [0, 1]. "
            "NULL means use system defaults. Partial dicts are valid."
        ),
    )
    description: str | None = Field(
        default=None,
        description="Optional human-readable explanation shown in the scoring config picker.",
    )

    model_config = {"extra": "forbid"}

    @field_validator("formula")
    @classmethod
    def validate_formula(cls, v: str) -> str:
        if v not in VALID_FORMULAS:
            raise ValueError(
                f"Unknown formula {v!r}. Valid formulas: {sorted(VALID_FORMULAS)}"
            )
        return v

    @field_validator("weights")
    @classmethod
    def validate_weights(cls, v: dict[str, float]) -> dict[str, float]:
        unknown = set(v.keys()) - set(DEFAULT_WEIGHTS.keys())
        if unknown:
            raise ValueError(
                f"Unknown signal keys: {sorted(unknown)}. "
                f"Valid signals: {sorted(DEFAULT_WEIGHTS.keys())}"
            )
        return v

    @field_validator("evidence_weights")
    @classmethod
    def validate_evidence_weights(cls, v: dict[str, float] | None) -> dict[str, float] | None:
        """Ensure all keys are known GO codes and all values are in [0, 1]."""
        if v is None:
            return None
        known_codes = set(DEFAULT_EVIDENCE_WEIGHTS.keys())
        unknown = set(v.keys()) - known_codes
        if unknown:
            raise ValueError(
                f"Unknown evidence codes: {sorted(unknown)}. Valid codes: {sorted(known_codes)}"
            )
        out_of_range = {k: val for k, val in v.items() if not (0.0 <= val <= 1.0)}
        if out_of_range:
            raise ValueError(f"Evidence weights must be in [0, 1]. Out-of-range: {out_of_range}")
        return v


class ScoringConfigResponse(BaseModel):
    """Serialised representation of a stored :class:`ScoringConfig`."""

    id: uuid.UUID = Field(..., description="UUID primary key of the ``ScoringConfig`` row.")
    name: str = Field(..., description="Unique human-readable name.")
    formula: str = Field(
        ...,
        description="Aggregation formula (``linear`` or ``evidence_weighted``).",
    )
    weights: dict[str, Any] = Field(
        ...,
        description="Resolved per-signal weight map as stored on the row.",
    )
    evidence_weights: dict[str, Any] | None = Field(
        ...,
        description=(
            "Per-evidence-code quality overrides; ``None`` means the "
            "row uses the system defaults."
        ),
    )
    description: str | None = Field(
        ...,
        description="Optional human-readable explanation; ``None`` if not set.",
    )
    created_at: Any = Field(
        ...,
        description="ISO 8601 timestamp of when the row was inserted.",
    )


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


#: Built-in preset ScoringConfigs seeded by the ``POST /configs/presets``
#: endpoint. Documents what the system defaults produce; none of them
#: override evidence weights.
PRESET_CONFIGS: list[dict[str, Any]] = [
    {
        "name": "embedding_only",
        "formula": "linear",
        "weights": {
            "embedding_similarity": 1.0,
            "identity_nw": 0.0,
            "identity_sw": 0.0,
            "evidence_weight": 0.0,
            "taxonomic_proximity": 0.0,
            "neighbor_vote_fraction": 0.0,
        },
        "description": (
            "Pure cosine similarity of the winning neighbour, converted to [0, 1]. "
            "Baseline; tests the hypothesis that the nearest-neighbour distance "
            "alone is enough signal."
        ),
    },
    {
        "name": "vote_fraction",
        "formula": "linear",
        "weights": {
            "embedding_similarity": 0.0,
            "identity_nw": 0.0,
            "identity_sw": 0.0,
            "evidence_weight": 0.0,
            "taxonomic_proximity": 0.0,
            "neighbor_vote_fraction": 1.0,
        },
        "description": (
            "Canonical KNN score: fraction of the K neighbours that vote for each "
            "GO term. Tests the hypothesis that consensus across neighbours beats "
            "the raw cosine distance of the top-1 neighbour."
        ),
    },
    {
        "name": "alignment_only",
        "formula": "linear",
        "weights": {
            "embedding_similarity": 0.0,
            "identity_nw": 0.6,
            "identity_sw": 0.4,
            "evidence_weight": 0.0,
            "taxonomic_proximity": 0.0,
            "neighbor_vote_fraction": 0.0,
        },
        "description": (
            "Pure sequence-identity score (NW global 60 % + SW local 40 %), no embedding. "
            "Tests whether classical sequence alignment alone can match PLM-based KNN. "
            "Requires compute_alignments=True."
        ),
    },
    {
        "name": "embedding_plus_alignment",
        "formula": "linear",
        "weights": {
            "embedding_similarity": 0.5,
            "identity_nw": 0.3,
            "identity_sw": 0.2,
            "evidence_weight": 0.0,
            "taxonomic_proximity": 0.0,
            "neighbor_vote_fraction": 0.0,
        },
        "description": (
            "Embedding (50 %) refined with global NW identity (30 %) and local SW "
            "identity (20 %). Tests whether alignment adds a usable signal on top "
            "of the embedding. Requires compute_alignments=True."
        ),
    },
    {
        "name": "embedding_plus_vote",
        "formula": "linear",
        "weights": {
            "embedding_similarity": 0.5,
            "identity_nw": 0.0,
            "identity_sw": 0.0,
            "evidence_weight": 0.0,
            "taxonomic_proximity": 0.0,
            "neighbor_vote_fraction": 0.5,
        },
        "description": (
            "Nearest-neighbour distance (50 %) combined with K-neighbour consensus "
            "(50 %). Tests whether adding voting on top of cosine distance improves "
            "the ranking vs either signal alone."
        ),
    },
    {
        "name": "evidence_veto",
        "formula": "evidence_weighted",
        "weights": {
            "embedding_similarity": 1.0,
            "identity_nw": 0.0,
            "identity_sw": 0.0,
            "evidence_weight": 0.0,
            "taxonomic_proximity": 0.0,
            "neighbor_vote_fraction": 0.0,
        },
        "description": (
            "Embedding similarity, multiplied by the resolved evidence weight as a "
            "final veto (evidence_weighted formula with evidence_weight=0 in the "
            "linear sum to avoid double-counting). Tests whether down-ranking IEA/ND "
            "predictions via a clean multiplier beats feeding evidence into the sum."
        ),
    },
    {
        "name": "composite",
        "formula": "linear",
        "weights": {
            "embedding_similarity": 0.4,
            "identity_nw": 0.2,
            "identity_sw": 0.1,
            "evidence_weight": 0.0,
            "taxonomic_proximity": 0.1,
            "neighbor_vote_fraction": 0.2,
        },
        "description": (
            "Kitchen-sink linear mix: embedding + alignment + taxonomy + voting. "
            "evidence_weight excluded from the linear sum (use evidence_veto when "
            "you want the multiplier). Requires compute_alignments=True and "
            "compute_taxonomy=True; tests whether more signals beat fewer."
        ),
    },
]
