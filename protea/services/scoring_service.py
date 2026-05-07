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

from pydantic import BaseModel, Field, field_validator
from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.evaluation import compute_evaluation_data
from protea.core.metrics import compute_cafa_metrics
from protea.core.reranker import load_reranker, model_from_string
from protea.core.reranker import predict as _reranker_predict
from protea.core.scoring import compute_score
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel
from protea.infrastructure.orm.models.embedding.scoring_config import (
    DEFAULT_EVIDENCE_WEIGHTS,
    DEFAULT_WEIGHTS,
    FORMULA_EVIDENCE_WEIGHTED,
    VALID_FORMULAS,
    ScoringConfig,
)
from protea.infrastructure.session import session_scope
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store


class ScoringServiceError(Exception):
    """Base class for scoring-service domain errors."""


class EntityNotFoundError(ScoringServiceError):
    """Generic 404 — a referenced entity does not exist.

    ``entity`` is a human-readable label (e.g. ``"PredictionSet"``)
    used in the error message; ``entity_id`` is the looked-up UUID.
    """

    def __init__(self, entity: str, entity_id: uuid.UUID) -> None:
        self.entity = entity
        self.entity_id = entity_id
        super().__init__(f"{entity} not found")


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


class RerankerResponse(BaseModel):
    """Serialised representation of a stored :class:`RerankerModel`."""

    id: uuid.UUID
    name: str
    prediction_set_id: uuid.UUID | None
    evaluation_set_id: uuid.UUID | None
    category: str
    aspect: str | None
    metrics: dict[str, Any]
    feature_importance: dict[str, Any]
    created_at: Any


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

    name: str = Field(..., min_length=1, max_length=255)
    formula: str = Field("linear")
    weights: dict[str, float] = Field(default_factory=lambda: dict(DEFAULT_WEIGHTS))
    evidence_weights: dict[str, float] | None = Field(
        default=None,
        description=(
            "Per-GO-evidence-code quality overrides in [0, 1]. "
            "NULL means use system defaults. Partial dicts are valid."
        ),
    )
    description: str | None = None

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

    id: uuid.UUID
    name: str
    formula: str
    weights: dict[str, Any]
    evidence_weights: dict[str, Any] | None
    description: str | None
    created_at: Any


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
            "Baseline — tests the hypothesis that the nearest-neighbour distance "
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


_SCORED_TSV_COLUMNS: tuple[str, ...] = (
    "protein_accession",
    "go_id",
    "score",
    "distance",
    "ref_protein_accession",
    "evidence_code",
    "qualifier",
    "identity_nw",
    "identity_sw",
    "taxonomic_distance",
    "neighbor_vote_fraction",
)


def _format_optional(value: Any) -> str:
    """Render ``None`` as the empty string; otherwise stringify."""
    return "" if value is None else str(value)


def iter_scored_predictions(
    factory: Any,
    *,
    prediction_set_id: uuid.UUID,
    config_snap: ScoringConfig,
    min_score: float | None = None,
    accession: str | None = None,
) -> Any:
    """Yield TSV rows (as bytes) of scored predictions.

    Opens its own session inside the generator so the route's initial
    validation phase can close its session before streaming starts —
    avoids holding a DB connection open for the duration of the
    response. The first yielded chunk is the header line; one row
    per GOPrediction follows.
    """
    header = "\t".join(_SCORED_TSV_COLUMNS) + "\n"
    yield header.encode()

    with session_scope(factory) as session:
        q = (
            session.query(GOPrediction, GOTerm.go_id)
            .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
            .filter(GOPrediction.prediction_set_id == prediction_set_id)
        )
        if accession:
            q = q.filter(GOPrediction.protein_accession == accession)

        for pred, go_id in q.yield_per(1000):
            pred_dict = {
                "distance": pred.distance,
                "identity_nw": pred.identity_nw,
                "identity_sw": pred.identity_sw,
                "evidence_code": pred.evidence_code,
                "taxonomic_distance": pred.taxonomic_distance,
                "neighbor_vote_fraction": pred.neighbor_vote_fraction,
            }
            score = compute_score(pred_dict, config_snap)
            if min_score is not None and score < min_score:
                continue

            row = (
                "\t".join(
                    [
                        pred.protein_accession,
                        go_id,
                        str(score),
                        _format_optional(pred.distance),
                        pred.ref_protein_accession or "",
                        pred.evidence_code or "",
                        pred.qualifier or "",
                        _format_optional(pred.identity_nw),
                        _format_optional(pred.identity_sw),
                        _format_optional(pred.taxonomic_distance),
                        _format_optional(pred.neighbor_vote_fraction),
                    ]
                )
                + "\n"
            )
            yield row.encode()


_TRAINING_TSV_COLUMNS: tuple[str, ...] = (
    "protein_accession",
    "go_id",
    "aspect",
    "label",
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


def prepare_training_data_request(
    session: Session,
    *,
    prediction_set_id: uuid.UUID,
    evaluation_set_id: uuid.UUID,
    category: str,
) -> set[tuple[str, str]]:
    """Validate the request and compute the ``(protein, go_id)`` ground-truth pair set.

    Looks up the PredictionSet and EvaluationSet, computes the temporal
    NK/LK/PK delta via :func:`compute_evaluation_data`, then flattens the
    requested category's ``{protein → set[go_id]}`` mapping into a flat
    set of pairs the streaming generator can probe in O(1) per row.

    Raises
    ------
    EntityNotFoundError
        Either ``PredictionSet`` or ``EvaluationSet`` does not exist.
    """
    from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet

    ps = session.get(PredictionSet, prediction_set_id)
    if ps is None:
        raise EntityNotFoundError("PredictionSet", prediction_set_id)
    es = session.get(EvaluationSet, evaluation_set_id)
    if es is None:
        raise EntityNotFoundError("EvaluationSet", evaluation_set_id)

    eval_data = compute_evaluation_data(
        session,
        old_annotation_set_id=es.old_annotation_set_id,
        new_annotation_set_id=es.new_annotation_set_id,
        ontology_snapshot_id=ps.ontology_snapshot_id,
    )

    ground_truth: dict[str, set[str]] = getattr(eval_data, category)
    gt_pairs: set[tuple[str, str]] = set()
    for protein, go_ids in ground_truth.items():
        for go_id in go_ids:
            gt_pairs.add((protein, go_id))
    return gt_pairs


def iter_training_data(
    factory: Any,
    *,
    prediction_set_id: uuid.UUID,
    gt_pairs: set[tuple[str, str]],
) -> Any:
    """Yield TSV rows (as bytes) of labeled training data for the re-ranker.

    Streaming generator over GOPrediction rows; opens its own session
    inside so the caller can close the validation session before
    streaming starts. Each row carries the canonical GOPrediction
    feature vector plus a binary ``label`` (1 if the
    ``(protein_accession, go_id)`` pair is in ``gt_pairs``, else 0).
    """
    yield ("\t".join(_TRAINING_TSV_COLUMNS) + "\n").encode()

    with session_scope(factory) as session:
        q = (
            session.query(GOPrediction, GOTerm.go_id, GOTerm.aspect)
            .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
            .filter(GOPrediction.prediction_set_id == prediction_set_id)
        )

        for pred, go_id, aspect in q.yield_per(1000):
            label = 1 if (pred.protein_accession, go_id) in gt_pairs else 0
            row = (
                "\t".join(
                    [
                        pred.protein_accession,
                        go_id,
                        aspect or "",
                        str(label),
                        _format_optional(pred.distance),
                        pred.ref_protein_accession or "",
                        pred.qualifier or "",
                        pred.evidence_code or "",
                        _format_optional(pred.identity_nw),
                        _format_optional(pred.similarity_nw),
                        _format_optional(pred.alignment_score_nw),
                        _format_optional(pred.gaps_pct_nw),
                        _format_optional(pred.alignment_length_nw),
                        _format_optional(pred.identity_sw),
                        _format_optional(pred.similarity_sw),
                        _format_optional(pred.alignment_score_sw),
                        _format_optional(pred.gaps_pct_sw),
                        _format_optional(pred.alignment_length_sw),
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
                        _format_optional(pred.neighbor_distance_std),
                    ]
                )
                + "\n"
            )
            yield row.encode()


_RERANK_TSV_COLUMNS: tuple[str, ...] = (
    "protein_accession",
    "go_id",
    "aspect",
    "reranker_score",
    "distance",
    "ref_protein_accession",
    "evidence_code",
    "qualifier",
)


def score_predictions_with_reranker(
    session: Session,
    *,
    prediction_set_id: uuid.UUID,
    reranker_id: uuid.UUID,
) -> Any:
    """Validate entities, load the booster, score every GOPrediction.

    Returns a sorted ``pandas.DataFrame`` (descending ``reranker_score``
    within each protein) ready for TSV emission, or an empty
    DataFrame when the prediction set has no rows.

    Materialises the full record set in memory; this matches the
    existing endpoint's behaviour (the LightGBM batch predict needs
    a single matrix).

    Raises
    ------
    EntityNotFoundError
        Either ``PredictionSet`` or ``RerankerModel`` does not exist.
    BoosterUnavailableError
        The RerankerModel row exists but no booster bytes are
        reachable.
    """
    import pandas as pd


    if session.get(PredictionSet, prediction_set_id) is None:
        raise EntityNotFoundError("PredictionSet", prediction_set_id)
    rm = session.get(RerankerModel, reranker_id)
    if rm is None:
        raise EntityNotFoundError("RerankerModel", reranker_id)
    model = load_booster(rm)

    records: list[dict[str, Any]] = []
    for pred, go_id, aspect in (
        session.query(GOPrediction, GOTerm.go_id, GOTerm.aspect)
        .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
        .filter(GOPrediction.prediction_set_id == prediction_set_id)
        .yield_per(5000)
    ):
        records.append(
            {
                "protein_accession": pred.protein_accession,
                "go_id": go_id,
                "aspect": aspect or "",
                "distance": pred.distance,
                "ref_protein_accession": pred.ref_protein_accession or "",
                "qualifier": pred.qualifier or "",
                "evidence_code": pred.evidence_code or "",
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
                "taxonomic_relation": pred.taxonomic_relation or "",
                "vote_count": pred.vote_count,
                "k_position": pred.k_position,
                "go_term_frequency": pred.go_term_frequency,
                "ref_annotation_density": pred.ref_annotation_density,
                "neighbor_distance_std": pred.neighbor_distance_std,
                # NOTE: do not add a ``label`` column here — its
                # presence makes ``predict`` route through
                # ``prepare_dataset`` which expects every training
                # column. At inference time we want the alignment
                # branch that fills missing v6 features as NaN.
            }
        )

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["reranker_score"] = _reranker_predict(model, df)
    return df.sort_values(
        ["protein_accession", "reranker_score"],
        ascending=[True, False],
    )


def iter_reranked_predictions_tsv(
    df: Any,
    *,
    min_score: float | None = None,
) -> Any:
    """Yield TSV rows (as bytes) from the scored DataFrame produced by
    :func:`score_predictions_with_reranker`.

    Empty input emits a header-only response (matches the legacy
    endpoint's "no predictions" shape).
    """
    import pandas as pd

    yield ("\t".join(_RERANK_TSV_COLUMNS) + "\n").encode()
    if df is None or df.empty:
        return

    for _, row in df.iterrows():
        if min_score is not None and row["reranker_score"] < min_score:
            continue
        line = (
            "\t".join(
                [
                    str(row["protein_accession"]),
                    str(row["go_id"]),
                    str(row["aspect"]),
                    f"{row['reranker_score']:.6f}",
                    str(row["distance"]) if pd.notna(row["distance"]) else "",
                    str(row["ref_protein_accession"]),
                    str(row["evidence_code"]),
                    str(row["qualifier"]),
                ]
            )
            + "\n"
        )
        yield line.encode()


def compute_reranker_metrics_data(
    session: Session,
    *,
    prediction_set_id: uuid.UUID,
    reranker_id: uuid.UUID,
    evaluation_set_id: uuid.UUID,
    category: str,
) -> dict[str, Any]:
    """Compute CAFA Fmax + AUC-PR using re-ranker scores instead of a ScoringConfig.

    Validates entities, materialises the prediction record set,
    loads the booster, scores each prediction, runs CAFA evaluation
    against the temporal ground truth of the EvaluationSet. Reuses
    the persisted ground-truth artifact when available (covers the
    ``mode=reconciled`` case where eval annotation snapshots differ
    from ``ps.ontology_snapshot_id``); falls back to on-the-fly
    ``compute_evaluation_data`` for legacy same-snapshot rows.

    Returns a JSON-ready dict matching the legacy endpoint shape.
    Empty record sets short-circuit with ``fmax=0.0`` / ``n_predictions=0``.

    Raises
    ------
    EntityNotFoundError
        Any of ``PredictionSet``, ``RerankerModel``, ``EvaluationSet``
        does not exist.
    BoosterUnavailableError
        The RerankerModel row exists but no booster bytes are
        reachable.
    """
    import pandas as pd

    from protea.core.evaluation import load_evaluation_data_for_set
    from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet

    ps = session.get(PredictionSet, prediction_set_id)
    if ps is None:
        raise EntityNotFoundError("PredictionSet", prediction_set_id)
    rm = session.get(RerankerModel, reranker_id)
    if rm is None:
        raise EntityNotFoundError("RerankerModel", reranker_id)
    es = session.get(EvaluationSet, evaluation_set_id)
    if es is None:
        raise EntityNotFoundError("EvaluationSet", evaluation_set_id)

    reranker_name = rm.name

    if es.groundtruth_uri:
        eval_data, _pivot_id = load_evaluation_data_for_set(session, es)
    else:
        eval_data = compute_evaluation_data(
            session,
            old_annotation_set_id=es.old_annotation_set_id,
            new_annotation_set_id=es.new_annotation_set_id,
            ontology_snapshot_id=ps.ontology_snapshot_id,
        )

    records: list[dict[str, Any]] = []
    for pred, go_id in (
        session.query(GOPrediction, GOTerm.go_id)
        .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
        .filter(GOPrediction.prediction_set_id == prediction_set_id)
        .yield_per(5000)
    ):
        records.append(
            {
                "protein_accession": pred.protein_accession,
                "go_id": go_id,
                "distance": pred.distance,
                "qualifier": pred.qualifier or "",
                "evidence_code": pred.evidence_code or "",
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
                "taxonomic_relation": pred.taxonomic_relation or "",
                "vote_count": pred.vote_count,
                "k_position": pred.k_position,
                "go_term_frequency": pred.go_term_frequency,
                "ref_annotation_density": pred.ref_annotation_density,
                "neighbor_distance_std": pred.neighbor_distance_std,
                # See note in score_predictions_with_reranker: omitting
                # ``label`` forces the alignment branch in ``predict``.
            }
        )

    if not records:
        return {
            "prediction_set_id": str(prediction_set_id),
            "reranker_id": str(reranker_id),
            "reranker_name": reranker_name,
            "category": category,
            "fmax": 0.0,
            "auc_pr": 0.0,
            "n_predictions": 0,
            "curve": [],
        }

    # Booster load + scoring inside the session scope: ``rm``'s lazy
    # columns (``model_data`` / ``artifact_uri``) are loaded against
    # the live session before downstream numeric work.
    model = load_booster(rm)
    df = pd.DataFrame(records)
    scores = _reranker_predict(model, df)

    scored: list[dict[str, Any]] = [
        {
            "protein_accession": records[i]["protein_accession"],
            "go_id": records[i]["go_id"],
            "score": float(scores[i]),
        }
        for i in range(len(records))
    ]

    metrics = compute_cafa_metrics(scored, eval_data, category=category)

    return {
        "prediction_set_id": str(prediction_set_id),
        "reranker_id": str(reranker_id),
        "reranker_name": reranker_name,
        **metrics.summary(),
        "curve": [
            {
                "threshold": p.threshold,
                "precision": p.precision,
                "recall": p.recall,
                "f1": p.f1,
            }
            for p in metrics.curve
        ],
    }


def compute_prediction_metrics(
    session: Session,
    *,
    prediction_set_id: uuid.UUID,
    scoring_config_id: uuid.UUID,
    old_annotation_set_id: uuid.UUID,
    new_annotation_set_id: uuid.UUID,
    ontology_snapshot_id: uuid.UUID,
    category: str,
) -> dict[str, Any]:
    """Compute CAFA Fmax and AUC-PR for a PredictionSet under a ScoringConfig.

    Loads ``PredictionSet`` and ``ScoringConfig``, validates signal
    coverage, materialises the temporal NK/LK ground-truth delta from
    the two AnnotationSets via :func:`compute_evaluation_data`,
    applies the config's formula to every ``GOPrediction`` row via
    :func:`compute_score`, and feeds the scored predictions through
    :func:`compute_cafa_metrics`. Returns a JSON-ready dict with the
    summary metrics + the full precision-recall curve.

    The session is read-only here; no flush/commit. The function
    returns plain Python types and does not depend on FastAPI.

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

    eval_data = compute_evaluation_data(
        session,
        old_annotation_set_id=old_annotation_set_id,
        new_annotation_set_id=new_annotation_set_id,
        ontology_snapshot_id=ontology_snapshot_id,
    )

    rows = (
        session.query(GOPrediction, GOTerm.go_id)
        .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
        .filter(GOPrediction.prediction_set_id == prediction_set_id)
        .all()
    )

    scored: list[dict[str, Any]] = []
    for pred, go_id in rows:
        pred_dict: dict[str, Any] = {
            "protein_accession": pred.protein_accession,
            "go_id": go_id,
            "distance": pred.distance,
            "identity_nw": pred.identity_nw,
            "identity_sw": pred.identity_sw,
            "evidence_code": pred.evidence_code,
            "taxonomic_distance": pred.taxonomic_distance,
            "neighbor_vote_fraction": pred.neighbor_vote_fraction,
        }
        pred_dict["score"] = compute_score(pred_dict, config_snap)
        scored.append(pred_dict)

    metrics = compute_cafa_metrics(scored, eval_data, category=category)

    return {
        "prediction_set_id": str(prediction_set_id),
        "scoring_config_id": str(scoring_config_id),
        "scoring_config_name": config_snap.name,
        **metrics.summary(),
        "curve": [
            {
                "threshold": p.threshold,
                "precision": p.precision,
                "recall": p.recall,
                "f1": p.f1,
            }
            for p in metrics.curve
        ],
    }


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
