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
from protea.core.scoring import compute_score
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel
from protea.infrastructure.orm.models.embedding.scoring_config import (
    ScoringConfig,
)
from protea.infrastructure.session import session_scope
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


# _TRAINING_TSV_COLUMNS lives in _scoring_training_helpers; aliased here
# for backwards compatibility.
from protea.services._scoring_training_helpers import (  # noqa: E402
    TRAINING_TSV_COLUMNS as _TRAINING_TSV_COLUMNS,
)
from protea.services._scoring_training_helpers import (  # noqa: E402
    format_training_row as _format_training_row,
)


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
    Row formatting is delegated to ``_format_training_row`` in
    ``_scoring_training_helpers`` so the orchestrator stays under
    the §3 method-LOC ceiling.
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
            yield (_format_training_row(pred, go_id, aspect, label) + "\n").encode()


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


# score_predictions_with_reranker lives in _scoring_pipeline_helpers and
# is re-exported below so existing router/CLI imports keep working unchanged.
from protea.services._scoring_pipeline_helpers import (  # noqa: E402,F401
    score_predictions_with_reranker,
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


# compute_reranker_metrics_data lives in _scoring_metrics_helpers and is
# re-exported below so existing router/CLI imports keep working unchanged.
# Mock targets for the tests inside the moved orchestrator point at the
# helper module (e.g. protea.services._scoring_metrics_helpers.compute_cafa_metrics).
from protea.services._scoring_metrics_helpers import (  # noqa: E402,F401
    compute_reranker_metrics_data,
)

# compute_prediction_metrics lives in _scoring_prediction_metrics_helpers and
# is re-exported below so existing router/CLI imports keep working unchanged.
from protea.services._scoring_prediction_metrics_helpers import (  # noqa: E402,F401
    compute_prediction_metrics,
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
