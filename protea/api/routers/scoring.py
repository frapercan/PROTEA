"""Scoring configuration management and analytical endpoints.

Provides full CRUD for :class:`ScoringConfig` and two read-only analytical
endpoints that apply a stored config to an existing ``PredictionSet``:

``GET /scoring/prediction-sets/{id}/score.tsv``
    Stream a TSV of scored predictions.  The score column is computed on-the-fly
    by applying the selected ``ScoringConfig`` formula to the raw signals stored
    in ``GOPrediction`` rows — no re-running of the KNN pipeline is required.

``GET /scoring/prediction-sets/{id}/metrics``
    Compute CAFA Fmax / AUC-PR for a (PredictionSet, ScoringConfig, category)
    triple.  Requires two ``AnnotationSet`` IDs to build the NK/LK ground truth
    following the CAFA4 protocol.

Evidence weights
----------------
Each ``ScoringConfig`` may carry an optional ``evidence_weights`` dict that
overrides the system-default per-GO-evidence-code quality multipliers.  The
API validates that:

- Every key in the dict is a known GO evidence code (one of the codes in
  :data:`DEFAULT_EVIDENCE_WEIGHTS`).
- Every value is a float in [0, 1].

Partial overrides are allowed: codes absent from the submitted dict will
continue to use the system default at score-computation time.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import text

from protea.api.deps import get_session_factory
from protea.core.evaluation import compute_evaluation_data, load_evaluation_data_for_set
from protea.core.metrics import compute_cafa_metrics
from protea.core.reranker import load_reranker, model_from_string
from protea.core.reranker import (
    predict as reranker_predict,
)
from protea.core.scoring import compute_score
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
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


def _load_booster(rm: RerankerModel) -> Any:
    """Load the LightGBM booster from either the legacy inline blob or
    the new ``artifact_uri`` path.

    Raises 409 when neither is available — the row exists but the
    backing model is missing.
    """
    if rm.model_data:
        return model_from_string(rm.model_data)
    if rm.artifact_uri:
        project_root = Path(__file__).resolve().parents[3]
        store = get_artifact_store(load_settings(project_root))
        return load_reranker(
            rm.artifact_uri,
            feature_schema_sha=rm.feature_schema_sha or rm.name,
            store=store,
        )
    raise HTTPException(
        status_code=409,
        detail=(
            f"RerankerModel {rm.id} has no booster — both ``model_data`` "
            f"(legacy inline) and ``artifact_uri`` (artifact-store path) are NULL."
        ),
    )


router = APIRouter(prefix="/scoring", tags=["scoring"])

# ---------------------------------------------------------------------------
# Built-in preset configurations
# ---------------------------------------------------------------------------
# These cover the most common use-cases and are designed to be instructive
# as reference points for custom configs.  None of them override evidence
# weights so they document what the system defaults produce.

_PRESET_CONFIGS: list[dict[str, Any]] = [
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


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class ScoringConfigCreate(BaseModel):
    """Request body for POST /scoring/configs.

    Attributes
    ----------
    name:
        Unique display name (1–255 characters).
    formula:
        Aggregation formula.  One of ``"linear"`` or ``"evidence_weighted"``.
    weights:
        Signal weights dict.  Valid keys: ``embedding_similarity``,
        ``identity_nw``, ``identity_sw``, ``evidence_weight``,
        ``taxonomic_proximity``.  Missing keys default to 0.
    evidence_weights:
        Optional per-GO-evidence-code quality overrides.  Keys must be valid
        GO evidence codes (e.g. ``"IEA"``); values must be in [0, 1].
        When ``None`` the system defaults from
        :data:`DEFAULT_EVIDENCE_WEIGHTS` are used at score-computation time.
        Partial dicts are allowed.
    description:
        Free-text description stored for display in the UI.
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
    """Serialised representation of a stored ScoringConfig."""

    id: uuid.UUID
    name: str
    formula: str
    weights: dict[str, Any]
    evidence_weights: dict[str, Any] | None
    description: str | None
    created_at: Any


def _to_response(c: ScoringConfig) -> ScoringConfigResponse:
    """Convert an ORM ScoringConfig to its API response model."""
    return ScoringConfigResponse(
        id=c.id,
        name=c.name,
        formula=c.formula,
        weights=c.weights,
        evidence_weights=c.evidence_weights,
        description=c.description,
        created_at=c.created_at,
    )


def _snapshot(c: ScoringConfig) -> ScoringConfig:
    """Create a detached ScoringConfig copy safe to use after a session closes.

    The scoring endpoints close the DB session before streaming the response
    body.  This helper captures all scoring-relevant fields into a plain ORM
    instance that does not require an open session.
    """
    return ScoringConfig(
        id=c.id,
        name=c.name,
        formula=c.formula,
        weights=c.weights,
        evidence_weights=c.evidence_weights,
        description=c.description,
    )


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


def _check_signal_coverage(session, prediction_set_id, config_snap: ScoringConfig) -> None:
    """Fail fast with 409 when the config needs signals absent from the PredictionSet.

    For every signal with a non-zero weight in ``config_snap.weights`` (plus
    ``evidence_code`` when the formula is ``evidence_weighted`` — the multiplier
    is always applied), count how many rows in the PredictionSet have the
    backing column non-NULL.  Zero coverage is a configuration mismatch
    (typically a ``ScoringConfig`` that requires ``compute_alignments=True`` or
    ``compute_taxonomy=True`` applied to a PredictionSet computed without
    those flags).  Raise HTTP 409 with the list of missing signals instead of
    silently producing a degraded score (``compute_score`` drops NULL signals
    from both numerator and denominator).
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
                f"SELECT COUNT(*) AS total, {cols_sql} "  # noqa: S608 — col names are hard-coded
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
        raise HTTPException(
            status_code=409,
            detail=(
                "ScoringConfig requires signals absent from the PredictionSet: "
                + "; ".join(missing)
                + ". Re-predict with the corresponding compute_* flag enabled."
            ),
        )


# ---------------------------------------------------------------------------
# ScoringConfig CRUD
# ---------------------------------------------------------------------------


@router.get("/configs", response_model=list[ScoringConfigResponse])
def list_scoring_configs(factory=Depends(get_session_factory)):
    """Return all stored ScoringConfigs ordered by creation time."""
    with session_scope(factory) as session:
        configs = session.query(ScoringConfig).order_by(ScoringConfig.created_at).all()
        return [_to_response(c) for c in configs]


@router.post("/configs", response_model=ScoringConfigResponse, status_code=201)
def create_scoring_config(
    body: ScoringConfigCreate,
    factory=Depends(get_session_factory),
):
    """Create a new ScoringConfig.

    Validates that ``formula`` is one of the supported values and that every
    key in ``weights`` is a recognised signal name.  Evidence weight validation
    is handled by the Pydantic model.
    """
    if body.formula not in VALID_FORMULAS:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid formula {body.formula!r}. Valid options: {list(VALID_FORMULAS)}",
        )
    known_signals = set(DEFAULT_WEIGHTS.keys())
    unknown_signals = set(body.weights.keys()) - known_signals
    if unknown_signals:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Unknown signal weight keys: {sorted(unknown_signals)}. "
                f"Valid keys: {sorted(known_signals)}"
            ),
        )

    with session_scope(factory) as session:
        config = ScoringConfig(
            name=body.name,
            formula=body.formula,
            weights=body.weights,
            evidence_weights=body.evidence_weights,
            description=body.description,
        )
        session.add(config)
        session.flush()
        return _to_response(config)


@router.post("/configs/presets", status_code=201)
def create_preset_configs(factory=Depends(get_session_factory)):
    """Seed the database with the four built-in preset ScoringConfigs.

    Idempotent — presets that already exist (matched by name) are silently
    skipped.  Returns the list of names that were actually created.
    """
    created: list[str] = []
    with session_scope(factory) as session:
        existing_names = {row[0] for row in session.query(ScoringConfig.name).all()}
        for preset in _PRESET_CONFIGS:
            if preset["name"] in existing_names:
                continue
            session.add(ScoringConfig(**preset))
            created.append(preset["name"])
    return {"created": created}


@router.get("/configs/{config_id}", response_model=ScoringConfigResponse)
def get_scoring_config(
    config_id: uuid.UUID,
    factory=Depends(get_session_factory),
):
    """Retrieve a single ScoringConfig by UUID."""
    with session_scope(factory) as session:
        config = session.get(ScoringConfig, config_id)
        if config is None:
            raise HTTPException(status_code=404, detail="ScoringConfig not found")
        return _to_response(config)


@router.delete("/configs/{config_id}", status_code=204)
def delete_scoring_config(
    config_id: uuid.UUID,
    factory=Depends(get_session_factory),
):
    """Delete a ScoringConfig by UUID."""
    with session_scope(factory) as session:
        config = session.get(ScoringConfig, config_id)
        if config is None:
            raise HTTPException(status_code=404, detail="ScoringConfig not found")
        session.delete(config)


# ---------------------------------------------------------------------------
# Scored TSV endpoint
# ---------------------------------------------------------------------------


@router.get("/prediction-sets/{set_id}/score.tsv")
def download_scored_predictions(
    set_id: uuid.UUID,
    scoring_config_id: uuid.UUID = Query(...),
    min_score: float | None = Query(None, ge=0.0, le=1.0),
    accession: str | None = Query(None),
    factory=Depends(get_session_factory),
):
    """Stream a TSV of predictions with computed confidence scores.

    The score is computed on-the-fly for every row using the selected
    ``ScoringConfig``, including any custom evidence-weight overrides stored
    in that config.  The session is closed before the generator starts so
    the response is streamed without holding a DB connection open.

    Query parameters
    ----------------
    scoring_config_id:
        UUID of the ``ScoringConfig`` to apply.
    min_score:
        Optional score threshold — rows below this value are omitted.
    accession:
        Optional protein accession filter.

    TSV columns
    -----------
    protein_accession, go_id, score, distance, ref_protein_accession,
    evidence_code, qualifier, identity_nw, identity_sw, taxonomic_distance.
    """
    with session_scope(factory) as session:
        if session.get(PredictionSet, set_id) is None:
            raise HTTPException(status_code=404, detail="PredictionSet not found")
        config = session.get(ScoringConfig, scoring_config_id)
        if config is None:
            raise HTTPException(status_code=404, detail="ScoringConfig not found")
        config_snap = _snapshot(config)
        _check_signal_coverage(session, set_id, config_snap)

    def _generate() -> Iterator[bytes]:
        header = (
            "\t".join(
                [
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
                ]
            )
            + "\n"
        )
        yield header.encode()

        with session_scope(factory) as session:
            q = (
                session.query(GOPrediction, GOTerm.go_id)
                .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
                .filter(GOPrediction.prediction_set_id == set_id)
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
                            str(pred.distance) if pred.distance is not None else "",
                            pred.ref_protein_accession or "",
                            pred.evidence_code or "",
                            pred.qualifier or "",
                            str(pred.identity_nw) if pred.identity_nw is not None else "",
                            str(pred.identity_sw) if pred.identity_sw is not None else "",
                            str(pred.taxonomic_distance)
                            if pred.taxonomic_distance is not None
                            else "",
                            str(pred.neighbor_vote_fraction)
                            if pred.neighbor_vote_fraction is not None
                            else "",
                        ]
                    )
                    + "\n"
                )
                yield row.encode()

    filename = f"scored_{set_id}_{scoring_config_id}.tsv"
    return StreamingResponse(
        _generate(),
        media_type="text/tab-separated-values",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---------------------------------------------------------------------------
# CAFA metrics endpoint
# ---------------------------------------------------------------------------


@router.get("/prediction-sets/{set_id}/metrics")
def compute_metrics(
    set_id: uuid.UUID,
    scoring_config_id: uuid.UUID = Query(...),
    old_annotation_set_id: uuid.UUID = Query(...),
    new_annotation_set_id: uuid.UUID = Query(...),
    ontology_snapshot_id: uuid.UUID = Query(...),
    category: str = Query("nk", pattern="^(nk|lk)$"),
    factory=Depends(get_session_factory),
):
    """Compute CAFA Fmax and AUC-PR for a PredictionSet under a ScoringConfig.

    Ground truth is the NK or LK delta between *old_annotation_set* and
    *new_annotation_set*, following the CAFA4 protocol: only experimental
    evidence codes, NOT-qualifier annotations excluded with full DAG propagation.

    The selected ``ScoringConfig`` — including any custom ``evidence_weights``
    — is applied to every ``GOPrediction`` row before computing the
    precision-recall curve.

    Parameters
    ----------
    scoring_config_id:
        Which stored ScoringConfig formula (and evidence weights) to apply.
    old_annotation_set_id / new_annotation_set_id:
        The two AnnotationSets used to compute the temporal ground-truth delta.
    ontology_snapshot_id:
        GO DAG snapshot used for NOT-qualifier propagation.
    category:
        ``"nk"`` (no-knowledge) or ``"lk"`` (limited-knowledge) protein set.
    """
    with session_scope(factory) as session:
        if session.get(PredictionSet, set_id) is None:
            raise HTTPException(status_code=404, detail="PredictionSet not found")
        config = session.get(ScoringConfig, scoring_config_id)
        if config is None:
            raise HTTPException(status_code=404, detail="ScoringConfig not found")
        config_snap = _snapshot(config)
        _check_signal_coverage(session, set_id, config_snap)

        eval_data = compute_evaluation_data(
            session,
            old_annotation_set_id=old_annotation_set_id,
            new_annotation_set_id=new_annotation_set_id,
            ontology_snapshot_id=ontology_snapshot_id,
        )

        rows = (
            session.query(GOPrediction, GOTerm.go_id)
            .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
            .filter(GOPrediction.prediction_set_id == set_id)
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
        "prediction_set_id": str(set_id),
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


# ---------------------------------------------------------------------------
# Training data endpoint (re-ranker)
# ---------------------------------------------------------------------------

_TRAINING_COLUMNS = [
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
]


@router.get(
    "/prediction-sets/{set_id}/training-data.tsv",
    summary="Export labeled training data for the re-ranker",
    response_class=StreamingResponse,
)
def download_training_data(
    set_id: uuid.UUID,
    evaluation_set_id: uuid.UUID = Query(
        ..., description="EvaluationSet to derive ground-truth labels from"
    ),
    category: str = Query(
        "nk", pattern="^(nk|lk|pk)$", description="Ground-truth category: nk, lk, or pk"
    ),
    factory=Depends(get_session_factory),
) -> StreamingResponse:
    """Stream labeled training data for the re-ranker model.

    Joins all GOPrediction feature columns with a binary ``label`` derived
    from the temporal ground-truth delta of the given EvaluationSet.

    A prediction is labeled **1** if the (protein_accession, go_id) pair
    appears in the selected category's ground truth, **0** otherwise.

    Parameters
    ----------
    evaluation_set_id:
        UUID of the EvaluationSet (old → new annotation sets).
    category:
        ``"nk"`` (no-knowledge), ``"lk"`` (limited-knowledge), or
        ``"pk"`` (partial-knowledge).
    """
    with session_scope(factory) as session:
        ps = session.get(PredictionSet, set_id)
        if ps is None:
            raise HTTPException(status_code=404, detail="PredictionSet not found")

        es = session.get(EvaluationSet, evaluation_set_id)
        if es is None:
            raise HTTPException(status_code=404, detail="EvaluationSet not found")

        ontology_snapshot_id = ps.ontology_snapshot_id

        eval_data = compute_evaluation_data(
            session,
            old_annotation_set_id=es.old_annotation_set_id,
            new_annotation_set_id=es.new_annotation_set_id,
            ontology_snapshot_id=ontology_snapshot_id,
        )

    ground_truth: dict[str, set[str]] = getattr(eval_data, category)
    gt_pairs: set[tuple[str, str]] = set()
    for protein, go_ids in ground_truth.items():
        for go_id in go_ids:
            gt_pairs.add((protein, go_id))

    def _generate() -> Iterator[bytes]:
        yield ("\t".join(_TRAINING_COLUMNS) + "\n").encode()

        with session_scope(factory) as session:
            q = (
                session.query(GOPrediction, GOTerm.go_id, GOTerm.aspect)
                .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
                .filter(GOPrediction.prediction_set_id == set_id)
            )

            for pred, go_id, aspect in q.yield_per(1000):
                label = 1 if (pred.protein_accession, go_id) in gt_pairs else 0

                def _v(val: object) -> str:
                    return "" if val is None else str(val)

                row = (
                    "\t".join(
                        [
                            pred.protein_accession,
                            go_id,
                            aspect or "",
                            str(label),
                            _v(pred.distance),
                            pred.ref_protein_accession or "",
                            pred.qualifier or "",
                            pred.evidence_code or "",
                            _v(pred.identity_nw),
                            _v(pred.similarity_nw),
                            _v(pred.alignment_score_nw),
                            _v(pred.gaps_pct_nw),
                            _v(pred.alignment_length_nw),
                            _v(pred.identity_sw),
                            _v(pred.similarity_sw),
                            _v(pred.alignment_score_sw),
                            _v(pred.gaps_pct_sw),
                            _v(pred.alignment_length_sw),
                            _v(pred.length_query),
                            _v(pred.length_ref),
                            _v(pred.query_taxonomy_id),
                            _v(pred.ref_taxonomy_id),
                            _v(pred.taxonomic_lca),
                            _v(pred.taxonomic_distance),
                            _v(pred.taxonomic_common_ancestors),
                            pred.taxonomic_relation or "",
                            _v(pred.vote_count),
                            _v(pred.k_position),
                            _v(pred.go_term_frequency),
                            _v(pred.ref_annotation_density),
                            _v(pred.neighbor_distance_std),
                        ]
                    )
                    + "\n"
                )
                yield row.encode()

    filename = f"training_data_{set_id}_{category}.tsv"
    return StreamingResponse(
        _generate(),
        media_type="text/tab-separated-values",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---------------------------------------------------------------------------
# Re-ranker model CRUD + train + apply
# ---------------------------------------------------------------------------


_ASPECT_MAP = {"bpo": "P", "mfo": "F", "cco": "C"}


class RerankerResponse(BaseModel):
    """Serialised representation of a stored RerankerModel."""

    id: uuid.UUID
    name: str
    prediction_set_id: uuid.UUID | None
    evaluation_set_id: uuid.UUID | None
    category: str
    aspect: str | None
    metrics: dict[str, Any]
    feature_importance: dict[str, Any]
    created_at: Any


def _reranker_to_response(m: RerankerModel) -> RerankerResponse:
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


@router.get("/rerankers", response_model=list[RerankerResponse])
def list_rerankers(factory=Depends(get_session_factory)):
    """Return all stored re-ranker models ordered by creation time."""
    with session_scope(factory) as session:
        models = session.query(RerankerModel).order_by(RerankerModel.created_at).all()
        return [_reranker_to_response(m) for m in models]


@router.get("/rerankers/{reranker_id}", response_model=RerankerResponse)
def get_reranker(reranker_id: uuid.UUID, factory=Depends(get_session_factory)):
    """Retrieve a single re-ranker model by UUID."""
    with session_scope(factory) as session:
        model = session.get(RerankerModel, reranker_id)
        if model is None:
            raise HTTPException(status_code=404, detail="RerankerModel not found")
        return _reranker_to_response(model)


@router.delete("/rerankers/{reranker_id}", status_code=204)
def delete_reranker(reranker_id: uuid.UUID, factory=Depends(get_session_factory)):
    """Delete a re-ranker model by UUID."""
    with session_scope(factory) as session:
        model = session.get(RerankerModel, reranker_id)
        if model is None:
            raise HTTPException(status_code=404, detail="RerankerModel not found")
        session.delete(model)


@router.get(
    "/prediction-sets/{set_id}/rerank.tsv",
    summary="Apply a trained re-ranker to predictions",
    response_class=StreamingResponse,
)
def download_reranked_predictions(
    set_id: uuid.UUID,
    reranker_id: uuid.UUID = Query(..., description="UUID of the trained RerankerModel to apply"),
    min_score: float | None = Query(
        None, ge=0.0, le=1.0, description="Minimum re-ranker score threshold"
    ),
    factory=Depends(get_session_factory),
) -> StreamingResponse:
    """Stream predictions re-scored by a trained LightGBM model.

    Each row includes the original prediction data plus a ``reranker_score``
    column (probability 0–1, higher = more likely correct).  Rows are sorted
    by descending score within each protein.
    """
    import pandas as pd

    with session_scope(factory) as session:
        ps = session.get(PredictionSet, set_id)
        if ps is None:
            raise HTTPException(status_code=404, detail="PredictionSet not found")
        rm = session.get(RerankerModel, reranker_id)
        if rm is None:
            raise HTTPException(status_code=404, detail="RerankerModel not found")
        model = _load_booster(rm)

        records: list[dict[str, Any]] = []
        for pred, go_id, aspect in (
            session.query(GOPrediction, GOTerm.go_id, GOTerm.aspect)
            .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
            .filter(GOPrediction.prediction_set_id == set_id)
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

        def _empty() -> Iterator[bytes]:
            yield b"protein_accession\tgo_id\taspect\treranker_score\tdistance\n"

        return StreamingResponse(
            _empty(),
            media_type="text/tab-separated-values",
            headers={"Content-Disposition": f'attachment; filename="reranked_{set_id}.tsv"'},
        )

    df = pd.DataFrame(records)
    scores = reranker_predict(model, df)
    df["reranker_score"] = scores

    # Sort by protein then descending score
    df = df.sort_values(["protein_accession", "reranker_score"], ascending=[True, False])

    _RERANK_COLUMNS = [
        "protein_accession",
        "go_id",
        "aspect",
        "reranker_score",
        "distance",
        "ref_protein_accession",
        "evidence_code",
        "qualifier",
    ]

    def _generate() -> Iterator[bytes]:
        yield ("\t".join(_RERANK_COLUMNS) + "\n").encode()
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

    filename = f"reranked_{set_id}.tsv"
    return StreamingResponse(
        _generate(),
        media_type="text/tab-separated-values",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/prediction-sets/{set_id}/reranker-metrics")
def compute_reranker_metrics(
    set_id: uuid.UUID,
    reranker_id: uuid.UUID = Query(..., description="UUID of the trained RerankerModel"),
    evaluation_set_id: uuid.UUID = Query(..., description="UUID of the EvaluationSet"),
    category: str = Query("nk", pattern="^(nk|lk|pk)$"),
    factory=Depends(get_session_factory),
):
    """Compute CAFA Fmax and AUC-PR using re-ranker scores instead of ScoringConfig.

    Applies the trained LightGBM model to all predictions in the PredictionSet,
    then evaluates against the temporal ground truth of the EvaluationSet.

    This closes the full re-ranker loop: train → apply → evaluate.
    """
    import pandas as pd

    with session_scope(factory) as session:
        ps = session.get(PredictionSet, set_id)
        if ps is None:
            raise HTTPException(status_code=404, detail="PredictionSet not found")
        rm = session.get(RerankerModel, reranker_id)
        if rm is None:
            raise HTTPException(status_code=404, detail="RerankerModel not found")
        es = session.get(EvaluationSet, evaluation_set_id)
        if es is None:
            raise HTTPException(status_code=404, detail="EvaluationSet not found")

        # Booster load is deferred until after the empty-predictions check
        # so a request against an empty PredictionSet doesn't pay the
        # MinIO download cost.
        reranker_name = rm.name

        # Reuse the persisted ground-truth artifact when available (the only
        # path that handles ``mode=reconciled`` correctly, where the eval set's
        # underlying annotation snapshots differ from ``ps.ontology_snapshot_id``).
        # Fall back to on-the-fly computation only for legacy same-snapshot rows.
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
            .filter(GOPrediction.prediction_set_id == set_id)
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
                    # See note in download_reranked_predictions: omitting
                    # ``label`` forces the alignment branch in ``predict``.
                }
            )

        if not records:
            return {
                "prediction_set_id": str(set_id),
                "reranker_id": str(reranker_id),
                "reranker_name": reranker_name,
                "category": category,
                "fmax": 0.0,
                "auc_pr": 0.0,
                "n_predictions": 0,
                "curve": [],
            }

        # Booster load and scoring stay inside the session scope: ``rm``'s lazy
        # columns (``model_data`` / ``artifact_uri``) are loaded against the
        # live session, then the heavy numeric work runs before the with-block
        # closes (the eval_data + records are already fully materialised).
        model = _load_booster(rm)

    df = pd.DataFrame(records)
    scores = reranker_predict(model, df)

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
        "prediction_set_id": str(set_id),
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
