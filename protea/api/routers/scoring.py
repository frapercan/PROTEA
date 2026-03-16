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
from typing import Any, Iterator

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.requests import Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, field_validator
from sqlalchemy.orm import Session

from protea.core.evaluation import compute_evaluation_data
from protea.core.metrics import compute_cafa_metrics
from protea.core.scoring import compute_score, score_predictions
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.scoring_config import (
    DEFAULT_EVIDENCE_WEIGHTS,
    DEFAULT_WEIGHTS,
    VALID_FORMULAS,
    ScoringConfig,
)
from protea.infrastructure.session import session_scope

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
        },
        "description": (
            "Pure cosine similarity converted to [0, 1]. "
            "Baseline config — no alignment, evidence, or taxonomy signals."
        ),
    },
    {
        "name": "embedding_plus_evidence",
        "formula": "evidence_weighted",
        "weights": {
            "embedding_similarity": 1.0,
            "identity_nw": 0.0,
            "identity_sw": 0.0,
            "evidence_weight": 1.0,
            "taxonomic_proximity": 0.0,
        },
        "description": (
            "Embedding similarity multiplied by evidence code quality (evidence_weighted formula). "
            "Penalises IEA-sourced annotations regardless of embedding distance."
        ),
    },
    {
        "name": "alignment_weighted",
        "formula": "linear",
        "weights": {
            "embedding_similarity": 0.5,
            "identity_nw": 0.3,
            "identity_sw": 0.2,
            "evidence_weight": 0.0,
            "taxonomic_proximity": 0.0,
        },
        "description": (
            "Combines embedding similarity (50 %) with global NW identity (30 %) "
            "and local SW identity (20 %). "
            "Requires PredictionSet computed with compute_alignments=True."
        ),
    },
    {
        "name": "composite",
        "formula": "evidence_weighted",
        "weights": {
            "embedding_similarity": 0.4,
            "identity_nw": 0.2,
            "identity_sw": 0.1,
            "evidence_weight": 0.2,
            "taxonomic_proximity": 0.1,
        },
        "description": (
            "Full composite: embedding + alignment + evidence quality + taxonomic proximity. "
            "Requires compute_alignments=True and compute_taxonomy=True."
        ),
    },
    {
        "name": "evidence_primary",
        "formula": "linear",
        "weights": {
            "embedding_similarity": 0.2,
            "identity_nw": 0.0,
            "identity_sw": 0.0,
            "evidence_weight": 0.8,
            "taxonomic_proximity": 0.0,
        },
        "description": (
            "Evidence quality as primary signal (80%), embedding similarity as tiebreaker (20%). "
            "Designed for datasets where cosine distances cluster tightly (>99% of predictions "
            "within distance < 0.1), making distance a poor tau discriminator. "
            "Creates three well-separated score tiers: "
            "EXP/IDA → ~1.0, ISS/IBA → ~0.76, IEA → ~0.46. "
            "Recommended when compute_alignments and compute_taxonomy are not available."
        ),
    },
]


# ---------------------------------------------------------------------------
# FastAPI dependency
# ---------------------------------------------------------------------------

def get_session_factory(request: Request):
    return request.app.state.session_factory


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
    weights: dict[str, float] = Field(
        default_factory=lambda: dict(DEFAULT_WEIGHTS)
    )
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
    def validate_evidence_weights(
        cls, v: dict[str, float] | None
    ) -> dict[str, float] | None:
        """Ensure all keys are known GO codes and all values are in [0, 1]."""
        if v is None:
            return None
        known_codes = set(DEFAULT_EVIDENCE_WEIGHTS.keys())
        unknown = set(v.keys()) - known_codes
        if unknown:
            raise ValueError(
                f"Unknown evidence codes: {sorted(unknown)}. "
                f"Valid codes: {sorted(known_codes)}"
            )
        out_of_range = {k: val for k, val in v.items() if not (0.0 <= val <= 1.0)}
        if out_of_range:
            raise ValueError(
                f"Evidence weights must be in [0, 1]. Out-of-range: {out_of_range}"
            )
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


# ---------------------------------------------------------------------------
# ScoringConfig CRUD
# ---------------------------------------------------------------------------

@router.get("/configs", response_model=list[ScoringConfigResponse])
def list_scoring_configs(factory=Depends(get_session_factory)):
    """Return all stored ScoringConfigs ordered by creation time."""
    with session_scope(factory) as session:
        configs = (
            session.query(ScoringConfig)
            .order_by(ScoringConfig.created_at)
            .all()
        )
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
        existing_names = {
            row[0] for row in session.query(ScoringConfig.name).all()
        }
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

    def _generate() -> Iterator[bytes]:
        header = "\t".join([
            "protein_accession", "go_id", "score", "distance",
            "ref_protein_accession", "evidence_code", "qualifier",
            "identity_nw", "identity_sw", "taxonomic_distance",
        ]) + "\n"
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
                }
                score = compute_score(pred_dict, config_snap)
                if min_score is not None and score < min_score:
                    continue

                row = "\t".join([
                    pred.protein_accession,
                    go_id,
                    str(score),
                    str(pred.distance) if pred.distance is not None else "",
                    pred.ref_protein_accession or "",
                    pred.evidence_code or "",
                    pred.qualifier or "",
                    str(pred.identity_nw) if pred.identity_nw is not None else "",
                    str(pred.identity_sw) if pred.identity_sw is not None else "",
                    str(pred.taxonomic_distance) if pred.taxonomic_distance is not None else "",
                ]) + "\n"
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
