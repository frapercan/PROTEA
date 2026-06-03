"""Sibling helper for ``RunCafaEvaluationOperation.summarize_payload``.

Builds the per-job CLI / event summary that the jobs router exposes.
Lives outside ``run_cafa_evaluation.py`` so the operation module stays
under §3 file-LOC budget. Each ``_bits_from_*`` helper is a small,
test-friendly chunk.
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig


def summarize_payload(payload: dict[str, Any], session: Session | None) -> str:
    """Build the human-readable run summary for the CAFA evaluator.

    Mirrors the Job summarize_payload contract: never raises, falls back
    to UUID prefixes when a FK row is missing or no session is available.
    """
    bits: list[str] = []
    bits += _bits_from_prediction(payload.get("prediction_set_id"), session)
    bits += _bits_from_evaluation(payload.get("evaluation_set_id"), session)
    bits += _bits_from_scoring(payload.get("scoring_config_id"), session)
    bits += _bits_from_payload_flags(payload)
    return " · ".join(bits)


def _bits_from_prediction(
    pred_id_raw: Any, session: Session | None
) -> list[str]:
    """``pred=<embedding-display-name>`` (or UUID prefix fallback)."""
    if not pred_id_raw:
        return []
    if session is None:
        return [f"pred={str(pred_id_raw)[:8]}"]
    try:
        pred = session.get(PredictionSet, uuid.UUID(str(pred_id_raw)))
    except Exception:
        pred = None
    if pred is None:
        return [f"pred={str(pred_id_raw)[:8]}"]
    if pred.embedding_config_id is None:
        return []
    from protea.infrastructure.orm.models.embedding.embedding_config import (
        EmbeddingConfig,
    )

    cfg = session.get(EmbeddingConfig, pred.embedding_config_id)
    if cfg is None:
        return []
    return [cfg.display_name or cfg.model_name or str(cfg.id)[:8]]


def _bits_from_evaluation(eval_id_raw: Any, session: Session | None) -> list[str]:
    """``eval=<old_v>→<new_v>`` (or UUID prefix fallback)."""
    if not eval_id_raw:
        return []
    if session is None:
        return [f"eval={str(eval_id_raw)[:8]}"]
    try:
        ev = session.get(EvaluationSet, uuid.UUID(str(eval_id_raw)))
    except Exception:
        ev = None
    if ev is None:
        return []
    old_v = getattr(ev, "old_source_version", None) or "?"
    new_v = getattr(ev, "new_source_version", None) or "?"
    return [f"eval={old_v}→{new_v}"]


def _bits_from_scoring(scoring_id_raw: Any, session: Session | None) -> list[str]:
    """``scoring=<config-name>`` (or UUID prefix fallback)."""
    if not scoring_id_raw:
        return []
    if session is None:
        return [f"scoring={str(scoring_id_raw)[:8]}"]
    try:
        sc = session.get(ScoringConfig, uuid.UUID(str(scoring_id_raw)))
    except Exception:
        sc = None
    if sc is None:
        return []
    return [f"scoring={sc.name}"]


def _bits_from_payload_flags(payload: dict[str, Any]) -> list[str]:
    """``+reranker`` and ``max_d=<float>`` flags, both optional."""
    bits: list[str] = []
    if payload.get("reranker_model_id"):
        bits.append("+reranker")
    if payload.get("max_distance") is not None:
        bits.append(f"max_d={payload['max_distance']}")
    return bits
