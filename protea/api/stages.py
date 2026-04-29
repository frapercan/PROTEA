"""Shared stage-classification helpers for the benchmark + showcase routers.

Both routers need to label an :class:`EvaluationResult` with the
pipeline stage that produced it (``"reranker"`` or whichever
``ScoringConfig.name`` was applied). The logic was duplicated across
both files until this module consolidated it — the inline copy in
``showcase.py`` carried a comment "Matches benchmark.py semantics
without cross-importing", which is exactly the dispensable-duplication
smell this module fixes.
"""

from __future__ import annotations

from typing import Literal

from protea.infrastructure.orm.models.annotation.evaluation_result import EvaluationResult

RERANKER_STAGE = "reranker"

StageKind = Literal["scoring", "reranker"]


def stage_of(result: EvaluationResult, scoring_name: str | None) -> str | None:
    """Classify an EvaluationResult into a stage.

    Reranker dominates scoring config. Evaluations without either a scoring
    config or a reranker are considered incomplete and excluded from the
    matrix (return ``None``).
    """
    if result.reranker_model_id is not None:
        return RERANKER_STAGE
    if scoring_name:
        return scoring_name
    return None


def stage_kind(stage: str) -> StageKind:
    """Return ``"reranker"`` for the reranker stage, ``"scoring"`` otherwise."""
    return "reranker" if stage == RERANKER_STAGE else "scoring"


__all__ = ["RERANKER_STAGE", "StageKind", "stage_kind", "stage_of"]
