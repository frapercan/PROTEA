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

#: What a run is when nothing was applied on top of the neighbour vote.
#: Not an absence: it is the baseline every later stage is measured
#: against, and it is what an entire rung of the campaign consists of.
BASELINE_STAGE = "knn"

StageKind = Literal["scoring", "reranker"]


def stage_of(result: EvaluationResult, scoring_name: str | None) -> str | None:
    """Classify an EvaluationResult into a stage.

    Reranker dominates scoring config, which dominates the bare neighbour
    vote.

    A result with neither used to return ``None``, which the benchmark
    matrix reads as "incomplete" and drops. The reasoning was that a run
    with no scoring config had not finished being configured. That is not
    what the record says. Every such result in the database, all 34 of
    them, is a completed plain-KNN evaluation with scores in it, and
    together they are an entire rung of the campaign: eight models times
    four values of K, the grid the model comparison rests on. The rule was
    hiding all of it and saying nothing.

    An unranked, unrescored KNN run is a configuration, not a gap. It is
    the baseline the other stages are compared against, so it gets a name
    and appears on the board under it.
    """
    if result.reranker_model_id is not None:
        return RERANKER_STAGE
    if scoring_name:
        return scoring_name
    return BASELINE_STAGE


def stage_kind(stage: str) -> StageKind:
    """Return ``"reranker"`` for the reranker stage, ``"scoring"`` otherwise."""
    return "reranker" if stage == RERANKER_STAGE else "scoring"


__all__ = ["BASELINE_STAGE", "RERANKER_STAGE", "StageKind", "stage_kind", "stage_of"]
