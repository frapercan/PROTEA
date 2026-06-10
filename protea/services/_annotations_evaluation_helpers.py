"""EvaluationSet + EvaluationResult helpers for ``annotations_service``.

The evaluation-side reads (``evaluation_set_to_dict``,
``evaluation_result_to_dict``, the list/get/delete handlers) plus the
baseline-scoring auto-attach helper share a divergent-change axis with
the benchmark matrix and the CAFA evaluation pipeline. Snapshot and
annotation-set CRUD evolve independently, so this cluster moves out
into its own sibling module while the parent service keeps the public
import surface via re-export.

Domain exceptions (``EntityNotFoundError``) are imported lazily inside
the functions that raise them to avoid a circular dependency with the
parent module.
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.annotation.evaluation_result import EvaluationResult
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig


def evaluation_result_to_dict(r: EvaluationResult) -> dict[str, Any]:
    """Serialise an :class:`EvaluationResult` to its API dict shape."""
    return {
        "id": str(r.id),
        "evaluation_set_id": str(r.evaluation_set_id),
        "prediction_set_id": str(r.prediction_set_id),
        "scoring_config_id": str(r.scoring_config_id) if r.scoring_config_id else None,
        "reranker_model_id": str(r.reranker_model_id) if r.reranker_model_id else None,
        "reranker_config": r.reranker_config,
        "job_id": str(r.job_id) if r.job_id else None,
        "created_at": r.created_at.isoformat(),
        # F-METHOD-EVAL-SURFACE provenance (read-through; ``None`` on legacy
        # rows so the UI shows an "unknown" empty state).
        "frame": r.frame,
        "temporal_window": r.temporal_window,
        "arms_enabled": r.arms_enabled,
        "leakage_role": r.leakage_role,
        "results": r.results,
    }


def list_evaluation_results_data(
    session: Session,
    eval_id: uuid.UUID,
) -> list[dict[str, Any]]:
    """List EvaluationResult rows for one EvaluationSet (newest first).

    Raises :class:`EntityNotFoundError` when the EvaluationSet does
    not resolve.
    """
    from protea.services.annotations_service import EntityNotFoundError

    if session.get(EvaluationSet, eval_id) is None:
        raise EntityNotFoundError("EvaluationSet", eval_id)
    rows = (
        session.query(EvaluationResult)
        .filter(EvaluationResult.evaluation_set_id == eval_id)
        .order_by(EvaluationResult.created_at.desc())
        .all()
    )
    return [evaluation_result_to_dict(r) for r in rows]


def get_eval_result_with_keys(
    session: Session,
    eval_id: uuid.UUID,
    result_id: uuid.UUID,
) -> tuple[EvaluationResult, list[str]]:
    """Fetch an EvaluationResult belonging to ``eval_id``; return (row, artifact_keys).

    Raises :class:`EntityNotFoundError` ("EvaluationResult") when
    the result does not exist or does not belong to ``eval_id``.
    """
    from protea.services.annotations_service import EntityNotFoundError

    result = session.get(EvaluationResult, result_id)
    if result is None or result.evaluation_set_id != eval_id:
        raise EntityNotFoundError("EvaluationResult", result_id)
    keys: list[str] = (result.results or {}).get("artifacts", {}).get("keys") or []
    return result, keys


def apply_baseline_scoring_default(
    session: Session,
    body: dict[str, Any],
    baseline_scoring_name: str | None,
) -> dict[str, Any]:
    """Auto-attach the baseline ``scoring_config_id`` to a CAFA evaluation
    payload when no scoring + reranker selection is provided.

    Without this, eval_result rows with both ``scoring_config_id`` and
    ``reranker_model_id`` NULL are filtered out of the benchmark matrix
    (``_stage_of()`` excludes them). When the caller supplies any of
    ``scoring_config_id`` / ``reranker_model_id`` / ``rerankers``, or
    when no baseline name is configured, the body is returned unchanged.
    """
    if (
        body.get("scoring_config_id")
        or body.get("reranker_model_id")
        or body.get("rerankers")
        or not baseline_scoring_name
    ):
        return body
    baseline = session.execute(
        select(ScoringConfig).where(ScoringConfig.name == baseline_scoring_name)
    ).scalar_one_or_none()
    if baseline is None:
        return body
    return {**body, "scoring_config_id": str(baseline.id)}


def assert_evaluation_set_exists(session: Session, eval_id: uuid.UUID) -> None:
    """Raise :class:`EntityNotFoundError` when the ``EvaluationSet`` UUID
    does not resolve. Cheap preflight for endpoints that dispatch
    background work but still need a 404 path."""
    from protea.services.annotations_service import EntityNotFoundError

    if session.get(EvaluationSet, eval_id) is None:
        raise EntityNotFoundError("EvaluationSet", eval_id)


def delete_eval_result_collect_keys(
    session: Session,
    eval_id: uuid.UUID,
    result_id: uuid.UUID,
) -> list[str]:
    """Delete the EvaluationResult and return the artifact keys to clean up.

    Same split as :func:`delete_evaluation_set_collect_keys`: the
    DB delete happens here; the artifact-store deletion is the
    router's responsibility (it owns the ``ArtifactStore`` factory).
    """
    result, keys = get_eval_result_with_keys(session, eval_id, result_id)
    session.delete(result)
    return keys


def evaluation_set_to_dict(e: EvaluationSet) -> dict[str, Any]:
    """Serialise an :class:`EvaluationSet` to its API dict shape."""
    return {
        "id": str(e.id),
        "old_annotation_set_id": str(e.old_annotation_set_id),
        "new_annotation_set_id": str(e.new_annotation_set_id),
        "job_id": str(e.job_id) if e.job_id else None,
        "created_at": e.created_at.isoformat(),
        "stats": e.stats,
        "window_role": e.window_role,
    }


def list_evaluation_sets_data(session: Session) -> list[dict[str, Any]]:
    """List all evaluation sets, newest first."""
    rows = session.query(EvaluationSet).order_by(EvaluationSet.created_at.desc()).all()
    return [evaluation_set_to_dict(e) for e in rows]


def get_evaluation_set_data(
    session: Session,
    eval_id: uuid.UUID,
) -> dict[str, Any]:
    """Return a single evaluation set.

    Raises :class:`EntityNotFoundError` when the UUID does not resolve.
    """
    from protea.services.annotations_service import EntityNotFoundError

    e = session.get(EvaluationSet, eval_id)
    if e is None:
        raise EntityNotFoundError("EvaluationSet", eval_id)
    return evaluation_set_to_dict(e)


def delete_evaluation_set_collect_keys(
    session: Session,
    eval_id: uuid.UUID,
) -> list[str]:
    """Delete the EvaluationSet and return the artifact-store keys to clean.

    The DB delete cascades to ``EvaluationResult`` rows; this helper
    walks the results before deleting and returns the union of all
    artifact keys those rows referenced (per-result cafaeval outputs)
    so the caller can wipe them from the store. The caller is also
    expected to delete the set's ground-truth artifact via
    ``protea.core.evaluation.groundtruth_key_for(eval_id)``;
    that key is not included here because it is a fixed function of
    ``eval_id``.

    Raises :class:`EntityNotFoundError` when the UUID does not resolve.
    """
    from protea.services.annotations_service import EntityNotFoundError

    e = session.get(EvaluationSet, eval_id)
    if e is None:
        raise EntityNotFoundError("EvaluationSet", eval_id)
    result_keys: list[str] = []
    for r in (
        session.query(EvaluationResult).filter(EvaluationResult.evaluation_set_id == eval_id).all()
    ):
        result_keys.extend((r.results or {}).get("artifacts", {}).get("keys") or [])
    session.delete(e)
    return result_keys
