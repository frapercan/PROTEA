"""Annotations service: pure-logic helpers extracted from
``protea.api.routers.annotations``.

ORM ↔ dict serialisers and the read-side handlers (snapshot/IA-url
operations) live here so non-router callers (CLI tools, batch
scripts) can reuse them without pulling FastAPI in.

The router translates the domain exceptions raised here to HTTP
responses:

- :class:`EntityNotFoundError` → ``404 Not Found`` (e.g. an
  ``OntologySnapshot`` or ``AnnotationSet`` UUID does not resolve).
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from protea.core.evaluation import load_evaluation_data_for_set
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.evaluation_result import EvaluationResult
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.annotation.protein_go_annotation import ProteinGOAnnotation
from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig


class AnnotationsServiceError(Exception):
    """Base class for annotations-service domain errors."""


class EntityNotFoundError(AnnotationsServiceError):
    """Generic 404; a referenced entity does not exist.

    Pickle-safe via ``__reduce__`` so the structured ``entity`` /
    ``entity_id`` attrs survive a round-trip without tripping
    flake8-bugbear B042.
    """

    def __init__(self, entity: str, entity_id: uuid.UUID) -> None:  # noqa: B042
        super().__init__(f"{entity} not found")
        self.entity = entity
        self.entity_id = entity_id

    def __reduce__(self) -> tuple[type, tuple[str, uuid.UUID]]:
        return (self.__class__, (self.entity, self.entity_id))


class AnnotationSetReferencedError(AnnotationsServiceError):
    """An :class:`AnnotationSet` cannot be deleted because PredictionSet
    rows still reference it; the FK CASCADE is intentionally absent.
    Maps to HTTP 409 at the router boundary.
    """


def snapshot_to_dict(s: OntologySnapshot, term_count: int) -> dict[str, Any]:
    """Serialise an :class:`OntologySnapshot` to its API dict shape."""
    return {
        "id": str(s.id),
        "obo_url": s.obo_url,
        "obo_version": s.obo_version,
        "ia_url": s.ia_url,
        "loaded_at": s.loaded_at.isoformat(),
        "go_term_count": term_count,
    }


def list_snapshots_data(session: Session) -> list[dict[str, Any]]:
    """Return all loaded snapshots with their GO term counts (newest first).

    Pure read; the caller is responsible for caching at the API
    boundary if desired (the GROUP BY over the multi-million row
    ``go_term`` table is the slow part).
    """
    count_sub = (
        session.query(
            GOTerm.ontology_snapshot_id,
            func.count(GOTerm.id).label("cnt"),
        )
        .group_by(GOTerm.ontology_snapshot_id)
        .subquery()
    )
    rows = (
        session.query(OntologySnapshot, count_sub.c.cnt)
        .outerjoin(count_sub, OntologySnapshot.id == count_sub.c.ontology_snapshot_id)
        .order_by(OntologySnapshot.loaded_at.desc())
        .all()
    )
    return [snapshot_to_dict(s, cnt or 0) for s, cnt in rows]


def get_snapshot_data(
    session: Session,
    snapshot_id: uuid.UUID,
) -> dict[str, Any]:
    """Return a single snapshot with its GO term count.

    Raises :class:`EntityNotFoundError` when the UUID does not resolve.
    """
    s = session.get(OntologySnapshot, snapshot_id)
    if s is None:
        raise EntityNotFoundError("OntologySnapshot", snapshot_id)
    term_count = (
        session.query(func.count(GOTerm.id))
        .filter(GOTerm.ontology_snapshot_id == snapshot_id)
        .scalar()
    )
    return snapshot_to_dict(s, term_count or 0)


def set_snapshot_ia_url(
    session: Session,
    snapshot_id: uuid.UUID,
    ia_url: str | None,
) -> dict[str, Any]:
    """Update the IA URL on a snapshot. Empty string is treated as ``None``.

    Returns a small confirmation dict shape compatible with the
    legacy endpoint. Raises :class:`EntityNotFoundError` for the
    404 path. The caller (router) is responsible for validating
    request body shape (e.g. presence of the ``ia_url`` key) before
    calling.
    """
    s = session.get(OntologySnapshot, snapshot_id)
    if s is None:
        raise EntityNotFoundError("OntologySnapshot", snapshot_id)
    s.ia_url = ia_url or None
    session.flush()
    return {
        "id": str(s.id),
        "obo_version": s.obo_version,
        "ia_url": s.ia_url,
    }


def annotation_set_to_dict(a: AnnotationSet, count: int) -> dict[str, Any]:
    """Serialise an :class:`AnnotationSet` to its API dict shape."""
    return {
        "id": str(a.id),
        "source": a.source,
        "source_version": a.source_version,
        "ontology_snapshot_id": str(a.ontology_snapshot_id),
        "job_id": str(a.job_id) if a.job_id else None,
        "created_at": a.created_at.isoformat(),
        "meta": a.meta,
        "annotation_count": count,
    }


def list_annotation_sets_data(
    session: Session,
    source: str | None = None,
) -> list[dict[str, Any]]:
    """List all annotation sets with their per-set annotation counts (newest first).

    Optionally filter by ``source`` (e.g. ``goa`` or ``quickgo``).
    Pure read; the caller caches at the API boundary.
    """
    count_sub = (
        session.query(
            ProteinGOAnnotation.annotation_set_id,
            func.count(ProteinGOAnnotation.id).label("cnt"),
        )
        .group_by(ProteinGOAnnotation.annotation_set_id)
        .subquery()
    )
    q = session.query(AnnotationSet, count_sub.c.cnt).outerjoin(
        count_sub, AnnotationSet.id == count_sub.c.annotation_set_id
    )
    if source is not None:
        q = q.filter(AnnotationSet.source == source)
    rows = q.order_by(AnnotationSet.created_at.desc()).all()
    return [annotation_set_to_dict(a, cnt or 0) for a, cnt in rows]


def get_annotation_set_data(
    session: Session,
    set_id: uuid.UUID,
) -> dict[str, Any]:
    """Return a single annotation set with its annotation count.

    Raises :class:`EntityNotFoundError` when the UUID does not resolve.
    """
    a = session.get(AnnotationSet, set_id)
    if a is None:
        raise EntityNotFoundError("AnnotationSet", set_id)
    annotation_count = (
        session.query(func.count(ProteinGOAnnotation.id))
        .filter(ProteinGOAnnotation.annotation_set_id == set_id)
        .scalar()
    )
    return annotation_set_to_dict(a, annotation_count or 0)


def delete_annotation_set_data(
    session: Session,
    set_id: uuid.UUID,
) -> dict[str, Any]:
    """Delete an annotation set and all its annotations.

    Returns the deletion summary dict. Raises:
    - :class:`EntityNotFoundError` if the UUID does not resolve.
    - :class:`AnnotationSetReferencedError` if a PredictionSet
      references this set (router maps to 409).
    """
    a = session.get(AnnotationSet, set_id)
    if a is None:
        raise EntityNotFoundError("AnnotationSet", set_id)
    annotation_count = (
        session.query(func.count(ProteinGOAnnotation.id))
        .filter(ProteinGOAnnotation.annotation_set_id == set_id)
        .scalar()
    )
    try:
        session.delete(a)
        session.flush()
    except IntegrityError as exc:
        raise AnnotationSetReferencedError(
            "This annotation set is referenced by one or more prediction "
            "sets. Delete those first."
        ) from exc
    return {"deleted": str(set_id), "annotations_deleted": annotation_count or 0}


def iter_groundtruth_tsv(
    session: Session,
    eval_id: uuid.UUID,
    category: str,
) -> list[str]:
    """Return the rows for a CAFA ``ground_truth_<CATEGORY>.tsv`` download.

    ``category`` is ``"nk"``, ``"lk"``, ``"pk"`` or ``"known"``.
    Each row is ``"<protein>\\t<go_id>\\n"``; sorted by protein then GO id
    so the output is deterministic. The caller wraps the list in a
    ``StreamingResponse`` (the materialised list is small enough, a
    few thousand rows for typical CAFA splits, to fit in memory and
    keeps the streaming generator simple).

    Raises :class:`EntityNotFoundError` when the EvaluationSet does
    not resolve.
    """
    e = session.get(EvaluationSet, eval_id)
    if e is None:
        raise EntityNotFoundError("EvaluationSet", eval_id)
    data, _ = load_evaluation_data_for_set(session, e)
    source: dict[str, set[str]] = getattr(data, category)
    return [
        f"{protein}\t{go_id}\n"
        for protein, go_ids in sorted(source.items())
        for go_id in sorted(go_ids)
    ]


# iter_delta_proteins_fasta + get_go_subgraph_data live in
# _annotations_method_helpers and are re-exported below so existing
# router/CLI imports keep working unchanged.
from protea.services._annotations_method_helpers import (  # noqa: E402,F401
    get_go_subgraph_data,
    iter_delta_proteins_fasta,
)

# iter_evaluation_artifacts_zip + render_evaluation_metrics_tsv live in
# _annotations_streaming_helpers (no domain-exception coupling).
from protea.services._annotations_streaming_helpers import (  # noqa: E402,F401
    iter_evaluation_artifacts_zip,
    render_evaluation_metrics_tsv,
)


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
    e = session.get(EvaluationSet, eval_id)
    if e is None:
        raise EntityNotFoundError("EvaluationSet", eval_id)
    result_keys: list[str] = []
    for r in (
        session.query(EvaluationResult)
        .filter(EvaluationResult.evaluation_set_id == eval_id)
        .all()
    ):
        result_keys.extend((r.results or {}).get("artifacts", {}).get("keys") or [])
    session.delete(e)
    return result_keys


__all__ = [
    "AnnotationSetReferencedError",
    "AnnotationsServiceError",
    "EntityNotFoundError",
    "annotation_set_to_dict",
    "delete_annotation_set_data",
    "delete_evaluation_set_collect_keys",
    "evaluation_set_to_dict",
    "get_annotation_set_data",
    "get_evaluation_set_data",
    "get_snapshot_data",
    "delete_eval_result_collect_keys",
    "evaluation_result_to_dict",
    "get_eval_result_with_keys",
    "get_go_subgraph_data",
    "iter_delta_proteins_fasta",
    "iter_groundtruth_tsv",
    "list_evaluation_results_data",
    "render_evaluation_metrics_tsv",
    "list_annotation_sets_data",
    "list_evaluation_sets_data",
    "list_snapshots_data",
    "set_snapshot_ia_url",
    "snapshot_to_dict",
]
