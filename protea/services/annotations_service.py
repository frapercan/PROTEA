"""Annotations service — pure-logic helpers extracted from
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

from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from protea.core.evaluation import load_evaluation_data_for_set
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.evaluation_result import EvaluationResult
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.annotation.protein_go_annotation import ProteinGOAnnotation


class AnnotationsServiceError(Exception):
    """Base class for annotations-service domain errors."""


class EntityNotFoundError(AnnotationsServiceError):
    """Generic 404 — a referenced entity does not exist.

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
    ``StreamingResponse`` (the materialised list is small enough — a
    few thousand rows for typical CAFA splits — to fit in memory and
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


def iter_delta_proteins_fasta(
    session: Session,
    eval_id: uuid.UUID,
    category: str,
) -> list[str]:
    """Return FASTA lines for delta proteins (``nk`` / ``lk`` / ``pk`` / ``all``).

    ``category`` selects which delta categories to include; ``"all"``
    is the union of NK ∪ LK ∪ PK. Only proteins whose sequence is in
    the DB are emitted. Header is
    ``>ACCESSION entry_name OS=organism OX=taxon (NK|LK|PK)``; the
    sequence is wrapped at 60 chars per line.

    Empty result returns an empty list. Raises
    :class:`EntityNotFoundError` if the EvaluationSet does not
    resolve.
    """
    from protea.infrastructure.orm.models.protein.protein import Protein
    from protea.infrastructure.orm.models.sequence.sequence import Sequence

    e = session.get(EvaluationSet, eval_id)
    if e is None:
        raise EntityNotFoundError("EvaluationSet", eval_id)
    data, _ = load_evaluation_data_for_set(session, e)

    accession_label: dict[str, str] = {}
    if category in ("nk", "all"):
        for acc in data.nk:
            accession_label[acc] = "NK"
    if category in ("lk", "all"):
        for acc in data.lk:
            accession_label[acc] = "LK"
    if category in ("pk", "all"):
        for acc in data.pk:
            accession_label.setdefault(acc, "PK")

    if not accession_label:
        return []

    rows = (
        session.query(Protein, Sequence)
        .join(Sequence, Protein.sequence_id == Sequence.id)
        .filter(Protein.accession.in_(accession_label.keys()))
        .order_by(Protein.accession)
        .all()
    )

    lines: list[str] = []
    for protein, seq in rows:
        label = accession_label.get(protein.accession, "")
        parts = [protein.accession]
        if protein.entry_name:
            parts.append(protein.entry_name)
        if protein.organism:
            parts.append(f"OS={protein.organism}")
        if protein.taxonomy_id:
            parts.append(f"OX={protein.taxonomy_id}")
        parts.append(f"({label})")
        lines.append(f">{' '.join(parts)}\n")
        s = seq.sequence
        for i in range(0, len(s), 60):
            lines.append(s[i : i + 60] + "\n")
    return lines


def get_go_subgraph_data(
    session: Session,
    snapshot_id: uuid.UUID,
    go_ids: str,
    depth: int,
) -> dict[str, Any]:
    """BFS the GO DAG upward from the requested seed terms.

    Returns ``{"nodes": [...], "edges": [...]}`` ready for the API.
    Each node has ``id`` (DB id), ``go_id``, ``name``, ``aspect``,
    ``is_query`` (True for the seed terms). Each edge has
    ``source`` (child id), ``target`` (parent id), ``relation_type``.

    Raises :class:`EntityNotFoundError` when the snapshot does not
    resolve.
    """
    from protea.infrastructure.orm.models.annotation.go_term_relationship import (
        GOTermRelationship,
    )

    snap = session.get(OntologySnapshot, snapshot_id)
    if snap is None:
        raise EntityNotFoundError("OntologySnapshot", snapshot_id)

    query_go_ids = {g.strip() for g in go_ids.split(",") if g.strip()}

    seed_terms = (
        session.query(GOTerm)
        .filter(
            GOTerm.ontology_snapshot_id == snapshot_id,
            GOTerm.go_id.in_(query_go_ids),
        )
        .all()
    )

    if not seed_terms:
        return {"nodes": [], "edges": []}

    visited_ids: set[int] = {t.id for t in seed_terms}
    frontier: set[int] = visited_ids.copy()
    all_terms: dict[int, GOTerm] = {t.id: t for t in seed_terms}
    all_edges: list[dict[str, Any]] = []

    for _ in range(depth):
        if not frontier:
            break
        rels = (
            session.query(GOTermRelationship)
            .filter(
                GOTermRelationship.ontology_snapshot_id == snapshot_id,
                GOTermRelationship.child_go_term_id.in_(frontier),
            )
            .all()
        )

        parent_ids = {r.parent_go_term_id for r in rels} - visited_ids
        for r in rels:
            all_edges.append(
                {
                    "source": r.child_go_term_id,
                    "target": r.parent_go_term_id,
                    "relation_type": r.relation_type,
                }
            )

        if parent_ids:
            parents = session.query(GOTerm).filter(GOTerm.id.in_(parent_ids)).all()
            for p in parents:
                all_terms[p.id] = p
            visited_ids |= parent_ids
            frontier = parent_ids
        else:
            break

    query_db_ids = {t.id for t in seed_terms}
    nodes = [
        {
            "id": t.id,
            "go_id": t.go_id,
            "name": t.name,
            "aspect": t.aspect,
            "is_query": t.id in query_db_ids,
        }
        for t in all_terms.values()
    ]
    return {"nodes": nodes, "edges": all_edges}


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


def render_evaluation_metrics_tsv(
    result: EvaluationResult,
    aspect_codes: tuple[str, ...],
) -> Any:
    """Yield TSV rows for the per-(setting, namespace) metrics summary.

    The caller passes the aspect-codes tuple (``ASPECT_CAFA_CODES``)
    so the service stays free of the domain layer. Returns a
    generator suitable for ``StreamingResponse``.
    """
    yield "setting\tnamespace\tfmax\tprecision\trecall\ttau\tcoverage\tn_proteins\n"
    for setting in ("NK", "LK", "PK"):
        ns_data = result.results.get(setting, {})
        for ns in aspect_codes:
            m = ns_data.get(ns)
            if m is None:
                continue
            yield (
                f"{setting}\t{ns}\t{m.get('fmax', '')}\t{m.get('precision', '')}\t"
                f"{m.get('recall', '')}\t{m.get('tau', '')}\t{m.get('coverage', '')}\t"
                f"{m.get('n_proteins', '')}\n"
            )


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
    ``protea.core.evaluation.groundtruth_key_for(eval_id)`` —
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
