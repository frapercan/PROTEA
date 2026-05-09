"""Per-protein listing helpers for ``embeddings_service``.

The orchestrator ``list_proteins_in_prediction_set`` lives here split
into private helpers (paginate / load / count) so neither it nor any
helper exceeds the §3 method-LOC ceiling, and ``embeddings_service.py``
can shrink toward the file-LOC budget.

The function is re-exported from ``protea.services.embeddings_service``
for backwards compatibility with existing router and CLI callers.
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.annotation.protein_go_annotation import ProteinGOAnnotation
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.protein.protein import Protein


def _paginate_protein_rows(
    session: Session,
    prediction_set_id: uuid.UUID,
    search: str | None,
    limit: int,
    offset: int,
) -> tuple[int, list[Any]]:
    """Build the GROUP-BY query and return ``(total, page_rows)``.

    Each row is a ``(accession, go_count, min_distance)`` tuple.
    """
    q = (
        session.query(
            GOPrediction.protein_accession,
            func.count(GOPrediction.id).label("go_count"),
            func.min(GOPrediction.distance).label("min_distance"),
        )
        .filter(GOPrediction.prediction_set_id == prediction_set_id)
        .group_by(GOPrediction.protein_accession)
    )
    if search:
        q = q.filter(GOPrediction.protein_accession.ilike(f"%{search}%"))

    total = q.count()
    rows = q.order_by(GOPrediction.protein_accession).offset(offset).limit(limit).all()
    return total, rows


def _load_protein_orm_map(
    session: Session, accessions: list[str]
) -> dict[str, Protein]:
    """Resolve which of the accessions have a matching ``Protein`` row."""
    if not accessions:
        return {}
    return {
        p.accession: p
        for p in session.query(Protein).filter(Protein.accession.in_(accessions)).all()
    }


def _load_annotation_counts(
    session: Session,
    accessions: list[str],
    annotation_set_id: uuid.UUID,
) -> dict[str, int]:
    """Per-accession known-annotation count against ``annotation_set_id``."""
    if not accessions:
        return {}
    return {
        acc: cnt
        for acc, cnt in session.query(
            ProteinGOAnnotation.protein_accession,
            func.count(ProteinGOAnnotation.id),
        )
        .filter(
            ProteinGOAnnotation.protein_accession.in_(accessions),
            ProteinGOAnnotation.annotation_set_id == annotation_set_id,
        )
        .group_by(ProteinGOAnnotation.protein_accession)
        .all()
    }


def _load_match_counts(
    session: Session,
    accessions: list[str],
    prediction_set_id: uuid.UUID,
    annotation_set_id: uuid.UUID,
) -> dict[str, int]:
    """Per-accession count of distinct GO terms matching known annotations.

    A "match" = one ``(protein, go_id)`` pair that is both predicted
    in this prediction set and present as a known annotation in
    ``annotation_set_id`` (a precision proxy).
    """
    if not accessions:
        return {}
    return {
        acc: cnt
        for acc, cnt in session.query(
            GOPrediction.protein_accession,
            func.count(func.distinct(GOPrediction.go_term_id)),
        )
        .join(
            ProteinGOAnnotation,
            (ProteinGOAnnotation.go_term_id == GOPrediction.go_term_id)
            & (ProteinGOAnnotation.protein_accession == GOPrediction.protein_accession)
            & (ProteinGOAnnotation.annotation_set_id == annotation_set_id),
        )
        .filter(
            GOPrediction.prediction_set_id == prediction_set_id,
            GOPrediction.protein_accession.in_(accessions),
        )
        .group_by(GOPrediction.protein_accession)
        .all()
    }


def list_proteins_in_prediction_set(
    session: Session,
    *,
    prediction_set_id: uuid.UUID,
    search: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    """Paginated list of proteins in a prediction set with derived stats.

    For each row returns ``go_count`` (number of predicted terms),
    ``min_distance`` (closest neighbour), ``annotation_count``
    (known annotations against the same AnnotationSet) and
    ``match_count`` (predictions whose ``(protein, go_id)`` is in
    the known set; a precision proxy).

    Decomposed into private helpers (``_paginate_protein_rows``,
    ``_load_protein_orm_map``, ``_load_annotation_counts``,
    ``_load_match_counts``) so this orchestrator stays under the §3
    method-LOC ceiling.

    Raises :class:`EntityNotFoundError` (imported lazily to avoid the
    circular dependency with ``embeddings_service``) when
    ``prediction_set_id`` does not resolve.
    """
    from protea.services.embeddings_service import EntityNotFoundError

    ps = session.get(PredictionSet, prediction_set_id)
    if ps is None:
        raise EntityNotFoundError("PredictionSet", prediction_set_id)

    total, rows = _paginate_protein_rows(session, prediction_set_id, search, limit, offset)
    accessions = [r[0] for r in rows]
    protein_map = _load_protein_orm_map(session, accessions)
    ann_counts = _load_annotation_counts(session, accessions, ps.annotation_set_id)
    match_counts = _load_match_counts(
        session, accessions, prediction_set_id, ps.annotation_set_id
    )

    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "items": [
            {
                "accession": acc,
                "go_count": go_count,
                "min_distance": round(min_dist, 4) if min_dist is not None else None,
                "annotation_count": ann_counts.get(acc, 0),
                "match_count": match_counts.get(acc, 0),
                "in_db": acc in protein_map,
            }
            for acc, go_count, min_dist in rows
        ],
    }
