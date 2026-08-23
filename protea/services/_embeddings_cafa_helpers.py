"""CAFA-export helpers for ``embeddings_service``.

The preflight (``prepare_cafa_export``) and the streaming TSV generator
(``iter_predictions_cafa_tsv``) own a self-contained divergent-change
cluster: they evolve with the CAFA file-format spec and the evaluation
data shape, not with the rest of the predictions API. Moving them out
keeps ``embeddings_service.py`` under the file-LOC budget while
preserving the public import surface (both names are re-exported from
``protea.services.embeddings_service``).

Domain exceptions (``EntityNotFoundError``) are imported lazily inside
the functions that raise them to avoid a circular dependency with the
parent module.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator

from sqlalchemy import func
from sqlalchemy.orm import Session, sessionmaker

from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.session import session_scope


def prepare_cafa_export(
    session: Session,
    *,
    prediction_set_id: uuid.UUID,
    eval_id: uuid.UUID | None,
) -> set[str] | None:
    """Preflight CAFA export: validate the PredictionSet exists and, if an
    ``EvaluationSet`` was supplied, compute the union of NK + LK delta
    proteins to restrict the export.

    Returns the delta-protein accession set when ``eval_id`` is provided
    (the streaming generator filters on it), otherwise ``None``.

    Raises :class:`EntityNotFoundError` for missing PredictionSet or
    EvaluationSet so the router can translate to 404.
    """
    from protea.core.evaluation import compute_evaluation_data_for_sets
    from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
    from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
    from protea.services.embeddings_service import EntityNotFoundError

    if session.get(PredictionSet, prediction_set_id) is None:
        raise EntityNotFoundError("PredictionSet", prediction_set_id)

    if eval_id is None:
        return None

    e = session.get(EvaluationSet, eval_id)
    if e is None:
        raise EntityNotFoundError("EvaluationSet", eval_id)
    ann_old = session.get(AnnotationSet, e.old_annotation_set_id)
    if ann_old is None:
        raise EntityNotFoundError("AnnotationSet", e.old_annotation_set_id)
    # The old set's snapshot stays the pivot, preserving which term universe
    # this answers in. What changes is that each annotation set is now resolved
    # under its OWN snapshot: passing the old one for both meant the new set
    # resolved to nothing, so nothing looked gained.
    data = compute_evaluation_data_for_sets(
        session,
        old_annotation_set_id=e.old_annotation_set_id,
        new_annotation_set_id=e.new_annotation_set_id,
        pivot_snapshot_id=ann_old.ontology_snapshot_id,
    )
    return set(data.nk) | set(data.lk)


def iter_predictions_cafa_tsv(
    factory: sessionmaker[Session],
    *,
    prediction_set_id: uuid.UUID,
    aspect: str | None,
    max_distance: float | None,
    delta_proteins: set[str] | None,
) -> Iterator[str]:
    """Stream the CAFA-format prediction TSV.

    DB-level deduplication: a ``GROUP BY (protein_accession, go_term_id)``
    + ``MIN(distance)`` subquery keeps the best row per pair so the
    Python side never needs an unbounded ``seen`` set; true streaming.
    Score is ``max(0.0, 1.0 - distance)`` clamped to ``[0, 1]``.
    """
    with session_scope(factory) as session:
        min_dist_q = session.query(
            GOPrediction.protein_accession,
            GOPrediction.go_term_id,
            func.min(GOPrediction.distance).label("min_distance"),
        ).filter(GOPrediction.prediction_set_id == prediction_set_id)
        if max_distance is not None:
            min_dist_q = min_dist_q.filter(GOPrediction.distance <= max_distance)
        min_dist = min_dist_q.group_by(
            GOPrediction.protein_accession, GOPrediction.go_term_id
        ).subquery()

        q = session.query(min_dist.c.protein_accession, GOTerm.go_id, min_dist.c.min_distance).join(
            GOTerm, min_dist.c.go_term_id == GOTerm.id
        )
        if aspect:
            q = q.filter(GOTerm.aspect == aspect.upper())
        if delta_proteins is not None:
            q = q.filter(min_dist.c.protein_accession.in_(delta_proteins))

        q = q.order_by(min_dist.c.protein_accession, GOTerm.go_id)

        for acc, go_id, dist in q.yield_per(1000):
            score = max(0.0, 1.0 - dist)
            yield f"{acc}\t{go_id}\t{score:.4f}\n"
