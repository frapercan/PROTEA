"""Shared imports + helpers for the annotations router subpackage.

Centralises the queue-name constants, the ``EvaluationSet`` 404
shortcut, and the GT-TSV streaming helper used by the four
``ground-truth-*.tsv`` + ``known-terms.tsv`` endpoints.
"""

from __future__ import annotations

from uuid import UUID

from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session, sessionmaker

from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.session import session_scope
from protea.services.annotations_service import EntityNotFoundError, iter_groundtruth_tsv

JOBS_QUEUE = "protea.jobs"
EVALUATIONS_QUEUE = "protea.evaluations"


def eval_set_or_404(session: Session, eval_id: UUID) -> EvaluationSet:
    """Look up an :class:`EvaluationSet`, raising HTTP 404 when missing."""
    e = session.get(EvaluationSet, eval_id)
    if e is None:
        raise HTTPException(status_code=404, detail="EvaluationSet not found")
    return e


def stream_groundtruth(
    factory: sessionmaker[Session],
    eval_id: UUID,
    category: str,
    filename: str,
) -> StreamingResponse:
    """Wrap :func:`iter_groundtruth_tsv` in a TSV streaming response.

    Translates ``EntityNotFoundError`` to HTTP 404 at the boundary;
    same shape used by the four GT/known-terms download endpoints.
    """
    try:
        with session_scope(factory) as session:
            lines = iter_groundtruth_tsv(session, eval_id, category)
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return StreamingResponse(
        iter(lines),
        media_type="text/tab-separated-values",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
