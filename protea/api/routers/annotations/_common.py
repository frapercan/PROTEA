"""Shared imports + helpers for the annotations router subpackage.

Centralises the queue-name constants, the ``EvaluationSet`` 404
shortcut, and the GT-TSV streaming helper used by the four
``ground-truth-*.tsv`` + ``known-terms.tsv`` endpoints.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from uuid import UUID

from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session, sessionmaker

from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.session import session_scope
from protea.services.annotations_service import EntityNotFoundError, iter_groundtruth_tsv

JOBS_QUEUE = "protea.jobs"
EVALUATIONS_QUEUE = "protea.evaluations"

#: Characters to accumulate before handing a block to the socket in
#: :func:`stream_groundtruth`. A GT row is ``<accession>\t<go_id>\n``, pure
#: ASCII, so characters and bytes coincide and this is ~64 KB.
_TSV_CHUNK_CHARS = 64 * 1024


def _blocks(lines: Sequence[str], size: int = _TSV_CHUNK_CHARS) -> Iterator[str]:
    """Join ``lines`` into blocks of roughly ``size`` characters, in order.

    The blocks concatenate back to ``"".join(lines)`` for any ``size``, so
    the body a client receives does not depend on this batching -- only the
    number of HTTP chunks does.
    """
    buf: list[str] = []
    pending = 0
    for line in lines:
        buf.append(line)
        pending += len(line)
        if pending >= size:
            yield "".join(buf)
            buf.clear()
            pending = 0
    if buf:
        yield "".join(buf)


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

    The rows go out in ``_TSV_CHUNK_CHARS`` blocks rather than one chunk
    per row. Handing ``StreamingResponse`` an iterator over the row list
    makes every row its own ASGI message, and ``known-terms.tsv`` is not
    a few thousand rows: ``"known"`` on evaluation set ``c35ee267`` is
    2.235.489 of them, 43,6 MB, which took 540 s to serve against
    localhost with no network in the way.

    MEASURED in-process on 2026-10-11, same rows through a bare
    ``FastAPI`` app, so no database and no socket:

        one chunk per row      172,83 s      0,3 MB/s
        64 KB blocks             0,21 s    206,7 MB/s

    823x, on byte-identical output. The cost is the message count, not
    the data, which is why the fix is batching rather than a faster query
    or a leaner row.
    """
    try:
        with session_scope(factory) as session:
            lines = iter_groundtruth_tsv(session, eval_id, category)
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return StreamingResponse(
        _blocks(lines),
        media_type="text/tab-separated-values",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
