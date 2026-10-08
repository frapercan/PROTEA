"""The page loop of a GOA load, with the accession test moved ahead of the record.

Out of ``load_goa_annotations.py`` for the reason ``_goa_load_report`` is: that
file holds a 500-line operation, and this is the part of it with arithmetic
worth testing on its own. The operation keeps ``_flush_page`` and
``_store_buffer``, so what a page does to the database is still in one place.

WHY THE TEST MOVES. Almost every line of a GOA release names a protein outside
the corpus: of release 167, 385.0 M of 392.8 M lines. The GOA plugin built a
validated ``GoaAnnotationRecord`` for each of them, the loop buffered it, and
``_store_buffer`` walked it again only to drop it. The plugin takes an
``accept`` predicate over the raw split columns and builds no record for a line
it rejects, so the accession test of ``_store_buffer`` now runs there.

WHY NOTHING ELSE MOVES. A load's report is the evidence that a release loaded
right: pages, lines, inserted and the three rejection buckets, and the
``page_done`` sequence that records its progress. A line rejected early carries
no data, so it is counted instead of buffered, but it keeps its place in
``lines``, in its page and in ``not_in_universe``. Pages end on the same lines
as before, a page of rejected lines flushes, commits and reports like any
other, and ``tests/test_goa_accession_prefilter.py`` holds the whole output to
the loop as it stood before.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Any

from protea.core.operations._goa_load_report import _GoaPageTotals, _Rejections

if TYPE_CHECKING:
    from protea_contracts import GoaAnnotationRecord
    from sqlalchemy.orm import Session

    from protea.core.contracts.operation import EmitFn

#: GAF column 2, DB Object ID, 0-based. The same column the GOA plugin copies
#: into ``GoaAnnotationRecord.accession``; its ``accept`` sees the raw split.
_GAF_ACCESSION = 1

#: Emitted by the plugin once it has read the last line of the GAF.
_DOWNLOAD_DONE = "source.goa.download_done"


def _accession_prefilter(
    universe: set[str],
) -> tuple[Callable[[list[str]], bool], Callable[[], int]]:
    """The accession test of ``_store_buffer``, for the plugin's ``accept``.

    The same test, character for character: ``strip`` and membership, with an
    empty accession rejected, exactly as ``_store_buffer`` applies it to
    ``rec.accession``. The record copies column 2 unchanged (its contract is
    strict, nothing coerces), so the two cannot disagree about a line.

    Returns the predicate and a reader of how many lines it has rejected so far.
    """
    rejected = 0

    def accept(cols: list[str]) -> bool:
        nonlocal rejected
        accession = cols[_GAF_ACCESSION].strip()
        if accession and accession in universe:
            return True
        rejected += 1
        return False

    def rejected_so_far() -> int:
        return rejected

    return accept, rejected_so_far


def _no_rejections() -> int:
    """``rejected_so_far`` without a prefilter: nothing is rejected early."""
    return 0


def _with_rejections_before(
    records: Iterator[GoaAnnotationRecord], rejected_so_far: Callable[[], int]
) -> Iterator[tuple[int, GoaAnnotationRecord | None]]:
    """Pair each kept record with the number of lines rejected just before it.

    The count is read when the plugin hands over a record, at which point its
    predicate has seen every line up to that one and no further, so the pairs
    come out in file order. A last pair with no record carries the lines
    rejected after the final kept one.
    """
    placed = 0
    for record in records:
        before = rejected_so_far() - placed
        placed += before
        yield before, record
    yield rejected_so_far() - placed, None


class _Pages:
    """The open page of a load, counted record by record as the loop always did.

    ``fill`` is how many records the open page holds, buffered or rejected, and
    is what ``len(buffer)`` was when every record was buffered. ``dropped`` is
    how many of them the prefilter rejected; they reach ``totals`` as this
    page's ``not_in_universe`` just before the page flushes, so its
    ``page_done`` reports the same running totals as before.
    """

    def __init__(self, op: Any, session: Session, p: Any, store_ctx: Any, emit: EmitFn) -> None:
        self.op, self.session, self.p, self.store_ctx, self.emit = op, session, p, store_ctx, emit
        self.totals = _GoaPageTotals()
        self.buffer: list[GoaAnnotationRecord] = []
        self.fill = 0
        self.dropped = 0
        self.stopped = False

    def _limit_reached(self) -> bool:
        """The loop's ``total_limit`` check, run for one record, after it counts."""
        limit = self.p.total_limit
        if limit is None or self.totals.inserted < limit:
            return False
        self.emit("load_goa_annotations.limit_reached", None, {"total_limit": limit}, "warning")
        self.stopped = True
        return True

    def skip(self, n: int) -> None:
        """Place ``n`` consecutive rejected records, a page at a time.

        Only a flush moves ``inserted``, so the limit verdict on the first record
        of a run within one page holds for every record of that run.
        """
        while n and not self.stopped:
            self.totals.lines += 1
            if self._limit_reached():
                return
            take = min(n, self.p.page_size - self.fill)
            self.totals.lines += take - 1
            self.fill += take
            self.dropped += take
            n -= take
            self._close_if_full()

    def keep(self, record: GoaAnnotationRecord) -> None:
        """Place one record that passed the prefilter, or every record without it."""
        self.totals.lines += 1
        if self._limit_reached():
            return
        self.buffer.append(record)
        self.fill += 1
        self._close_if_full()

    def _close_if_full(self) -> None:
        if self.fill >= self.p.page_size:
            self._flush(self.emit)
            if self.p.commit_every_page:
                self.session.commit()

    def _flush(self, emit: EmitFn | None) -> None:
        if self.dropped:
            self.totals.absorb(_Rejections(not_in_universe=self.dropped))
        self.op._flush_page(self.session, self.buffer, self.store_ctx, self.totals, emit)
        self.fill = self.dropped = 0

    def finish(self) -> None:
        """Flush the last, partial page, without ``page_done`` as always."""
        if self.fill:
            self._flush(None)


def stream_into_pages(
    op: Any, session: Session, p: Any, store_ctx: Any, emit: EmitFn, *, prefilter: bool
) -> _GoaPageTotals:
    """Stream the GAF of ``p`` into pages of ``p.page_size`` records; see the module.

    The plugin reports the end of the download as soon as it has read the last
    line. With the prefilter, the lines it rejected after the last kept record
    are placed only after that, so the report is held until they are: it came
    after their ``page_done`` before. A load stopped by ``total_limit`` never
    reported the end of a download it had not finished, even if the prefilter
    read on to the last line, so a held report is dropped then.
    """
    accept: Callable[[list[str]], bool] | None = None
    rejected_so_far: Callable[[], int] = _no_rejections
    if prefilter:
        accept, rejected_so_far = _accession_prefilter(store_ctx.admissible_accessions)
    held: list[tuple[str, Any, Any, str]] = []

    def stream_emit(event: str, message: Any, fields: Any, level: str) -> None:
        if event == _DOWNLOAD_DONE:
            held.append((event, message, fields, level))
        else:
            emit(event, message, fields, level)

    pages = _Pages(op, session, p, store_ctx, emit)
    records = op._stream_gaf(p, stream_emit if prefilter else emit, accept=accept)
    for before, record in _with_rejections_before(records, rejected_so_far):
        pages.skip(before)
        if pages.stopped or record is None:
            break
        pages.keep(record)
        if pages.stopped:
            break
    if not pages.stopped:
        for event, message, fields, level in held:
            emit(event, message, fields, level)
    pages.finish()
    return pages.totals
