"""Counters and the result record of a GOA annotation load.

Out of ``load_goa_annotations.py`` because that file already holds a 500-line
operation and did not need to also hold its bookkeeping. It is the same split
``_universe_sources`` makes for the extraction side, and everything here is a
pure function of its inputs or a plain accumulator, so it is testable without a
session, a GAF or a network.

WHY THE SKIP COUNT IS THREE NUMBERS AND NOT ONE. ``skipped`` mixed one expected
rejection with two that are information loss, and the expected one is a thousand
times larger: of GOA 156's 280.9 million rows, some 270 million belong to proteins
that are not in this corpus at all. A loss of 50,000 rows of OUR OWN superset
inside that is invisible. It is the same defect ``malformed_skipped`` had on the
extraction side (ADR-D49), at the other end of the pipeline.
"""

from __future__ import annotations

import uuid
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

#: How many DISTINCT unmapped GO ids one pass reports BY NAME. A cap, because a
#: release paired with the wrong OBO would otherwise put tens of thousands of ids
#: into a single job event. The COUNT is never capped; only the sample of names.
_MAX_LOST_GO_IDS = 50


@dataclass
class _Rejections:
    """Why rows of ONE page were dropped, by reason.

    ``not_in_universe``
        The accession is not in ``protein``. Expected, and enormous.
    ``go_term_unknown``
        The accession IS ours but the GO id is not in the paired ontology
        snapshot. **This one is real loss from our own superset.** Two sources: a
        term absent from that OBO, and a term the OBO carries only as an
        ``alt_id``, which ``load_ontology_snapshot`` does not register.

        Measured 2026-10-06 over 2.7 million sampled rows of releases 221 and 231
        against their paired OBOs: **zero** of either kind, because GOA normalises
        to primary ids against its own GO release. So the ``alt_id`` registry was
        NOT built on spec. This counter exists so that if the old end of the
        series behaves differently, it is a number instead of a silence.
    ``duplicate_in_batch``
        An exact repeat inside one page. Harmless, counted so the three buckets
        sum to the old single number.

    Returned by ``_store_buffer`` instead of a bare integer for the same reason
    ``StoreCounts`` replaced a 4-tuple in ``_protein_store``: a reordering of
    positional members is silently wrong, and this one would be wrong for hours.
    """

    not_in_universe: int = 0
    go_term_unknown: int = 0
    duplicate_in_batch: int = 0
    lost_go_ids: Counter[str] = field(default_factory=Counter)

    @property
    def total(self) -> int:
        return self.not_in_universe + self.go_term_unknown + self.duplicate_in_batch


@dataclass
class _GoaPageTotals:
    """Mutable accumulator for the GAF page loop."""

    pages: int = 0
    lines: int = 0
    inserted: int = 0
    skipped: int = 0
    not_in_universe: int = 0
    go_term_unknown: int = 0
    duplicate_in_batch: int = 0
    lost_go_ids: Counter[str] = field(default_factory=Counter)

    def absorb(self, r: _Rejections) -> None:
        """Fold one page's rejections into the run totals.

        ``skipped`` stays as the sum, so nothing that already reads it breaks.
        The name sample is capped, but an id already in it keeps accumulating, so
        the most frequent offender does not freeze at one.
        """
        self.skipped += r.total
        self.not_in_universe += r.not_in_universe
        self.go_term_unknown += r.go_term_unknown
        self.duplicate_in_batch += r.duplicate_in_batch
        for go_id, n in r.lost_go_ids.items():
            if go_id in self.lost_go_ids or len(self.lost_go_ids) < _MAX_LOST_GO_IDS:
                self.lost_go_ids[go_id] += n


def load_report(
    *,
    annotation_set_id: uuid.UUID,
    totals: _GoaPageTotals,
    ontology_check: Any,
    elapsed: float,
) -> dict[str, Any]:
    """The job's result dict, which is the only record of what the pass did.

    Here and not in the operation because it is a pure function of its inputs,
    exactly like ``extraction_report`` on the other side of the pipeline.
    """
    return {
        "annotation_set_id": str(annotation_set_id),
        "pages": totals.pages,
        "total_lines_read": totals.lines,
        "annotations_inserted": totals.inserted,
        "annotations_skipped": totals.skipped,
        "skipped_not_in_universe": totals.not_in_universe,
        "skipped_go_term_unknown": totals.go_term_unknown,
        "skipped_duplicate_in_batch": totals.duplicate_in_batch,
        "lost_go_ids": dict(totals.lost_go_ids.most_common()),
        "elapsed_seconds": elapsed,
        "ontology_check": ontology_check,
    }
