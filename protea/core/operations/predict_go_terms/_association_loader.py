"""Co-occurrence + frequency lookups for the cross-aspect association feature.

Reads the ``term_cooccurrence`` / ``term_frequency`` tables populated by the
``build_go_cooccurrence`` operation. Kept in its own module so the predict
path can monkeypatch the loader in unit tests without standing up the tables.

The lookup keys on the snapshot-invariant ``go_id`` STRING (``known_go_id`` /
``candidate_go_id`` / ``go_id``), not the per-snapshot integer ids. GO term
int ids differ per ``ontology_snapshot_id``, so an int-keyed match only worked
when the t0 set's snapshot equalled the candidate snapshot; string keying makes
the feature correct across any snapshot pair.
"""

from __future__ import annotations

import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.annotation.term_cooccurrence import (
    TermCooccurrence,
    TermFrequency,
)

# Module-level memo: each (annotation_set_id, known_go_id) loaded from the DB
# ONCE per process, reused across every parity chunk of a split.
#
# ``apply_association`` (the caller) runs once per 512-query parity chunk during
# the export build, so for a ~30k-query split the loader is hit ~59 times. The
# ``annotation_set_id`` (t0) is constant across all chunks of a split, while
# common GO terms (e.g. GO:0005515) sit in nearly every chunk's known set, so
# the same 318M-row ``term_cooccurrence`` rows were re-read every chunk. Caching
# per (set, known_go_id) collapses that to one read per distinct known term.
#
# Memory bound: within one split only that split's t0 known terms are cached
# (bounded). Across many splits/sets the cache grows, so ``clear_cooccurrence_cache``
# resets it for long-running workers.
_COOC_CACHE: dict[tuple[uuid.UUID, str], dict[str, int]] = {}
_FREQ_CACHE: dict[tuple[uuid.UUID, str], int | None] = {}


def clear_cooccurrence_cache() -> None:
    """Reset the per-(set, known_go_id) cooccurrence/frequency memo.

    Call between splits/sets in a long-running worker so the cache does not grow
    unbounded across many annotation sets. Within a single split the cache stays
    bounded by that split's t0 known-term vocabulary.
    """
    _COOC_CACHE.clear()
    _FREQ_CACHE.clear()


def load_cooccurrence_for_known(
    session: Session,
    annotation_set_id: uuid.UUID,
    known_go_ids: set[str],
) -> tuple[dict[str, dict[str, int]], dict[str, int]]:
    """Load co-occurrence rows + term frequencies for one annotation set.

    Keyed on the snapshot-invariant ``go_id`` strings. ``known_go_ids`` are the
    go_id strings of the query proteins' known (anchor) terms.

    Returns ``(cooc_by_known, freq)`` where:

    - ``cooc_by_known[k][t]`` is the number of distinct proteins carrying both
      known go_id ``k`` and candidate go_id ``t`` (only ``k`` in
      ``known_go_ids`` are loaded).
    - ``freq[k]`` is the distinct-protein frequency of go_id ``k`` (the
      denominator of ``P(t | k)``); only the known go_ids are loaded.

    Both maps are empty when the tables hold nothing for this set, so the
    caller degrades gracefully to the zero-fill default. Rows whose go_id
    columns are NULL (legacy pre-migration builds) never match a string key and
    are simply ignored; the predict path requires a fresh string-keyed build.

    Each ``(annotation_set_id, known_go_id)`` is read from the DB ONCE and
    memoized (see module docstring + ``clear_cooccurrence_cache``); subsequent
    calls with overlapping known sets serve those keys from the cache. The
    return value is identical to a no-cache DB-only load for any input.
    """
    cooc_by_known: dict[str, dict[str, int]] = {}
    freq: dict[str, int] = {}
    if not known_go_ids:
        return cooc_by_known, freq

    uncached = [k for k in known_go_ids if (annotation_set_id, k) not in _COOC_CACHE]
    if uncached:
        _load_into_cache(session, annotation_set_id, uncached)

    for k in known_go_ids:
        row = _COOC_CACHE[(annotation_set_id, k)]
        if row:
            cooc_by_known[k] = row
        f = _FREQ_CACHE[(annotation_set_id, k)]
        if f is not None:
            freq[k] = f

    return cooc_by_known, freq


def _load_into_cache(
    session: Session,
    annotation_set_id: uuid.UUID,
    uncached: list[str],
) -> None:
    """Query the DB for the uncached known go_ids and populate the memo.

    Every requested key gets a cache entry (an empty dict / ``None`` freq when it
    has no rows) so a known go_id with no cooccurrence or frequency row is not
    re-queried on a later call.
    """
    cooc_rows = session.execute(
        select(
            TermCooccurrence.known_go_id,
            TermCooccurrence.candidate_go_id,
            TermCooccurrence.cooccurrence_count,
        ).where(
            TermCooccurrence.annotation_set_id == annotation_set_id,
            TermCooccurrence.known_go_id.in_(uncached),
        )
    ).all()
    rows_by_known: dict[str, dict[str, int]] = {}
    for known_go, candidate_go, count in cooc_rows:
        if known_go is None or candidate_go is None:
            continue
        rows_by_known.setdefault(str(known_go), {})[str(candidate_go)] = int(count)

    freq_rows = session.execute(
        select(TermFrequency.go_id, TermFrequency.freq).where(
            TermFrequency.annotation_set_id == annotation_set_id,
            TermFrequency.go_id.in_(uncached),
        )
    ).all()
    freq_by_known: dict[str, int] = {}
    for go_id, f in freq_rows:
        if go_id is None:
            continue
        freq_by_known[str(go_id)] = int(f)

    # Cache an entry for EVERY uncached key (empty dict / None when absent) so
    # the no-cooccurrence / no-frequency case is never re-queried.
    for k in uncached:
        _COOC_CACHE[(annotation_set_id, k)] = rows_by_known.get(k, {})
        _FREQ_CACHE[(annotation_set_id, k)] = freq_by_known.get(k)
