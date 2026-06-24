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

from sqlalchemy.orm import Session

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

    Also clears the in-memory CSR cooccurrence memo (the optional
    :mod:`_association_csr` path), since cooccurrence is t0-specific and must be
    dropped on the same per-split boundary regardless of which loader is active.
    """
    _COOC_CACHE.clear()
    _FREQ_CACHE.clear()
    from protea.core.operations.predict_go_terms._association_csr import (
        clear_csr_cooccurrence_cache,
    )

    clear_csr_cooccurrence_cache()


def load_cooccurrence_for_known(
    session: Session,
    annotation_set_id: uuid.UUID,
    known_go_ids: set[str],
) -> tuple[dict[str, dict[str, int]], dict[str, int]]:
    """Load co-occurrence rows + term frequencies for one annotation set.

    When ``PROTEA_ASSOCIATION_CSR`` is set, delegates to the in-memory CSR loader
    (:mod:`_association_csr`): one whole-set COPY scan into ``scipy.sparse``, then
    every per-chunk lookup from RAM (the export is disk-I/O-bound, not CPU-bound).
    Bit-exact with the per-chunk DB path below; default (env unset) is that path.

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
    from protea.core.operations.predict_go_terms import _association_csr

    if _association_csr.csr_enabled():
        return _association_csr.load_cooccurrence_for_known(
            session, annotation_set_id, known_go_ids
        )

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

    Bulk-reads both tables with ``COPY (SELECT ...) TO STDOUT`` on the raw
    psycopg3 connection (skips the ``.all()`` Row-object materialization that was
    ~48% of profiled export samples). The known go_id filter is bound as a single
    ``= ANY(%s)`` array PARAMETER (psycopg3 ``cursor.copy(sql, params)`` accepts
    bound params) instead of an inlined quoted IN-list literal: the prior inline
    path re-parsed a megabyte-class SQL string server-side every call (profiled
    ~5.4% of build_records as ``_split_query`` under ``apply_association``) and
    needed a hand-rolled quoting / injection guard, both of which the array bind
    eliminates. ``known_go_id IS NOT NULL`` keeps the legacy-NULL skip.

    Every requested key gets a cache entry (empty dict / ``None`` freq when
    absent) so a no-row key is not re-queried. Byte-identical to the prior load.
    """
    raw = session.connection().connection  # psycopg3 DBAPI connection
    cooc_sql = (
        "COPY (SELECT known_go_id, candidate_go_id, cooccurrence_count "
        "FROM term_cooccurrence "
        "WHERE annotation_set_id = %s "
        "AND known_go_id = ANY(%s) "
        "AND known_go_id IS NOT NULL AND candidate_go_id IS NOT NULL"
        ") TO STDOUT"
    )
    rows_by_known: dict[str, dict[str, int]] = {}
    with raw.cursor() as cur, cur.copy(cooc_sql, (annotation_set_id, uncached)) as cp:
        for known_go, candidate_go, count in cp.rows():
            rows_by_known.setdefault(known_go, {})[candidate_go] = int(count)

    freq_sql = (
        "COPY (SELECT go_id, freq FROM term_frequency "
        "WHERE annotation_set_id = %s "
        "AND go_id = ANY(%s) "
        "AND go_id IS NOT NULL"
        ") TO STDOUT"
    )
    freq_by_known: dict[str, int] = {}
    with raw.cursor() as cur, cur.copy(freq_sql, (annotation_set_id, uncached)) as cp:
        for go_id, f in cp.rows():
            freq_by_known[go_id] = int(f)

    # Cache an entry for EVERY uncached key (empty dict / None when absent) so
    # the no-cooccurrence / no-frequency case is never re-queried.
    for k in uncached:
        _COOC_CACHE[(annotation_set_id, k)] = rows_by_known.get(k, {})
        _FREQ_CACHE[(annotation_set_id, k)] = freq_by_known.get(k)
