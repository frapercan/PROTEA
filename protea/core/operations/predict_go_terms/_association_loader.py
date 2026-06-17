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
    """
    cooc_by_known: dict[str, dict[str, int]] = {}
    freq: dict[str, int] = {}
    if not known_go_ids:
        return cooc_by_known, freq

    known_list = list(known_go_ids)
    cooc_rows = session.execute(
        select(
            TermCooccurrence.known_go_id,
            TermCooccurrence.candidate_go_id,
            TermCooccurrence.cooccurrence_count,
        ).where(
            TermCooccurrence.annotation_set_id == annotation_set_id,
            TermCooccurrence.known_go_id.in_(known_list),
        )
    ).all()
    for known_go, candidate_go, count in cooc_rows:
        if known_go is None or candidate_go is None:
            continue
        cooc_by_known.setdefault(str(known_go), {})[str(candidate_go)] = int(count)

    freq_rows = session.execute(
        select(TermFrequency.go_id, TermFrequency.freq).where(
            TermFrequency.annotation_set_id == annotation_set_id,
            TermFrequency.go_id.in_(known_list),
        )
    ).all()
    for go_id, f in freq_rows:
        if go_id is None:
            continue
        freq[str(go_id)] = int(f)

    return cooc_by_known, freq
