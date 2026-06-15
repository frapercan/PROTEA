"""Co-occurrence + frequency lookups for the cross-aspect association feature.

Reads the ``term_cooccurrence`` / ``term_frequency`` tables populated by the
``build_go_cooccurrence`` operation. Kept in its own module so the predict
path can monkeypatch the loader in unit tests without standing up the tables.
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
    known_term_ids: set[int],
) -> tuple[dict[int, dict[int, int]], dict[int, int]]:
    """Load co-occurrence rows + term frequencies for one annotation set.

    Returns ``(cooc_by_known, freq)`` where:

    - ``cooc_by_known[k][t]`` is the number of distinct proteins carrying both
      known term ``k`` and candidate term ``t`` (only ``k`` in
      ``known_term_ids`` are loaded).
    - ``freq[k]`` is the distinct-protein frequency of term ``k`` (the
      denominator of ``P(t | k)``); only the known terms are loaded.

    Both maps are empty when the tables hold nothing for this set, so the
    caller degrades gracefully to the zero-fill default.
    """
    cooc_by_known: dict[int, dict[int, int]] = {}
    freq: dict[int, int] = {}
    if not known_term_ids:
        return cooc_by_known, freq

    known_list = list(known_term_ids)
    cooc_rows = session.execute(
        select(
            TermCooccurrence.known_term_id,
            TermCooccurrence.candidate_term_id,
            TermCooccurrence.cooccurrence_count,
        ).where(
            TermCooccurrence.annotation_set_id == annotation_set_id,
            TermCooccurrence.known_term_id.in_(known_list),
        )
    ).all()
    for known_id, candidate_id, count in cooc_rows:
        cooc_by_known.setdefault(int(known_id), {})[int(candidate_id)] = int(count)

    freq_rows = session.execute(
        select(TermFrequency.term_id, TermFrequency.freq).where(
            TermFrequency.annotation_set_id == annotation_set_id,
            TermFrequency.term_id.in_(known_list),
        )
    ).all()
    for term_id, f in freq_rows:
        freq[int(term_id)] = int(f)

    return cooc_by_known, freq
