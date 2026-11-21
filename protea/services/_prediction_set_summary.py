"""A prediction set summarised once, so its pages stop counting its rows on every visit.

A set does not change after it is written. Its page asked three questions of its
rows on every visit (how many, which proteins with how many terms, which GO
terms most often), and on a set of 1.8 million candidate pairs each answer took
up to 20 s. They are answered once and kept: the totals and the most frequent
terms in ``prediction_set.summary``, each protein in ``prediction_set_protein``.

A set's pairs live in one of two shapes: candidate rows in ``go_prediction``, or,
for a scored set, one ``prediction_score`` row per protein holding its terms as
an array. Both are read, so a scored set's page lists its proteins and terms too,
which the grouping over ``go_prediction`` alone never did.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

#: How many of a set's most frequent terms the summary keeps.
TOP_TERMS = 50

#: One scan of the set's pairs, grouped two ways at once: by protein (its terms and
#: closest distance) and by term (how often). Two separate groupings read the set
#: twice, about 20 s each on 1.8 million pairs.
_COUNTS = text(
    """
    WITH pairs AS (
        SELECT protein_accession AS accession, go_term_id, distance
        FROM go_prediction WHERE prediction_set_id = CAST(:s AS uuid)
        UNION ALL
        SELECT ps.protein_accession, u.go_term_id, NULL::double precision
        FROM prediction_score AS ps, unnest(ps.go_term_ids) AS u(go_term_id)
        WHERE ps.prediction_set_id = CAST(:s AS uuid)
    )
    SELECT accession, go_term_id, count(*) AS n, min(distance) AS d, GROUPING(accession) AS by_term
    FROM pairs
    GROUP BY GROUPING SETS ((accession), (go_term_id))
    """
)

_PROTEINS = text(
    """
    INSERT INTO prediction_set_protein (prediction_set_id, protein_accession, go_count, min_distance)
    SELECT CAST(:s AS uuid), a, n, d
    FROM unnest(CAST(:accessions AS text[]), CAST(:counts AS int[]), CAST(:distances AS double precision[]))
        AS t(a, n, d)
    ON CONFLICT DO NOTHING
    """
)

_TERMS = text("SELECT id, go_id, name, aspect FROM go_term WHERE id = ANY(CAST(:ids AS bigint[]))")

_KEEP = text(
    "UPDATE prediction_set SET summary = CAST(:summary AS jsonb) "
    "WHERE id = CAST(:s AS uuid) AND summary IS NULL"
)


def summarize(session: Session, set_id: uuid.UUID) -> dict[str, Any]:
    """Count the set once, keep the counts, and return the summary the set now holds.

    Idempotent and safe against a second reader doing the same at once: the
    proteins are inserted with ``ON CONFLICT DO NOTHING`` and the summary is only
    written where there is none, then read back.
    """
    import json

    proteins: list[tuple[str, int, float | None]] = []
    by_term: dict[int, int] = {}
    for accession, go_term_id, n, d, grouped_by_term in session.execute(
        _COUNTS, {"s": str(set_id)}
    ):
        if grouped_by_term:
            by_term[int(go_term_id)] = int(n)
        else:
            proteins.append((accession, int(n), d))
    session.execute(
        _PROTEINS,
        {
            "s": str(set_id),
            "accessions": [p[0] for p in proteins],
            "counts": [p[1] for p in proteins],
            "distances": [p[2] for p in proteins],
        },
    )
    terms = [
        (go_id, name, aspect, by_term[term_id])
        for term_id, go_id, name, aspect in session.execute(_TERMS, {"ids": list(by_term)})
    ]
    totals: dict[str, int] = {}
    for _go_id, _name, aspect, n in terms:
        key = aspect or "other"
        totals[key] = totals.get(key, 0) + int(n)
    top = sorted(terms, key=lambda r: (-int(r[3]), r[0]))[:TOP_TERMS]
    summary = {
        "rows": sum(totals.values()),
        "proteins": len(proteins),
        "aspect_totals": totals,
        "top_terms": [
            {"go_id": go_id, "name": name, "aspect": aspect, "count": int(n)}
            for go_id, name, aspect, n in top
        ],
        "computed_at": datetime.now(UTC).isoformat(),
    }
    session.execute(_KEEP, {"summary": json.dumps(summary), "s": str(set_id)})
    stored = session.execute(
        text("SELECT summary FROM prediction_set WHERE id = CAST(:s AS uuid)"), {"s": str(set_id)}
    ).scalar_one()
    return stored or summary


def summary_of(
    session: Session, set_id: uuid.UUID, stored: dict[str, Any] | None
) -> dict[str, Any]:
    """The set's summary: the one it holds, or counted now and kept."""
    return stored if stored else summarize(session, set_id)


def go_term_distribution(summary: dict[str, Any], limit: int) -> dict[str, Any]:
    """The set's most frequent terms, by aspect, as the page shows them (up to ``TOP_TERMS``)."""
    top = summary["top_terms"][: max(0, min(limit, TOP_TERMS))]
    by_aspect: dict[str, list[dict[str, Any]]] = {"F": [], "P": [], "C": [], "other": []}
    for term in top:
        entry = {"go_id": term["go_id"], "name": term["name"], "count": term["count"]}
        by_aspect.get(term["aspect"] or "other", by_aspect["other"]).append(entry)
    return {"by_aspect": by_aspect, "aspect_totals": summary["aspect_totals"], "top_terms": top}


def protein_page(
    session: Session, set_id: uuid.UUID, search: str | None, limit: int, offset: int
) -> tuple[int, list[tuple[str, int, float | None]]]:
    """``(total, rows)`` of the set's proteins, by accession, from the counted table."""
    where = "prediction_set_id = CAST(:s AS uuid)"
    params: dict[str, Any] = {"s": str(set_id), "limit": limit, "offset": offset}
    if search:
        where += " AND protein_accession ILIKE :search"
        params["search"] = f"%{search}%"
    total = session.execute(
        text(f"SELECT count(*) FROM prediction_set_protein WHERE {where}"), params
    ).scalar_one()
    rows = session.execute(
        text(
            "SELECT protein_accession, go_count, min_distance FROM prediction_set_protein "
            f"WHERE {where} ORDER BY protein_accession LIMIT :limit OFFSET :offset"
        ),
        params,
    ).all()
    return int(total), [(r[0], int(r[1]), r[2]) for r in rows]


def per_protein_counts(session: Session, set_id: uuid.UUID) -> dict[str, int]:
    """Every protein of the set and its number of terms, from the counted table."""
    return {
        acc: int(n)
        for acc, n in session.execute(
            text(
                "SELECT protein_accession, go_count FROM prediction_set_protein "
                "WHERE prediction_set_id = CAST(:s AS uuid)"
            ),
            {"s": str(set_id)},
        )
    }
