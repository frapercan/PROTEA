"""Whether two prediction sets are the same, row for row (ADR-D55, the instrument).

A repeatability test is an assertion of identity, not a comparison of two
populations. Bootstrap intervals and power answer "could this difference be
noise", which is the wrong question when the claim is "there is no difference
at all, bit for bit". For an identity assertion the test is exact equality, and
its way of failing is the first row that differs, not an interval.

THE ROWS ARE THE TEST, THE PANELS CORROBORATE. Every stored metric is rounded
to four decimals, so two panels that agree exactly are an upper bound on the
equality of the runs, not a proof of it. And the scoring is already known to be
deterministic (117 of 117 metrics reproduced exactly), so what is under
suspicion is retrieval, which shows in rows: which donor, at what distance.

A row is the prediction's key, ``(protein_accession, go_term_id)``, with what
retrieval decided for it: the donor, the distance, the votes, the position, the
whole donor ledger and the method's own score. Floats are compared through
Postgres's shortest exact text, so two doubles agree only when they are the same
double. NULL is written as a marker of its own, so a value that moved from one
column to another cannot read as the same row.

LIGHT ON PURPOSE. Each set is summarised on the database by one pass through its
``prediction_set_id`` index: the row count and an order-independent sum of a
64-bit hash of each row. Nothing is joined and nothing leaves the database. Only
when the two summaries differ is the first difference located, first by protein
and then by row inside that protein.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

__all__ = ["row_identity"]

#: The text of one row, every field in a fixed order and NULL as its own marker.
_ROW_TEXT = """
    concat_ws('|',
        protein_accession,
        go_term_id::text,
        ref_protein_accession,
        coalesce(distance::text, '~'),
        coalesce(vote_count::text, '~'),
        coalesce(k_position::text, '~'),
        coalesce(donor_accessions::text, '~'),
        coalesce(donor_distances::text, '~'),
        coalesce(method_score::text, '~'))
"""

#: Sixty bits of the row's md5, as a non-negative bigint; summed as numeric.
_ROW_HASH = f"('x' || substr(md5({_ROW_TEXT}), 1, 15))::bit(60)::bigint"

_SUMMARY_SQL = text(
    f"SELECT count(*) AS n, coalesce(sum({_ROW_HASH}), 0) AS digest "  # noqa: S608 - fixed SQL
    "FROM go_prediction WHERE prediction_set_id = CAST(:ps AS uuid)"
)

_FIRST_PROTEIN_SQL = text(
    f"""
    WITH a AS (
        SELECT protein_accession, count(*) AS n, sum({_ROW_HASH}) AS d
        FROM go_prediction WHERE prediction_set_id = CAST(:a AS uuid)
        GROUP BY protein_accession
    ), b AS (
        SELECT protein_accession, count(*) AS n, sum({_ROW_HASH}) AS d
        FROM go_prediction WHERE prediction_set_id = CAST(:b AS uuid)
        GROUP BY protein_accession
    )
    SELECT coalesce(a.protein_accession, b.protein_accession) AS protein
    FROM a FULL OUTER JOIN b USING (protein_accession)
    WHERE a.d IS DISTINCT FROM b.d OR a.n IS DISTINCT FROM b.n
    ORDER BY 1
    LIMIT 1
    """  # noqa: S608 - fixed SQL
)

_PROTEIN_ROWS_SQL = text(
    f"SELECT go_term_id, {_ROW_TEXT} AS row_text FROM go_prediction "  # noqa: S608 - fixed SQL
    "WHERE prediction_set_id = CAST(:ps AS uuid) AND protein_accession = :protein "
    "ORDER BY go_term_id"
)


def _summary(session: Session, prediction_set_id: str) -> tuple[int, int]:
    row = session.execute(_SUMMARY_SQL, {"ps": prediction_set_id}).one()
    return int(row.n), int(row.digest)


def _first_difference(session: Session, a: str, b: str) -> dict[str, Any] | None:
    protein = session.execute(_FIRST_PROTEIN_SQL, {"a": a, "b": b}).scalar()
    if protein is None:
        return None
    rows: dict[str, dict[int, str]] = {
        side: {
            int(r.go_term_id): str(r.row_text)
            for r in session.execute(_PROTEIN_ROWS_SQL, {"ps": ps, "protein": protein})
        }
        for side, ps in (("a", a), ("b", b))
    }
    for term in sorted(set(rows["a"]) | set(rows["b"])):
        if rows["a"].get(term) != rows["b"].get(term):
            return {
                "protein_accession": protein,
                "go_term_id": term,
                "a": rows["a"].get(term),
                "b": rows["b"].get(term),
            }
    return {"protein_accession": protein, "go_term_id": None, "a": None, "b": None}


def row_identity(session: Session, a: str, b: str) -> dict[str, Any]:
    """Whether prediction sets ``a`` and ``b`` hold the same rows, and if not, where first.

    Zero tolerance: an identity claim with a tolerance is no claim. The first
    difference is the lowest protein accession whose rows differ and, inside it,
    the lowest GO term, with both sides' row text.
    """
    (n_a, digest_a), (n_b, digest_b) = _summary(session, a), _summary(session, b)
    identical = n_a == n_b and digest_a == digest_b
    return {
        "identical": identical,
        "rows": {"a": n_a, "b": n_b},
        "first_difference": None if identical else _first_difference(session, a, b),
    }
