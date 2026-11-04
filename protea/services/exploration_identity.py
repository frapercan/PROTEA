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

THE PREDICATE READS EVERY ROW, AND NOTHING ELSE DECIDES IT. The two sets are
joined on the prediction's key, ``(protein_accession, go_term_id)``, and every
pair's full text is compared; "identical" means the join found no pair that
differs and no key on one side only. An earlier version decided from an
order-independent sum of row hashes, and the server session's review named why
that cannot carry an identity claim: a sum is linear, so it is a screen, and a
module that says power does not apply cannot then emit ``for`` on a screen.
The row counts are reported beside the answer and decide nothing.

A row is the key with what retrieval decided for it: the donor, the distance,
the votes, the position, the whole donor ledger and the method's own score.
Doubles are compared by their bytes, ``float8send`` and ``array_send``, so two
agree only when they are the same double. Their text would depend on the
session's ``extra_float_digits``: it renders the shortest exact form today,
but an ``ALTER DATABASE ... SET extra_float_digits = 0`` would turn "the same
double" into "the same fifteen digits" without anything noticing, and the
predicate would still say identical (the server session's review). NULL is
written as a marker of its own, so a value that moved from one column to
another cannot read as the same row. The first difference is shown in readable
text beside the bytes it was decided on.

The join is a merge over the unique index on ``(prediction_set_id,
protein_accession, go_term_id)``, which hands both sides over in key order, and
it stops at the first difference in key order. Nothing leaves the database but
that row and the two counts.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

__all__ = ["row_identity"]

#: One row as compared: every field in a fixed order, doubles as their exact
#: bytes in hex, and NULL as its own marker. No GUC can change it.
_ROW_TEXT = """
    concat_ws('|',
        protein_accession,
        go_term_id::text,
        ref_protein_accession,
        coalesce(encode(float8send(distance), 'hex'), '~'),
        coalesce(vote_count::text, '~'),
        coalesce(k_position::text, '~'),
        coalesce(donor_accessions::text, '~'),
        coalesce(encode(array_send(donor_distances), 'hex'), '~'),
        coalesce(encode(float8send(method_score), 'hex'), '~'))
"""

#: The same row for a reader: doubles as text. Shown, never compared.
_ROW_SHOWN = """
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

_COUNT_SQL = text("SELECT count(*) FROM go_prediction WHERE prediction_set_id = CAST(:ps AS uuid)")

_FIRST_DIFFERENCE_SQL = text(
    f"""
    SELECT coalesce(a.protein_accession, b.protein_accession) AS protein_accession,
           coalesce(a.go_term_id, b.go_term_id) AS go_term_id,
           a.row_text AS a_text,
           b.row_text AS b_text,
           a.row_shown AS a_shown,
           b.row_shown AS b_shown
    FROM (SELECT protein_accession, go_term_id, {_ROW_TEXT} AS row_text, {_ROW_SHOWN} AS row_shown
          FROM go_prediction WHERE prediction_set_id = CAST(:a AS uuid)) a
    FULL OUTER JOIN
         (SELECT protein_accession, go_term_id, {_ROW_TEXT} AS row_text, {_ROW_SHOWN} AS row_shown
          FROM go_prediction WHERE prediction_set_id = CAST(:b AS uuid)) b
      ON a.protein_accession = b.protein_accession AND a.go_term_id = b.go_term_id
    WHERE a.row_text IS DISTINCT FROM b.row_text
    ORDER BY 1, 2
    LIMIT 1
    """  # noqa: S608 - fixed SQL
)


def row_identity(session: Session, a: str, b: str) -> dict[str, Any]:
    """Whether prediction sets ``a`` and ``b`` hold the same rows, and if not, where first.

    Zero tolerance: an identity claim with a tolerance is no claim. The first
    difference is the lowest key whose rows differ or that one side lacks, with
    both sides' row text (None for the side that lacks it).
    """
    counts = {
        side: int(session.execute(_COUNT_SQL, {"ps": ps}).scalar_one())
        for side, ps in (("a", a), ("b", b))
    }
    found = session.execute(_FIRST_DIFFERENCE_SQL, {"a": a, "b": b}).mappings().first()
    first = None
    if found is not None:
        first = {
            "protein_accession": found["protein_accession"],
            "go_term_id": found["go_term_id"],
            "a": found["a_shown"],
            "b": found["b_shown"],
            "a_exact": found["a_text"],
            "b_exact": found["b_text"],
        }
    return {"identical": first is None, "rows": counts, "first_difference": first}
