"""A method's stored scores into cafaeval's prediction file, straight from Postgres (ADR-D60).

The evaluator scores what a method stored and nothing else (the closure's
question 6, 2026-11-20). A set stores its scores in one of two forms, and one
query reads both:

- a method's own rows, one ``go_prediction`` row per (protein, term) whose
  ``method_score`` is the method's score: a method run elsewhere and ingested,
  the own-annotations baseline, or the nearest-neighbour method when a reranker
  scored its candidates as it predicted;
- a scored set, one ``prediction_score`` row per protein with its terms and
  their scores, written by ``score_prediction_set`` from a candidate set.

Nothing here scores, cuts, reranks or filters, and the bank's denials are the
method's to honour. The rows are streamed with COPY into a temporary file and
read in one pass, ordered by protein and term, and written as the four-decimal
file cafaeval reads (``'%.4f' % score``).
"""

from __future__ import annotations

import logging
import os
import tempfile
from typing import TYPE_CHECKING, Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv
from sqlalchemy import text

from protea.core.scoring import MethodScoreMissing

if TYPE_CHECKING:
    import uuid

    from sqlalchemy.orm import Session

    from protea.core.operations._run_cafa_artifacts import WritePredictionsContext

_LOG = logging.getLogger(__name__)

#: The stored scores of the window's targets, in whichever form the set holds
#: them. A set holds one form only (``refuse_unless_scored``), so the union
#: never counts a pair twice.
_ROWS = (
    "COPY ("
    "SELECT p.protein_accession, t.go_id, p.method_score "
    "FROM go_prediction p JOIN go_term t ON t.id = p.go_term_id "
    "WHERE p.prediction_set_id = %s AND p.protein_accession = ANY(%s) "
    "UNION ALL "
    "SELECT s.protein_accession, t.go_id, u.score "
    "FROM prediction_score s "
    "CROSS JOIN LATERAL unnest(s.go_term_ids, s.scores) AS u(go_term_id, score) "
    "JOIN go_term t ON t.id = u.go_term_id "
    "WHERE s.prediction_set_id = %s AND s.protein_accession = ANY(%s)"
    ") TO STDOUT"
)
_COLUMNS = ("protein_accession", "go_id", "method_score")
_TYPES = {"protein_accession": pa.string(), "go_id": pa.string(), "method_score": pa.float64()}


#: Where a set of candidates is sent to become one the evaluator can read.
_SCORE_IT = (
    "score its candidates with score_prediction_set, or apply a reranker when "
    "predicting, and evaluate the set that stores the scores "
    "(docs: architecture/scoring)"
)


def refuse_unless_scored(session: Session, pred_set_id: uuid.UUID) -> None:
    """Refuse, before anything is staged, a set that does not store a score per pair.

    Three existence probes: whether the set holds rows, whether any of them
    lacks a score, and whether it holds a scored set's rows. A set of
    candidates is the common case, and the refusal names the step that scores
    it. A set holding both forms is refused too: the union would count its
    pairs twice.
    """
    has_rows, unscored, scored = session.execute(
        text(
            "SELECT EXISTS (SELECT 1 FROM go_prediction WHERE prediction_set_id = :s), "
            "EXISTS (SELECT 1 FROM go_prediction "
            "        WHERE prediction_set_id = :s AND method_score IS NULL), "
            "EXISTS (SELECT 1 FROM prediction_score WHERE prediction_set_id = :s)"
        ),
        {"s": pred_set_id},
    ).one()
    if scored and has_rows:
        raise MethodScoreMissing(
            f"prediction set {pred_set_id} stores scores in both forms, as rows and as a "
            "scored set's arrays; one set stores one form"
        )
    if scored:
        return
    if not has_rows:
        raise MethodScoreMissing(f"prediction set {pred_set_id} holds no predictions to score")
    if unscored:
        raise MethodScoreMissing(
            f"prediction set {pred_set_id} stores candidates without scores (rows with no "
            f"method_score), and the evaluator scores what a method stored: {_SCORE_IT}"
        )


def method_rows(session: Session, ctx: WritePredictionsContext) -> pa.Table:
    """(protein, term, score) of the delta proteins, streamed out of Postgres."""
    raw = session.connection().connection.driver_connection
    with tempfile.NamedTemporaryFile("wb", suffix=".tsv", delete=False) as handle:
        path = handle.name
        with raw.cursor() as cur:
            proteins = sorted(ctx.delta_proteins)
            with cur.copy(_ROWS, (ctx.pred_set_id, proteins, ctx.pred_set_id, proteins)) as copy:
                for block in copy:
                    handle.write(block)
    try:
        if os.path.getsize(path) == 0:
            return pa.table({c: pa.array([], type=_TYPES[c]) for c in _COLUMNS})
        return pcsv.read_csv(
            path,
            read_options=pcsv.ReadOptions(column_names=list(_COLUMNS)),
            parse_options=pcsv.ParseOptions(delimiter="\t", quote_char=False),
            convert_options=pcsv.ConvertOptions(column_types=_TYPES, null_values=["\\N"]),
        )
    finally:
        os.unlink(path)


def scored_table(session: Session, ctx: WritePredictionsContext) -> pa.Table:
    """The stored scores of ``ctx``'s proteins, ordered by protein and term, four decimals.

    Columns ``protein_accession``, ``go_id`` and ``score`` (text). The one
    rendering of a set's scores: the evaluator's prediction file and the CAFA
    export of the API are both this table.
    """
    rows = method_rows(session, ctx)
    missing = rows["method_score"].null_count
    if missing:
        raise MethodScoreMissing(
            f"{missing} of {rows.num_rows} rows of prediction set {ctx.pred_set_id} carry no "
            f"method_score: {_SCORE_IT}"
        )
    order = pc.sort_indices(
        rows, sort_keys=[("protein_accession", "ascending"), ("go_id", "ascending")]
    )
    rows = rows.take(order)
    scores = np.char.mod("%.4f", rows["method_score"].to_numpy())
    return pa.table(
        {
            "protein_accession": rows["protein_accession"],
            "go_id": rows["go_id"],
            "score": pa.array(scores.tolist(), type=pa.string()),
        }
    )


def write_method_predictions(session: Session, ctx: WritePredictionsContext) -> int:
    """Write ``protein\\tgo_id\\tscore`` for a method's set; how many rows went out."""
    table = scored_table(session, ctx)
    _write_tsv(table, ctx.path)
    return table.num_rows


def proteins_of(session: Session, pred_set_id: uuid.UUID) -> set[str]:
    """Every protein a set stores a score for, in either form."""
    rows = session.execute(
        text(
            "SELECT protein_accession FROM go_prediction WHERE prediction_set_id = :s "
            "UNION SELECT protein_accession FROM prediction_score WHERE prediction_set_id = :s"
        ),
        {"s": pred_set_id},
    )
    return {row[0] for row in rows}


def _write_tsv(table: Any, path: str) -> None:
    if table.num_rows == 0:
        with open(path, "w"):
            pass
        return
    pcsv.write_csv(
        table,
        path,
        write_options=pcsv.WriteOptions(include_header=False, delimiter="\t", quoting_style="none"),
    )


__all__ = [
    "method_rows",
    "proteins_of",
    "refuse_unless_scored",
    "scored_table",
    "write_method_predictions",
]
