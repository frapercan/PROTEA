"""A method's own scores into cafaeval's prediction file, straight from Postgres (ADR-D60).

A method that scores its own candidates (the ``method_score`` formula, no
reranker, no depth cut) has one row per (protein, term), no neighbour and no
distance. The base frame's path fetches some twenty retrieval columns into
Python objects, keeps the nearest neighbour per pair and recounts neighbour
aggregates: for a method that is all identity, and it cost 13 of the 20 minutes
of the first count map's evaluation (6,243,290 rows, 211 -> 227, 2026-11-18).

This path asks for the three columns that matter, streamed with COPY into a
temporary file and read in one pass, drops the pairs the bank denies by the
same rule (``protea.core.operations._denials.denied_pairs``), orders the rows by
protein and term as the base frame does, and writes the same four-decimal file
(``'%.4f' % score``, the formatting of ``_write_scored_base``).
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

from protea.core.operations._denials import denied_pairs
from protea.core.scoring import MethodScoreMissing
from protea.infrastructure.orm.models.embedding.scoring_config import FORMULA_METHOD_SCORE

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from protea.core.operations._run_cafa_artifacts import WritePredictionsContext
    from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig

_LOG = logging.getLogger(__name__)

_ROWS = (
    "COPY (SELECT p.protein_accession, t.go_id, p.method_score "
    "FROM go_prediction p JOIN go_term t ON t.id = p.go_term_id "
    "WHERE p.prediction_set_id = %s AND p.protein_accession = ANY(%s)) TO STDOUT"
)
_COLUMNS = ("protein_accession", "go_id", "method_score")
_TYPES = {"protein_accession": pa.string(), "go_id": pa.string(), "method_score": pa.float64()}


def applies(ctx: WritePredictionsContext, scoring_config: ScoringConfig | None) -> bool:
    """Whether the set is read by the method's own score with no depth cut to apply."""
    return (
        scoring_config is not None
        and scoring_config.formula == FORMULA_METHOD_SCORE
        and ctx.max_distance is None
        and ctx.max_k_position is None
        and getattr(ctx, "max_sequence_rank", None) is None
    )


def method_rows(session: Session, ctx: WritePredictionsContext) -> pa.Table:
    """(protein, term, score) of the delta proteins, streamed out of Postgres."""
    raw = session.connection().connection.driver_connection
    with tempfile.NamedTemporaryFile("wb", suffix=".tsv", delete=False) as handle:
        path = handle.name
        with raw.cursor() as cur:
            with cur.copy(_ROWS, (ctx.pred_set_id, sorted(ctx.delta_proteins))) as copy:
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


def without_denials(session: Session, ctx: WritePredictionsContext, rows: pa.Table) -> pa.Table:
    """The rows minus every (protein, term) the bank denies, by the base frame's rule."""
    denied = denied_pairs(session, ctx)
    if not denied or rows.num_rows == 0:
        return rows
    keys = pc.binary_join_element_wise(rows["protein_accession"], rows["go_id"], "\t")
    out = pc.is_in(keys, value_set=pa.array([f"{p}\t{g}" for p, g in denied]))
    removed = int(pc.sum(out).as_py() or 0)
    if removed:
        _LOG.warning(
            "dropped %d candidate(s) the bank explicitly denies about the query; "
            "a NOT annotation is curated evidence against exactly this inference",
            removed,
        )
    return rows.filter(pc.invert(out))


def write_method_predictions(session: Session, ctx: WritePredictionsContext) -> int:
    """Write ``protein\\tgo_id\\tscore`` for a method's set; how many rows went out."""
    rows = without_denials(session, ctx, method_rows(session, ctx))
    missing = rows["method_score"].null_count
    if missing:
        raise MethodScoreMissing(
            f"{missing} of {rows.num_rows} rows carry no method_score while the "
            "scoring config asks for the method's own score"
        )
    order = pc.sort_indices(
        rows, sort_keys=[("protein_accession", "ascending"), ("go_id", "ascending")]
    )
    rows = rows.take(order)
    scores = np.char.mod("%.4f", rows["method_score"].to_numpy())
    table = pa.table(
        {
            "protein_accession": rows["protein_accession"],
            "go_id": rows["go_id"],
            "score": pa.array(scores.tolist(), type=pa.string()),
        }
    )
    _write_tsv(table, ctx.path)
    return table.num_rows


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


__all__ = ["applies", "method_rows", "without_denials", "write_method_predictions"]
