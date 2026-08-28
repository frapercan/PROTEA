"""A column with a producer and no writer fills with silence.

This exists because it happened. ``sequence_rank`` shipped with its
migration, its ORM column and a producer in ``protea-method``, and
nothing copied it from the prediction dict onto the insert row. Nothing
failed: the column simply stayed null on every row written after it was
added, which reads as "the retrieval predates the question" and is
exactly the sentence the column was documented to mean.

The check is deliberately narrow. It asserts that every key the method
emits which has a matching ``GOPrediction`` column reaches the row, and
it walks the real ``_row_from_prediction`` rather than a list of names,
so a new key wired to a new column is covered the day it lands.
"""

from __future__ import annotations

import uuid
from typing import Any

from protea.core.operations.predict_go_terms._common import _row_from_prediction
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction

#: One prediction dict in the shape the method emits, with a recognisable
#: value per key so a dropped key is visible as a missing value, not as a
#: coincidence.
_PRED: dict[str, Any] = {
    "protein_accession": "Q1",
    "go_term_id": 7,
    "ref_protein_accession": "R1",
    "distance": 0.11,
    "qualifier": "enables",
    "evidence_code": "EXP",
    "vote_count": 9,
    "k_position": 3,
    "sequence_rank": 2,
    "go_term_frequency": 41,
    "ref_annotation_density": 17,
    "neighbor_distance_std": 0.05,
    "neighbor_vote_fraction": 0.9,
    "neighbor_min_distance": 0.11,
    "neighbor_mean_distance": 0.22,
    "donor_accessions": ["R1", "R2"],
    "donor_k_positions": [1, 3],
    "donor_sequence_ranks": [1, 2],
    "donor_distances": [0.11, 0.33],
    "donor_count": 2,
}

_COLUMNS = {c.name for c in GOPrediction.__table__.columns}


def test_every_key_that_names_a_column_reaches_the_row() -> None:
    row = _row_from_prediction(_PRED, uuid.uuid4())
    produced = {k for k in _PRED if k in _COLUMNS}
    dropped = sorted(k for k in produced if row.get(k) is None)
    assert not dropped, (
        f"{len(dropped)} produced column(s) never reach the insert row and "
        f"would stay null on every future row: {dropped}"
    )


def test_the_depth_columns_in_particular() -> None:
    """Named separately because this is the pair that shipped without one."""
    row = _row_from_prediction(_PRED, uuid.uuid4())
    assert row["k_position"] == 3
    assert row["sequence_rank"] == 2


def test_the_donor_ledger_survives_as_lists_rather_than_being_floated() -> None:
    """The float cleaner would turn each array into None on its way through."""
    row = _row_from_prediction(_PRED, uuid.uuid4())
    assert row["donor_accessions"] == ["R1", "R2"]
    assert row["donor_k_positions"] == [1, 3]
    assert row["donor_distances"] == [0.11, 0.33]
    assert row["donor_count"] == 2


def test_a_producer_that_did_not_run_leaves_the_column_null_not_empty() -> None:
    """Null says the producer was off; an empty array would say zero donors."""
    row = _row_from_prediction(
        {k: v for k, v in _PRED.items() if not k.startswith("donor_")},
        uuid.uuid4(),
    )
    assert row["donor_accessions"] is None
    assert row["donor_count"] is None
