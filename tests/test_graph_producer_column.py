"""The producer column names a producer, and the graph has to say which kind.

``go_prediction.ref_protein_accession`` is NOT NULL, and the sentence three
nodes argue off it used to read "no candidate CAN arrive without a donor". That
is true of the column and false of the record: the constraint is about NULL, and
two of this deployment's ten prediction sets carry a method's name there.

Measured 2026-10-28: 2 distinct sentinel values over 18.415.500 rows, 82,1% of
all candidates, against 136.948 donor accessions over 4.008.013.
"""

from __future__ import annotations

from protea.api.routers._graph_producer_column import (
    NO_DONORLESS_ROW,
    sentinel_sentence,
    sentinel_sets,
)
from protea.infrastructure.orm.models.embedding.embedding_config import (
    INGESTED_METHOD_BACKEND,
)


def _record(*backends: str) -> dict[str, list[dict[str, object]]]:
    """One substrate and one prediction set per backend named."""
    return {
        "substrates": [
            {"id": f"ec-{i}", "model_backend": b} for i, b in enumerate(backends)
        ],
        "prediction_sets": [
            {"id": f"ps-{i}", "embedding_config_id": f"ec-{i}"} for i, _ in enumerate(backends)
        ],
    }


def test_the_constraint_sentence_no_longer_claims_every_candidate_names_a_donor():
    """The edit this module exists for. The old wording was a single-field claim
    stated in the language of a different quantity."""
    assert "NOT NULL" in NO_DONORLESS_ROW
    assert "producer" in NO_DONORLESS_ROW
    # And it names the three admitted forms, so a reader does not have to go
    # find the two operations that write the other two.
    assert "InterPro" in NO_DONORLESS_ROW
    assert "method's name" in NO_DONORLESS_ROW


def test_an_ingested_set_is_counted_and_a_retrieval_set_is_not():
    record = _record("ankh", INGESTED_METHOD_BACKEND, "ankh")
    assert sentinel_sets(record) == (1, 3)


def test_a_record_with_no_ingested_set_says_nothing_extra():
    """A deployment where every candidate really does name a donor reads exactly
    as it did before, which is why the clause is conditional and not a count
    that happens to be zero."""
    assert sentinel_sentence(_record("ankh", "esm2")) == ""


def test_the_sentence_names_how_many_of_how_many():
    sentence = sentinel_sentence(_record(INGESTED_METHOD_BACKEND, INGESTED_METHOD_BACKEND, "ankh"))
    assert "2 of 3 prediction sets" in sentence
    assert "not a donor protein" in sentence


def test_an_empty_record_does_not_raise():
    """The graph reads one record for every window, including one with nothing
    in it, and a node that raised there would take the whole page down."""
    assert sentinel_sets({}) == (0, 0)
    assert sentinel_sentence({}) == ""


def test_a_set_whose_substrate_is_absent_is_not_counted_as_a_sentinel():
    """Absent is not ingested. Guessing either way would be a claim about a
    config the record did not return, and the conservative direction is the one
    that does not accuse a retrieval arm of carrying a sentinel."""
    record = {
        "substrates": [],
        "prediction_sets": [{"id": "ps-0", "embedding_config_id": "ec-gone"}],
    }
    assert sentinel_sets(record) == (0, 1)
