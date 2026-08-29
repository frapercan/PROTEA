"""The whole path, from PROTEA's map to the rank on the row.

The two halves are tested apart elsewhere: the producer of the map in
``test_sequence_identity_producer``, the ranking rule in
``protea-method``. This asserts they meet, which is the part that has
failed before: a field that exists on both sides and travels between
neither.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pytest
from protea_contracts import PredictGOTermsBatchPayload

from protea.core.operations._donor_recount import DepthCut, recount_at_depth
from protea.core.operations._predict_go_terms_adapter import (
    AdapterInputs,
    call_pipeline_predict,
)

_TERM = 7
_ANNOTATION = {"go_term_id": _TERM, "qualifier": "enables", "evidence_code": "EXP"}
_UUID = "11111111-1111-1111-1111-111111111111"


def _payload(exclude_self: bool = False) -> PredictGOTermsBatchPayload:
    """The real contract, so this test cannot drift from what ships."""
    return PredictGOTermsBatchPayload.model_validate({
        "embedding_config_id": _UUID,
        "annotation_set_id": _UUID,
        "ontology_snapshot_id": _UUID,
        "prediction_set_id": _UUID,
        "parent_job_id": _UUID,
        "query_accessions": ["Q1"],
        "limit_per_entry": 3,
        "metric": "cosine",
        "search_backend": "numpy",
        "exclude_self_neighbour": exclude_self,
    })


def _run(
    identities: dict[str, str] | None, *, exclude_self: bool = False
) -> list[dict[str, Any]]:
    """One query over three references, two of which share a sequence."""
    refs = np.array(
        [[1.0, 0.0], [1.0, 0.0], [0.9, 0.1], [0.0, 1.0]], dtype=np.float32
    )
    return call_pipeline_predict(
        AdapterInputs(
            p=_payload(exclude_self),
            valid_accessions=["Q1"],
            query_embeddings=np.array([[1.0, 0.0]], dtype=np.float32),
            ref_data={
                "accessions": ["Q1", "R1", "R2", "R3"],
                "embeddings_f32": refs,
                "embeddings_f32_cos": refs,
            },
            annotations=dict.fromkeys(("Q1", "R1", "R2", "R3"), [dict(_ANNOTATION)]),
            go_id_map={_TERM: "GO:0000007"},
            go_aspect_map={_TERM: "F"},
            prediction_set_id=None,
            ref_sequences={},
            query_sequences={},
            ref_tax_ids={},
            query_tax_ids={},
            ref_sequence_identities=identities,
        )
    ).predictions


def test_the_map_reaches_the_row_as_a_sequence_rank() -> None:
    """R1 and R2 are one sequence, so the nearest one ranks 1."""
    rows = _run({"Q1": "s0", "R1": "s1", "R2": "s1", "R3": "s2"})
    assert rows, "the adapter produced no rows to rank"
    assert rows[0]["sequence_rank"] == 1


def test_without_the_map_the_column_is_empty_rather_than_guessed() -> None:
    """Null says the retrieval predates the question. Zero would lie."""
    rows = _run(None)
    assert rows
    assert all(row["sequence_rank"] is None for row in rows)


def test_a_neighbour_missing_from_the_map_stops_the_run() -> None:
    """Silence here is how an arm gets scored against the wrong depth."""
    with pytest.raises(Exception, match="sequence identity"):
        _run({"R1": "s1"})


def test_the_donor_ledger_arrives_with_the_rank() -> None:
    """The two repos meet here: the method produces it, PROTEA stores it.

    R1 and R2 are one sequence, so a cut at sequence depth 1 admits both
    donors while a cut at protein depth 1 admits one. Recounting that from
    the row is the whole point of carrying the ledger.
    """
    # Q1 is its own sequence s0; R1 and R2 share s1. k=3 over a bank of four,
    # so the farthest never enters the neighbourhood.
    rows = _run({"Q1": "s0", "R1": "s1", "R2": "s1", "R3": "s2"})
    row = rows[0]
    assert row["donor_accessions"] == ["Q1", "R1", "R2"]
    assert row["donor_k_positions"] == [1, 2, 3]
    assert row["donor_sequence_ranks"] == [1, 2, 2]
    assert row["donor_count"] == 3

    by_sequence = recount_at_depth(row, DepthCut(max_sequence_rank=2))
    by_protein = recount_at_depth(row, DepthCut(max_k_position=2))
    assert by_sequence is not None and by_protein is not None
    # Depth 2 in sequences admits three donors, because R1 and R2 are one
    # sequence. Depth 2 in proteins admits two. That disagreement is the
    # whole reason the rank exists.
    assert by_sequence.donor_count == 3
    assert by_sequence.sequence_count == 2
    assert by_protein.donor_count == 2
    # Three donors over two sequences is a fraction of 1.0, not 1.5: the
    # numerator counts in the unit of its denominator.
    assert by_sequence.vote_fraction() == 1.0


def test_the_dispatched_flag_reaches_the_row_through_every_layer() -> None:
    """Contract, adapter and method, in one pass.

    The query Q1 is in the bank under its own name, and R1 carries its
    sequence under another. Dispatched with the flag off, Q1 donates to
    itself. Dispatched with it on, neither Q1 nor its twin may donate, and
    the only donor left is the one that is genuinely somebody else.

    This is the loop that was open: the flag validated, was recorded on the
    prediction set, and never reached the search that produced the rows.
    """
    con = _run({"Q1": "s1", "R1": "s1", "R2": "s2", "R3": "s3"}, exclude_self=True)
    sin = _run({"Q1": "s1", "R1": "s1", "R2": "s2", "R3": "s3"}, exclude_self=False)

    assert sin, "the run without exclusion produced nothing to compare against"
    donantes_sin = {r["ref_protein_accession"] for r in sin}
    donantes_con = {r["ref_protein_accession"] for r in con}

    assert "Q1" not in donantes_con, "the query donated to itself with the flag on"
    assert "R1" not in donantes_con, (
        "the query's TWIN donated. Excluding by accession leaves it; by "
        "sequence it must not survive."
    )
    assert donantes_con, "the exclusion removed every donor, so the margin was short"
    assert donantes_sin != donantes_con, (
        "the flag changed nothing, which is exactly the failure it is meant "
        "to have stopped being able to have"
    )
