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

from protea.core.operations._predict_go_terms_adapter import (
    AdapterInputs,
    call_pipeline_predict,
)

_TERM = 7
_ANNOTATION = {"go_term_id": _TERM, "qualifier": "enables", "evidence_code": "EXP"}
_UUID = "11111111-1111-1111-1111-111111111111"


def _payload() -> PredictGOTermsBatchPayload:
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
    })


def _run(identities: dict[str, str] | None) -> list[dict[str, Any]]:
    """One query over three references, two of which share a sequence."""
    refs = np.array([[1.0, 0.0], [0.9, 0.1], [0.0, 1.0]], dtype=np.float32)
    return call_pipeline_predict(
        AdapterInputs(
            p=_payload(),
            valid_accessions=["Q1"],
            query_embeddings=np.array([[1.0, 0.0]], dtype=np.float32),
            ref_data={
                "accessions": ["R1", "R2", "R3"],
                "embeddings_f32": refs,
                "embeddings_f32_cos": refs,
            },
            annotations=dict.fromkeys(("R1", "R2", "R3"), [dict(_ANNOTATION)]),
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
    rows = _run({"R1": "s1", "R2": "s1", "R3": "s2"})
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
