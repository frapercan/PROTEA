"""Unit tests for the ProtST text-to-GO transfer producer (protst_text family).

Targets ``apply_protst_text`` in
``protea.core.operations.predict_go_terms._protst_text``. The DB-bound pieces
(query-embedding load, reference-bank load, int->go_id resolver) are mocked so
the test exercises only the stamping semantics + the cosine-kNN vote math:

* the kNN vote math on a tiny fixture (cosine-weighted, per-query-max normalise,
  fraction = carriers / eligible-neighbours);
* the leakage guard: the query is excluded from its own neighbour set;
* the ADR-D45 default: a query with NO ProtST embedding keeps NaN on all three
  columns (declared absent), and the leaf builder's default is NaN;
* a covered query stamps measured 0.0 on protst_present / protst_vote_fraction
  for a term no neighbour voted, and leaves protst_text_score NaN for it.
"""

from __future__ import annotations

import math
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from protea.core.operations.predict_go_terms import _protst_text as pt


@pytest.fixture(autouse=True)
def _clear_bank_cache():
    pt._BANK_CACHE.clear()
    yield
    pt._BANK_CACHE.clear()


def _unit(vec: list[float]) -> np.ndarray:
    v = np.asarray(vec, dtype=np.float32)
    return v / float(np.linalg.norm(v))


def _pred(acc: str, go_id: str, go_term_id: int) -> dict[str, Any]:
    """A candidate rec at the leaf builder's declared-absent (NaN) default."""
    nan = float("nan")
    return {
        "protein_accession": acc,
        "go_id": go_id,
        "go_term_id": go_term_id,
        "protst_text_score": nan,
        "protst_vote_fraction": nan,
        "protst_present": nan,
    }


def _run(predictions, *, query_emb, bank, go_id_by_int=None, k=30):
    """Drive ``apply_protst_text`` with every DB call mocked."""
    with (
        patch.object(pt, "_load_query_embeddings", return_value=query_emb),
        patch.object(pt, "_load_reference_bank", return_value=bank),
        patch.object(pt, "_load_go_id_and_aspect", return_value=(go_id_by_int or {}, {})),
    ):
        pt.apply_protst_text(MagicMock(), predictions, MagicMock(), k=k)
    return predictions


def test_knn_vote_math_and_normalisation() -> None:
    # Query Q1 == axis 0. Bank: R1 cos 1.0 (A,B), R2 cos 0.6 (A), R3 cos 0.0 (C).
    # votes A = 1.0 + 0.6 = 1.6, B = 1.0; max 1.6 -> score A 1.0, B 0.625.
    # R3's non-positive cosine is dropped, so C never votes.
    bank = (
        ["R1", "R2", "R3"],
        np.vstack([_unit([1, 0, 0]), _unit([0.6, 0.8, 0]), _unit([0, 1, 0])]),
        {"R1": {"GO:A", "GO:B"}, "R2": {"GO:A"}, "R3": {"GO:C"}},
    )
    preds = [
        _pred("Q1", "GO:A", 1),
        _pred("Q1", "GO:B", 2),
        _pred("Q1", "GO:C", 3),
    ]
    _run(preds, query_emb={"Q1": _unit([1, 0, 0])}, bank=bank)
    by_go = {r["go_id"]: r for r in preds}

    assert by_go["GO:A"]["protst_text_score"] == pytest.approx(1.0)
    assert by_go["GO:B"]["protst_text_score"] == pytest.approx(0.625)
    # 3 eligible neighbours -> fraction = carriers / 3.
    assert by_go["GO:A"]["protst_vote_fraction"] == pytest.approx(2 / 3)
    assert by_go["GO:B"]["protst_vote_fraction"] == pytest.approx(1 / 3)
    assert by_go["GO:A"]["protst_present"] == 1.0
    assert by_go["GO:B"]["protst_present"] == 1.0
    # GO:C drew no positive vote -> measured absence: present/fraction 0.0,
    # score stays NaN (a missing measurement, not a measured zero).
    assert by_go["GO:C"]["protst_present"] == 0.0
    assert by_go["GO:C"]["protst_vote_fraction"] == 0.0
    assert math.isnan(by_go["GO:C"]["protst_text_score"])


def test_leakage_query_excluded_from_its_own_neighbours() -> None:
    # Q1 is also a reference (same accession) carrying GO:LEAK with cos 1.0. It
    # MUST be excluded from its own neighbour set, so GO:LEAK never gets stamped
    # and the vote comes only from the genuine neighbour R2.
    bank = (
        ["Q1", "R2"],
        np.vstack([_unit([1, 0, 0]), _unit([1, 0, 0])]),
        {"Q1": {"GO:LEAK"}, "R2": {"GO:A"}},
    )
    preds = [_pred("Q1", "GO:A", 1), _pred("Q1", "GO:LEAK", 9)]
    _run(preds, query_emb={"Q1": _unit([1, 0, 0])}, bank=bank)
    by_go = {r["go_id"]: r for r in preds}

    assert by_go["GO:A"]["protst_present"] == 1.0
    assert by_go["GO:A"]["protst_vote_fraction"] == pytest.approx(1.0)  # 1 eligible ref
    # The self-hit's own term drew no (non-self) vote -> not present, score NaN.
    assert by_go["GO:LEAK"]["protst_present"] == 0.0
    assert math.isnan(by_go["GO:LEAK"]["protst_text_score"])


def test_uncovered_query_keeps_nan_declared_absent() -> None:
    # Q2 has NO ProtST embedding in the bank -> every column stays at the NaN
    # declared-absent default (ADR-D45), never a stamped zero.
    bank = (["R1"], np.vstack([_unit([1, 0, 0])]), {"R1": {"GO:A"}})
    preds = [_pred("Q2", "GO:A", 1)]
    _run(preds, query_emb={}, bank=bank)  # empty coverage
    rec = preds[0]
    assert math.isnan(rec["protst_text_score"])
    assert math.isnan(rec["protst_vote_fraction"])
    assert math.isnan(rec["protst_present"])


def test_go_id_resolved_from_int_when_string_absent() -> None:
    # A candidate carrying only ``go_term_id`` (no stamped ``go_id``) is resolved
    # via the int->go_id map, mirroring apply_self_prior / apply_association.
    bank = (["R1"], np.vstack([_unit([1, 0, 0])]), {"R1": {"GO:A"}})
    preds = [
        {
            "protein_accession": "Q1",
            "go_term_id": 7,
            "protst_text_score": float("nan"),
            "protst_vote_fraction": float("nan"),
            "protst_present": float("nan"),
        }
    ]
    _run(preds, query_emb={"Q1": _unit([1, 0, 0])}, bank=bank, go_id_by_int={7: "GO:A"})
    assert preds[0]["protst_present"] == 1.0
    assert preds[0]["protst_text_score"] == pytest.approx(1.0)


def test_leaf_default_fields_are_nan() -> None:
    from protea.core._leaf_record_builder import _protst_default_fields

    fields = _protst_default_fields()
    assert set(fields) == {"protst_text_score", "protst_vote_fraction", "protst_present"}
    for col, val in fields.items():
        assert isinstance(val, float) and math.isnan(val), f"{col} must default to NaN"


def test_empty_predictions_is_a_noop() -> None:
    # No records -> no DB access, no error.
    pt.apply_protst_text(MagicMock(), [], MagicMock())
