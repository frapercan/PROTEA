"""Unit tests for the GOA self-prior helper.

Targets ``apps/method_runtime/self_prior.py``. Covers row building, the
non-experimental evidence filter, NOT-qualifier dropping, max-combine with
neighbour transfer, graceful degradation, and the load-bearing leakage
guardrail: no experimental / IC / TAS code can ever enter the self-prior.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Make ``apps/method_runtime`` importable for these tests.
_REPO_ROOT = Path(__file__).resolve().parents[1]
_RUNTIME_DIR = _REPO_ROOT / "apps" / "method_runtime"
if str(_RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(_RUNTIME_DIR))

from self_prior import (  # noqa: E402
    FORBIDDEN_CODES,
    NONEXP_CODES,
    build_self_prior_rows,
    combine_with_self_prior,
)


def _score_of(pred: dict) -> float:
    """Mirror the runtime's ``_score_for_output`` for combine tests."""
    distance = float(pred.get("min_distance", pred.get("distance", 1.0)))
    return max(0.0, 1.0 - distance)


def test_build_self_prior_keeps_nonexp_codes() -> None:
    annotations = {
        "Q1": [
            {"go_term_id": 10, "qualifier": "enables", "evidence_code": "IEA"},
            {"go_term_id": 11, "qualifier": "involved_in", "evidence_code": "ISS"},
        ],
    }
    rows = build_self_prior_rows(annotations, ["Q1"], self_score=1.0)
    assert {r["go_term_id"] for r in rows} == {10, 11}
    assert all(r["self_prior_score"] == 1.0 for r in rows)
    assert all(r["protein_accession"] == "Q1" for r in rows)


def test_build_self_prior_drops_experimental_codes() -> None:
    annotations = {
        "Q1": [
            {"go_term_id": 1, "qualifier": "enables", "evidence_code": "IDA"},
            {"go_term_id": 2, "qualifier": "enables", "evidence_code": "EXP"},
            {"go_term_id": 3, "qualifier": "enables", "evidence_code": "TAS"},
            {"go_term_id": 4, "qualifier": "enables", "evidence_code": "IC"},
            {"go_term_id": 5, "qualifier": "enables", "evidence_code": "IEA"},
        ],
    }
    rows = build_self_prior_rows(annotations, ["Q1"])
    assert {r["go_term_id"] for r in rows} == {5}


def test_leakage_guardrail_no_forbidden_code_ever_passes() -> None:
    """Every forbidden evidence code must be rejected by the filter."""
    # The allow-set and forbid-set must be disjoint by construction.
    assert NONEXP_CODES.isdisjoint(FORBIDDEN_CODES)
    for code in FORBIDDEN_CODES:
        annotations = {
            "Q1": [{"go_term_id": 1, "qualifier": "enables", "evidence_code": code}],
        }
        rows = build_self_prior_rows(annotations, ["Q1"])
        assert rows == [], f"forbidden code {code} leaked into self-prior"
    # Case-insensitivity: a lowercase experimental code must also be rejected.
    annotations = {"Q1": [{"go_term_id": 1, "qualifier": "", "evidence_code": "ida"}]}
    assert build_self_prior_rows(annotations, ["Q1"]) == []


def test_build_self_prior_drops_not_qualified() -> None:
    annotations = {
        "Q1": [
            {"go_term_id": 1, "qualifier": "NOT|enables", "evidence_code": "IEA"},
            {"go_term_id": 2, "qualifier": "enables", "evidence_code": "IEA"},
        ],
    }
    rows = build_self_prior_rows(annotations, ["Q1"])
    assert {r["go_term_id"] for r in rows} == {2}


def test_build_self_prior_dedups_per_term() -> None:
    annotations = {
        "Q1": [
            {"go_term_id": 1, "qualifier": "enables", "evidence_code": "IEA"},
            {"go_term_id": 1, "qualifier": "enables", "evidence_code": "ISS"},
        ],
    }
    rows = build_self_prior_rows(annotations, ["Q1"])
    assert len(rows) == 1


def test_build_self_prior_only_for_requested_targets() -> None:
    annotations = {
        "Q1": [{"go_term_id": 1, "qualifier": "", "evidence_code": "IEA"}],
        "Q2": [{"go_term_id": 2, "qualifier": "", "evidence_code": "IEA"}],
    }
    rows = build_self_prior_rows(annotations, ["Q1"])
    assert {r["protein_accession"] for r in rows} == {"Q1"}


def test_combine_self_prior_dominates_neighbour() -> None:
    predictions = [
        {"protein_accession": "Q1", "go_term_id": 1, "min_distance": 0.1, "distance": 0.1},
    ]
    self_rows = [
        {"protein_accession": "Q1", "go_term_id": 1, "self_prior_score": 1.0},
    ]
    out = combine_with_self_prior(
        predictions, self_rows, neighbour_scale=0.95, score_of=_score_of,
    )
    assert len(out) == 1
    # neighbour 0.9 * 0.95 = 0.855 < self 1.0 -> self wins
    assert out[0]["reranker_score"] == 1.0
    assert out[0]["self_prior"] is True


def test_combine_scales_neighbour_when_no_self() -> None:
    predictions = [
        {"protein_accession": "Q1", "go_term_id": 2, "min_distance": 0.2, "distance": 0.2},
    ]
    out = combine_with_self_prior(predictions, [], neighbour_scale=0.95, score_of=_score_of)
    assert len(out) == 1
    # (1 - 0.2) * 0.95 = 0.76
    assert out[0]["reranker_score"] == 0.76
    assert "self_prior" not in out[0]


def test_combine_adds_self_only_candidates() -> None:
    predictions = [
        {"protein_accession": "Q1", "go_term_id": 1, "min_distance": 0.1, "distance": 0.1},
    ]
    self_rows = [
        {"protein_accession": "Q1", "go_term_id": 99, "self_prior_score": 1.0},
    ]
    out = combine_with_self_prior(
        predictions, self_rows, neighbour_scale=0.95, score_of=_score_of,
    )
    keys = {(r["protein_accession"], r["go_term_id"]) for r in out}
    assert keys == {("Q1", 1), ("Q1", 99)}


def test_combine_graceful_when_no_self_rows() -> None:
    predictions = [
        {"protein_accession": "Q1", "go_term_id": 1, "min_distance": 0.3, "distance": 0.3},
    ]
    out = combine_with_self_prior(predictions, [], neighbour_scale=1.0, score_of=_score_of)
    assert out[0]["reranker_score"] == _score_of(predictions[0])
