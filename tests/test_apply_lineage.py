"""Unit tests for the serve-side GO-DAG lineage producer.

Targets ``apply_lineage`` in
``protea.core.operations.predict_go_terms._post_knn_pipeline``. It is the
serve-path analogue of the export-time lineage producer
(``_KnnTransferRunner._apply_lineage_features``): gated by the
``compute_lineage_features`` payload flag, it fills the four ``lineage_*``
columns on every candidate from the GO DAG so a lineage-aware booster sees
the same feature it trained on (no served-zero vs trained-real skew).

The DB-bound pieces (annotation loader, int->go_id resolver, parent-map
loader) are mocked so the test exercises only the wiring + the per-candidate
math, which it pins against the stock ``protea_method.lineage`` library.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

from protea.core.operations.predict_go_terms import _post_knn_pipeline as pkp

_LINEAGE_COLS = (
    "lineage_is_ancestor_of_known",
    "lineage_is_descendant_of_known",
    "lineage_ancestor_of_count",
    "lineage_descendant_of_count",
)

# Small DAG: 1 and 2 are children of root 10; 3 is child of root 20.
_PARENT_MAP: dict[str, set[str]] = {
    "GO:0000001": {"GO:0000010"},
    "GO:0000002": {"GO:0000010"},
    "GO:0000003": {"GO:0000020"},
}
_GO_ID_BY_INT = {
    1: "GO:0000001",
    2: "GO:0000002",
    3: "GO:0000003",
    10: "GO:0000010",
    20: "GO:0000020",
}
_ASPECT_BY_GO = {g: "P" for g in _GO_ID_BY_INT.values()}


def _emit(*_args: Any, **_kwargs: Any) -> None:
    return None


def _run(prediction_dicts: list[dict[str, Any]], *, annotations: dict[str, Any]):
    """Drive ``apply_lineage`` with all DB calls mocked."""
    op = MagicMock()
    op._load_annotations_for.return_value = annotations
    with (
        patch.object(pkp, "_load_go_id_and_aspect", return_value=(_GO_ID_BY_INT, _ASPECT_BY_GO)),
        patch(
            "protea.core.feature_enricher.load_parent_map",
            return_value=_PARENT_MAP,
        ),
    ):
        pkp.apply_lineage(op, MagicMock(), MagicMock(), MagicMock(), prediction_dicts, _emit)
    return prediction_dicts


def _cand(acc: str, gtid: int, **extra: Any) -> dict[str, Any]:
    return {"protein_accession": acc, "go_term_id": gtid, **extra}


def test_no_known_terms_fills_well_defined_zeros() -> None:
    """A query with no experimental known terms => all four columns 0.0."""
    # Only a non-experimental annotation => no known term.
    annotations = {"Q1": [{"go_term_id": 10, "evidence_code": "IEA"}]}
    out = _run([_cand("Q1", 1)], annotations=annotations)
    for col in _LINEAGE_COLS:
        assert col in out[0]
        assert out[0][col] == 0.0


def test_descendant_of_known_signal_fires() -> None:
    """Candidate that is a descendant of a known root gets the descendant flag.

    Q1 knows the root GO:0000010 (experimental). Candidate GO:0000001 is a
    child of that root, so its ancestor closure contains the known term =>
    ``lineage_is_descendant_of_known = 1.0`` and count >= 1.
    """
    annotations = {"Q1": [{"go_term_id": 10, "evidence_code": "IDA"}]}
    out = _run([_cand("Q1", 1)], annotations=annotations)
    assert out[0]["lineage_is_descendant_of_known"] == 1.0
    assert out[0]["lineage_descendant_of_count"] >= 1.0
    assert out[0]["lineage_is_ancestor_of_known"] == 0.0


def test_ancestor_of_known_signal_fires() -> None:
    """Candidate that is an ancestor of a known leaf gets the ancestor flag.

    Q1 knows the leaf GO:0000001 (child of GO:0000010). Candidate GO:0000010
    is its ancestor, so ``lineage_is_ancestor_of_known = 1.0``.
    """
    annotations = {"Q1": [{"go_term_id": 1, "evidence_code": "IDA"}]}
    out = _run([_cand("Q1", 10)], annotations=annotations)
    assert out[0]["lineage_is_ancestor_of_known"] == 1.0
    assert out[0]["lineage_ancestor_of_count"] >= 1.0


def test_candidate_go_id_stamped_from_int_when_missing() -> None:
    """Records lacking ``go_id`` get it stamped from the int resolver."""
    rec = _cand("Q1", 1)
    assert "go_id" not in rec
    out = _run([rec], annotations={"Q1": [{"go_term_id": 10, "evidence_code": "IDA"}]})
    assert out[0]["go_id"] == "GO:0000001"


def test_parity_with_stock_library() -> None:
    """Served lineage values match the stock ``compute_lineage_features``.

    Recomputes the four columns from scratch with the UNMODIFIED library
    function (grouped per protein, the original call boundary) and asserts
    every value matches exactly (same float, same type).
    """
    from protea_method.lineage import compute_lineage_features

    annotations = {
        "Q1": [{"go_term_id": 10, "evidence_code": "IDA"}],  # knows root 10
        "Q2": [
            {"go_term_id": 1, "evidence_code": "EXP"},  # knows leaf 1
            {"go_term_id": 20, "evidence_code": "IEA"},  # non-exp -> dropped
        ],
    }
    preds = [
        _cand("Q1", 1),
        _cand("Q1", 2),
        _cand("Q1", 10),
        _cand("Q2", 1),
        _cand("Q2", 10),
        _cand("Q2", 3),  # unrelated branch
    ]
    out = _run(preds, annotations=annotations)

    parents_list = {c: list(ps) for c, ps in _PARENT_MAP.items()}
    known_by_protein = {"Q1": {"GO:0000010"}, "Q2": {"GO:0000001"}}
    by_protein: dict[str, list[dict[str, Any]]] = {}
    for rec in out:
        by_protein.setdefault(rec["protein_accession"], []).append(rec)

    for prot, recs in by_protein.items():
        clones = [dict(r) for r in recs]
        compute_lineage_features(
            clones,
            parents=parents_list,
            known_by_protein={prot: known_by_protein.get(prot, set())},
        )
        for served, lib in zip(recs, clones, strict=True):
            for col in _LINEAGE_COLS:
                assert served[col] == lib[col], (
                    f"lineage mismatch {prot}/{served['go_id']} {col}: "
                    f"served={served[col]!r} library={lib[col]!r}"
                )
                assert type(served[col]) is type(lib[col])
