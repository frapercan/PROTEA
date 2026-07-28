"""Unit tests for the classifier candidate-merge (INT-4).

Targets ``apply_classifier`` / ``_merge_classifier_preds`` in
``protea.core.operations.predict_go_terms._classifier``. The producer
(model load + embedding fetch) is mocked so the test exercises only the union
merge: an existing ``(protein, go_term_id)`` gets ``classifier_score`` /
``classifier_present`` set; an unseen term becomes a NEW candidate dict with
the ``"classifier"`` ref marker; KNN candidates are never dropped.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from protea.core.classifier_producer import ClassifierPrediction
from protea.core.operations.predict_go_terms import _classifier as cl


def _emit(*_args, **_kwargs) -> None:
    return None


def _run(prediction_dicts, *, preds, gid_by_go):
    """Drive ``apply_classifier`` with the producer fully mocked."""
    import numpy as np

    clf = MagicMock()
    clf.predict.return_value = preds
    with (
        patch.object(cl, "_emit_classifier_done", wraps=cl._emit_classifier_done),
        patch(
            "protea.core.classifier_producer.load_concat_features",
            return_value=(np.zeros((1, 8320), dtype=np.float32), ["Q1"]),
        ),
        patch("protea.core.classifier_producer.get_classifier", return_value=clf),
        patch(
            "protea.core.classifier_producer.resolve_go_term_ids",
            return_value=gid_by_go,
        ),
    ):
        return cl.apply_classifier(MagicMock(), MagicMock(), ["Q1"], prediction_dicts, _emit)


def test_existing_candidate_gets_classifier_score_set() -> None:
    preds = [ClassifierPrediction("Q1", "GO:0000001", 0.9)]
    rec = {
        "protein_accession": "Q1",
        "go_term_id": 11,
        "classifier_score": 0.0,
        "classifier_present": 0.0,
    }
    out = _run([rec], preds=preds, gid_by_go={"GO:0000001": 11})
    assert len(out) == 1
    assert out[0]["classifier_score"] == 0.9
    assert out[0]["classifier_present"] == 1.0


def test_new_term_becomes_a_classifier_candidate() -> None:
    preds = [ClassifierPrediction("Q1", "GO:0000002", 0.7)]
    knn_rec = {"protein_accession": "Q1", "go_term_id": 11, "classifier_score": 0.0}
    out = _run([knn_rec], preds=preds, gid_by_go={"GO:0000002": 22})
    # KNN candidate preserved + one new classifier candidate appended.
    assert len(out) == 2
    new = [r for r in out if r["go_term_id"] == 22][0]
    assert new["ref_protein_accession"] == "classifier"
    assert new["classifier_score"] == 0.7
    assert new["classifier_present"] == 1.0
    assert new["self_prior_score"] == 0.0
    assert new["go_id"] == "GO:0000002"


def test_term_not_in_snapshot_is_skipped() -> None:
    preds = [ClassifierPrediction("Q1", "GO:9999999", 0.8)]
    knn_rec = {"protein_accession": "Q1", "go_term_id": 11}
    out = _run([knn_rec], preds=preds, gid_by_go={})  # unresolved go_id
    assert len(out) == 1
    assert out[0]["go_term_id"] == 11


def test_no_valid_features_returns_unchanged() -> None:
    import numpy as np

    knn_rec = {"protein_accession": "Q1", "go_term_id": 11}
    with patch(
        "protea.core.classifier_producer.load_concat_features",
        return_value=(np.empty((0, 8320), np.float32), []),
    ):
        out = cl.apply_classifier(MagicMock(), MagicMock(), ["Q1"], [knn_rec], _emit)
    assert out == [knn_rec]


# --- Per-category composite routing delegation (serve knob) --------------------


def test_apply_classifier_delegates_to_composite_when_enabled() -> None:
    """With the knob on and a route_ctx supplied, delegate to the composite."""
    sentinel = [{"routed": True}]
    with (
        patch.object(cl, "classifier_route_by_category_enabled", return_value=True),
        patch.object(cl, "apply_classifier_composite", return_value=sentinel) as comp,
    ):
        out = cl.apply_classifier(
            MagicMock(),
            MagicMock(),
            ["Q1"],
            [{"protein_accession": "Q1", "go_term_id": 11}],
            _emit,
            route_ctx=MagicMock(),
        )
    assert out is sentinel
    comp.assert_called_once()


def test_apply_classifier_single_path_when_route_ctx_absent() -> None:
    """No route_ctx -> single global-impl path even if the knob is on."""
    with (
        patch.object(cl, "classifier_route_by_category_enabled", return_value=True),
        patch.object(cl, "apply_classifier_composite") as comp,
    ):
        out = _run(
            [{"protein_accession": "Q1", "go_term_id": 11}],
            preds=[ClassifierPrediction("Q1", "GO:0000001", 0.9)],
            gid_by_go={"GO:0000001": 11},
        )
    comp.assert_not_called()
    assert out[0]["classifier_present"] == 1.0
