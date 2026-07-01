"""Unit tests for the per-category composite classifier routing.

Targets ``protea.core.operations.predict_go_terms._classifier``: the opt-in seam
(``serve.classifier_impl_by_category``) that routes classifier candidates
PK -> two-tower sparse, NK / LK -> M2 anc2vec, reproducing the composite champion
pool. The producer / DB access is mocked so the tests exercise only the routing
+ merge, not the model load.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from protea.core.classifier_producer import ClassifierPrediction
from protea.core.operations.predict_go_terms import _classifier as cl


def _emit(*_args, **_kwargs) -> None:
    return None


def test_route_composite_preds_splits_pk_to_two_tower() -> None:
    """PK cells come from the two-tower head; NK / LK cells from M2."""
    known_aspects = {"Q1": frozenset({"F"})}  # Q1 knows aspect F (MFO)
    m2_preds = [
        ClassifierPrediction("Q1", "GO:lk", 0.5),  # aspect P -> LK -> keep M2
        ClassifierPrediction("Q1", "GO:pk", 0.6),  # aspect F -> PK -> drop M2
    ]
    tt_preds = [
        ClassifierPrediction("Q1", "GO:pk", 0.9),  # aspect F -> PK -> keep TT
        ClassifierPrediction("Q1", "GO:ttlk", 0.7),  # aspect P -> LK -> drop TT
    ]
    gid_by_go = {"GO:lk": 1, "GO:pk": 2, "GO:ttlk": 3}
    aspect_by_gid = {1: "P", 2: "F", 3: "P"}

    routed = cl._route_composite_preds(m2_preds, tt_preds, known_aspects, gid_by_go, aspect_by_gid)
    assert {(pr.go_id, pr.score) for pr in routed} == {("GO:lk", 0.5), ("GO:pk", 0.9)}


def test_route_composite_preds_nk_protein_is_all_m2() -> None:
    """An NK protein (no known aspects) keeps every M2 pred and no TT pred."""
    m2_preds = [ClassifierPrediction("Q1", "GO:a", 0.5)]
    tt_preds = [ClassifierPrediction("Q1", "GO:a", 0.9)]
    routed = cl._route_composite_preds(m2_preds, tt_preds, {}, {"GO:a": 1}, {1: "F"})
    assert [(pr.go_id, pr.score) for pr in routed] == [("GO:a", 0.5)]


def test_route_composite_preds_unresolved_or_null_aspect_is_not_pk() -> None:
    """A candidate whose go_id is unresolved or aspect-less is never PK (goes M2)."""
    known_aspects = {"Q1": frozenset({"F"})}
    m2_preds = [
        ClassifierPrediction("Q1", "GO:unresolved", 0.5),  # not in gid_by_go
        ClassifierPrediction("Q1", "GO:noaspect", 0.4),  # aspect ""
    ]
    tt_preds = [ClassifierPrediction("Q1", "GO:unresolved", 0.9)]
    gid_by_go = {"GO:noaspect": 7}
    aspect_by_gid = {7: ""}
    routed = cl._route_composite_preds(m2_preds, tt_preds, known_aspects, gid_by_go, aspect_by_gid)
    assert {pr.go_id for pr in routed} == {"GO:unresolved", "GO:noaspect"}


def test_apply_classifier_composite_end_to_end() -> None:
    """Composite path routes PK->TT / LK->M2 and merges into the KNN list."""
    m2 = [ClassifierPrediction("Q1", "GO:lk", 0.5)]
    tt = [ClassifierPrediction("Q1", "GO:pk", 0.9)]

    def _pred(_session, _accs, impl):
        return m2 if impl == "m2" else tt

    with (
        patch.object(cl, "_predict_for_impl", side_effect=_pred),
        patch(
            "protea.core.operations.predict_go_terms._category_dispatch._own_exp_for",
            return_value={"Q1": {2}},
        ),
        patch(
            "protea.core.operations.predict_go_terms._category_dispatch._known_aspects_by_protein",
            return_value={"Q1": frozenset({"F"})},
        ),
        patch(
            "protea.core.classifier_producer.resolve_go_term_ids",
            return_value={"GO:lk": 1, "GO:pk": 2},
        ),
        patch(
            "protea.core.operations.predict_go_terms._post_knn_pipeline._load_known_aspects",
            return_value={1: "P", 2: "F"},
        ),
    ):
        out = cl.apply_classifier_composite(
            MagicMock(), MagicMock(), MagicMock(), ["Q1"], [], _emit
        )
    # LK candidate (GO:lk, aspect P) from M2 + PK candidate (GO:pk, aspect F) from TT.
    assert {r["go_id"] for r in out} == {"GO:lk", "GO:pk"}
    pk_rec = [r for r in out if r["go_id"] == "GO:pk"][0]
    assert pk_rec["classifier_score"] == 0.9
    assert pk_rec["classifier_present"] == 1.0


def test_route_by_category_enabled_reads_serve_tuning() -> None:
    """The gate reflects serve.classifier_impl_by_category (default False)."""
    from protea.config.tuning import get_tuning

    get_tuning.cache_clear()
    assert cl.classifier_route_by_category_enabled() is False
    with patch("protea.config.tuning.get_tuning") as gt:
        gt.return_value.serve.classifier_impl_by_category = True
        assert cl.classifier_route_by_category_enabled() is True
