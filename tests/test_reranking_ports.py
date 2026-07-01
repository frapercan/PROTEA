"""Unit tests for the meta-reranker foundation (MR-0 + MR-1).

Covers the two ports and their reference impls, the dict-backed registry, the
``Category`` domain enum, and the four MR-1 scorer adapters. The adapter tests
include a producer-parity check: the actual ``apply_self_prior`` /
``apply_association`` / ``apply_classifier`` producers (DB mocked) stamp signals
on synthetic prediction dicts, and the adapter is asserted to read back exactly
those producer-written values through the ``EvidenceScorer.score`` contract.

Nothing here touches a live DB, GPU, or the predict path; the producers are
exercised with mocked sessions exactly as ``test_apply_*`` already do.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from protea.core.classifier_producer import ClassifierPrediction
from protea.core.domain.category import CATEGORY_CODES, Category
from protea.core.operations.predict_go_terms import _category_dispatch
from protea.core.operations.predict_go_terms import _post_knn_pipeline as pkp
from protea.core.reranking import (
    AlignmentScorer,
    AssociationScorer,
    ClassifierScorer,
    Combiner,
    EvidenceScorer,
    InterproScorer,
    KnnSimilarityScorer,
    LabelEmbeddingScorer,
    LinearCombiner,
    PassThroughCombiner,
    QueryContext,
    ScorerRegistry,
    SelfPriorScorer,
    TaxonomyScorer,
    TermFrequencyScorer,
    default_scorer_registry,
)

NK = Category.NO_KNOWLEDGE
LK = Category.LIMITED_KNOWLEDGE
PK = Category.PARTIAL_KNOWLEDGE


def _emit(*_args, **_kwargs) -> None:
    return None


def _ctx(candidates: list[dict]) -> QueryContext:
    return QueryContext("Q1", candidates)


# --------------------------------------------------------------------------- #
# Category enum
# --------------------------------------------------------------------------- #


def test_category_codes_match_dispatch_literals() -> None:
    assert CATEGORY_CODES == _category_dispatch.CATEGORIES
    assert Category.NO_KNOWLEDGE.code == _category_dispatch.CATEGORY_NK
    assert Category.LIMITED_KNOWLEDGE.code == _category_dispatch.CATEGORY_LK
    assert Category.PARTIAL_KNOWLEDGE.code == _category_dispatch.CATEGORY_PK


def test_category_round_trips_through_code() -> None:
    for cat in Category:
        assert Category.from_code(cat.code) is cat


def test_category_from_unknown_code_raises() -> None:
    with pytest.raises(KeyError):
        Category.from_code("ZZ")


# --------------------------------------------------------------------------- #
# Registry (register / resolve)
# --------------------------------------------------------------------------- #


def test_registry_register_and_get() -> None:
    reg = ScorerRegistry()
    scorer = KnnSimilarityScorer()
    reg.register(scorer)
    assert reg.get("knn_similarity") is scorer
    assert "knn_similarity" in reg
    assert len(reg) == 1


def test_registry_rejects_duplicate_name() -> None:
    reg = ScorerRegistry()
    reg.register(KnnSimilarityScorer())
    with pytest.raises(ValueError, match="already registered"):
        reg.register(KnnSimilarityScorer())


def test_registry_unknown_name_raises() -> None:
    reg = ScorerRegistry()
    with pytest.raises(KeyError, match="Unknown scorer"):
        reg.get("nope")


def test_default_registry_order_and_membership() -> None:
    reg = default_scorer_registry()
    # canonical score-vector order: base evidence first, then the original four.
    assert reg.names() == [
        "alignment",
        "taxonomy",
        "label_embedding",
        "interpro",
        "term_frequency",
        "knn_similarity",
        "classifier",
        "self_prior",
        "association",
    ]
    assert len(reg) == 9


def test_registry_for_category_excludes_priors_on_nk() -> None:
    reg = default_scorer_registry()
    nk_names = [s.name for s in reg.for_category(NK)]
    pk_names = [s.name for s in reg.for_category(PK)]
    # base evidence + KNN + classifier apply everywhere; priors are NOT
    # available to NK proteins.
    base_plus_seq = [
        "alignment",
        "taxonomy",
        "label_embedding",
        "interpro",
        "term_frequency",
        "knn_similarity",
        "classifier",
    ]
    assert nk_names == base_plus_seq
    assert pk_names == [*base_plus_seq, "self_prior", "association"]


def test_scorers_satisfy_the_port_protocol() -> None:
    for scorer in default_scorer_registry():
        assert isinstance(scorer, EvidenceScorer)


# --------------------------------------------------------------------------- #
# applies_to declarations
# --------------------------------------------------------------------------- #


def test_applies_to_declarations() -> None:
    # base evidence + KNN + classifier propose from sequence / embeddings /
    # domains for every protein.
    assert AlignmentScorer().applies_to == {NK, LK, PK}
    assert TaxonomyScorer().applies_to == {NK, LK, PK}
    assert LabelEmbeddingScorer().applies_to == {NK, LK, PK}
    assert InterproScorer().applies_to == {NK, LK, PK}
    assert TermFrequencyScorer().applies_to == {NK, LK, PK}
    assert KnnSimilarityScorer().applies_to == {NK, LK, PK}
    assert ClassifierScorer().applies_to == {NK, LK, PK}
    # priors need the query's own known terms.
    assert SelfPriorScorer().applies_to == {LK, PK}
    assert AssociationScorer().applies_to == {LK, PK}


# --------------------------------------------------------------------------- #
# Base-evidence adapters: read their stored key, coerce, omit on missing / NaN
# --------------------------------------------------------------------------- #


def test_alignment_scorer_reads_sw_key() -> None:
    cands = [
        {"go_id": "GO:0000001", "alignment_score_sw": 123.5},
        {"go_id": "GO:0000002", "alignment_score_sw": float("nan")},
        {"go_id": "GO:0000003"},  # key missing
        {"alignment_score_sw": 9.0},  # no go_id
    ]
    out = AlignmentScorer().score(_ctx(cands), cands)
    assert out == {"GO:0000001": pytest.approx(123.5)}


def test_taxonomy_scorer_reads_distance_as_is() -> None:
    cands = [
        {"go_id": "GO:0000001", "taxonomic_distance": 7.0},
        {"go_id": "GO:0000002", "taxonomic_distance": float("nan")},
        {"go_id": "GO:0000003"},
    ]
    out = TaxonomyScorer().score(_ctx(cands), cands)
    # emitted as-is (no inversion); the combiner learns the sign.
    assert out == {"GO:0000001": pytest.approx(7.0)}


def test_label_embedding_scorer_reads_anc2vec_maxcos() -> None:
    cands = [
        {"go_id": "GO:0000001", "anc2vec_neighbor_maxcos": 0.83},
        {"go_id": "GO:0000002", "anc2vec_neighbor_maxcos": float("nan")},
        {"go_id": "GO:0000003"},
    ]
    out = LabelEmbeddingScorer().score(_ctx(cands), cands)
    assert out == {"GO:0000001": pytest.approx(0.83)}


def test_interpro_scorer_reads_interpro_score() -> None:
    cands = [
        {"go_id": "GO:0000001", "interpro_score": 4.2},
        {"go_id": "GO:0000002", "interpro_score": float("nan")},
        {"go_id": "GO:0000003"},
    ]
    out = InterproScorer().score(_ctx(cands), cands)
    assert out == {"GO:0000001": pytest.approx(4.2)}


def test_term_frequency_scorer_reads_go_term_frequency() -> None:
    cands = [
        {"go_id": "GO:0000001", "go_term_frequency": 0.015},
        {"go_id": "GO:0000002", "go_term_frequency": float("nan")},
        {"go_id": "GO:0000003"},
    ]
    out = TermFrequencyScorer().score(_ctx(cands), cands)
    assert out == {"GO:0000001": pytest.approx(0.015)}


def test_base_evidence_scorers_coerce_string_values() -> None:
    # the JSONB rehydration path can hand back stringified numbers.
    cands = [{"go_id": "GO:0000001", "alignment_score_sw": "42.0"}]
    assert AlignmentScorer().score(_ctx(cands), cands) == {"GO:0000001": pytest.approx(42.0)}


# --------------------------------------------------------------------------- #
# Adapter score() output shape: go_id-keyed, sparse, finite
# --------------------------------------------------------------------------- #


def test_scorers_key_on_go_id_string() -> None:
    cands = [
        {
            "go_id": "GO:0000001",
            "distance": 0.2,
            "classifier_score": 0.9,
            "self_prior_score": 1.0,
            "association_total": 0.4,
        },
        {"go_id": "GO:0000002", "distance": 0.5},
    ]
    knn = KnnSimilarityScorer().score(_ctx(cands), cands)
    assert set(knn) == {"GO:0000001", "GO:0000002"}
    assert knn["GO:0000001"] == pytest.approx(0.8)


def test_knn_scorer_folds_in_vote_fraction() -> None:
    cands = [{"go_id": "GO:0000001", "distance": 0.2, "neighbor_vote_fraction": 0.5}]
    out = KnnSimilarityScorer().score(_ctx(cands), cands)
    assert out["GO:0000001"] == pytest.approx(0.8 * 0.5)


def test_adapter_omits_missing_or_nan_signals() -> None:
    cands = [
        {"go_id": "GO:0000001", "classifier_score": float("nan")},
        {"go_id": "GO:0000002"},  # no classifier_score at all
        {"distance": 0.1},  # no go_id -> dropped even with a signal
    ]
    assert ClassifierScorer().score(_ctx(cands), cands) == {}
    assert KnnSimilarityScorer().score(_ctx(cands), cands) == {}


def test_prior_scorers_read_their_keys() -> None:
    cands = [{"go_id": "GO:0000001", "self_prior_score": 1.0, "association_total": 0.42}]
    assert SelfPriorScorer().score(_ctx(cands), cands) == {"GO:0000001": 1.0}
    assert AssociationScorer().score(_ctx(cands), cands) == {"GO:0000001": pytest.approx(0.42)}


# --------------------------------------------------------------------------- #
# Producer parity: the adapter reads back exactly what the producer wrote
# --------------------------------------------------------------------------- #


def test_self_prior_adapter_matches_producer() -> None:
    """``apply_self_prior`` marks an own-non-exp term; the adapter reads it back."""
    op = MagicMock()
    # query Q1 owns go_term_id 11 as a non-experimental annotation.
    op._load_annotations_for.return_value = {"Q1": [{"go_term_id": 11, "evidence_code": "IEA"}]}
    cands = [
        {"protein_accession": "Q1", "go_term_id": 11, "go_id": "GO:0000011"},
        {"protein_accession": "Q1", "go_term_id": 22, "go_id": "GO:0000022"},
    ]
    # apply_self_prior now resolves int ids -> snapshot-invariant go_id strings.
    with patch.object(
        pkp, "_load_go_id_and_aspect", return_value=({11: "GO:0000011", 22: "GO:0000022"}, {})
    ):
        pkp.apply_self_prior(op, MagicMock(), MagicMock(), ["Q1"], cands, _emit)

    out = SelfPriorScorer().score(_ctx(cands), cands)
    # only the owned term carries the producer's self_prior_score=1.0.
    assert out == {"GO:0000011": 1.0}
    assert cands[0]["self_prior_score"] == 1.0


def test_association_adapter_matches_producer() -> None:
    """``apply_association`` writes ``association_total``; the adapter reads it back."""
    op = MagicMock()
    op._load_annotations_for.return_value = {"Q1": [{"go_term_id": 100, "evidence_code": "EXP"}]}
    cands = [{"protein_accession": "Q1", "go_term_id": 11, "go_id": "GO:0000011"}]
    # known term 100 (aspect F) co-occurs with candidate GO:0000011 (aspect P):
    # P(t|k) = cooc(k,t) / freq(k) = 3 / 6 = 0.5, and it is cross-aspect.
    go_id_by_int = {100: "GO:0000100", 11: "GO:0000011"}
    aspect_by_go = {"GO:0000100": "F", "GO:0000011": "P"}
    cooc = {"GO:0000100": {"GO:0000011": 3}}
    freq = {"GO:0000100": 6}
    with (
        patch.object(
            pkp,
            "_load_go_id_and_aspect",
            return_value=(go_id_by_int, aspect_by_go),
        ),
        patch(
            "protea.core.operations.predict_go_terms._association_loader.load_cooccurrence_for_known",
            return_value=(cooc, freq),
        ),
    ):
        pkp.apply_association(op, MagicMock(), MagicMock(), ["Q1"], cands, _emit)

    out = AssociationScorer().score(_ctx(cands), cands)
    assert out == {"GO:0000011": pytest.approx(0.5)}
    assert cands[0]["association_total"] == pytest.approx(0.5)


def test_classifier_adapter_matches_producer() -> None:
    """``apply_classifier`` stamps ``classifier_score``; the adapter reads it back."""
    import numpy as np

    preds = [ClassifierPrediction("Q1", "GO:0000011", 0.9)]
    clf = MagicMock()
    clf.predict.return_value = preds
    knn_rec = {
        "protein_accession": "Q1",
        "go_term_id": 11,
        "go_id": "GO:0000011",
        "classifier_score": 0.0,
    }
    with (
        patch(
            "protea.core.classifier_producer.load_concat_features",
            return_value=(np.zeros((1, 8320), dtype=np.float32), ["Q1"]),
        ),
        patch("protea.core.classifier_producer.get_classifier", return_value=clf),
        patch(
            "protea.core.classifier_producer.resolve_go_term_ids",
            return_value={"GO:0000011": 11},
        ),
    ):
        from protea.core.operations.predict_go_terms._classifier import apply_classifier

        out_dicts = apply_classifier(MagicMock(), MagicMock(), ["Q1"], [knn_rec], _emit)

    out = ClassifierScorer().score(_ctx(out_dicts), out_dicts)
    assert out == {"GO:0000011": pytest.approx(0.9)}


# --------------------------------------------------------------------------- #
# Combiner interface + reference impls
# --------------------------------------------------------------------------- #


def test_combiners_satisfy_the_port_protocol() -> None:
    assert isinstance(PassThroughCombiner(), Combiner)
    assert isinstance(LinearCombiner([1.0]), Combiner)


def test_pass_through_combiner_returns_first_component() -> None:
    c = PassThroughCombiner()
    c.fit([[0.1, 0.2]], [1], NK)  # no-op, must not raise
    assert c.combine([0.7, 0.3, 0.9], NK) == pytest.approx(0.7)
    assert c.combine([], NK) == 0.0


def test_linear_combiner_weighted_sum() -> None:
    c = LinearCombiner([0.5, 0.25, 0.25])
    assert c.combine([1.0, 1.0, 1.0], LK) == pytest.approx(1.0)
    assert c.combine([0.8, 0.0, 0.4], LK) == pytest.approx(0.5)


def test_linear_combiner_per_category_weights() -> None:
    c = LinearCombiner({NK: [1.0, 0.0], PK: [0.0, 1.0]})
    assert c.combine([0.3, 0.9], NK) == pytest.approx(0.3)
    assert c.combine([0.3, 0.9], PK) == pytest.approx(0.9)
    # a category with no configured weights combines to 0.
    assert c.combine([0.3, 0.9], LK) == 0.0


def test_linear_combiner_truncates_shorter_vector() -> None:
    # priors absent for this category -> 2-long vector vs 4 weights.
    c = LinearCombiner([0.25, 0.25, 0.25, 0.25])
    assert c.combine([0.8, 0.8], NK) == pytest.approx(0.4)
