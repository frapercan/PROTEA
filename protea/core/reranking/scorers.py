"""MR-1 scorer adapters: the existing producers exposed as evidence scorers.

Each adapter wraps an ALREADY-COMPUTED per-candidate signal as an
:class:`EvidenceScorer` emitting one score per candidate keyed on ``go_id``. The
adapters do NOT reimplement any producer: they read the exact values the live
producers stamp on each prediction dict (the ``GOPrediction`` columns / the keys
the candidate dict already carries), namely:

Base sequence / taxonomy / domain / label-embedding evidence (applies to all):

* alignment            <- ``alignment_score_sw`` (Smith-Waterman local alignment, higher is stronger)
* taxonomy             <- ``taxonomic_distance`` (lower is closer; emitted as-is, the combiner learns the sign)
* label_embedding      <- ``anc2vec_neighbor_maxcos`` (GO label-embedding KNN cosine)
* interpro             <- ``interpro_score`` (InterPro domain-signature evidence)
* term_frequency       <- ``go_term_frequency`` (base-rate / calibration signal)

The original four MR-1 producer signals:

* KNN similarity       <- ``distance`` (cosine) + ``neighbor_vote_fraction``
* M2 classifier        <- ``classifier_score`` (``apply_classifier`` / ``classifier_producer``)
* self_prior           <- ``self_prior_score`` (``apply_self_prior``)
* association          <- ``association_total`` (``apply_association``, snapshot-agnostic per #644)

Because those keys are persisted on the typed ``GOPrediction`` columns (the
signal-store code-switch moved the LAFA scalars off the ``features`` JSONB
blob), an adapter reads identically whether the candidate dict came straight
off the live predict path or was rehydrated from the typed columns for the
eval / combiner path (MR-2). The adapters are thin by design: the producer owns
the computation, the adapter owns only the port contract (``name``,
``applies_to``, ``score``).

``applies_to`` follows the CAFA evidence model: the base-evidence scorers
(alignment, taxonomy, label_embedding, interpro, term_frequency), KNN, and the
classifier all propose from sequence / embeddings / domains for every protein, so
they apply to NK / LK / PK; the two priors (``self_prior``, ``association``) need
the query's own pre-cutoff known terms, which NK proteins by definition lack, so
they apply to LK / PK only. This base-evidence set recovers the sequence /
taxonomy / label-embedding signals the priors-plus-classifier-plus-KNN vector
omitted (the omission that lost NK / LK in the combiner).
"""

from __future__ import annotations

from typing import Any

from protea.core.domain.category import Category
from protea.core.reranking.ports import Candidate, QueryContext
from protea.core.reranking.registry import ScorerRegistry

#: Categories every protein has (sequence / embedding evidence always exists).
_ALL_CATEGORIES = {Category.NO_KNOWLEDGE, Category.LIMITED_KNOWLEDGE, Category.PARTIAL_KNOWLEDGE}
#: Categories with pre-cutoff known terms (priors are undefined on NK).
_KNOWN_CATEGORIES = {Category.LIMITED_KNOWLEDGE, Category.PARTIAL_KNOWLEDGE}


def _as_float(value: Any) -> float | None:
    """Coerce a stored signal to a finite float, or ``None`` if missing / NaN.

    Producers leave a signal absent (key missing) or NaN-filled when they have
    no evidence for a candidate; both map to ``None`` so the adapter omits the
    candidate from its sparse output rather than emitting a spurious zero.
    """
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if f != f:  # NaN
        return None
    return f


class _KeyedScorer:
    """Base adapter: emit ``{go_id: f(candidate)}`` from a stored signal key.

    Subclasses set ``name`` / ``applies_to`` and override :meth:`_value`. The
    candidate key is the ``go_id`` STRING; a candidate whose ``go_id`` is
    missing, or for which :meth:`_value` returns ``None``, is omitted (sparse
    output, the combiner zero-fills).
    """

    name: str
    applies_to: set[Category]

    def _value(self, candidate: Candidate) -> float | None:
        raise NotImplementedError

    def score(self, query_ctx: QueryContext, candidates: list[Candidate]) -> dict[str, float]:
        out: dict[str, float] = {}
        for cand in candidates:
            go_id = cand.get("go_id")
            if not go_id:
                continue
            value = self._value(cand)
            if value is not None:
                out[go_id] = value
        return out


class AlignmentScorer(_KeyedScorer):
    """Sequence-homology evidence from the stored Smith-Waterman score.

    Reads the per-candidate ``alignment_score_sw`` (Smith-Waterman local
    alignment score against the carrying neighbour, the strongest
    sequence-homology signal). Higher is stronger. Applies to every category
    (homology is defined from sequence alone, no known terms needed).
    """

    name = "alignment"
    applies_to = _ALL_CATEGORIES

    def _value(self, candidate: Candidate) -> float | None:
        return _as_float(candidate.get("alignment_score_sw"))


class TaxonomyScorer(_KeyedScorer):
    """Taxonomic-proximity evidence from the stored ``taxonomic_distance``.

    Reads the per-candidate ``taxonomic_distance`` (lower is closer). The raw
    value is emitted as-is (no inversion): the combiner learns the sign, so the
    adapter stays a thin read. Applies to every category.
    """

    name = "taxonomy"
    applies_to = _ALL_CATEGORIES

    def _value(self, candidate: Candidate) -> float | None:
        return _as_float(candidate.get("taxonomic_distance"))


class LabelEmbeddingScorer(_KeyedScorer):
    """GO label-embedding evidence from the stored anc2vec neighbour cosine.

    Reads the per-candidate ``anc2vec_neighbor_maxcos`` (the max cosine of the
    candidate term against the neighbour-carried terms in anc2vec GO
    label-embedding space). Higher is stronger. Applies to every category.
    """

    name = "label_embedding"
    applies_to = _ALL_CATEGORIES

    def _value(self, candidate: Candidate) -> float | None:
        return _as_float(candidate.get("anc2vec_neighbor_maxcos"))


class InterproScorer(_KeyedScorer):
    """InterPro domain-signature evidence from the stored ``interpro_score``.

    Reads the per-candidate ``interpro_score`` (domain-signature support for the
    candidate term). Higher is stronger. Applies to every category (domain
    signatures are defined from sequence alone, no known terms needed).
    """

    name = "interpro"
    applies_to = _ALL_CATEGORIES

    def _value(self, candidate: Candidate) -> float | None:
        return _as_float(candidate.get("interpro_score"))


class TermFrequencyScorer(_KeyedScorer):
    """Base-rate / calibration evidence from the stored ``go_term_frequency``.

    Reads the per-candidate ``go_term_frequency`` (how common the candidate term
    is in the reference pool). It is a base-rate / calibration signal the
    combiner uses to discount ubiquitous terms; emitted as-is so the combiner
    learns the sign. Applies to every category.
    """

    name = "term_frequency"
    applies_to = _ALL_CATEGORIES

    def _value(self, candidate: Candidate) -> float | None:
        return _as_float(candidate.get("go_term_frequency"))


class KnnSimilarityScorer(_KeyedScorer):
    """KNN neighbour-similarity evidence from the stored ``distance`` / vote signals.

    The KNN producer stamps a cosine ``distance`` (lower is closer) and a
    ``neighbor_vote_fraction`` on each neighbour-carried candidate. This adapter
    turns them into one bounded similarity score per candidate: cosine
    similarity ``1 - distance`` combined multiplicatively with the vote fraction
    when present (a term voted by more neighbours is stronger), so the output is
    monotone in "closer + more votes" exactly as the KNN signals intend. No KNN
    recomputation: the values are the ones the producer already wrote.
    """

    name = "knn_similarity"
    applies_to = _ALL_CATEGORIES

    def _value(self, candidate: Candidate) -> float | None:
        distance = _as_float(candidate.get("distance"))
        if distance is None:
            return None
        similarity = 1.0 - distance
        vote = _as_float(candidate.get("neighbor_vote_fraction"))
        if vote is not None:
            similarity *= vote
        return similarity


class ClassifierScorer(_KeyedScorer):
    """M2 full-vocabulary classifier evidence (``classifier_producer`` / ``apply_classifier``).

    Reads the per-candidate ``classifier_score`` the seed-averaged M2 anc2vec
    classifier emits. Applies to every category (the classifier proposes from
    embeddings alone, no known terms needed).
    """

    name = "classifier"
    applies_to = _ALL_CATEGORIES

    def _value(self, candidate: Candidate) -> float | None:
        return _as_float(candidate.get("classifier_score"))


class SelfPriorScorer(_KeyedScorer):
    """Self-prior evidence (``apply_self_prior``): the query's own pre-cutoff non-exp term.

    Reads the per-candidate ``self_prior_score`` (``1.0`` when the query already
    carries the candidate among its OWN pre-cutoff NON-experimental
    annotations). A prior needs the query's known terms, so it does NOT apply to
    NK proteins (which have none), only LK / PK.
    """

    name = "self_prior"
    applies_to = _KNOWN_CATEGORIES

    def _value(self, candidate: Candidate) -> float | None:
        return _as_float(candidate.get("self_prior_score"))


class AssociationScorer(_KeyedScorer):
    """Cross-aspect association evidence (``apply_association``, snapshot-agnostic per #644).

    Reads the per-candidate ``association_total`` (``sum_k P(t | k)`` over the
    query's pre-cutoff EXPERIMENTAL known terms ``K(p)``). Like the self-prior it
    needs known terms, so it applies to LK / PK only.
    """

    name = "association"
    applies_to = _KNOWN_CATEGORIES

    def _value(self, candidate: Candidate) -> float | None:
        return _as_float(candidate.get("association_total"))


class ProtstTextScorer(_KeyedScorer):
    """ProtST text-to-GO transfer evidence (``apply_protst_text``, protst_text family).

    Reads the per-candidate ``protst_text_score`` the ProtST producer stamps (the
    per-query-max-normalised cosine-weighted kNN vote of the query's ProtST
    protein embedding over the reference bank's pre-cutoff t0 GO terms). It is a
    thin read, NOT a recompute: the producer owns the kNN. Applies to every
    category (ProtST proposes from sequence-aligned text embeddings alone, no
    known terms needed); text evidence exists for NK too, and NK is the
    leakage-free BP anchor where the signal is validated.
    """

    name = "protst_text"
    applies_to = _ALL_CATEGORIES

    def _value(self, candidate: Candidate) -> float | None:
        return _as_float(candidate.get("protst_text_score"))


def default_scorer_registry() -> ScorerRegistry:
    """A :class:`ScorerRegistry` pre-loaded with the MR-1 adapters.

    Registration order is the canonical score-vector order the MR-2 combiner
    will consume (base evidence first, then the original four producer signals,
    then the ProtST text lever): alignment, taxonomy, label_embedding, interpro,
    term_frequency, knn_similarity, classifier, self_prior, association,
    protst_text.
    """
    registry = ScorerRegistry()
    registry.register(AlignmentScorer())
    registry.register(TaxonomyScorer())
    registry.register(LabelEmbeddingScorer())
    registry.register(InterproScorer())
    registry.register(TermFrequencyScorer())
    registry.register(KnnSimilarityScorer())
    registry.register(ClassifierScorer())
    registry.register(SelfPriorScorer())
    registry.register(AssociationScorer())
    registry.register(ProtstTextScorer())
    return registry


__all__ = [
    "AlignmentScorer",
    "AssociationScorer",
    "ClassifierScorer",
    "InterproScorer",
    "KnnSimilarityScorer",
    "LabelEmbeddingScorer",
    "ProtstTextScorer",
    "SelfPriorScorer",
    "TaxonomyScorer",
    "TermFrequencyScorer",
    "default_scorer_registry",
]
