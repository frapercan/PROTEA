"""Stacked meta-reranker foundation (slices MR-0 + MR-1).

A reranking architecture built from a WIDE set of independent **evidence
scorers** (the :class:`EvidenceScorer` port) feeding a SHALLOW, per-category
**combiner** (the :class:`Combiner` port). Depth lives INSIDE a scorer (N-hop
propagation) or in a compute cascade, never in combiner stacking; the lever is
evidence diversity, not stacking depth. See
``agent-farm/plans/meta-reranker/ARCHITECTURE.md`` and ADR-D16.

This package is the FOUNDATION only. MR-0 defines the two ports plus the
dict-backed :class:`ScorerRegistry` (the hexagonal seam) and a trivial
reference :class:`Combiner` (pass-through / linear). MR-1 adapts the
already-computed producer signals as :class:`EvidenceScorer` instances that read
the per-candidate values the existing producers stamp on each prediction dict:
the base sequence / taxonomy / domain / label-embedding evidence (alignment,
taxonomy, label_embedding, interpro, term_frequency) plus KNN similarity, the M2
classifier, ``self_prior``, and ``association``.

Nothing here is wired into the live predict / eval path yet (that is MR-2); the
existing single-level reranker remains the only path the platform runs. These
ports are additive scaffolding plus unit tests plus an ADR.
"""

from __future__ import annotations

from protea.core.reranking.combiners import LinearCombiner, PassThroughCombiner
from protea.core.reranking.ports import (
    Candidate,
    Combiner,
    EvidenceScorer,
    QueryContext,
)
from protea.core.reranking.registry import ScorerRegistry
from protea.core.reranking.scorers import (
    AlignmentScorer,
    AssociationScorer,
    ClassifierScorer,
    InterproScorer,
    KnnSimilarityScorer,
    LabelEmbeddingScorer,
    SelfPriorScorer,
    TaxonomyScorer,
    TermFrequencyScorer,
    default_scorer_registry,
)

__all__ = [
    "AlignmentScorer",
    "AssociationScorer",
    "Candidate",
    "ClassifierScorer",
    "Combiner",
    "EvidenceScorer",
    "InterproScorer",
    "KnnSimilarityScorer",
    "LabelEmbeddingScorer",
    "LinearCombiner",
    "PassThroughCombiner",
    "QueryContext",
    "ScorerRegistry",
    "SelfPriorScorer",
    "TaxonomyScorer",
    "TermFrequencyScorer",
    "default_scorer_registry",
]
