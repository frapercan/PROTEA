ADR-D19: F-RESEARCH targets
============================

:Status: Accepted
:Date: 2026-05-05
:Phase: F-RESEARCH

Context
-------
F-RESEARCH risks becoming an open campaign. Without an explicit cap,
research questions multiply and the phase loses focus. The plan needs
a small, ordered set of ideas.

Decision
--------
Three directed targets, in order:

1. **Lineage feature** (1 week). Whether a candidate GO term is an
   ancestor or descendant of a term already known for the protein.
   Implementable as a single registry feature.
2. **GeOKG embeddings** (1 week, conditional). Replace ``anc2vec``
   features with multi-curvature hyperbolic + Euclidean GO embeddings
   (Bioinformatics 2025).
3. **Multi-K ensemble** (1 week, optional). Combine K ∈ {5, 10, 20}
   instead of K=5 fixed.

Each idea produces an ``ExperimentRun``. Wins integrate into the
canonical pipeline; losses go to the insights appendix (D30).

Consequences
------------
- Phase capped at 2-3 weeks.
- Ideas that did not make the list (PROTEA-DL, retrieval-neural,
  R-GCN over GO-DAG) deferred to F11 post-defense.

Resolution
----------
Closed.
