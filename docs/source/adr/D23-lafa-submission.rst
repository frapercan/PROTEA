ADR-D23: LAFA submission strategy
===================================

:Status: Accepted
:Date: 2026-05-05
:Phase: F-LAFA

Context
-------
LAFA (functionbench.net) provides a public benchmark surface for
protein function annotation methods, comparable in spirit to CAFA.
PROTEA needs a credible adoption story; LAFA also exposes the method
to comparison against external systems on identical evaluation
conditions.

Decision
--------
F-LAFA at the end of the timeline (~1.5 weeks). Three containers
built on top of ``protea-method-runtime``:

- **knn-v1** (one PLM, KNN baseline, GO propagation).
- **knn-8plm** (ensemble across the eight PLMs).
- **v18** (full pipeline with selective re-ranking).

Each container submitted to the LAFA test suite per
``anphan0828/LAFA_container_guide``.

Consequences
------------
- Reuses F-OPS deliverables (``protea-method-runtime``).
- Material for chapter 7 conclusion: external adoption.
- ``apps/lafa_container/`` and ``protea-lafa-container/`` (existing
  preliminaries) are not iterated on until F-LAFA opens; F-LAFA
  rewrites them on top of ``protea-method-runtime``.

Resolution
----------
Closed.
