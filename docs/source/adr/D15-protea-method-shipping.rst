ADR-D15: ``protea-method`` distribution channels
==================================================

:Status: Accepted
:Date: 2026-05-05
:Phase: F-OPS

Context
-------
``protea-method`` is the pure inference path (KNN, feature compute,
re-ranker apply). Because it has no FastAPI / SQLAlchemy dependencies
it can ship independently of the platform, addressing audiences that
want the method without operating the full stack.

Decision
--------
Three distribution channels:

- **PyPI** public package (``pip install protea-method``).
- **Docker Hub** minimal image (``protea-method-runtime``,
  ~3-4 GB with one canonical PLM and one default booster, CLI
  ``protea-predict input.fasta output.tsv``).
- **Companion engineering paper** covering the F-OPS pillars
  (containers, multi-target deployment, observability, secrets).

Consequences
------------
- Two release surfaces to maintain.
- LAFA submission containers (D23) build on top of
  ``protea-method-runtime``.
- Engineering paper aligned with F-OPS deliverables.

Resolution
----------
Closed.
