ADR-D31: T2B.5 Method Object reframe
=====================================

:Status: Accepted
:Date: 2026-05-09
:Phase: F2C / §24

Context
-------

Master plan v3.2 §24 T2B.5 (added 2026-05-08) listed four methods for
a Replace-Method-with-Method-Object refactor:

- ``training_dump_helpers.execute`` (658 LOC at listing time)
- ``_knn_transfer_and_label`` (621 LOC)
- ``run_cafa_evaluation.execute`` (471 LOC)
- ``predict_go_terms.execute`` (333 LOC)

By 2026-05-09, investigation before PR #267 revealed that the spec was
stale:

- ``predict_go_terms.execute`` had already been reduced to 49 and 52 LOC
  across its two execute entrypoints by PRs #162, #169, #170, and #177.
- AST analysis of ``run_cafa_evaluation.execute`` confirmed it is already
  53 LOC, the parent class is 353 LOC, and all methods are below the
  60 LOC budget.
- The ``training_dump_helpers`` cluster had been progressively broken up
  across 8 partial merges found in the shepherd scan of 2026-05-09.

The remaining measurable smell on ``predict_go_terms.py`` was the size of
the parent class ``PredictGOTermsBatchOperation`` (1589 LOC), not of any
single method.

Decision
--------

1. When an ``execute()`` method listed in T2B.5 is already below 60 LOC
   at the time of refactor, do NOT introduce a Method Object class for
   the entrypoint itself.

2. Instead, identify the largest cohesive sub-cluster of methods on the
   parent class that share mutable state, and extract those into a
   dedicated ``_<Cluster>Runner`` class. Shared state is passed as a
   ``Context`` namedtuple or dataclass.

3. Acceptance criterion (carried forward from §24 T2B.5): no productive
   method exceeds 60 LOC in the listed files. Bench Fmax must remain
   within 1 % after refactor.

4. PR #267 implements this reframe for ``_AspectSeparatedKnnRunner``
   (347 LOC, 10 methods all below 60 LOC), reducing
   ``PredictGOTermsBatchOperation`` from 1589 LOC to 1283 LOC.

5. T2B.5 is declared essentially closed after PR #267. Future methods
   that exceed 60 LOC and surface in the listed files are tracked as
   T2B.5 follow-ups, not as new §24 slices.

Consequences
------------

**Positive**

- Avoids a redundant Method Object class on already-budgeted entrypoints.
- The acceptance criterion (no method over 60 LOC) is preserved with
  the minimum possible diff.
- Reduces parent class size meaningfully without touching the public
  operation API.

**Negative**

- The §24 T2B.5 entry as written (4 named methods, 4 Method Object
  classes) does not literally match what shipped. The master plan memory
  entry has been updated with a STATUS 2026-05-09 reframe note.

**Neutral**

- This ADR is the canonical reference for any future executor task that
  considers T2B.5 a refactor target. Consult it before treating an
  in-budget method as unfinished work.

References
----------

- PR #267 (``feat/T2B5-predict-go-terms-method-object``)
- Master plan v3.2 §24 (memory ``project_protea_master_plan.md``)
- Prior PRs that refactored execute(): #162, #169, #170, #177
- Shepherd scan 2026-05-09
- Executor task records pinning the AST audit (T2B.5 partial #4 closure)
