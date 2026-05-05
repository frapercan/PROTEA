ADR-D13: Early UI track parallel to F2
========================================

:Status: Accepted
:Date: 2026-05-05
:Phase: F8a (parallel to F2 final), F8b (parallel to F-EXP)

Context
-------
Postponing the front-end until F8 risks shipping a pipeline whose
state is invisible to its operator. Issues that surface only through
the UI (job state mismatches, narrative gaps, dashboard latency) would
arrive too late to influence the design.

Decision
--------
Two-stage UI track:

- **F8a** (2 weeks, parallel to F2 final): basic narrative jobs page,
  generic operation launcher, basic evaluation dashboard, dark mode,
  a11y AA.
- **F8b** (2 weeks, during F-EXP): SSE streaming, advanced evaluation
  dashboard, prediction visualisation, UMAP embeddings page,
  experiments page.

Consequences
------------
- shadcn/ui is a hard prerequisite (D8).
- F-EXP has a usable UI surface from day one.

Resolution
----------
Closed.
