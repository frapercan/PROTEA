ADR-D12: F-EXP as QA reproduction of the canonical pipeline
============================================================

:Status: Accepted
:Date: 2026-05-05
:Phase: F-EXP

Context
-------
After the structural refactor (F0-F5), the rebuilt pipeline needs
end-to-end validation. Independently, the thesis needs a clean
campaign whose numbers can be cited without caveat. Running two
campaigns is duplicative.

Decision
--------
Treat F-EXP as both: a QA reproduction of the canonical pipeline and
the production run that supplies thesis chapter 6 numbers. Each Job
records its narrative (D11). At the close, material is distilled into
~8-12 thesis pages.

Consequences
------------
- Wipe-and-rebuild executed once on a backed-up database.
- Tagging convention ``study_v_thesis`` makes the campaign navigable
  as a single experiment unit.
- Replay drill (1 % Fmax tolerance) verifies reproducibility of any
  ``ExperimentRun`` row.

Resolution
----------
Closed.
