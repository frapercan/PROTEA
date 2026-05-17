ADR-D30: Insights appendix
============================

:Status: Accepted
:Date: 2026-05-05
:Phase: F7

Context
-------
Several lessons learned during the project deserve a written record
that is neither a peer-reviewed publication nor a reluctant footnote
inside an unrelated chapter. Examples: the ``anc2vec`` feature
leakage discovery (2026-05-05), the ``schema_sha`` drift incident,
the selective re-ranking discovery, the PK coverage cafaeval
upstream bug. None of these belong in the canonical evaluation; all
of them taught something.

Decision
--------
A short appendix at ``docs/source/appendix/insights.rst`` with one
paragraph to one page per insight. No formalisms. Honest tone:
described as encountered, with the workaround or fix that closed it.

Consequences
------------
- Companion to chapter 6 of the thesis but not part of the chapter
  itself.
- Linked from chapter 7 conclusion as a pointer to the operational
  history.
- Stable home for future incidents discovered post-defense.

Resolution
----------
Closed.
