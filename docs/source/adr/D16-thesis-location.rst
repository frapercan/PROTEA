ADR-D16: Thesis repository location
=====================================

:Status: Accepted
:Date: 2026-05-05
:Phase: F0

Context
-------
Thesis sources need a stable home tracked under git, separate from the
code repositories so that prose iteration does not pollute code
history.

Decision
--------
``~/Thesis/thesis/``, git-initialised on 2026-05-05 with master branch
at commit ``4fcd449``. ``.gitignore`` filters LaTeX intermediate
artefacts.

Consequences
------------
- Thesis history is independent of code history; both can be tagged
  per phase.
- Cross-references to code commit SHAs are explicit citations, not
  implicit imports.

Resolution
----------
Closed.
