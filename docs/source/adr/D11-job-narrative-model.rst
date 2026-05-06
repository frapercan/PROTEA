ADR-D11: Operational narrative attached to ``Job``
====================================================

:Status: Accepted
:Date: 2026-05-05
:Phase: F3

Context
-------
Past experimental campaigns left no narrative beyond raw metrics.
Reproducing the why of a past run required archaeology in chat logs
and notebooks. The thesis (chapter 6) needs a curated journey, not a
raw chronological log; the operational layer needs a place to record
the reasoning behind each Job.

Decision
--------
Two kinds of narrative artefact:

- ``Job`` rows gain ``description``, ``findings``, ``tags`` columns.
- A new ``JobComment`` table holds chronological commentary tied to a
  Job.

Material doubles as an internal operational record and as the source
from which thesis chapter 6 is distilled.

Consequences
------------
- Hard rule for F-EXP: a Job does not close without ``findings``
  populated.
- UI surfaces narrative inline (D13).
- Thesis writing track (D21) reads from this corpus, not from logs.

Resolution
----------
Closed; implementation in F3 (T3.9, T3.10).
