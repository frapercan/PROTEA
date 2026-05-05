ADR-D10: ``schema_sha`` v2 parallel migration
==============================================

:Status: Pending
:Date: 2026-05-05
:Phase: F1
:Gate: T1.6 (requires_human, Alembic on live DB)

Context
-------
``schema_sha`` is the load-bearing fingerprint that prevents inference
from running with a re-ranker booster trained against a different
feature schema. Historically, two definitions of ``compute_schema_sha``
co-existed (lab and PROTEA); silent drift caused at least one
non-reproducible run (v9 study, 2026-05-01) before the parity bug was
found and fixed.

Decision
--------
Add a parallel ``schema_sha_v2`` column to ``Dataset`` and
``RerankerModel``. Backfill from
``protea_contracts.compute_schema_sha``. Production reads ``v2``;
``v1`` kept until F3 for audit and then dropped.

Consequences
------------
- One Alembic migration plus one backfill script.
- Mismatch between v1 and v2 surfaces past silent drift; documented in
  a regression test rather than fixed retroactively.
- Boosters loaded for inference compare their stored ``schema_sha``
  against the live ``v2`` value.

Resolution
----------
Pending human review of the live-DB migration. Rolls in F1 with T1.6.
