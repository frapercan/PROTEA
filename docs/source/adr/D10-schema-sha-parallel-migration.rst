ADR-D10: ``schema_sha_v2`` parallel migration
==============================================

:Status: Accepted (implementation pending)
:Date: 2026-05-05
:Decided: 2026-05-06 (user confirmation)
:Phase: F1
:Gate: T1.6 (requires_human, Alembic on live DB)

Context
-------
``schema_sha`` is the load-bearing fingerprint that prevents inference
from running with a re-ranker booster trained against a different
feature schema. Historically, two definitions of ``compute_schema_sha``
co-existed (lab and PROTEA); silent drift caused at least one
non-reproducible run (the per-cell lambdarank study on 2026-05-01)
before the parity bug was found and fixed.

Decision
--------
Add a parallel ``schema_sha_v2`` column to ``Dataset`` and
``RerankerModel``. Backfill from
``protea_contracts.compute_schema_sha``. Production reads the new
``schema_sha_v2`` column; the original ``schema_sha`` column is kept
until F3 for audit and then dropped.

Consequences
------------
- One Alembic migration plus one backfill script.
- Mismatch between the original and the parallel columns surfaces past
  silent drift; documented in a regression test rather than fixed
  retroactively.
- Boosters loaded for inference compare their stored ``schema_sha``
  against the live ``schema_sha_v2`` value.

Resolution
----------
**Accepted as recommended.** User greenlight 2026-05-06 with the
explicit constraint **"no subir a prod hasta que no esté listo"**:
implementation must land in staging (or a local-DB rehearsal) and the
backfill must be verified there before any production migration.
Implementation order: (1) Alembic migration adding ``schema_sha_v2``
column, (2) backfill script populating from
``protea_contracts.compute_schema_sha``, (3) regression test exposing
``schema_sha`` / ``schema_sha_v2`` drift on historical rows (rather
than retroactively fixing), (4) inference path reads ``schema_sha_v2``.
Production rollout only after staging verification.
