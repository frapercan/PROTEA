ADR-D3: ``GOPrediction.features`` stored as JSONB
==================================================

:Status: Accepted
:Date: 2026-05-05
:Phase: F3

Context
-------
The re-ranker feature set grew from the initial 22-feature schema
through the 52-feature selective-rerank schema, and is expected to keep
evolving (lineage, GeOKG, ensembles). Adding a column to
``GOPrediction`` for every new feature
required an Alembic migration per iteration and bound feature engineering
to DB schema cadence.

Decision
--------
Store re-ranker features in a single JSONB column on ``GOPrediction``,
with a transitional dual-write window of approximately one week before
dropping the legacy physical columns.

Consequences
------------
- New features cost a registry entry, not an Alembic migration.
- Querying features by name requires JSONB operators; indexed on the
  most-queried subset.
- Schema drift caught at the boundary by ``schema_sha`` (see D10).

Resolution
----------
Closed; implementation in F3 (T3.1-T3.4).
