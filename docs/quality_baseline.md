# Quality Baseline — 2026-03-14

> **Status: historical snapshot.** This is the first quality assessment of
> PROTEA, recorded for traceability. Several entries below have been
> resolved since (API versioning landed via `/v1/`, the proteins router
> paginates, the openapi snapshot is enforced by `openapi-drift.yml`, the
> 16-nullable-column shape of `GOPrediction` was reworked into the
> JSONB columns described in :doc:`/architecture/data_model`, and the
> emit-failure swallowing was addressed by `make_safe_emit` in
> `protea/core/contracts/operation.py`). Read this file as a record of
> where the platform started, not where it is today.

Initial code quality assessment. Objective: track improvement over time.

## Scores

| Area | Score |
|---|---|
| Architecture | 8.5/10 |
| Code Quality | 7/10 |
| Tests | 7/10 (65.8% coverage) |
| API Design | 7.5/10 |
| Database | 8.5/10 |
| Frontend | 8/10 |
| Documentation | 9/10 |
| **Overall** | **7.7/10** |

**Status:** Beta-ready. Not yet production-ready.

## Open risks (priority order)

| # | Risk | Area |
|---|---|---|
| 1 | `emit()` failures swallowed silently in `OperationConsumer` — progress errors are never logged | Workers |
| 2 | No transaction retries — a deadlock or timeout kills the job with no recovery | Database |
| 3 | CORS wildcard (`*`) | API |
| 4 | 16 nullable columns in `GOPrediction` — feature engineering coupled to ORM model | Database |
| 5 | Missing indexes on `ProteinGOAnnotation(protein_id, go_term_id)` — slow queries at scale | Database |
| 6 | No API versioning (no `api_v1` prefix yet), so breaking changes would affect external integrations | API |
| 7 | Duplicate validation in embeddings router (manual checks + Pydantic) | API |
| 8 | No pagination on endpoints that can return thousands of results | API |
