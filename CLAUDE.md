# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Context

PROTEA is the target platform for the progressive consolidation of the **PIS** (protein-information-system) and **FANTASIA** codebases. The goal is not a full rewrite but an incremental migration that redesigns the system around a clean separation of concerns: infrastructure, execution flow, and domain logic are deliberately decoupled. Workers in PIS/FANTASIA conflate database sessions, queue management, orchestration, and business logic into single classes — PROTEA is architected to eliminate that coupling.

New capabilities and data model extensions are expected continuously. Architectural decisions must accommodate evolution without regression, and computational efficiency must be preserved or improved at each step.

## Commands

All commands run from `repositories/PROTEA/`.

```bash
# Install dependencies (including dev group)
poetry install

# Run unit tests
poetry run pytest

# Run integration tests (requires Docker — spins up a temporary pgvector/pg16 container)
poetry run pytest --with-postgres

# Run a single test
poetry run pytest tests/test_jobs_pg.py::test_name -v

# Initialize the database schema
poetry run python scripts/init_db.py

# Execute a queued job manually by UUID (useful during development)
poetry run python scripts/run_one_job.py <job_id_uuid>

# Apply Alembic migrations
alembic upgrade head
```

Settings load from `protea/config/system.yaml` and are overridden by env vars `PROTEA_DB_URL` and `PROTEA_AMQP_URL`.

## Architecture

### Core Abstractions (`protea/core/`)

**`Operation` protocol** (`contracts/operation.py`): every unit of domain logic implements `name: str` and `execute(session, payload, *, emit) -> OperationResult`. Progress and structured events are reported through the `emit` callback (`EmitFn`), which writes `JobEvent` rows to the DB in real time. Operations are pure domain logic — they receive a session and emit events; they do not manage sessions or queues themselves.

**`OperationRegistry`** (`contracts/registry.py`): a dict-backed registry. Operations are registered at startup; `BaseWorker` resolves them by name at dispatch time.

**Current operations** (`core/operations/`):
- `insert_proteins` — paginates the UniProt REST API (FASTA format, cursor-based, exponential backoff + jitter), deduplicates sequences by MD5 hash, and upserts `Protein` + `Sequence` rows.
- `fetch_uniprot_metadata` — fetches TSV annotations from UniProt and upserts `ProteinUniProtMetadata` by `canonical_accession`. Currently imports from `protein_information_system` (legacy — to be migrated).
- `ping` — smoke-test operation.

### Job Lifecycle (`protea/workers/base_worker.py`)

`BaseWorker.handle_job(job_id)` uses **two separate sessions** by design:
1. **Claim session**: transitions `QUEUED → RUNNING`, flushes `job.started`.
2. **Execute session**: resolves the operation, runs it, transitions to `SUCCEEDED` or `FAILED` (storing `error_code` / `error_message`).

Every state transition is recorded as a `JobEvent` row for a full audit trail. This is the primary extension point: new worker implementations (e.g. queue-driven) must preserve this two-session pattern and the `emit` contract.

### HTTP API (`protea/api/routers/jobs.py`)

FastAPI router at `/jobs`. The `session_factory` is injected via `app.state.session_factory` (set at app startup — not hardcoded in the router). Endpoints: `POST /jobs`, `GET /jobs` (filterable by `status`/`operation`), `GET /jobs/{id}`, `GET /jobs/{id}/events`, `POST /jobs/{id}/cancel`.

### Data Model (`protea/infrastructure/orm/models/`)

- **`Sequence`**: deduplicated by MD5 hash. Multiple `Protein` rows can reference the same `Sequence` — `sequence_id` is explicitly non-unique.
- **`Protein`**: one row per UniProt accession (including isoforms `<canonical>-<n>`). Isoform parsing via `Protein.parse_isoform()`. Grouped by `canonical_accession`. Has a viewonly relationship to `ProteinUniProtMetadata`.
- **`ProteinUniProtMetadata`**: raw UniProt functional annotations keyed by `canonical_accession`.
- **`Job` / `JobEvent`**: job queue state machine and structured event log. `payload`, `meta`, and `fields` are PostgreSQL `JSONB`.

### Infrastructure (`protea/infrastructure/`)

- `settings.py`: `load_settings(project_root)` reads `protea/config/system.yaml` then env overrides.
- `session.py`: `build_session_factory(db_url)` and `session_scope(factory)` context manager (commit on success, rollback on exception).
- Alembic is present but `target_metadata` is not yet wired — schema changes require manual migration authoring.

### Testing

Integration tests require `--with-postgres`. The `conftest.py` `postgres_url` session-scoped fixture pulls `pgvector/pgvector:pg16` via Docker, waits for readiness, enables the `vector` extension, yields the connection URL, then tears down the container. Configurable via: `PROTEA_PG_IMAGE`, `PROTEA_PG_USER`, `PROTEA_PG_PASSWORD`, `PROTEA_PG_DB`, `PROTEA_PG_PORT`, `PROTEA_PG_TIMEOUT`.
