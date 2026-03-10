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

# Start the full dev stack (API + workers + frontend) in one shot
bash scripts/start_dev.sh

# Stop everything
pkill -f "uvicorn protea|scripts/worker.py|next dev"

# Run unit tests
poetry run pytest

# Run integration tests (requires Docker — spins up a temporary pgvector/pg16 container)
poetry run pytest --with-postgres

# Run a single test
poetry run pytest tests/test_jobs_pg.py::test_name -v

# Initialize the database schema (first time or after DB reset)
poetry run python scripts/init_db.py

# Execute a queued job manually by UUID
poetry run python scripts/run_one_job.py <job_id_uuid>

# Apply Alembic migrations
alembic upgrade head
```

Settings load from `protea/config/system.yaml` and are overridden by env vars `PROTEA_DB_URL` and `PROTEA_AMQP_URL`.

## Dev Stack

Prerequisites: Postgres and RabbitMQ must be running before starting the stack.

```
protea/config/system.yaml       ← DB URL and AMQP URL (created manually, not committed)
logs/api.log                    ← FastAPI logs
logs/worker-ping.log            ← Worker for protea.ping queue
logs/worker-jobs.log            ← Worker for protea.jobs queue (insert_proteins, fetch_uniprot_metadata)
logs/frontend.log               ← Next.js dev server
```

**Queue routing:**
- `protea.ping` → ping operation (smoke test)
- `protea.jobs` → insert_proteins, fetch_uniprot_metadata, load_ontology_snapshot, load_goa_annotations, load_quickgo_annotations
- `protea.embeddings` → compute_embeddings coordinator (serialized: one at a time, retries with 60s delay if GPU busy)
- `protea.embeddings.batch` → compute_embeddings_batch (actual GPU inference)

The frontend (`apps/web/`) is a Next.js 16 app with Tailwind v4. API URL is configured in `apps/web/.env.local` (`NEXT_PUBLIC_API_URL=http://127.0.0.1:8000`).

**Known issue:** Tailwind CSS resolution warnings appear in the Next.js dev server console (`Can't resolve 'tailwindcss'`). These are non-blocking — the app renders correctly. The `npm run build` produces clean output.

## Architecture

### Core Abstractions (`protea/core/`)

**`Operation` protocol** (`contracts/operation.py`): every unit of domain logic implements `name: str` and `execute(session, payload, *, emit) -> OperationResult`. Progress and structured events are reported through the `emit` callback (`EmitFn`), which writes `JobEvent` rows to the DB in real time. Operations are pure domain logic — they receive a session and emit events; they do not manage sessions or queues themselves.

**`OperationRegistry`** (`contracts/registry.py`): a dict-backed registry. Operations are registered at startup; `BaseWorker` resolves them by name at dispatch time.

**Current operations** (`core/operations/`):
- `insert_proteins` — paginates the UniProt REST API (FASTA format, cursor-based, exponential backoff + jitter), deduplicates sequences by MD5 hash, and upserts `Protein` + `Sequence` rows.
- `fetch_uniprot_metadata` — fetches TSV annotations from UniProt and upserts `ProteinUniProtMetadata` by `canonical_accession`. Fully migrated to PROTEA models — no legacy dependencies.
- `load_ontology_snapshot` — downloads a GO OBO file and populates `OntologySnapshot` + `GOTerm` rows. Versioned by `obo_version` (unique constraint).
- `load_goa_annotations` — bulk-loads GO annotations from a GAF file into `AnnotationSet` + `ProteinGOAnnotation` rows, filtered against canonical accessions already in the DB.
- `load_quickgo_annotations` — streams GO annotations from the QuickGO bulk download API (TSV), with optional ECO→evidence code mapping, pagination, and per-page commits.
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
- **`OntologySnapshot`**: one row per loaded OBO file release. Versioned by `obo_version` (unique).
- **`GOTerm`**: one row per GO term per snapshot. `(go_id, ontology_snapshot_id)` is unique.
- **`AnnotationSet`**: groups a batch of annotations by source (`quickgo`, `goa`) and ontology snapshot.
- **`ProteinGOAnnotation`**: association between a protein accession and a GO term within an annotation set. Stores qualifier, evidence code, assigned_by, db_reference, with_from, annotation_date.
- **`Job` / `JobEvent`**: job queue state machine and structured event log. `payload`, `meta`, and `fields` are PostgreSQL `JSONB`.

### Infrastructure (`protea/infrastructure/`)

- `settings.py`: `load_settings(project_root)` reads `protea/config/system.yaml` then env overrides.
- `session.py`: `build_session_factory(db_url)` and `session_scope(factory)` context manager (commit on success, rollback on exception).
- Alembic `env.py` is wired with `Base.metadata` and reads DB URL from `load_settings()`. Run `alembic revision --autogenerate -m "desc"` to generate migrations.

### Testing

Integration tests require `--with-postgres`. The `conftest.py` `postgres_url` session-scoped fixture pulls `pgvector/pgvector:pg16` via Docker, waits for readiness, enables the `vector` extension, yields the connection URL, then tears down the container. Configurable via: `PROTEA_PG_IMAGE`, `PROTEA_PG_USER`, `PROTEA_PG_PASSWORD`, `PROTEA_PG_DB`, `PROTEA_PG_PORT`, `PROTEA_PG_TIMEOUT`.
