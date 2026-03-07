# PROTEA

Unified architectural foundation for protein data ingestion and processing.
Progressive consolidation of the **PIS** (protein-information-system) and **FANTASIA** codebases.

## Prerequisites

- Python 3.12+
- PostgreSQL (with pgvector extension)
- RabbitMQ

## Quick start

```bash
# Install dependencies
poetry install

# Create config (not committed)
cp protea/config/system.yaml.example protea/config/system.yaml
# Edit system.yaml with your DB and AMQP URLs

# Initialize the database schema
poetry run python scripts/init_db.py

# Start the full stack (API + workers + frontend)
bash scripts/start_dev.sh
```

## Dev stack

| Service   | URL                              |
|-----------|----------------------------------|
| Frontend  | http://localhost:3000            |
| API       | http://localhost:8000            |
| RabbitMQ  | http://localhost:15672 (guest/guest) |

Logs are written to `logs/`.

## Common commands

```bash
# Run tests
poetry run pytest

# Run integration tests (requires Docker)
poetry run pytest --with-postgres

# Lint
poetry run task lint

# Type check
poetry run task typecheck

# Format
poetry run task format

# Coverage
poetry run task coverage

# Apply DB migrations
alembic upgrade head

# Run a single job manually
poetry run python scripts/run_one_job.py <job_uuid>
```

## Architecture

```
protea/
  api/            FastAPI router (/jobs endpoints)
  core/
    contracts/    Operation protocol, ProteaPayload base, OperationResult
    operations/   insert_proteins, fetch_uniprot_metadata, ping
  infrastructure/
    orm/models/   SQLAlchemy 2.x models (Job, Protein, Sequence, ProteinUniProtMetadata)
    queue/        RabbitMQ consumer + publisher (pika)
    session.py    session_scope context manager
    settings.py   YAML + env-var config loader
  workers/
    base_worker.py  Two-session job lifecycle (QUEUED → RUNNING → SUCCEEDED/FAILED)
apps/
  web/            Next.js 16 frontend (Tailwind v4)
scripts/
  start_dev.sh    Start full dev stack
  worker.py       Queue worker entry point
  init_db.py      Schema initialisation
```

### Queue routing

| Queue         | Operations                                    |
|---------------|-----------------------------------------------|
| protea.ping   | ping                                          |
| protea.jobs   | insert_proteins, fetch_uniprot_metadata       |

### Job lifecycle

Every job transitions through: `QUEUED → RUNNING → SUCCEEDED | FAILED | CANCELLED`.
Each transition is recorded as a `JobEvent` row for full audit trail.
