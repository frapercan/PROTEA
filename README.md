# PROTEA

Unified architectural foundation for protein data ingestion and processing.
Progressive consolidation of the **PIS** (protein-information-system) and **FANTASIA** codebases.

[![Lint](https://github.com/frapercan/PROTEA/actions/workflows/lint.yml/badge.svg)](https://github.com/frapercan/PROTEA/actions/workflows/lint.yml)
[![Tests](https://github.com/frapercan/PROTEA/actions/workflows/test.yml/badge.svg)](https://github.com/frapercan/PROTEA/actions/workflows/test.yml)
[![Docs](https://github.com/frapercan/PROTEA/actions/workflows/docs.yml/badge.svg)](https://github.com/frapercan/PROTEA/actions/workflows/docs.yml)
[![Documentation](https://readthedocs.org/projects/protea/badge/?version=latest)](https://protea.readthedocs.io/en/latest/)
[![codecov](https://codecov.io/gh/frapercan/PROTEA/branch/main/graph/badge.svg)](https://codecov.io/gh/frapercan/PROTEA)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)

## Prerequisites

- Python 3.12+
- PostgreSQL 16 with pgvector extension
- RabbitMQ 3.x

## Quick start

```bash
# Install dependencies
poetry install

# Create config (not committed)
cp protea/config/system.yaml.example protea/config/system.yaml
# Edit system.yaml with your DB and AMQP URLs

# Frontend config
echo "NEXT_PUBLIC_API_URL=http://127.0.0.1:8000" > apps/web/.env.local

# Initialize the database schema
poetry run python scripts/init_db.py

# Start the full stack (API + workers + frontend)
bash scripts/manage.sh start
```

## Dev stack

| Service   | URL                                  |
|-----------|--------------------------------------|
| Frontend  | http://localhost:3000                |
| API       | http://localhost:8000                |
| RabbitMQ  | http://localhost:15672 (guest/guest) |

Logs are written to `logs/`.

## Common commands

```bash
# Stack management
bash scripts/manage.sh start [N]      # start stack (N batch workers, default 1)
bash scripts/manage.sh stop           # stop everything
bash scripts/manage.sh status         # show PID + RAM per worker
bash scripts/manage.sh logs [name]    # tail logs

# Expose to internet (ngrok static domain)
bash scripts/expose.sh                # → https://protea.ngrok.app

# Tests
poetry run pytest                     # unit tests
poetry run pytest --with-postgres     # integration tests (requires Docker)

# Code quality
poetry run task lint                  # ruff + flake8
poetry run task typecheck             # mypy
poetry run task format                # ruff format (auto-fix)
poetry run task coverage              # pytest + coverage report

# Docs
poetry run task html_docs             # build Sphinx → docs/build/html

# DB
alembic upgrade head                  # apply migrations
poetry run python scripts/init_db.py  # full schema init (dev/reset)
```

## Architecture

```
protea/
  api/              FastAPI routers (jobs, proteins, embeddings, query-sets)
  core/
    contracts/      Operation protocol, ProteaPayload, OperationResult
    operations/     8 operations: ping, insert_proteins, fetch_uniprot_metadata,
                    load_ontology_snapshot, load_goa_annotations,
                    load_quickgo_annotations, compute_embeddings, predict_go_terms
    feature_engineering.py  Needleman-Wunsch, Smith-Waterman, taxonomic distance
  infrastructure/
    orm/models/     SQLAlchemy 2.x models (Job, Protein, Sequence, GOTerm, ...)
    queue/          RabbitMQ consumers (QueueConsumer, OperationConsumer)
    session.py      session_scope context manager
    settings.py     YAML + env-var config loader
  workers/
    base_worker.py  Two-session job lifecycle (QUEUED → RUNNING → SUCCEEDED/FAILED)
apps/
  web/              Next.js 19 frontend (Tailwind v4)
scripts/
  manage.sh         Stack orchestration (start/stop/status/logs/scale)
  expose.sh         Cloudflare Tunnel / ngrok tunnel
  worker.py         Queue worker entry point
  init_db.py        Schema initialisation
```

## Queue routing

| Queue                      | Operations                                              |
|----------------------------|---------------------------------------------------------|
| `protea.ping`              | ping                                                    |
| `protea.jobs`              | insert_proteins, fetch_uniprot_metadata, load_ontology_snapshot, load_goa_annotations, load_quickgo_annotations, compute_embeddings (coord), predict_go_terms (coord) |
| `protea.embeddings`        | compute_embeddings coordinator                          |
| `protea.embeddings.batch`  | compute_embeddings_batch (GPU inference)                |
| `protea.embeddings.write`  | store_embeddings (pgvector bulk insert)                 |
| `protea.predictions.batch` | predict_go_terms_batch (KNN + GO transfer)              |
| `protea.predictions.write` | store_predictions                                       |

## Job lifecycle

Every job transitions through: `QUEUED → RUNNING → SUCCEEDED | FAILED | CANCELLED`.
Each transition is recorded as a `JobEvent` row for full audit trail.
