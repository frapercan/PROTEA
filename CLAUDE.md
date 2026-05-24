# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Context

PROTEA is the target platform for the progressive consolidation of the **PIS** (protein-information-system) and **FANTASIA** codebases. The goal is not a full rewrite but an incremental migration that redesigns the system around a clean separation of concerns: infrastructure, execution flow, and domain logic are deliberately decoupled. Workers in PIS/FANTASIA conflate database sessions, queue management, orchestration, and business logic into single classes: PROTEA is architected to eliminate that coupling.

New capabilities and data model extensions are expected continuously. Architectural decisions must accommodate evolution without regression, and computational efficiency must be preserved or improved at each step.

## Commands

All commands run from `repositories/PROTEA/`.

```bash
# Install dependencies (including dev group)
poetry install

# Start the full dev stack (API + workers + frontend) in one shot
bash scripts/manage.sh start [N]   # N = number of batch workers per pipeline (default 1)

# Stop everything
bash scripts/manage.sh stop

# Check worker status (PID, RAM, running/dead)
bash scripts/manage.sh status

# Tail logs (interactive picker or direct name fragment)
bash scripts/manage.sh logs [name]

# Add extra workers to a queue without restart
bash scripts/manage.sh scale protea.predictions.batch 2

# Run unit tests
poetry run pytest

# Run a single test
poetry run pytest tests/test_jobs_pg.py::test_name -v

# Expose PROTEA to the internet via Cloudflare Tunnel (no account required)
bash scripts/expose.sh

# Initialize the database schema (first time or after DB reset)
poetry run python scripts/init_db.py

# Execute a queued job manually by UUID
poetry run python scripts/run_one_job.py <job_id_uuid>

# Apply Alembic migrations
alembic upgrade head
```

Integration tests (requires Docker) use the `--with-postgres` pytest flag: `poetry run pytest --with-postgres`.

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
- `protea.jobs` → insert_proteins, fetch_uniprot_metadata, load_ontology_snapshot, load_goa_annotations, load_quickgo_annotations, generate_evaluation_set, load_interpro_go_mapping, run_interproscan_batch, predict_go_terms_from_interpro, refresh_goa_release_dates
- `protea.embeddings` → compute_embeddings coordinator (serialized: one at a time, retries with 60s delay if GPU busy)
- `protea.embeddings.batch` → compute_embeddings_batch (GPU inference; OperationConsumer, no DB Job row)
- `protea.embeddings.write` → store_embeddings (bulk pgvector insert; OperationConsumer)
- `protea.predictions` → predict_go_terms coordinator (partitions queries into batches and dispatches them to `protea.predictions.batch`)
- `protea.predictions.batch` → predict_go_terms_batch (KNN + GO transfer; OperationConsumer, no DB Job row)
- `protea.predictions.write` → store_predictions (bulk GOPrediction insert; OperationConsumer)
- `protea.evaluations` → run_cafa_evaluation (Fmax / AuPRC / coverage; isolated so long evals don't block the general jobs queue)
- `protea.training` → export_research_dataset (serialized; GPU/RAM-intensive KNN + feature generation + artifact-store upload). Re-ranker *training* itself no longer runs in PROTEA: see "Re-ranker training decoupling" below.

The frontend (`apps/web/`) is a Next.js 16 app with Tailwind v4. API URL is configured in `apps/web/.env.local` (`NEXT_PUBLIC_API_URL=http://127.0.0.1:8000`).

**Known issue:** Tailwind CSS resolution warnings appear in the Next.js dev server console (`Can't resolve 'tailwindcss'`). These are non-blocking: the app renders correctly. The `npm run build` produces clean output.

## Architecture

### Core Abstractions (`protea/core/`)

**`Operation` protocol** (`contracts/operation.py`): every unit of domain logic implements `name: str` and `execute(session, payload, *, emit) -> OperationResult`. Progress and structured events are reported through the `emit` callback (`EmitFn`), which writes `JobEvent` rows to the DB in real time. Operations are pure domain logic: they receive a session and emit events; they do not manage sessions or queues themselves. Long-running bulk-load operations (`load_goa_annotations`, `load_quickgo_annotations`, `fetch_uniprot_metadata`, `run_cafa_evaluation`) are explicitly allowed to call `session.commit()` per page so that an interrupted job leaves a partial-but-consistent dataset rather than rolling everything back; `BaseWorker` still owns the final transition commit.

**`OperationRegistry`** (`contracts/registry.py`): a dict-backed registry. Operations are registered at startup; `BaseWorker` resolves them by name at dispatch time.

**`core/utils.py`**: small set of shared utilities (`utcnow()`, `chunks(seq, n)`). The previous `UniProtHttpMixin` was inlined into its callers when those operations were rewritten; HTTP retry / backoff / Retry-After / cursor handling now lives directly inside `insert_proteins` and `fetch_uniprot_metadata`.

**Current operations** (`core/operations/`):
- `insert_proteins`: paginates the UniProt REST API (FASTA format, cursor-based, exponential backoff + jitter), deduplicates sequences by MD5 hash, and upserts `Protein` + `Sequence` rows.
- `fetch_uniprot_metadata`: fetches TSV annotations from UniProt and upserts `ProteinUniProtMetadata` by `canonical_accession`. Fully migrated to PROTEA models: no legacy dependencies.
- `load_ontology_snapshot`: downloads a GO OBO file and populates `OntologySnapshot` + `GOTerm` rows. Versioned by `obo_version` (unique constraint).
- `load_goa_annotations`: bulk-loads GO annotations from a GAF file into `AnnotationSet` + `ProteinGOAnnotation` rows, filtered against canonical accessions already in the DB.
- `load_quickgo_annotations`: streams GO annotations from the QuickGO bulk download API (TSV), with optional ECO→evidence code mapping, pagination, and per-page commits.
- `generate_evaluation_set`: materializes a `EvaluationSet` by snapshotting the ground-truth annotations for a target ontology snapshot / accession set, used downstream by `run_cafa_evaluation`.
- `run_cafa_evaluation`: runs the standalone `cafaeval` fork against a `PredictionSet` and persists `EvaluationResult` rows (Fmax, AuPRC, coverage, per-aspect breakdowns).
- `export_research_dataset`: runs KNN + feature generation over temporal snapshot pairs and publishes `train.parquet` + `eval.parquet` + `manifest.json` to the configured artifact store (local FS or MinIO). Inserts a `Dataset` row once upload completes. Consumed by `protea-reranker-lab` for offline LightGBM training.
- `ping`: smoke-test operation.
- `load_interpro_go_mapping` (`LoadInterProGoMappingOperation`): downloads the EBI InterPro2GO flat file and upserts `(ipr_accession, go_id)` mappings; idempotent per release version. Runs on `protea.jobs`.
- `run_interproscan_batch` (`RunInterProScanBatchOperation`): annotates proteins in fixed-size chunks via InterProScan, resumable through release-floor filtering. Runs on `protea.jobs`.
- `predict_go_terms_from_interpro` (`PredictGOTermsFromInterProOperation`): predicts GO terms by joining InterPro hits against the InterPro2GO mapping, aggregates per-protein votes, and emits a tagged `PredictionSet`. Runs on `protea.jobs`.
- `refresh_goa_release_dates` (`RefreshGoaReleaseDatesOperation`): scrapes the EBI FTP index (`ftp.ebi.ac.uk/pub/databases/GO/goa/old/UNIPROT/`), extracts the official publication date for each `goa_uniprot_all.gaf.<N>.gz` release, and upserts `source_published_at` onto matching `goa` AnnotationSet rows. Backs the temporal release-timeline component on `/evaluation`. Runs on `protea.jobs`.

These three operations form the **post-reranker functional-enrichment stage, the last component to be developed before uploading results to LAFA**. All three are fully wired and registered in the `OperationRegistry`; the remaining prerequisite is an InterProScan binary install on the host.

**Internal helpers** (not registered as standalone operations):
- `TrainRerankerAutoOperation` (in `protea/core/training_dump/_runner.py`, with loaders in `protea/core/_training_dump_loaders.py` and reusable helpers in `protea/core/training_dump_helpers.py`) is importable but **not** wired into the `OperationRegistry`. LightGBM training has been moved to [`protea-reranker-lab`](https://github.com/frapercan/protea-reranker-lab); this class survives only as the in-process KNN + feature-generation runner that `ExportResearchDatasetOperation` reuses in `dump_only` mode to produce frozen parquet shards. The old `TrainRerankerOperation` and the standalone `protea/core/operations/train_reranker.py` module were deleted in F0 (T0.6); any external doc or commit still referencing that path is stale.

### Job Lifecycle (`protea/workers/base_worker.py`)

`BaseWorker.handle_job(job_id)` uses **two separate sessions** by design:
1. **Claim session**: transitions `QUEUED → RUNNING`, flushes `job.started`.
2. **Execute session**: resolves the operation, runs it, transitions to `SUCCEEDED` or `FAILED` (storing `error_code` / `error_message`).

Every state transition is recorded as a `JobEvent` row for a full audit trail. This is the primary extension point: new worker implementations (e.g. queue-driven) must preserve this two-session pattern and the `emit` contract.

### HTTP API (`protea/api/routers/jobs.py`)

FastAPI router at `/jobs`. The `session_factory` is injected via `app.state.session_factory` (set at app startup: not hardcoded in the router). Endpoints: `POST /jobs`, `GET /jobs` (filterable by `status`/`operation`), `GET /jobs/{id}`, `GET /jobs/{id}/events`, `POST /jobs/{id}/cancel`.

### Data Model (`protea/infrastructure/orm/models/`)

- **`Sequence`**: deduplicated by MD5 hash. Multiple `Protein` rows can reference the same `Sequence`: `sequence_id` is explicitly non-unique.
- **`Protein`**: one row per UniProt accession (including isoforms `<canonical>-<n>`). Isoform parsing via `Protein.parse_isoform()`. Grouped by `canonical_accession`. Has a viewonly relationship to `ProteinUniProtMetadata`.
- **`ProteinUniProtMetadata`**: raw UniProt functional annotations keyed by `canonical_accession`.
- **`OntologySnapshot`**: one row per loaded OBO file release. Versioned by `obo_version` (unique).
- **`GOTerm`**: one row per GO term per snapshot. `(go_id, ontology_snapshot_id)` is unique.
- **`AnnotationSet`**: groups a batch of annotations by source (`quickgo`, `goa`) and ontology snapshot.
- **`ProteinGOAnnotation`**: association between a protein accession and a GO term within an annotation set. Stores qualifier, evidence code, assigned_by, db_reference, with_from, annotation_date.
- **`EmbeddingConfig`** / **`SequenceEmbedding`** / **`PredictionSet`** / **`GOPrediction`**: embedding provenance, per-sequence halfvec embeddings (migrated 2026-04-11), one row per KNN prediction run, and its per-candidate GO term rows (with re-ranker feature columns).
- **`Dataset`**: frozen re-ranker dataset published by `export_research_dataset` (or the legacy `scripts/dump_reranker_dataset.py`). Unique by `name`. Stores `storage_backend` + `key_prefix` + `train_uri` / `eval_uri` / `manifest_uri` (opaque URIs resolved by `ArtifactStore`), content fingerprints (`schema_sha`, `manifest_sha`), row counts, dump parameters (`k`, `annotation_source`, `embedding_config_id`, `ontology_snapshot_id`, `train_snapshot_pairs`, `eval_snapshot_pair`), and producer provenance (`producer_version`, `producer_git_sha`). The lab's `pull_dataset.py` resolves by id or name.
- **`RerankerModel`**: a trained LightGBM booster registered for inference. Booster bytes live either inline (`model_data`, legacy) or by reference (`artifact_uri`, preferred). Provenance: `feature_schema_sha` (load-bearing at inference: predict refuses to use a booster whose schema drifts from the live pipeline), `dataset_id` (FK → `Dataset` the lab consumed), `external_source` (e.g. `"protea-reranker-lab@<sha>"`), `spec_yaml`, `metrics`, `feature_importance`.
- **`EvaluationSet`** / **`EvaluationResult`**: frozen ground-truth snapshot for a CAFA-style evaluation and the Fmax / AuPRC / coverage metrics persisted by `run_cafa_evaluation`.
- **`Job` / `JobEvent`**: job queue state machine and structured event log. `payload`, `meta`, and `fields` are PostgreSQL `JSONB`.

### Infrastructure (`protea/infrastructure/`)

- `settings.py`: `load_settings(project_root)` reads `protea/config/system.yaml` then env overrides.
- `session.py`: `build_session_factory(db_url)` and `session_scope(factory)` context manager (commit on success, rollback on exception).
- `storage/`: `ArtifactStore` abstraction with two backends: `LocalArtifactStore` (`file://…` URIs under a project-relative root) and `MinioArtifactStore` (`s3://bucket/key` via the MinIO SDK). Backend selected by `PROTEA_STORAGE_BACKEND` (`local` | `minio`); MinIO config via `PROTEA_MINIO_ENDPOINT` / `PROTEA_MINIO_BUCKET` / `PROTEA_MINIO_ACCESS_KEY` / `PROTEA_MINIO_SECRET_KEY` / `PROTEA_MINIO_SECURE`. `get_artifact_store(settings)` is the single factory used by both `export_research_dataset` and `/reranker-models/import`.
- Alembic `env.py` is wired with `Base.metadata` and reads DB URL from `load_settings()`. Run `alembic revision --autogenerate -m "desc"` to generate migrations.

### Re-ranker training decoupling

LightGBM training has been moved out of PROTEA into the standalone [`protea-reranker-lab`](https://github.com/frapercan/protea-reranker-lab) repo. PROTEA owns the KNN + feature pipeline, dataset publishing, and inference; the lab owns training, evaluation, and booster production. The two repos talk via the artifact store (MinIO in prod, local FS in dev) and a thin HTTP surface on PROTEA.

**Flow:**
1. `POST /datasets` → enqueues an `export_research_dataset` job on `protea.training`. The worker runs KNN + feature generation, uploads `train.parquet` / `eval.parquet` / `manifest.json` via `ArtifactStore`, and inserts a `Dataset` row.
2. The lab's `pull_dataset.py` hits `GET /datasets/{id_or_name}`, resolves the URIs, downloads the dump, trains a LightGBM booster, and writes `runs/<run_id>/{model.txt, spec.yaml, run.json}`.
3. `POST /reranker-models/import` (multipart) or `POST /reranker-models/import-by-reference` (JSON, booster already in MinIO) → uploads the booster if needed, validates `feature_schema_sha`, and inserts a `RerankerModel` row linked back to the `Dataset` via `dataset_id`.
4. Inference consumes registered `RerankerModel` rows via the scoring router (`GET /scoring/prediction-sets/{id}/score.tsv`, `…/metrics`, `…/reranker-metrics`). Boosters are loaded from `artifact_uri` (or inline `model_data` on legacy rows) and scored with `protea.core.reranker.predict`. There is no in-PROTEA training endpoint: `TrainRerankerAutoOperation` survives only as an internal helper used by `export_research_dataset` in `dump_only` mode and should not be exposed by new code.

Related HTTP routers: `protea/api/routers/datasets.py` (dataset registry), `protea/api/routers/reranker_models.py` (model registry + import).

### Testing

Integration tests require `--with-postgres`. The `conftest.py` `postgres_url` session-scoped fixture pulls `pgvector/pgvector:pg16` via Docker, waits for readiness, enables the `vector` extension, yields the connection URL, then tears down the container. Configurable via: `PROTEA_PG_IMAGE`, `PROTEA_PG_USER`, `PROTEA_PG_PASSWORD`, `PROTEA_PG_DB`, `PROTEA_PG_PORT`, `PROTEA_PG_TIMEOUT`.
