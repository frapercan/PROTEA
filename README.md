# PROTEA

**PROtein functional Embedding-based Annotation**. A distributed platform for large-scale GO term prediction, sequence embedding, and functional analysis.

PROTEA provides a unified backend for ingesting protein data from UniProt, computing protein language model embeddings (ESMC, ProstT5, ESM2), and predicting Gene Ontology terms via KNN transfer plus a learned LightGBM re-ranker, with a full job queue, REST API, and web interface.

It is built as a **contracts-first plugin platform**: this repository holds the core (SQLAlchemy ORM, a RabbitMQ-backed job queue of 10 queues, a versioned FastAPI surface of two dozen routers, a Next.js frontend, and the orchestration that ties them together), while embedding backends, annotation sources, experiment runners, and the offline re-ranker lab live in **seven satellite repositories** that plug in through a shared contract package and Python entry points. See [Repositories in the PROTEA stack](#repositories-in-the-protea-stack).

[![Lint](https://github.com/frapercan/PROTEA/actions/workflows/lint.yml/badge.svg)](https://github.com/frapercan/PROTEA/actions/workflows/lint.yml)
[![Tests](https://github.com/frapercan/PROTEA/actions/workflows/test.yml/badge.svg)](https://github.com/frapercan/PROTEA/actions/workflows/test.yml)
[![Docs](https://github.com/frapercan/PROTEA/actions/workflows/docs.yml/badge.svg)](https://github.com/frapercan/PROTEA/actions/workflows/docs.yml)
[![Documentation](https://readthedocs.org/projects/protea/badge/?version=latest)](https://protea.readthedocs.io/en/latest/)
[![codecov](https://codecov.io/gh/frapercan/PROTEA/branch/develop/graph/badge.svg)](https://codecov.io/gh/frapercan/PROTEA/branch/develop)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)
[![License: Unlicense](https://img.shields.io/badge/license-Unlicense-blue.svg)](https://unlicense.org/)

**Status:** v0.10.0, production. The platform is actively deployed and drives live CAFA 6 evaluation and research dataset exports. The public REST API is not yet stable across minor releases.

---

## Live demo

> **https://protea.ngrok.app**
>
> Currently running on a personal research machine. Availability is best-effort. If it is unreachable, use the Docker setup below to run your own instance.

---

## Why PROTEA?

PROTEA is the successor to [PIS](https://github.com/CBBIO/protein-information-system) and [FANTASIA](https://github.com/CBBIO/fantasia), rebuilt around three goals:

1. **Clean architecture**: infrastructure, orchestration, and domain logic are explicitly decoupled. Operations are pure domain logic; workers own sessions and queue state; routers expose HTTP. No more God-classes that mix everything.
2. **Learned re-ranking on top of KNN transfer**: beyond classical embedding-KNN annotation, PROTEA trains **LightGBM rerankers on dated GOA windows** (LambdaRank + CAFA IA weighting, per-tier NK/LK/PK models). Candidates retrieved by KNN are re-scored with alignment, taxonomy, and retrieval features.
3. **Honest, date-windowed evaluation**: PROTEA evaluates the way LAFA does, on real temporal holdouts between two dated GOA releases rather than random splits. The model sees only the annotations known at the earlier date and is scored against those that appeared by the later one, with the official `cafaeval` library and information-accretion weighting. The platform automates the windowing, so the benchmark is leakage-free by construction and re-runs itself as each new GOA release lands.

PROTEA is the post-CAFA productisation of a protein-function-prediction method that reached #19 in CAFA 6, turning it into a maintainable, deployable, continuously-evaluable system.

---

## The temporal split

Evaluation is keyed to dated GOA releases, so a prediction is always scored against the future relative to what the model could know:

| Role | GOA release | Date | Meaning |
|---|---|---|---|
| Training / reference history | 160 ... 220 | 2016-11-01 ... 2024-04-16 | KNN reference pool and reranker training pairs |
| `t0` (inputs) | 227 | 2025-09-04 | the model sees only annotations known at this date |
| `t1` (ground truth) | 230 | 2026-03-04 | predictions are scored against annotations that appeared by this date |

The `227 -> 230` window is the live **LAFA Sep-2025 -> Mar-2026** evaluation window, so the same artifact that scores on this benchmark is the one submitted to LAFA. Each (protein, namespace) pair is classed into the CAFA **NK / LK / PK** tiers by what was already known at `t0`. Earlier windows (for example `220 -> 229`) are used as validated checkpoints; the split points and their dates come from the live GOA release timeline on `/evaluation`.

---

## Methods and levers

The score is built from a stack of levers explored across the project:

- **In the pipeline today**: KNN annotation transfer over PLM embeddings (eight PLM backends); learned LightGBM re-ranking of KNN candidates (LambdaRank, IA weighting, per-tier NK/LK/PK); a self-prior that injects each target's own `t0` non-experimental annotations; leakage-free, date-windowed evaluation with information-accretion weighting.
- **Validated offline, integration in progress**: a learned k-WTA hard-negative retrieval encoder that projects a PLM embedding into a GO-aligned sparse code and beats dense KNN by a wide margin in reranked tests.
- **On the roadmap** (levers from the strongest CAFA solutions, being adopted): conditional-probability hierarchical modulation, meta-stacking over evidence arms, soft two-way (Pmin/Pmax) ontology propagation, and literature TF-IDF plus PPI-graph features.

---

## What PROTEA does

| Capability | Details |
|---|---|
| **Protein ingestion** | Paginated UniProt REST API, MD5-deduplicated sequences |
| **GO ontology** | Load OBO snapshots, full DAG stored per release |
| **GO annotations** | Bulk import from GOA (GAF) and QuickGO (TSV) |
| **Embeddings** | ESMC, ProstT5, and ESM2 backends via GPU workers; stored as pgvector `VECTOR` columns |
| **GO prediction** | KNN transfer (FAISS IVFFlat / numpy) with optional NW/SW alignment and taxonomic features |
| **Learning-to-rank** | LightGBM rerankers trained on dated GOA windows (LambdaRank + IA weighting, per-tier NK/LK/PK models); import-by-reference from `protea-reranker-lab` via `POST /reranker-models/import-by-reference` |
| **CAFA evaluation** | Benchmark pipeline with `cafaeval` integration, Fmax + IA-weighted scoring, per-aspect (BPO/MFO/CCO) results, NK/LK/PK tier breakdown with CI bands (PR #451) |
| **Dataset export** | `POST /datasets` dispatches `export_research_dataset`; parallelised pair-feature compute with persistent alignment cache (PR #421); `/datasets` registry view in the web UI (PR #453) |
| **Reranker UI** | Import-by-reference dialog, compute-embeddings dialog, feature-schema SHA + manifest SHA provenance on collapsed cards (PR #452, #455); reranker-features toggle on the annotation page (PR #444) |
| **Job queue** | RabbitMQ-backed, 10 queues (ingestion, embeddings, predictions, evaluations, training), full audit trail per job |
| **REST API** | FastAPI routers for jobs, proteins, embeddings, query sets, scoring, evaluation, datasets, reranker models, and admin |
| **Web UI** | Next.js frontend with responsive sidebar shell, protein explorer, annotation viewer, prediction browser with benchmark CI bands, live job widget, and onboarding stepper |
| **Observability** | OpenTelemetry SDK (OTLP traces/metrics), SQLAlchemy + pika instrumentation, Grafana dashboards for API latency, queues, workers, DB, and embeddings; Loki log aggregation via Promtail |
| **Proteins stats prewarm** | API server prewarns `proteins/stats` at startup and refreshes in the background; stale data is served on error rather than blocking page renders (PR #450) |

---

## Getting started

### Docker

> **Not yet validated.** The Docker configuration exists but has not been tested end-to-end. It will likely need adjustments before it works out of the box (contributions welcome).

```bash
git clone https://github.com/frapercan/PROTEA.git
cd PROTEA
docker compose up
```

Services available at:
- Frontend: http://localhost:3000
- API: http://localhost:8000
- RabbitMQ management: http://localhost:15672 (guest/guest)

### From source (recommended)

**Requirements:** Python 3.12, PostgreSQL 16 + pgvector, RabbitMQ 3.x

```bash
git clone https://github.com/frapercan/PROTEA.git
cd PROTEA

poetry install

cp protea/config/system.yaml.example protea/config/system.yaml
# Edit system.yaml: set DB and AMQP URLs

# Environment variables: keep secrets in ~/.secrets/protea.env and source
# before starting the stack:
#   set -a && source ~/.secrets/protea.env && set +a && bash scripts/manage.sh start
# See the env vars table below for the full list.

poetry run python scripts/init_db.py
bash scripts/manage.sh start
```

### Environment variables

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `PROTEA_DB_URL` | yes | | PostgreSQL connection URL |
| `PROTEA_AMQP_URL` | yes | | RabbitMQ connection URL |
| `PROTEA_ANC2VEC_PATH` | for export/inference | auto-resolved via artifact store | Path to `anc2vec_2020-10.npz`; set explicitly when running `export_research_dataset` outside the deploy worktree |
| `PROTEA_PAIR_FEATURE_WORKERS` | no | CPU count | Process-pool size for parallel pair-feature compute during dataset export (PR #421) |
| `PROTEA_ALIGN_CACHE_DIR` | no | `artifacts/align_cache` | Directory for persistent SQLite alignment cache; empty string disables caching; warm cache gives ~21x speedup on repeated export runs (PR #421) |
| `PROTEA_STORAGE_BACKEND` | no | `local` | `local` or `minio` |
| `PROTEA_MINIO_ENDPOINT` | if minio | | MinIO endpoint |
| `PROTEA_MINIO_BUCKET` | if minio | | MinIO bucket |
| `PROTEA_MINIO_ACCESS_KEY` | if minio | | MinIO access key |
| `PROTEA_MINIO_SECRET_KEY` | if minio | | MinIO secret key |
| `NEXT_PUBLIC_API_URL` | for frontend | `http://127.0.0.1:8000` | Backend API URL seen by the browser |
| `NEXT_PUBLIC_ENABLE_DB_RESET` | no | `false` | Set to `true` to show the destructive DB-reset button in the admin UI (PR #454) |

---

## 5 minutes to your first job

With the stack running locally, you can submit a job and watch it
move through the queue + worker + DB lifecycle in under 5 minutes.

```bash
# 1. Submit a `ping` job (the smoke-test operation).
JOB_ID=$(curl -s -X POST http://localhost:8000/jobs \
  -H 'content-type: application/json' \
  -d '{"operation": "ping", "queue_name": "protea.ping", "payload": {}}' \
  | jq -r '.id')
echo "queued: $JOB_ID"

# 2. Tail the structured-event log until the job reaches a terminal state.
curl -s "http://localhost:8000/jobs/$JOB_ID/events" | jq -c '.[]'
# {"event":"ping.start","fields":null,"level":"info","ts":"..."}
# {"event":"ping.done","fields":{"latency_ms":1.2},"level":"info","ts":"..."}

# 3. Check the final job row + result.
curl -s "http://localhost:8000/jobs/$JOB_ID" | jq '{status, result, error_code}'
# {"status":"succeeded","result":{"echo":"pong"},"error_code":null}
```

That round-trip exercises the full machinery: HTTP enqueue → AMQP
publish → worker claim → operation execute → JobEvent stream → DB
commit → REST query. Real operations (`insert_proteins`,
`load_goa_annotations`, `compute_embeddings`, `predict_go_terms`)
are submitted the same way; their payloads are documented at
`/docs` (Swagger UI) and in the operation-catalog page of the
Sphinx docs.

Dispatching a dataset export (the first step before reranker training):

```bash
# POST /datasets enqueues export_research_dataset on protea.training.
curl -s -X POST http://localhost:8000/datasets \
  -H 'content-type: application/json' \
  -d '{"operation": "export_research_dataset",
       "payload": {"cell": "nk-mfo",
                   "train_versions": [160,200,210,215,220],
                   "test_versions": [230],
                   "k": 5,
                   "embedding_config_id": "<uuid>"}}'
```

Discovering the installed plugins:

```bash
curl -s http://localhost:8000/backends | jq '.plugins[].name'
# "ankh", "esm", "esm3c", "t5"

curl -s http://localhost:8000/sources | jq '.plugins[].name'
# "goa", "quickgo", "uniprot"

curl -s http://localhost:8000/runners | jq '.plugins[].name'
# "baseline", "knn", "lightgbm"
```

---

## Deployment

The same service set (api, workers, Postgres, RabbitMQ, optional MinIO, frontend) runs under **five deployment modes**; pick the entry point that matches your infrastructure. Per-mode assets live under [`deploy/`](deploy/README.md); the narrative guide is in the [docs](https://protea.readthedocs.io).

| Mode | Best for | Entry point |
|---|---|---|
| **Docker Compose** | Local development on a single host, fastest iteration | `docker compose up -d` |
| **Compose bundle** | Smoke test from pre-built images, laptop or CI | `docker compose -f docker-compose.bundle.yml --env-file .env.bundle up -d` |
| **Docker Swarm** | Multi-host production cluster without Kubernetes | `docker stack deploy -c deploy/swarm/stack.yml protea` |
| **Helm / Kubernetes** | Existing K8s cluster, GitOps-style rollouts | `helm install protea deploy/helm/protea/` |
| **SLURM** | HPC batch site, worker fleet on a scheduler | `sbatch deploy/slurm/<worker>.sbatch` |

For bare-metal development without a Docker daemon, `bash scripts/manage.sh start` runs the api, worker fleet, and frontend as supervised host processes (see [Getting started](#getting-started)).

---

## Documentation

Full documentation at **https://protea.readthedocs.io**

Topics covered: architecture, data model, operations, job lifecycle, deployment, how-to guides.

---

## Contributing

PROTEA is written and maintained by **Francisco Miguel Pérez Canales** (author and sole maintainer).
Contributions from research institutions and individual developers are welcome.
See [CONTRIBUTING.md](CONTRIBUTING.md) for the branching strategy and development workflow.

**Requirements:** Python 3.12, Docker (for integration tests)

```bash
poetry install --with lint,test       # add ,docs if you build Sphinx
poetry run pytest                     # unit tests
poetry run pytest --with-postgres     # integration tests
poetry run task lint                  # ruff
poetry run mypy protea                # type checking
```

> Default `poetry install` ships **CPU torch** (`pytorch-cpu` source) so CI
> runners and the slim production Docker image stay lean. GPU embedding
> workers run `bash scripts/install_gpu_torch.sh` after install to swap in
> the CUDA wheel.

---

## Stack

| Component | Technology |
|---|---|
| API | FastAPI + SQLAlchemy 2.x + PostgreSQL 16 + pgvector |
| Queue | RabbitMQ (pika) |
| Embeddings | ESMC (ESM SDK), ProstT5 / prot_t5_xl (T5Encoder), ESM2 (Hugging Face Transformers) |
| KNN search | FAISS IVFFlat / numpy (chunked brute-force) |
| Re-ranker | LightGBM (LambdaRank, IA-weighted samples) |
| Frontend | Next.js 19 + Tailwind 4.x |
| Deployment | Docker Compose, Docker Swarm (stack file), Helm chart skeleton, `scripts/manage.sh` process supervisor |
| Observability | OpenTelemetry SDK, OTLP export, Grafana dashboards |

---

<!-- protea-stack:start -->

## Repositories in the PROTEA stack

Single source of truth: [`docs/source/_data/stack.yaml`](https://github.com/frapercan/PROTEA/blob/develop/docs/source/_data/stack.yaml) in PROTEA. Run `python scripts/sync_stack.py` to regenerate this block.

| Repo | Role | Status | Summary |
|------|------|--------|---------|
| **PROTEA** (this repo) | Platform | `active` | Backend platform. Hosts the ORM, job queue, FastAPI surface, frontend, and orchestration. |
| [protea-contracts](https://github.com/frapercan/protea-contracts) | Contracts | `active` | Shared contract surface. ABCs, pydantic payloads, feature schema, schema_sha. Imported by every other repo. |
| [protea-method](https://github.com/frapercan/protea-method) | Inference | `active` | Pure inference path (KNN, feature compute, reranker apply). Delegation target for the F2C extraction; live in production since F2C.5b. Bind-mounted by the LAFA containers. |
| [protea-sources](https://github.com/frapercan/protea-sources) | Source plugin | `active` | Annotation source plugins (GOA, QuickGO, UniProt). Discovered via Python entry_points (goa, quickgo, uniprot). |
| [protea-runners](https://github.com/frapercan/protea-runners) | Runner plugin | `active` | Experiment runner plugins (LightGBM, KNN, baseline). Discovered via Python entry_points (lightgbm, knn, baseline). |
| [protea-backends](https://github.com/frapercan/protea-backends) | Backend plugin | `active` | Protein language model embedding backends (ESM family, T5/ProstT5, Ankh, ESM3-C). Discovered via Python entry_points (esm, t5, ankh, esm3c). |
| [protea-reranker-lab](https://github.com/frapercan/protea-reranker-lab) | Lab | `active` | LightGBM reranker training lab. Pulls datasets from PROTEA, trains boosters, publishes them back via /reranker-models/import-by-reference. |
| [cafaeval-protea](https://github.com/frapercan/cafaeval-protea) | Evaluator | `active` | Standalone fork of cafaeval (CAFA-evaluator-PK) with the PK-coverage fix and a bit-exact parity guarantee against the upstream. |

<!-- protea-stack:end -->

---

## License

Released into the public domain under the [Unlicense](LICENSE). You are free to copy, modify, publish, use, compile, sell, or distribute PROTEA for any purpose, commercial or non-commercial, without attribution.

---

## Acknowledgements

PROTEA is the natural evolution of two prior systems developed at **Ana Rojas' Lab (CBBIO)**, Andalusian Center for Developmental Biology (CSIC), in collaboration with **Rosa Fernández's Lab** (Metazoa Phylogenomics Lab, Institute of Evolutionary Biology, CSIC-UPF):

- [**Protein Information System (PIS)**](https://github.com/CBBIO/protein-information-system): Large-scale protein data extraction and management from UniProt, PDB, and GOA. PROTEA adopts and extends PIS's data model and ingestion pipelines with a clean architecture designed for scalability and collaborative development.

- [**FANTASIA**](https://github.com/CBBIO/fantasia): Functional annotation via protein language model embeddings and KNN transfer. PROTEA consolidates FANTASIA's prediction capabilities into a unified platform with a web interface, job queue, and REST API.

PROTEA was designed to unify and supersede both systems under a single, maintainable codebase, removing the tight coupling between infrastructure, orchestration, and domain logic that accumulated across those projects.

The evaluation pipeline and scoring methodology are directly informed by following the **CAFA** (Critical Assessment of protein Function Annotation) competition series. This benchmarking framework shaped PROTEA's prediction and evaluation architecture, including the integration of [cafaeval](https://github.com/claradepaolis/CAFA-evaluator-PK) for standardised GO term prediction assessment.
