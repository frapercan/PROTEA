# PROTEA

**PROtein funcTional Embedding-based Annotation**. A distributed platform for large-scale GO term prediction, sequence embedding, and functional analysis.

PROTEA provides a unified backend for ingesting protein data from UniProt, computing protein language model embeddings (ESMC, ProstT5, ESM2), and predicting Gene Ontology terms via KNN transfer plus a learned LightGBM re-ranker, with a full job queue, REST API, and web interface.

[![Lint](https://github.com/frapercan/PROTEA/actions/workflows/lint.yml/badge.svg)](https://github.com/frapercan/PROTEA/actions/workflows/lint.yml)
[![Tests](https://github.com/frapercan/PROTEA/actions/workflows/test.yml/badge.svg)](https://github.com/frapercan/PROTEA/actions/workflows/test.yml)
[![Docs](https://github.com/frapercan/PROTEA/actions/workflows/docs.yml/badge.svg)](https://github.com/frapercan/PROTEA/actions/workflows/docs.yml)
[![Documentation](https://readthedocs.org/projects/protea/badge/?version=latest)](https://protea.readthedocs.io/en/latest/)
[![codecov](https://codecov.io/gh/frapercan/PROTEA/branch/main/graph/badge.svg)](https://codecov.io/gh/frapercan/PROTEA)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)

---

## Live demo

> **https://protea.ngrok.app**
>
> Currently running on a personal research machine. Availability is best-effort. If it is unreachable, use the Docker setup below to run your own instance.

---

## Why PROTEA?

PROTEA is the successor to [PIS](https://github.com/CBBIO/protein-information-system) and [FANTASIA](https://github.com/CBBIO/fantasia), rebuilt around three goals:

1. **Clean architecture**: infrastructure, orchestration, and domain logic are explicitly decoupled. Operations are pure domain logic; workers own sessions and queue state; routers expose HTTP. No more God-classes that mix everything.
2. **Learned re-ranking on top of KNN transfer**: beyond classical embedding-KNN annotation, PROTEA trains **LightGBM rerankers on temporal GOA splits** (LambdaRank + CAFA IA weighting, per-tier NK/LK/PK models). Candidates retrieved by KNN are re-scored with alignment, taxonomy, and retrieval features.
3. **Honest temporal evaluation**: benchmarking uses **temporal holdout deltas** between historical GOA releases (e.g. 220→229), evaluated with the official `cafaeval` library and information-accretion weighting, avoiding the optimistic leakage of random splits.

---

## What PROTEA does

| Capability | Details |
|---|---|
| **Protein ingestion** | Paginated UniProt REST API, MD5-deduplicated sequences |
| **GO ontology** | Load OBO snapshots, full DAG stored per release |
| **GO annotations** | Bulk import from GOA (GAF) and QuickGO (TSV) |
| **Embeddings** | ESMC, ProstT5, and ESM2 backends via GPU workers; stored as pgvector `VECTOR` columns |
| **GO prediction** | KNN transfer (FAISS IVFFlat / numpy) with optional NW/SW alignment and taxonomic features |
| **Learning-to-rank** | LightGBM rerankers trained on temporal GOA splits (LambdaRank + IA weighting, per-tier NK/LK/PK models) |
| **CAFA evaluation** | Benchmark pipeline with `cafaeval` integration, Fmax + IA-weighted scoring, per-aspect (BPO/MFO/CCO) results |
| **Job queue** | RabbitMQ-backed, 8 queues (ingestion, embeddings, predictions, training), full audit trail per job |
| **REST API** | FastAPI routers for jobs, proteins, embeddings, query sets, scoring, evaluation, and admin |
| **Web UI** | Next.js frontend with protein explorer, annotation viewer, prediction browser, and live job widget |

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

poetry run python scripts/init_db.py
bash scripts/manage.sh start
```

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

Discovering the installed plugins (added in F2B turn 36):

```bash
curl -s http://localhost:8000/backends | jq '.plugins[].name'
# "ankh", "esm", "esm3c", "t5"

curl -s http://localhost:8000/sources | jq '.plugins[].name'
# "goa", "quickgo", "uniprot"

curl -s http://localhost:8000/runners | jq '.plugins[].name'
# "baseline", "knn", "lightgbm"
```

---

## Documentation

Full documentation at **https://protea.readthedocs.io**

Topics covered: architecture, data model, operations, job lifecycle, deployment, how-to guides.

---

## Contributing

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
| Frontend | Next.js 19 + Tailwind v4 |
| Deployment | Docker Compose, `scripts/manage.sh` process supervisor |

---

<!-- protea-stack:start -->

## Repositories in the PROTEA stack

Single source of truth: [`docs/source/_data/stack.yaml`](https://github.com/frapercan/PROTEA/blob/develop/docs/source/_data/stack.yaml) in PROTEA. Run `python scripts/sync_stack.py` to regenerate this block.

| Repo | Role | Status | Summary |
|------|------|--------|---------|
| **PROTEA** (this repo) | Platform | `active` | Backend platform. Hosts the ORM, job queue, FastAPI surface, frontend, and orchestration. |
| [protea-contracts](https://github.com/frapercan/protea-contracts) | Contracts | `beta` | Shared contract surface. ABCs, pydantic payloads, feature schema, schema_sha. Imported by every other repo. |
| [protea-method](https://github.com/frapercan/protea-method) | Inference | `skeleton` | Pure inference path (KNN, feature compute, reranker apply). Target of the F2C extraction. Bind-mounted by the LAFA containers. |
| [protea-sources](https://github.com/frapercan/protea-sources) | Source plugin | `skeleton` | Annotation source plugins (GOA, QuickGO, UniProt). Discovered via Python entry_points. |
| [protea-runners](https://github.com/frapercan/protea-runners) | Runner plugin | `skeleton` | Experiment runner plugins (LightGBM lab, KNN baseline, future GNN). Discovered via Python entry_points. |
| [protea-backends](https://github.com/frapercan/protea-backends) | Backend plugin | `skeleton` | Protein language model embedding backends (ESM family, T5/ProstT5, Ankh, ESM3-C). Discovered via Python entry_points. |
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
