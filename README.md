# PROTEA

**Protein annotation platform** for large-scale GO term prediction, sequence embedding, and functional analysis.

PROTEA provides a unified backend for ingesting protein data from UniProt, computing ESM2 embeddings, and predicting Gene Ontology terms via KNN transfer — with a full job queue, REST API, and web interface.

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
> Currently running on a personal research machine. Availability is best-effort — if it is unreachable, use the Docker setup below to run your own instance.

---

## What PROTEA does

| Capability | Details |
|---|---|
| **Protein ingestion** | Paginated UniProt REST API, MD5-deduplicated sequences |
| **GO ontology** | Load OBO snapshots, full DAG stored per release |
| **GO annotations** | Bulk import from GOA (GAF) and QuickGO (TSV) |
| **Embeddings** | ESM2 via GPU workers, stored as pgvector VECTOR columns |
| **GO prediction** | KNN transfer with optional NW/SW alignment and taxonomic features |
| **CAFA evaluation** | Benchmark pipeline with cafaeval integration |
| **Job queue** | RabbitMQ-backed, 7 queues, full audit trail per job |
| **REST API** | 21 FastAPI endpoints across 5 routers |
| **Web UI** | Next.js frontend with protein explorer, annotation viewer, prediction browser |

---

## Getting started

### Docker (recommended)

```bash
git clone https://github.com/frapercan/PROTEA.git
cd PROTEA
docker compose up
```

Services available at:
- Frontend: http://localhost:3000
- API: http://localhost:8000
- RabbitMQ management: http://localhost:15672 (guest/guest)

### From source

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

## Documentation

Full documentation at **https://protea.readthedocs.io**

Topics covered: architecture, data model, operations, job lifecycle, deployment, how-to guides.

---

## Contributing

Contributions from research institutions and individual developers are welcome.
See [CONTRIBUTING.md](CONTRIBUTING.md) for the branching strategy and development workflow.

**Requirements:** Python 3.12, Docker (for integration tests)

```bash
poetry install
poetry run pytest              # unit tests
poetry run pytest --with-postgres  # integration tests
poetry run task lint           # ruff + flake8 + mypy
```

---

## Stack

| Component | Technology |
|---|---|
| API | FastAPI + SQLAlchemy 2.x + PostgreSQL 16 + pgvector |
| Queue | RabbitMQ (pika) |
| Embeddings | ESM2 (Meta) via Hugging Face Transformers |
| KNN search | FAISS IVFFlat / numpy |
| Frontend | Next.js 19 + Tailwind v4 |
| Deployment | Docker, manage.sh, vast.ai GPU instances |

---

## Acknowledgements

PROTEA is the natural evolution of two prior systems developed at **Ana Rojas' Lab (CBBIO)**, Andalusian Center for Developmental Biology (CSIC), in collaboration with **Rosa Fernández's Lab** (Metazoa Phylogenomics Lab, Institute of Evolutionary Biology, CSIC-UPF):

- [**Protein Information System (PIS)**](https://github.com/CBBIO/protein-information-system) — Large-scale protein data extraction and management from UniProt, PDB, and GOA. PROTEA adopts and extends PIS's data model and ingestion pipelines with a clean architecture designed for scalability and collaborative development.

- [**FANTASIA**](https://github.com/CBBIO/fantasia) — Functional annotation via protein language model embeddings and KNN transfer. PROTEA consolidates FANTASIA's prediction capabilities into a unified platform with a web interface, job queue, and REST API.

PROTEA was designed to unify and supersede both systems under a single, maintainable codebase — removing the tight coupling between infrastructure, orchestration, and domain logic that accumulated across those projects.

The evaluation pipeline and scoring methodology are directly informed by our participation in **CAFA6** (Critical Assessment of protein Function Annotation, 6th edition). The competition provided real-world benchmarking experience that shaped PROTEA's prediction and evaluation architecture, including the integration of [cafaeval](https://github.com/claradepaolis/CAFA-evaluator-PK) for standardised GO term prediction assessment.
