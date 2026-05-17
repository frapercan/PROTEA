System Overview
===============

Requirements and design goals
------------------------------

The design of PROTEA is governed by five requirements derived from the
limitations of its predecessors (PIS and FANTASIA):

**R1. Reproducibility.**
   A prediction produced today must be exactly reproducible in the future.
   This requires recording the ontology version, reference annotation set, and
   embedding model configuration used for every prediction run.

**R2. Scalability.**
   The system must handle reference sets of hundreds of thousands of proteins
   and query sets of thousands without holding all data in memory simultaneously.

**R3. Separation of concerns.**
   Domain logic (what to compute), execution flow (how jobs are dispatched and
   tracked), and infrastructure (database, message queue) must be independently
   replaceable.

**R4. Observability.**
   Every job must produce a structured audit trail so that failures can be
   diagnosed without replaying the computation.

**R5. Accessibility.**
   Researchers without machine-learning infrastructure expertise must be able
   to submit sequences and retrieve predictions through a web interface or a
   REST API.

Four-layer architecture
-----------------------

PROTEA is structured in four horizontal layers with strict downward dependency:

.. code-block:: text

   ┌─────────────────────────────────────────────────────────────────────────┐
   │  PRESENTATION LAYER                                                      │
   │  Next.js SPA (port 3000)   ·   REST clients   ·   LAFA containers       │
   │  (lafa_knn_v1, lafa_knn_8plm, lafa_v18 — each wraps protea-predict)     │
   └──────────────────────────────────────┬──────────────────────────────────┘
                                          │  HTTP (port 8000)
   ┌──────────────────────────────────────▼──────────────────────────────────┐
   │  API LAYER                                                               │
   │  FastAPI   /v1/jobs   /v1/datasets   /v1/reranker-models     │
   │  /v1/scoring   /v1/auth   /v1/stack   + 11 more routers      │
   │  Auth gate: ApiKey header (T5.6a) · Bearer JWT (T5.6b, active)           │
   │  Rate limiting: slowapi per-principal (T5.6b, active)                    │
   └──────────────────────────────────────┬──────────────────────────────────┘
                                          │  publishes job UUID to queue
   ┌──────────────────────────────────────▼──────────────────────────────────┐
   │  WORKER LAYER                                                            │
   │  RabbitMQ queues                     Worker processes (one+ per queue)   │
   │  ┌──────────────────────────┐        BaseWorker (QueueConsumer) or       │
   │  │ protea.ping              │        OperationConsumer                   │
   │  │ protea.jobs              │                                            │
   │  │ protea.training          │ coord  predict_go_terms_batch delegates to │
   │  │ protea.embeddings        │ coord  protea_method.pipeline.predict()    │
   │  │ protea.embeddings.batch  │ eph.   (pure inference library, F2C.5b)    │
   │  │ protea.embeddings.write  │ eph.                                       │
   │  │ protea.predictions       │ coord  OperationRegistry (15 operations)   │
   │  │ protea.predictions.batch │ eph.   11 job-backed + 4 ephemeral         │
   │  │ protea.predictions.write │ eph.                                       │
   │  │ protea.evaluations       │                                            │
   │  └──────────────────────────┘                                            │
   └──────────────────────────────────────┬──────────────────────────────────┘
                                          │  SQLAlchemy 2.x ORM
   ┌──────────────────────────────────────▼──────────────────────────────────┐
   │  DATA LAYER                                                              │
   │  PostgreSQL 16 + pgvector (embeddings storage only; KNN on CPU)          │
   │  ArtifactStore: local FS (file://) or MinIO (s3://) for blobs            │
   │  (train.parquet, eval.parquet, manifest.json, booster model.txt)         │
   └─────────────────────────────────────────────────────────────────────────┘

Runtime stack
-------------

PROTEA runs as a set of cooperative processes managed by ``scripts/manage.sh``:

.. code-block:: text

   ┌──────────────────────────────────────────────────────────────────────┐
   │                          PROTEA Stack                                │
   │                                                                      │
   │  Next.js (port 3000)  ──HTTP──▶  FastAPI (port 8000)                │
   │                                        │                            │
   │                                   publishes UUID / payload          │
   │                                        │                            │
   │                                        ▼                            │
   │                                   RabbitMQ                          │
   │                         ┌─────────────────────────┐                 │
   │                         │  protea.ping            │                 │
   │                         │  protea.jobs            │                 │
   │                         │  protea.training        │ coordinator     │
   │                         │  protea.embeddings      │ coordinator     │
   │                         │  protea.embeddings.batch│ ephemeral       │
   │                         │  protea.embeddings.write│ ephemeral       │
   │                         │  protea.predictions     │ coordinator     │
   │                         │  protea.predictions.batch│ ephemeral      │
   │                         │  protea.predictions.write│ ephemeral      │
   │                         │  protea.evaluations     │                 │
   │                         └───────────┬─────────────┘                 │
   │                                     │                               │
   │                             Worker processes                        │
   │                          (one or more per queue)                    │
   │                          predict_go_terms_batch delegates           │
   │                          KNN + feature compute to                   │
   │                          protea_method.pipeline.predict() (F2C.5b)  │
   │                                     │                               │
   │                                     ▼                               │
   │                                PostgreSQL + pgvector                │
   └──────────────────────────────────────────────────────────────────────┘

Services and data stores
------------------------

**FastAPI (port 8000)**

   RESTful HTTP API. Handles job creation, status queries, event retrieval, and
   cancellation. On ``POST /jobs``, it creates a ``Job`` row in QUEUED status, commits,
   then publishes the job UUID to RabbitMQ. The session factory and AMQP URL are injected
   via ``app.state`` at startup, keeping the router free of global state.

**RabbitMQ (port 5672 / 15672)**

   Message broker. Standard queues carry the job UUID; all state lives in PostgreSQL.
   Ephemeral batch queues carry the full operation payload (no DB row per message).
   Durable queues ensure messages survive broker restarts.

   .. list-table:: Queue routing
      :header-rows: 1

      * - Queue
        - Consumer type
        - Operations
      * - ``protea.ping``
        - QueueConsumer
        - ``ping``
      * - ``protea.jobs``
        - QueueConsumer
        - ``insert_proteins``, ``fetch_uniprot_metadata``, ``load_ontology_snapshot``,
          ``load_goa_annotations``, ``load_quickgo_annotations``,
          ``generate_evaluation_set``
      * - ``protea.training``
        - QueueConsumer
        - ``export_research_dataset``: serialised, GPU/RAM-intensive KNN + feature
          generation + artifact-store upload. LightGBM training itself has been
          moved to ``protea-reranker-lab`` and no longer runs inside PROTEA.
      * - ``protea.embeddings``
        - QueueConsumer
        - ``compute_embeddings`` coordinator (serialised: one at a time, 60 s retry delay if GPU busy)
      * - ``protea.embeddings.batch``
        - OperationConsumer
        - ``compute_embeddings_batch``: GPU inference per batch (ephemeral, no DB Job row)
      * - ``protea.embeddings.write``
        - OperationConsumer
        - ``store_embeddings``: bulk pgvector insert (ephemeral, no DB Job row)
      * - ``protea.predictions``
        - QueueConsumer
        - ``predict_go_terms`` coordinator (serialised; fans out KNN batches)
      * - ``protea.predictions.batch``
        - OperationConsumer
        - ``predict_go_terms_batch``: KNN search + GO transfer (ephemeral, no DB Job row)
      * - ``protea.predictions.write``
        - OperationConsumer
        - ``store_predictions``: bulk GOPrediction insert (ephemeral, no DB Job row)
      * - ``protea.evaluations``
        - QueueConsumer
        - ``run_cafa_evaluation``: runs ``cafaeval`` for NK/LK/PK against a
          prediction set; serialised because cafaeval is single-process and
          each run can take minutes

**QueueConsumer vs OperationConsumer**

   Two consumer patterns exist in ``protea/infrastructure/queue/consumer.py``:

   - **QueueConsumer.** Reads a job UUID from the queue, delegates to ``BaseWorker.handle_job()``.
     Creates a full Job row with status transitions and event log.
   - **OperationConsumer.** Reads a raw operation payload from the queue and executes it directly.
     Used for high-throughput batch workers where creating thousands of child Job rows would cause
     queue bloat. Progress is tracked at the parent level only.

**Worker processes**

   Long-running Python processes, one per queue. Launched and managed by ``scripts/manage.sh``.
   Workers reconnect automatically on broker disconnection and can be scaled horizontally:

   .. code-block:: bash

      bash scripts/manage.sh scale protea.predictions.batch 2   # add 2 more batch workers

**PostgreSQL + pgvector (port 5432)**

   Persistent store for all state. Holds job queues, event logs, protein sequences,
   UniProt metadata, GO ontologies, annotation sets, sequence embeddings (pgvector),
   and GO predictions. SQLAlchemy 2.x ORM with ``Mapped[]`` annotations.

   .. note::
      pgvector is used only for **storage** of embeddings (VECTOR type columns).
      KNN search is performed in Python using numpy or FAISS, never at the DB layer.
      See :ref:`knn-constraint` in the howto guides.

**Artifact store (local FS by default, optional MinIO)**

   Large produced blobs (re-ranker boosters, exported research datasets
   ``train.parquet`` / ``eval.parquet`` / ``manifest.json``) do not live
   in PostgreSQL. They are written through the ``ArtifactStore`` protocol
   defined in ``protea/infrastructure/storage/``. Two backends are
   available:

   - **LocalFsArtifactStore** (default): blobs land under
     ``storage/artifacts/`` on the API host. URIs are ``file:///…``.
   - **MinioArtifactStore** (optional): an S3-compatible object store.
     Activated by setting ``storage.backend: minio`` in ``system.yaml``
     (or ``PROTEA_STORAGE_BACKEND=minio``) and starting the compose
     profile: ``docker compose --profile storage up``. URIs are
     ``s3://<bucket>/<key>``.

   Both backends satisfy the same four-method protocol (``put``, ``get``,
   ``url``, ``exists``), so operation code is agnostic of which backend
   is active. If MinIO is configured but unreachable at startup the
   factory logs a warning and degrades to the local FS; a missing
   optional service never crashes the stack.

**Next.js frontend (port 3000)**

   Single-page application for job management. Displays job list with status filtering,
   live auto-refresh (2 s polling while a job is active), progress bar, and structured
   event timeline. Built with React 19 and Tailwind CSS 4.x.

Stack management
----------------

All processes are managed through ``scripts/manage.sh``:

.. code-block:: bash

   bash scripts/manage.sh start [N]          # start full stack (N batch workers per pipeline)
   bash scripts/manage.sh stop               # stop all processes
   bash scripts/manage.sh status             # show PID, RAM, running/dead per worker
   bash scripts/manage.sh logs [name]        # tail logs (interactive picker or name fragment)
   bash scripts/manage.sh scale <queue> [N]  # add N extra workers to a queue without restart

Logs are written to ``logs/<name>.log``. PIDs are tracked in ``logs/pids/``.

Code layout
-----------

.. code-block:: text

   protea/
     api/                 FastAPI application and routers
       routers/           jobs, proteins, annotations, embeddings,
                          query_sets, maintenance, admin, scoring,
                          annotate, showcase, support, benchmark,
                          datasets, registry, reranker_models, stack,
                          experiment_runs   (17 routers total)
     core/
       contracts/         Operation protocol, ProteaPayload, OperationResult
       operations/        Domain logic (11 operation modules, 15 registered instances)
       knn_search.py      KNN backends: numpy brute-force and FAISS (Flat/IVFFlat/HNSW)
       feature_engineering.py  Alignment (parasail NW/SW) and taxonomy (ete3 NCBITaxa)
       scoring.py         Scoring engine (weighted formulas, composite scores)
       metrics.py         CAFA-style Fmax, precision, recall, coverage
       evidence_codes.py  ECO→GO evidence code mapping
       evaluation.py      CAFA5 evaluation protocol (NK/LK/PK delta)
       reranker.py        LightGBM binary classifier for re-ranking predictions
       utils.py           UniProtHttpMixin, chunks(), utcnow()
     infrastructure/
       orm/models/        SQLAlchemy 2.x ORM models (protein, sequence, annotation,
                          embedding, prediction, query, job, evaluation, scoring,
                          dataset, reranker_model, support, experiment_run, visitor_event)
       queue/             RabbitMQ consumer (QueueConsumer, OperationConsumer) and publisher
       logging.py         Structured JSON logging
       session.py         session_scope context manager
       settings.py        YAML + env-var config loader
     workers/
       base_worker.py     Two-session job lifecycle orchestrator
       stale_job_reaper.py  Periodic cleanup of stuck RUNNING jobs
   apps/
     web/                 Next.js frontend
   scripts/
     manage.sh            Unified stack manager (start/stop/status/logs/scale)
     worker.py            Worker entry point (registers all 15 operations)
     init_db.py           Schema initialisation

Technology stack
----------------

.. list-table::
   :header-rows: 1

   * - Component
     - Technology
     - Version
   * - API framework
     - FastAPI
     - 0.115+
   * - ORM / migrations
     - SQLAlchemy 2.0 + Alembic
     - 2.0 / 1.13
   * - Database
     - PostgreSQL 16 + pgvector
     - 16 / 0.7
   * - Message broker
     - RabbitMQ + aio-pika
     - 3.x / 9.x
   * - Data validation
     - Pydantic (2.x line)
     - 2.x
   * - Protein LM inference
     - Hugging Face Transformers
     - 4.x
   * - Alignment
     - parasail-python (BLOSUM62)
     - 1.x
   * - Taxonomy
     - ete3 + NCBITaxa
     - 3.x
   * - ANN search
     - NumPy / FAISS
     - n/a
   * - Frontend
     - Next.js + React + Tailwind
     - 16 / 19 / 4
   * - Dependency management
     - Poetry
     - 1.x

All Python dependencies are declared in ``pyproject.toml`` with pinned version
ranges; ``poetry.lock`` guarantees reproducible installs. The ``dev`` dependency
group adds pytest, pytest-cov, and related tooling without affecting production.

Re-ranker lab integration
--------------------------

Re-ranker model development is deliberately split into a **separate
repository** (``protea-reranker-lab``) consumed by PROTEA through a
narrow, contract-first interface. PROTEA does not import lab training
code at runtime, and the lab does not import PROTEA session or queue
code. The coupling is mediated by three files:

- **Frozen dataset**: PROTEA writes ``train.parquet``, ``eval.parquet``,
  and ``manifest.json`` (schema version 2) to the configured
  ``ArtifactStore`` via the ``export_research_dataset`` operation.
- **Booster artefact**: the lab produces ``runs/<name>/model.txt``
  (LightGBM ``Booster``) together with ``run.json`` and ``spec.yaml``.
- **Shared contract**: the lab module
  ``protea_reranker_lab.contracts`` exposes ``ManifestV1`` and
  ``compute_feature_schema_sha(feature_families)``; PROTEA imports
  ``compute_feature_schema_sha`` at predict time only, to validate
  feature compatibility.

.. code-block:: text

   ┌──────────────────────┐        export_research_dataset        ┌────────────────────────┐
   │       PROTEA         │───────────────────────────────────────▶│       Artifact          │
   │ (KNN + features)     │     train.parquet / eval.parquet        │       Store             │
   │                      │     manifest.json (schema_version=2)    │  (local FS or MinIO)    │
   └──────────┬───────────┘                                         └──────────┬──────────────┘
              │                                                                 │
              │                                                                 ▼
              │                                                     ┌──────────────────────┐
              │                                                     │ protea-reranker-lab  │
              │                                                     │  trains LightGBM     │
              │                                                     │  booster offline     │
              │                                                     └──────────┬───────────┘
              │                                                                 │
              │                 scripts/register_reranker.py ─ uploads ─┐       │
              │                                                          ▼      │
              │                                                     ┌──────────────────────┐
              │                                                     │  RerankerModel row   │
              │                                                     │  artifact_uri,       │
              │                                                     │  feature_schema_sha  │
              │                                                     └──────────┬───────────┘
              │                                                                 │
              ▼                                                                 │
   predict_go_terms ─ payload.reranker_model_id ──────────────────────────────▶─┘

At predict time, ``predict_go_terms`` accepts an optional
``reranker_model_id``. The coordinator snapshots the booster's
``artifact_uri`` and ``feature_schema_sha`` into every batch payload;
each batch worker re-computes a live schema sha from the active
feature flags and applies the booster **only** when the shas match
exactly. Strict equality is intentional: a subset match would silently
score the booster with missing columns, so mismatch fails safe and the
batch continues with KNN distance ordering.

Testing strategy
----------------

The test suite is split into two categories:

**Unit tests**
   Run with plain ``pytest``. Mock external services (HTTP, RabbitMQ) and use
   minimal fixtures. Cover operation logic, alignment and taxonomy utilities,
   FASTA parsing, and API router behaviour. Currently **283 tests passing**
   across 17 test files; coverage enforced at 70 % by ``pytest-cov``.

**Integration tests**
   Run with ``pytest --with-postgres``. The ``conftest.py`` fixture pulls a
   ``pgvector/pgvector:pg16`` Docker image, initialises the schema, and tears
   down the container after the session. These tests exercise the full
   round-trip from job submission to database state.

.. code-block:: bash

   poetry run pytest                   # unit tests only
   poetry run pytest --with-postgres   # full suite including integration tests

.. seealso::

   - :doc:`job_lifecycle`: how a single job moves through the worker layer.
   - :doc:`data_model`: the relational tables that back every layer above.
   - :doc:`operations`: the units of domain logic dispatched by workers.
   - :doc:`/adr/index`: design decisions behind the layering above.
