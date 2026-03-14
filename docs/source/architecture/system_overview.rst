System Overview
===============

Requirements and design goals
------------------------------

The design of PROTEA is governed by five requirements derived from the
limitations of its predecessors (PIS and FANTASIA):

**R1 — Reproducibility**
   A prediction produced today must be exactly reproducible in the future.
   This requires recording the ontology version, reference annotation set, and
   embedding model configuration used for every prediction run.

**R2 — Scalability**
   The system must handle reference sets of hundreds of thousands of proteins
   and query sets of thousands without holding all data in memory simultaneously.

**R3 — Separation of concerns**
   Domain logic (what to compute), execution flow (how jobs are dispatched and
   tracked), and infrastructure (database, message queue) must be independently
   replaceable.

**R4 — Observability**
   Every job must produce a structured audit trail so that failures can be
   diagnosed without replaying the computation.

**R5 — Accessibility**
   Researchers without machine-learning infrastructure expertise must be able
   to submit sequences and retrieve predictions through a web interface or a
   REST API.

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
   │                         │  protea.embeddings      │ coordinator     │
   │                         │  protea.embeddings.batch│ ephemeral       │
   │                         │  protea.embeddings.write│ ephemeral       │
   │                         │  protea.predictions.batch│ ephemeral      │
   │                         │  protea.predictions.write│ ephemeral      │
   │                         └───────────┬─────────────┘                 │
   │                                     │                               │
   │                             Worker processes                        │
   │                          (one or more per queue)                    │
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

   Message broker. Standard queues carry the job UUID — all state lives in PostgreSQL.
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
          ``compute_embeddings`` (coordinator), ``predict_go_terms`` (coordinator)
      * - ``protea.embeddings``
        - QueueConsumer
        - ``compute_embeddings`` coordinator (serialised: one at a time, 60 s retry delay if GPU busy)
      * - ``protea.embeddings.batch``
        - OperationConsumer
        - ``compute_embeddings_batch`` — GPU inference per batch (ephemeral, no DB Job row)
      * - ``protea.embeddings.write``
        - OperationConsumer
        - ``store_embeddings`` — bulk pgvector insert (ephemeral, no DB Job row)
      * - ``protea.predictions.batch``
        - OperationConsumer
        - ``predict_go_terms_batch`` — KNN search + GO transfer (ephemeral, no DB Job row)
      * - ``protea.predictions.write``
        - OperationConsumer
        - ``store_predictions`` — bulk GOPrediction insert (ephemeral, no DB Job row)

**QueueConsumer vs OperationConsumer**

   Two consumer patterns exist in ``protea/infrastructure/queue/consumer.py``:

   - **QueueConsumer** — reads a job UUID from the queue, delegates to ``BaseWorker.handle_job()``.
     Creates a full Job row with status transitions and event log.
   - **OperationConsumer** — reads a raw operation payload from the queue and executes it directly.
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

**Next.js frontend (port 3000)**

   Single-page application for job management. Displays job list with status filtering,
   live auto-refresh (2 s polling while a job is active), progress bar, and structured
   event timeline. Built with React 19 and Tailwind CSS v4.

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
                          query_sets, maintenance, admin
     core/
       contracts/         Operation protocol, ProteaPayload, OperationResult
       operations/        Domain logic (all 9 operations)
       knn_search.py      KNN backends: numpy brute-force and FAISS (Flat/IVFFlat/HNSW)
       feature_engineering.py  Alignment (parasail NW/SW) and taxonomy (ete3 NCBITaxa)
       utils.py           UniProtHttpMixin, chunks(), utcnow()
     infrastructure/
       orm/models/        SQLAlchemy 2.x ORM models (protein, sequence, annotation,
                          embedding, prediction, query, job)
       queue/             RabbitMQ consumer (QueueConsumer, OperationConsumer) and publisher
       session.py         session_scope context manager
       settings.py        YAML + env-var config loader
     workers/
       base_worker.py     Two-session job lifecycle orchestrator
   apps/
     web/                 Next.js frontend
   scripts/
     manage.sh            Unified stack manager (start/stop/status/logs/scale)
     worker.py            Worker entry point (registers all operations)
     init_db.py           Schema initialisation
     run_one_job.py       Manual job runner (debugging)

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
     - Pydantic v2
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
     - —
   * - Frontend
     - Next.js 19 + Tailwind v4
     - 19 / 4
   * - Dependency management
     - Poetry
     - 1.x

All Python dependencies are declared in ``pyproject.toml`` with pinned version
ranges; ``poetry.lock`` guarantees reproducible installs. The ``dev`` dependency
group adds pytest, pytest-cov, and related tooling without affecting production.

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
