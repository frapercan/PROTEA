System Overview
===============

Runtime stack
-------------

PROTEA runs as four cooperative processes managed by ``scripts/start_dev.sh``:

.. code-block:: text

   ┌─────────────────────────────────────────────────────────────┐
   │                        PROTEA Stack                         │
   │                                                             │
   │  Next.js (port 3000)  ──HTTP──▶  FastAPI (port 8000)       │
   │                                       │                    │
   │                                  publishes UUID            │
   │                                       │                    │
   │                                       ▼                    │
   │                                  RabbitMQ                  │
   │                              ┌────────────────┐            │
   │                              │  protea.ping   │            │
   │                              │  protea.jobs   │            │
   │                              └───────┬────────┘            │
   │                                      │                     │
   │                              Worker processes              │
   │                              (one per queue)               │
   │                                      │                     │
   │                                      ▼                     │
   │                                 PostgreSQL                 │
   └─────────────────────────────────────────────────────────────┘

Services and data stores
------------------------

**FastAPI (port 8000)**

   RESTful HTTP API. Handles job creation, status queries, event retrieval, and
   cancellation. On ``POST /jobs``, it creates a ``Job`` row in QUEUED status, commits,
   then publishes the job UUID to RabbitMQ. The session factory and AMQP URL are injected
   via ``app.state`` at startup, keeping the router free of global state.

**RabbitMQ (port 5672 / 15672)**

   Message broker. Queues carry only the job UUID — all state lives in PostgreSQL.
   Durable queues ensure messages survive broker restarts. One worker process per queue.

   .. list-table:: Queue routing
      :header-rows: 1

      * - Queue
        - Operations
      * - ``protea.ping``
        - ``ping``
      * - ``protea.jobs``
        - ``insert_proteins``, ``fetch_uniprot_metadata``

**Worker processes**

   Long-running Python processes consuming from a single queue. Each worker delegates
   job execution to ``BaseWorker.handle_job()``, which implements the two-session pattern
   (see :doc:`job_lifecycle`). Workers reconnect automatically on broker disconnection.

**PostgreSQL (port 5432)**

   Persistent store for all state. Holds job queues, event logs, protein sequences,
   and UniProt metadata. SQLAlchemy 2.x ORM with ``Mapped[]`` annotations.

**Next.js frontend (port 3000)**

   Single-page application for job management. Displays job list with status filtering,
   live auto-refresh (2 s polling while a job is active), progress bar, and structured
   event timeline. Built with React 19 and Tailwind CSS v4.

Code layout
-----------

.. code-block:: text

   protea/
     api/                 FastAPI application and routers
     core/
       contracts/         Operation protocol, ProteaPayload, OperationResult
       operations/        Domain logic (insert_proteins, fetch_uniprot_metadata, ping)
     infrastructure/
       orm/models/        SQLAlchemy 2.x ORM models
       queue/             RabbitMQ consumer and publisher (pika)
       database/          Engine factory
       session.py         session_scope context manager
       settings.py        YAML + env-var config loader
     workers/
       base_worker.py     Two-session job lifecycle orchestrator
   apps/
     web/                 Next.js frontend
   scripts/
     start_dev.sh         Starts all processes
     worker.py            Worker entry point
     init_db.py           Schema initialisation
     run_one_job.py       Manual job runner (debugging)
