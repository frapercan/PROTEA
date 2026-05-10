Installation and Quickstart
===========================

Prerequisites
-------------

Before starting PROTEA you need:

- **Python 3.12+** with `Poetry <https://python-poetry.org/>`_
- **PostgreSQL 16** (local or remote)
- **RabbitMQ 3.x** with the management plugin enabled
- **Node.js 20+** with ``npm`` (for the Next.js frontend)

Install dependencies
--------------------

.. code-block:: bash

   git clone <repo-url> PROTEA
   cd PROTEA
   poetry install                              # runtime only (slimmest install)
   poetry install --with lint,test,docs        # full local dev environment

The dev tooling is split into three optional Poetry groups so each CI
job installs only the packages it needs:

- ``--with lint``: ruff, mypy, type stubs, taskipy.
- ``--with test``: pytest, pytest-cov, httpx, uvicorn, plus
  ``protea-reranker-lab`` for parity tests.
- ``--with docs``: Sphinx, furo, sphinx-copybutton, sphinx-design,
  shibuya theme, sphinxcontrib-bibtex.

A bare ``poetry install`` no longer installs Sphinx or pytest; pick
the groups you need.

Optional extras:

.. code-block:: bash

   poetry install -E storage   # adds the 'minio' client for the
                               # MinIO artifact-store backend

The ``[storage]`` extra is only required when ``storage.backend: minio``
is set in ``system.yaml`` (or ``PROTEA_STORAGE_BACKEND=minio``). The
default local-filesystem backend works with the base install.

Configuration
-------------

Copy the example configuration and adjust for your environment:

.. code-block:: bash

   mkdir -p protea/config
   cat > protea/config/system.yaml <<EOF
   database:
     url: postgresql+psycopg://user:pass@localhost:5432/biodata

   queue:
     amqp_url: amqp://guest:guest@localhost:5672/
   EOF

.. note::
   ``system.yaml`` is not committed to version control. Do not store
   production credentials in the repository.

Environment variables ``PROTEA_DB_URL`` and ``PROTEA_AMQP_URL`` override the
YAML values and take precedence.

Frontend configuration:

.. code-block:: bash

   echo "NEXT_PUBLIC_API_URL=http://127.0.0.1:8000" > apps/web/.env.local

Bring up infrastructure
-----------------------

Postgres, RabbitMQ and MinIO run in ``docker compose``; the application
runs bare-metal (next section). The split keeps hot-reload natural for
the application while pinning infra versions.

.. code-block:: bash

   # Bring up postgres, rabbitmq and minio (the storage profile activates MinIO)
   docker compose --profile storage up -d postgres rabbitmq minio

   # Wait for healthchecks
   docker compose --profile storage ps

The MinIO console is then available at http://localhost:9001
(default credentials ``minioadmin`` / ``minioadmin``).

.. note::
   ``docker-compose.yml`` also declares ``api``, ``frontend`` and the
   worker services so that a single ``docker compose --profile storage
   up -d`` can run the entire platform in containers (production-style
   deployment, see ``docker-compose.prod.yml``). For dev work where you
   iterate on Python and Next.js code, leave those services down and
   use ``manage.sh`` instead so file changes hot-reload without an
   image rebuild.

Initialise the database
-----------------------

The Compose ``postgres`` service runs ``docker/init.sql`` at first
volume creation, which only enables the ``vector`` extension. Tables
are created either by ``init_db.py`` (fresh setup) or by Alembic
migrations (existing schema):

.. code-block:: bash

   # Fresh: create every table from SQLAlchemy metadata
   poetry run python scripts/init_db.py

   # Existing: bring schema up to head
   alembic upgrade head

If you are restoring from a backup instead, skip both — see
:doc:`runbook` "Disaster recovery" for the ``pg_restore`` procedure.

Start the application stack
---------------------------

.. code-block:: bash

   bash scripts/manage.sh start [N]   # N = batch workers per pipeline (default 1)

This starts all processes in the background and writes PIDs to ``logs/pids/``:

.. list-table::
   :header-rows: 1

   * - Process
     - Address
     - Log file
   * - FastAPI (uvicorn)
     - http://127.0.0.1:8000
     - ``logs/api.log``
   * - Worker: ``protea.ping``
     - n/a
     - ``logs/worker-ping.log``
   * - Worker: ``protea.jobs``
     - n/a
     - ``logs/worker-jobs.log``
   * - Worker: ``protea.training``
     - n/a
     - ``logs/worker-training.log``
   * - Worker: ``protea.embeddings`` (serialised coordinator)
     - n/a
     - ``logs/worker-embeddings-coord.log``
   * - Worker: ``protea.embeddings.batch`` (×N)
     - n/a
     - ``logs/worker-embeddings-batch-*.log``
   * - Worker: ``protea.embeddings.write``
     - n/a
     - ``logs/worker-embeddings-write.log``
   * - Worker: ``protea.predictions`` (serialised coordinator)
     - n/a
     - ``logs/worker-predictions-coord.log``
   * - Worker: ``protea.predictions.batch`` (×N)
     - n/a
     - ``logs/worker-predictions-batch-*.log``
   * - Worker: ``protea.predictions.write``
     - n/a
     - ``logs/worker-predictions-write.log``
   * - Worker: ``protea.evaluations``
     - n/a
     - ``logs/worker-evaluations.log``
   * - Stale job reaper (``reaper``)
     - n/a
     - ``logs/worker-reaper.log``
   * - Next.js frontend
     - http://127.0.0.1:3000
     - ``logs/frontend.log``

Stack management commands:

.. code-block:: bash

   bash scripts/manage.sh stop               # stop all processes
   bash scripts/manage.sh status             # show PID, RAM, running/dead per worker
   bash scripts/manage.sh logs [name]        # tail logs (interactive picker or name fragment)
   bash scripts/manage.sh scale <queue> [N]  # add N extra workers without restart

Verify the installation
-----------------------

Open http://127.0.0.1:3000 in a browser and submit a **ping** job from the
UI. The job should transition ``QUEUED → RUNNING → SUCCEEDED`` within a
second. The event timeline will show a ``ping.pong`` event.

Alternatively, use the API directly:

.. code-block:: bash

   curl -s -X POST http://127.0.0.1:8000/jobs \
     -H "Content-Type: application/json" \
     -d '{"operation":"ping","queue_name":"protea.ping","payload":{}}' | python -m json.tool

Expose to the internet
----------------------

To share PROTEA with an external reviewer (e.g. a supervisor) without a
public server, run:

.. code-block:: bash

   bash scripts/expose.sh

The script uses **ngrok** with a free static domain
(``protea.ngrok.app``).
It opens a single tunnel to the Next.js frontend (:3000).
API calls are transparently proxied through the frontend via the
``/api-proxy/:path*`` rewrite rule in ``apps/web/next.config.ts``, so the
API port (:8000) is never exposed directly.

**Prerequisites:**

1. Install ngrok: https://ngrok.com/download
2. Authenticate once: ``ngrok config add-authtoken <TOKEN>``

Press **Ctrl+C** to close the tunnel.

.. note::
   The stack must already be running (``bash scripts/manage.sh start``) before
   calling ``expose.sh``.

Run tests
---------

.. code-block:: bash

   # Unit tests (no external services required)
   poetry run pytest

   # Integration tests (pulls a pgvector/pg16 Docker image)
   poetry run pytest --with-postgres

   # Single test
   poetry run pytest tests/test_insert_proteins.py::TestInsertProteinsPayload -v
