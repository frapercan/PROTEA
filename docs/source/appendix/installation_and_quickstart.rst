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
   poetry install          # installs runtime + dev dependencies

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

Initialise the database
-----------------------

Run this once (or after a full DB reset) to create all tables:

.. code-block:: bash

   poetry run python scripts/init_db.py

To apply Alembic migrations instead:

.. code-block:: bash

   alembic upgrade head

Start the dev stack
-------------------

.. code-block:: bash

   bash scripts/start_dev.sh

This starts four processes in the background:

.. list-table::
   :header-rows: 1

   * - Process
     - Address
     - Log file
   * - FastAPI (uvicorn)
     - http://127.0.0.1:8000
     - ``logs/api.log``
   * - Worker — ``protea.ping``
     - —
     - ``logs/worker-ping.log``
   * - Worker — ``protea.jobs``
     - —
     - ``logs/worker-jobs.log``
   * - Next.js frontend
     - http://127.0.0.1:3000
     - ``logs/frontend.log``

Stop everything:

.. code-block:: bash

   pkill -f "uvicorn protea|scripts/worker.py|next dev"

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

Run tests
---------

.. code-block:: bash

   # Unit tests (no external services required)
   poetry run pytest

   # Integration tests (pulls a pgvector/pg16 Docker image)
   poetry run pytest --with-postgres

   # Single test
   poetry run pytest tests/test_insert_proteins.py::TestInsertProteinsPayload -v
