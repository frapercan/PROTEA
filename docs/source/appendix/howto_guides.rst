How-to Guides
=============

Submit a job via the API
------------------------

Every job requires ``operation``, ``queue_name``, and an optional ``payload``
dict. The ``payload`` must match the fields expected by the target operation's
``ProteaPayload`` subclass.

Example — insert Swiss-Prot human proteins:

.. code-block:: bash

   curl -s -X POST http://127.0.0.1:8000/jobs \
     -H "Content-Type: application/json" \
     -d '{
       "operation": "insert_proteins",
       "queue_name": "protea.jobs",
       "payload": {
         "search_criteria": "reviewed:true AND organism_id:9606",
         "page_size": 500,
         "include_isoforms": true
       }
     }' | python -m json.tool

The response contains the job UUID:

.. code-block:: json

   {"id": "3fa85f64-5717-4562-b3fc-2c963f66afa6", "status": "queued"}

Fetch UniProt metadata for existing proteins
---------------------------------------------

Run ``fetch_uniprot_metadata`` with the same query used during ingestion.
The operation uses ``canonical_accession`` as the upsert key so it is safe
to re-run at any time.

.. code-block:: bash

   curl -s -X POST http://127.0.0.1:8000/jobs \
     -H "Content-Type: application/json" \
     -d '{
       "operation": "fetch_uniprot_metadata",
       "queue_name": "protea.jobs",
       "payload": {
         "search_criteria": "reviewed:true AND organism_id:9606",
         "page_size": 200,
         "commit_every_page": true,
         "update_protein_core": true
       }
     }'

Monitor job progress
--------------------

Poll the job status endpoint:

.. code-block:: bash

   curl -s http://127.0.0.1:8000/jobs/<job-id> | python -m json.tool

Stream the event timeline:

.. code-block:: bash

   curl -s http://127.0.0.1:8000/jobs/<job-id>/events | python -m json.tool

The frontend at http://127.0.0.1:3000 auto-refreshes every 2 seconds while
a job is active and renders the event timeline in chronological order.

Cancel a queued job
-------------------

.. code-block:: bash

   curl -s -X POST http://127.0.0.1:8000/jobs/<job-id>/cancel

Jobs in terminal states (``SUCCEEDED``, ``FAILED``) are unaffected —
the endpoint is a no-op. Cancelling a ``RUNNING`` job marks the DB row as
``CANCELLED`` but does not interrupt the worker process (soft cancel).

Run a job manually without RabbitMQ
-------------------------------------

Useful for debugging a specific job without the full message-broker pipeline:

.. code-block:: bash

   poetry run python scripts/run_one_job.py <job-id-uuid>

The script loads the job from the DB and runs it through ``BaseWorker``
directly. The job must already exist in QUEUED status (created via the API
before calling this script, for example).

Add a new operation
--------------------

1. Create ``protea/core/operations/my_operation.py``:

   .. code-block:: python

      from protea.core.contracts.operation import (
          EmitFn, Operation, OperationResult, ProteaPayload
      )
      from sqlalchemy.orm import Session
      from typing import Any

      class MyPayload(ProteaPayload, frozen=True):
          some_param: str

      class MyOperation(Operation):
          name = "my_operation"

          def execute(
              self, session: Session, payload: dict[str, Any], *, emit: EmitFn
          ) -> OperationResult:
              p = MyPayload.model_validate(payload)
              emit("my_operation.start", None, {"param": p.some_param}, "info")
              # ... domain logic ...
              return OperationResult(result={"done": True})

2. Register it in the worker entry point (``scripts/worker.py``):

   .. code-block:: python

      from protea.core.operations.my_operation import MyOperation
      registry.register(MyOperation())

3. Route jobs to the appropriate queue (``protea.jobs`` or a new dedicated queue).

No changes to ``BaseWorker``, the FastAPI router, or the DB schema are needed.

Generate and apply a database migration
-----------------------------------------

After modifying an ORM model, generate an Alembic migration:

.. code-block:: bash

   alembic revision --autogenerate -m "add my_column to protein"
   alembic upgrade head

Always review auto-generated migrations before applying them to production.
Alembic's ``autogenerate`` detects column additions and removals but may miss
index changes or server-default modifications.

Build the documentation locally
--------------------------------

.. code-block:: bash

   poetry run task html_docs
   # or directly:
   cd docs && poetry run sphinx-build -b html source build/html

Open ``docs/build/html/index.html`` in a browser.
