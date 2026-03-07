Workers
=======

Base worker
-----------

.. automodule:: protea.workers.base_worker
   :members:
   :undoc-members:
   :show-inheritance:

Worker entry points
-------------------

Workers are started by ``scripts/worker.py``. Each worker process consumes
from a single RabbitMQ queue and delegates job execution to
``BaseWorker.handle_job()``.

.. code-block:: bash

   # Start the jobs worker manually (normally managed by start_dev.sh)
   poetry run python scripts/worker.py protea.jobs

   # Run a single queued job by UUID (bypass RabbitMQ)
   poetry run python scripts/run_one_job.py <job-id>

The ``run_one_job.py`` script is intended for debugging. It loads the job
from the DB, executes it through ``BaseWorker``, and exits. No RabbitMQ
connection is needed.
