HTTP API
========

Application factory
-------------------

.. automodule:: protea.api.app
   :members:
   :undoc-members:
   :show-inheritance:

Jobs router
-----------

.. automodule:: protea.api.routers.jobs
   :members:
   :undoc-members:
   :show-inheritance:

Endpoints summary
-----------------

.. list-table::
   :header-rows: 1
   :widths: 10 35 55

   * - Method
     - Path
     - Description
   * - ``POST``
     - ``/jobs``
     - Create a job and publish its UUID to RabbitMQ.
   * - ``GET``
     - ``/jobs``
     - List jobs; filter by ``status`` and/or ``operation``. Max 500 rows.
   * - ``GET``
     - ``/jobs/{id}``
     - Retrieve a single job with full payload and meta.
   * - ``GET``
     - ``/jobs/{id}/events``
     - Retrieve the event timeline for a job (up to 2 000 events).
   * - ``POST``
     - ``/jobs/{id}/cancel``
     - Transition a ``QUEUED`` or ``RUNNING`` job to ``CANCELLED``.
   * - ``DELETE``
     - ``/jobs/{id}``
     - Delete a job that is not in ``RUNNING`` status.

Request body for ``POST /jobs``
--------------------------------

.. code-block:: json

   {
     "operation": "insert_proteins",
     "queue_name": "protea.jobs",
     "payload": {
       "search_criteria": "reviewed:true AND organism_id:9606"
     },
     "meta": {}
   }

``operation`` and ``queue_name`` are required. ``payload`` defaults to ``{}``
if omitted. ``meta`` is stored as-is in ``Job.meta`` and not interpreted by
the API layer.
