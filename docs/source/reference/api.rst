HTTP API
========

The PROTEA HTTP API is a FastAPI application that exposes six routers.
All state mutations flow through this layer: it writes ``Job`` rows to
PostgreSQL and publishes messages to RabbitMQ. The API is stateless between
requests — the session factory and AMQP URL are injected via ``app.state``
at startup, keeping every router free of global state and infrastructure
imports.

All endpoints return JSON. Error responses follow FastAPI's default
``{"detail": "..."}`` format. Timestamps are ISO 8601 UTC strings.
UUID identifiers are lowercase hyphenated strings.

Application factory
-------------------

``protea.api.app`` creates the FastAPI application, registers all routers,
and wires the session factory and AMQP URL into ``app.state`` at startup.
It also configures CORS and mounts any static middleware.

.. automodule:: protea.api.app
   :members:
   :undoc-members:
   :show-inheritance:

Jobs router
-----------

The ``/jobs`` router is the primary interface for job lifecycle management.
Jobs are created by ``POST /jobs`` with an ``operation`` name, a
``queue_name``, and an optional JSON ``payload``. The API creates a ``Job``
row in ``QUEUED`` status, commits, then publishes the UUID to RabbitMQ —
in that order, so workers always find the row before they try to claim it.

Job status and the structured event timeline can be polled via
``GET /jobs/{id}`` and ``GET /jobs/{id}/events`` respectively. The frontend
uses 2-second polling on the events endpoint to render a live progress
timeline.

.. automodule:: protea.api.routers.jobs
   :members:
   :undoc-members:
   :show-inheritance:

Proteins router
---------------

The ``/proteins`` router provides read access to the protein and sequence
catalogue. Proteins are not created directly through this router — they are
inserted asynchronously by the ``insert_proteins`` operation. The router
exposes list and detail endpoints with filtering by organism and review
status.

.. automodule:: protea.api.routers.proteins
   :members:
   :undoc-members:
   :show-inheritance:

Annotations router
------------------

The ``/annotations`` router exposes the GO ontology and annotation set data.
It provides:

- Ontology snapshot listing and detail, including GO term counts per aspect.
- Annotation set listing and detail.
- A BFS ancestor subgraph endpoint (``GET /annotations/snapshots/{id}/subgraph``)
  that returns the ancestor closure for a given set of GO term IDs within a
  snapshot. Used by the frontend to render the GO hierarchy for a prediction
  result.

.. automodule:: protea.api.routers.annotations
   :members:
   :undoc-members:
   :show-inheritance:

Embeddings router
-----------------

The ``/embeddings`` router manages embedding configurations and prediction
sets. Embedding configurations are immutable recipes: once created, they
can be referenced by any number of embedding computation and prediction
jobs. Creating a new configuration with different parameters produces a
new UUID, preserving reproducibility.

Prediction sets are created by submitting a ``predict_go_terms`` job and
are queryable once the job completes. The
``GET /embeddings/prediction-sets/{id}/predictions.tsv`` endpoint streams
prediction results as a tab-separated file using ``StreamingResponse`` with
``yield_per(1000)``, avoiding loading the full result set into memory.

.. automodule:: protea.api.routers.embeddings
   :members:
   :undoc-members:
   :show-inheritance:

Query sets router
-----------------

The ``/query-sets`` router handles user-uploaded FASTA files. On
``POST /query-sets``, the server parses the multipart upload, creates a
``QuerySet`` row, upserts one ``Sequence`` row per unique amino-acid string
(deduplicating by MD5 hash), and creates ``QuerySetEntry`` rows preserving
the original FASTA headers. The returned query set ID can then be referenced
in ``compute_embeddings`` and ``predict_go_terms`` job payloads.

.. automodule:: protea.api.routers.query_sets
   :members:
   :undoc-members:
   :show-inheritance:

Endpoints summary
-----------------

.. list-table::
   :header-rows: 1
   :widths: 8 42 50

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
   * - ``GET``
     - ``/proteins``
     - List proteins with pagination; filter by ``organism`` / ``reviewed``.
   * - ``GET``
     - ``/proteins/{accession}``
     - Retrieve a single protein with its UniProt metadata.
   * - ``GET``
     - ``/annotations/snapshots``
     - List ontology snapshots with GO term counts per aspect.
   * - ``GET``
     - ``/annotations/snapshots/{id}``
     - Retrieve a snapshot with its full list of GO terms.
   * - ``GET``
     - ``/annotations/snapshots/{id}/subgraph``
     - BFS ancestor subgraph for a given set of GO term IDs.
   * - ``GET``
     - ``/annotations/sets``
     - List annotation sets with protein GO annotation counts.
   * - ``GET``
     - ``/annotations/sets/{id}``
     - Retrieve a single annotation set with summary statistics.
   * - ``GET``
     - ``/embeddings/configs``
     - List all embedding configurations.
   * - ``POST``
     - ``/embeddings/configs``
     - Create a new (immutable) embedding configuration.
   * - ``GET``
     - ``/embeddings/configs/{id}``
     - Retrieve an embedding configuration by UUID.
   * - ``GET``
     - ``/embeddings/prediction-sets``
     - List prediction sets with entry counts.
   * - ``GET``
     - ``/embeddings/prediction-sets/{id}``
     - Retrieve a prediction set with summary statistics.
   * - ``GET``
     - ``/embeddings/prediction-sets/{id}/predictions``
     - List GO predictions for a set (paginated JSON).
   * - ``GET``
     - ``/embeddings/prediction-sets/{id}/predictions.tsv``
     - Stream all predictions as a TSV file (27 columns, filtered by accession / aspect / distance).
   * - ``POST``
     - ``/query-sets``
     - Upload a FASTA file and create a ``QuerySet``.
   * - ``GET``
     - ``/query-sets``
     - List all query sets with entry counts.
   * - ``GET``
     - ``/query-sets/{id}``
     - Retrieve a query set with its full entry list.
   * - ``DELETE``
     - ``/query-sets/{id}``
     - Delete a query set and all its entries.

Request body for ``POST /jobs``
--------------------------------

The ``operation`` and ``queue_name`` fields are required. ``payload`` is
passed verbatim to the operation's ``execute`` method after Pydantic
validation; its schema depends on the operation. ``meta`` is stored on
the ``Job`` row and never interpreted by the API.

.. code-block:: json

   {
     "operation": "insert_proteins",
     "queue_name": "protea.jobs",
     "payload": {
       "search_criteria": "reviewed:true AND organism_id:9606"
     },
     "meta": {}
   }

Common payload examples by operation:

.. code-block:: json

   { "operation": "fetch_uniprot_metadata",  "queue_name": "protea.jobs",
     "payload": { "accessions": ["P04637", "P53350"] } }

.. code-block:: json

   { "operation": "compute_embeddings", "queue_name": "protea.embeddings",
     "payload": { "embedding_config_id": "<uuid>", "batch_size": 64 } }

.. code-block:: json

   { "operation": "predict_go_terms", "queue_name": "protea.jobs",
     "payload": {
       "embedding_config_id": "<uuid>",
       "annotation_set_id": "<uuid>",
       "ontology_snapshot_id": "<uuid>",
       "query_set_id": "<uuid>",
       "k": 5,
       "compute_alignments": false,
       "compute_taxonomy": false
     }
   }
