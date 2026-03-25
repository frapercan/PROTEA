HTTP API
========

The PROTEA HTTP API is a FastAPI application that exposes eleven routers.
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
prediction results as a tab-separated file (32 columns including re-ranker
features) using ``StreamingResponse`` with ``yield_per(1000)``, avoiding
loading the full result set into memory.

.. automodule:: protea.api.routers.embeddings
   :members:
   :undoc-members:
   :show-inheritance:

Scoring router
--------------

The ``/scoring`` router provides endpoints for training and applying LightGBM
re-ranker models. The re-ranker is a binary classifier trained on temporal
holdout data: predictions made with annotations at time t0 are labeled against
ground truth derived from t1 annotations.

Key endpoints:

- ``GET /scoring/prediction-sets/{id}/training-data.tsv`` — generates a
  31-column TSV with binary labels from temporal ground truth, suitable for
  LightGBM training.
- ``POST /scoring/rerankers/train`` — trains a LightGBM model from a
  PredictionSet + EvaluationSet pair and stores it in the DB.
- ``GET /scoring/rerankers`` / ``GET /scoring/rerankers/{id}`` / ``DELETE`` —
  CRUD for trained re-ranker models.
- ``GET /scoring/prediction-sets/{id}/rerank.tsv`` — applies a trained
  re-ranker to a prediction set, streaming re-scored predictions.
- ``GET /scoring/prediction-sets/{id}/reranker-metrics`` — computes CAFA-style
  Fmax and AUC-PR using re-ranker probability scores.

.. automodule:: protea.api.routers.scoring
   :members:
   :undoc-members:
   :show-inheritance:
   :no-index:

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

Annotate router
---------------

The ``/annotate`` router provides a one-click annotation endpoint. It accepts
a FASTA file (or raw text), auto-selects the best available embedding config,
annotation set, and ontology snapshot, creates a ``QuerySet``, and queues a
``compute_embeddings`` job. Returns all the IDs the frontend needs to chain
``predict_go_terms`` once embeddings finish.

.. automodule:: protea.api.routers.annotate
   :members:
   :undoc-members:
   :show-inheritance:

Maintenance router
------------------

The ``/maintenance`` router provides housekeeping endpoints for identifying
and removing orphaned data. Two pairs of preview/execute endpoints handle
orphan sequences (not referenced by any ``Protein`` or ``QuerySetEntry``) and
unindexed embeddings (for sequences not referenced by any ``Protein``).
Preview endpoints are read-only; execute endpoints perform the actual deletion.

.. automodule:: protea.api.routers.maintenance
   :members:
   :undoc-members:
   :show-inheritance:

Admin router
------------

The ``/admin`` router exposes destructive administrative operations.
Currently provides ``POST /admin/reset-db``, which drops and recreates
the public schema and re-applies all Alembic migrations. Protected by a
bearer token (``PROTEA_ADMIN_TOKEN`` environment variable).

.. automodule:: protea.api.routers.admin
   :members:
   :undoc-members:
   :show-inheritance:

Showcase router
---------------

The ``/showcase`` router aggregates platform statistics and best evaluation
results for the landing page. Returns protein counts, embedding counts,
prediction counts, best Fmax per aspect per evaluation category (NK/LK/PK),
and a method comparison table — all in a single JSON response.

.. automodule:: protea.api.routers.showcase
   :members:
   :undoc-members:
   :show-inheritance:

Support router
--------------

The ``/support`` router handles community feedback. ``GET /support`` returns
the total thumbs-up count and recent comments. ``POST /support`` submits a
new thumbs-up with an optional comment (max 500 characters).

.. automodule:: protea.api.routers.support
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

   * -
     - **Health**
     -
   * - ``GET``
     - ``/health``
     - Liveness probe — returns 200 if the API process is up.
   * - ``GET``
     - ``/health/ready``
     - Readiness probe — verifies database and RabbitMQ connections.

   * -
     - **Jobs**
     -
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

   * -
     - **Proteins**
     -
   * - ``GET``
     - ``/proteins/stats``
     - Aggregate protein statistics (total, canonical, reviewed, organisms).
   * - ``GET``
     - ``/proteins``
     - List proteins with pagination; filter by ``organism`` / ``reviewed``.
   * - ``GET``
     - ``/proteins/{accession}``
     - Retrieve a single protein with its UniProt metadata.
   * - ``GET``
     - ``/proteins/{accession}/annotations``
     - List GO annotations for a protein across all annotation sets.

   * -
     - **Annotations**
     -
   * - ``GET``
     - ``/annotations/snapshots``
     - List ontology snapshots with GO term counts per aspect.
   * - ``GET``
     - ``/annotations/snapshots/{id}``
     - Retrieve a snapshot with its full list of GO terms.
   * - ``PATCH``
     - ``/annotations/snapshots/{id}/ia-url``
     - Set the InterPro Archive URL on an ontology snapshot.
   * - ``POST``
     - ``/annotations/snapshots/load``
     - Queue a ``load_ontology_snapshot`` job.
   * - ``GET``
     - ``/annotations/snapshots/{id}/subgraph``
     - BFS ancestor subgraph for a given set of GO term IDs.
   * - ``GET``
     - ``/annotations/sets``
     - List annotation sets with protein GO annotation counts.
   * - ``GET``
     - ``/annotations/sets/{id}``
     - Retrieve a single annotation set with summary statistics.
   * - ``DELETE``
     - ``/annotations/sets/{id}``
     - Delete an annotation set and all its annotations.
   * - ``POST``
     - ``/annotations/sets/load-goa``
     - Queue a ``load_goa_annotations`` job.
   * - ``POST``
     - ``/annotations/sets/load-quickgo``
     - Queue a ``load_quickgo_annotations`` job.
   * - ``POST``
     - ``/annotations/evaluation-sets/generate``
     - Queue a ``generate_evaluation_set`` job.
   * - ``GET``
     - ``/annotations/evaluation-sets``
     - List evaluation sets with summary statistics.
   * - ``GET``
     - ``/annotations/evaluation-sets/{id}``
     - Get evaluation set details.
   * - ``DELETE``
     - ``/annotations/evaluation-sets/{id}``
     - Delete an evaluation set.
   * - ``GET``
     - ``/annotations/evaluation-sets/{id}/ground-truth-NK.tsv``
     - Download NK ground truth in CAFA format.
   * - ``GET``
     - ``/annotations/evaluation-sets/{id}/ground-truth-LK.tsv``
     - Download LK ground truth in CAFA format.
   * - ``GET``
     - ``/annotations/evaluation-sets/{id}/ground-truth-PK.tsv``
     - Download PK ground truth in CAFA format.
   * - ``GET``
     - ``/annotations/evaluation-sets/{id}/known-terms.tsv``
     - Download known terms from old annotation set (for PK evaluation).
   * - ``GET``
     - ``/annotations/evaluation-sets/{id}/delta-proteins.fasta``
     - Download delta proteins as FASTA.
   * - ``POST``
     - ``/annotations/evaluation-sets/{id}/run``
     - Queue a ``run_cafa_evaluation`` job.
   * - ``GET``
     - ``/annotations/evaluation-sets/{id}/results``
     - List evaluation results for an evaluation set.
   * - ``GET``
     - ``/annotations/evaluation-sets/{id}/results/{rid}/metrics.tsv``
     - Download evaluation metrics as TSV.
   * - ``GET``
     - ``/annotations/evaluation-sets/{id}/results/{rid}/artifacts.zip``
     - Download all cafaeval artifacts as a zip.
   * - ``DELETE``
     - ``/annotations/evaluation-sets/{id}/results/{rid}``
     - Delete an evaluation result.

   * -
     - **Embeddings**
     -
   * - ``GET``
     - ``/embeddings/configs``
     - List all embedding configurations.
   * - ``POST``
     - ``/embeddings/configs``
     - Create a new (immutable) embedding configuration.
   * - ``GET``
     - ``/embeddings/configs/{id}``
     - Retrieve an embedding configuration by UUID.
   * - ``DELETE``
     - ``/embeddings/configs/{id}``
     - Delete an embedding configuration.
   * - ``POST``
     - ``/embeddings/predict``
     - Queue a ``predict_go_terms`` job.
   * - ``GET``
     - ``/embeddings/prediction-sets``
     - List prediction sets with entry counts.
   * - ``GET``
     - ``/embeddings/prediction-sets/{id}``
     - Retrieve a prediction set with summary statistics.
   * - ``GET``
     - ``/embeddings/prediction-sets/{id}/proteins``
     - List proteins in a prediction set.
   * - ``GET``
     - ``/embeddings/prediction-sets/{id}/proteins/{accession}``
     - Get predictions for one protein.
   * - ``GET``
     - ``/embeddings/prediction-sets/{id}/go-terms``
     - GO term distribution in a prediction set.
   * - ``GET``
     - ``/embeddings/prediction-sets/{id}/predictions.tsv``
     - Stream all predictions as TSV (filtered by accession / aspect / distance).
   * - ``GET``
     - ``/embeddings/prediction-sets/{id}/predictions-cafa.tsv``
     - Download predictions in CAFA submission format.
   * - ``DELETE``
     - ``/embeddings/prediction-sets/{id}``
     - Delete a prediction set.

   * -
     - **Scoring**
     -
   * - ``GET``
     - ``/scoring/configs``
     - List scoring configurations.
   * - ``POST``
     - ``/scoring/configs``
     - Create a scoring configuration.
   * - ``POST``
     - ``/scoring/configs/presets``
     - Create preset scoring configurations.
   * - ``GET``
     - ``/scoring/configs/{id}``
     - Retrieve a scoring configuration.
   * - ``DELETE``
     - ``/scoring/configs/{id}``
     - Delete a scoring configuration.
   * - ``GET``
     - ``/scoring/prediction-sets/{id}/score.tsv``
     - Stream scored predictions as TSV.
   * - ``GET``
     - ``/scoring/prediction-sets/{id}/metrics``
     - Compute CAFA-style metrics for scored predictions.
   * - ``GET``
     - ``/scoring/prediction-sets/{id}/training-data.tsv``
     - Export labeled training data for the re-ranker.
   * - ``POST``
     - ``/scoring/rerankers/train``
     - Train a LightGBM re-ranker from a PredictionSet + EvaluationSet.
   * - ``GET``
     - ``/scoring/rerankers``
     - List all trained re-ranker models.
   * - ``GET``
     - ``/scoring/rerankers/{id}``
     - Retrieve a re-ranker model's metadata, metrics, and feature importance.
   * - ``DELETE``
     - ``/scoring/rerankers/{id}``
     - Delete a trained re-ranker model.
   * - ``GET``
     - ``/scoring/prediction-sets/{id}/rerank.tsv``
     - Apply a re-ranker to a prediction set and stream re-scored TSV.
   * - ``GET``
     - ``/scoring/prediction-sets/{id}/reranker-metrics``
     - Compute CAFA Fmax and AUC-PR using re-ranker scores.

   * -
     - **Query Sets**
     -
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

   * -
     - **Annotate**
     -
   * - ``POST``
     - ``/annotate``
     - One-click annotation: upload FASTA, auto-run the full pipeline.

   * -
     - **Maintenance**
     -
   * - ``GET``
     - ``/maintenance/vacuum-sequences/preview``
     - Count orphan sequences (preview).
   * - ``POST``
     - ``/maintenance/vacuum-sequences``
     - Delete orphan sequences.
   * - ``GET``
     - ``/maintenance/vacuum-embeddings/preview``
     - Count unindexed embeddings (preview).
   * - ``POST``
     - ``/maintenance/vacuum-embeddings``
     - Delete unindexed embeddings.

   * -
     - **Admin**
     -
   * - ``POST``
     - ``/admin/reset-db``
     - Drop and recreate the public schema (requires admin token).

   * -
     - **Showcase**
     -
   * - ``GET``
     - ``/showcase``
     - Platform statistics and best evaluation results.

   * -
     - **Support**
     -
   * - ``GET``
     - ``/support``
     - Total thumbs-up count and recent comments.
   * - ``POST``
     - ``/support``
     - Submit a thumbs-up with optional comment.

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
       "compute_taxonomy": false,
       "compute_reranker_features": false
     }
   }
