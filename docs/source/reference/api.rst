HTTP API
========

.. contents:: On this page
   :local:
   :depth: 2

The PROTEA HTTP API is a FastAPI application that exposes seventeen routers.
All state mutations flow through this layer: it writes ``Job`` rows to
PostgreSQL and publishes messages to RabbitMQ. The API is stateless between
requests — the session factory and AMQP URL are injected via ``app.state``
at startup, keeping every router free of global state and infrastructure
imports.

All endpoints return JSON. Error responses follow the **RFC 7807
``application/problem+json``** shape (T4.4 / D4): every error body
includes ``type`` (relative URI under ``/problems/{slug}``,
e.g. ``/problems/not-found``), ``title`` (short stable summary),
``status`` (mirror of the HTTP code), and an optional ``detail`` +
``instance`` (request URI). Validation errors carry an extra
``errors`` array with the offending field paths. Existing route code
keeps raising ``HTTPException`` exactly as before — only the wire
format changed. Timestamps are ISO 8601 UTC strings. UUID identifiers
are lowercase hyphenated strings.

Versioning and the ``/v1/`` prefix
----------------------------------

Every router is mounted twice (T4.1, decision D4):

- **Canonical** under ``/v1/`` — the prefix surfaced in OpenAPI /
  Swagger and the only path schema exporters and codegen tools see.
  All new clients should target this form.
- **Legacy alias** at the root path — the same handler reachable
  without a prefix, ``include_in_schema=False`` so OpenAPI does *not*
  advertise it. This exists for the deprecation window so existing
  frontend, CLI, and CI traffic keeps working without a coordinated
  cutover.

The endpoint paths in the per-router sections and the *Endpoints
summary* below are listed without the prefix for terseness; both
``/jobs`` and ``/v1/jobs`` resolve to the same handler today. Health
endpoints (``/health``, ``/health/ready``) stay at the root by
convention. When the legacy aliases are retired the second
``include_router`` call in
``protea.api.app._register_routers`` will be removed; this page is
the source of truth for that timing.

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

The ``/scoring`` router exposes scoring configurations, the training-data
export, and read-only endpoints for applying LightGBM re-ranker models.
In-process re-ranker training was retired in F0/T0.6: boosters are now
trained offline in ``protea-reranker-lab`` and registered through the
:ref:`reranker-models-router` (``POST /reranker-models/import``).

Key endpoints:

- ``GET /scoring/prediction-sets/{id}/training-data.tsv`` — generates a
  31-column TSV with binary labels from temporal ground truth, consumed
  by ``protea-reranker-lab`` to fit a booster.
- ``GET /scoring/rerankers`` / ``GET /scoring/rerankers/{id}`` /
  ``DELETE /scoring/rerankers/{id}`` — read/delete operations for
  registered re-ranker models. Creation lives at
  ``POST /reranker-models/import``.
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

Benchmark router
----------------

The ``/benchmark`` router powers the per-PLM comparison grid in the UI.
Where ``/showcase`` collapses every model into a few buckets and reports
the maximum, this router preserves *which* embedding produced each number
and *which* scoring config was used, exposing one stage per distinct
``ScoringConfig.name`` plus an implicit ``"reranker"`` stage for evaluations
that used a re-ranker. Stage labels, GO categories, and the baseline tag are
read from ``protea/config/benchmark.yaml`` — no hardcoded constants.

.. automodule:: protea.api.routers.benchmark
   :members:
   :undoc-members:
   :show-inheritance:

Datasets router
---------------

The ``/datasets`` router is the registry for frozen re-ranker training
datasets. ``POST /datasets`` enqueues an ``export_research_dataset`` job
that runs the KNN + feature pipeline, publishes the
``train.parquet`` / ``eval.parquet`` / ``manifest.json`` triple to the
configured ``ArtifactStore`` (local FS or MinIO), and inserts a
``Dataset`` row once the upload completes. ``GET /datasets`` and
``GET /datasets/{id_or_name}`` expose the registry to
``protea-reranker-lab``'s ``pull_dataset.py`` and to UI consumers.

.. automodule:: protea.api.routers.datasets
   :members:
   :undoc-members:
   :show-inheritance:

Registry router
---------------

The ``/backends``, ``/sources``, and ``/runners`` endpoints list the plugins
discovered at runtime via ``importlib.metadata.entry_points`` for the three
plugin groups: embedding backends, annotation sources, and experiment
runners. The router is intentionally stateless — it re-scans entry points
on every call rather than caching, so a worker that has just been restarted
with a newly-installed extra surfaces in the next request without an API
restart.

.. automodule:: protea.api.routers.registry
   :members:
   :undoc-members:
   :show-inheritance:

.. _reranker-models-router:

Reranker models router
----------------------

The ``/reranker-models`` router accepts boosters trained offline in
``protea-reranker-lab`` (or any compatible trainer) and registers them
in PROTEA. ``POST /reranker-models/import`` is the multipart flow:
the lab sends ``model.txt`` + ``spec.yaml`` + ``run.json`` inline and
the server uploads ``model.txt`` to the artifact store under
``rerankers/<run_id>/``. ``POST /reranker-models/import-by-reference``
is the production flow: the lab pre-uploads ``model.txt`` to MinIO under
its own key and posts JSON with ``artifact_uri`` + ``run_json`` +
``spec_yaml``. Both flows share ``_register_model`` so the resulting
``RerankerModel`` row is identical.

.. automodule:: protea.api.routers.reranker_models
   :members:
   :undoc-members:
   :show-inheritance:

Stack router
------------

The ``/stack`` router exposes metadata about the eight-repo PROTEA stack
to the UI. ``GET /stack`` returns the registry from
``docs/source/_data/stack.yaml``. ``GET /stack/pulls`` aggregates open
pull requests across every repo in the stack via the GitHub REST API and
caches the result in-process to stay under the unauthenticated 60 req/h
rate limit (set ``PROTEA_GITHUB_TOKEN`` to lift to 5000 req/h).

.. automodule:: protea.api.routers.stack
   :members:
   :undoc-members:
   :show-inheritance:

Experiment runs router
----------------------

The ``/experiment-runs`` router exposes CRUD over the
``ExperimentRun`` ORM (T4.7-T4.9, decision D11). One row aggregates
multiple ``Job`` / ``EvaluationResult`` / ``RerankerModel`` rows
under a unique human ``name`` and carries the narrative trio
(``description`` / ``hypothesis`` / ``findings``) plus JSONB
``config`` / ``provenance`` and ``Text[]`` ``tags``.
``PATCH /experiment-runs/{run_id}`` accepts partial updates; status
transitions stamp ``started_at`` (on ``planned → running``) and
``finished_at`` (on ``running → done`` or ``→ abandoned``)
idempotently — re-entering a state never resets its timestamp.

.. automodule:: protea.api.routers.experiment_runs
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
       Cursor pagination (T4.2): pass ``after=<created_at>`` to walk forward
       past the limit.
   * - ``GET``
     - ``/jobs/{id}``
     - Retrieve a single job with full payload and meta.
   * - ``GET``
     - ``/jobs/{id}/events``
     - Retrieve the event timeline for a job (up to 2 000 events).
       Cursor pagination (T4.2): pass ``after=<ts>`` to walk forward.
   * - ``POST``
     - ``/jobs/{id}/cancel``
     - Transition a ``QUEUED`` or ``RUNNING`` job to ``CANCELLED``.
   * - ``DELETE``
     - ``/jobs/{id}``
     - Delete a job that is not in ``RUNNING`` status.
   * - ``POST``
     - ``/jobs/{id}/comments``
     - Append a ``JobComment`` (T3.10 / D11). Body fields: ``body``
       (required, non-empty), ``author`` (optional). Returns 201.
   * - ``GET``
     - ``/jobs/{id}/comments``
     - List the ``JobComment`` thread chronologically (``created_at``
       ASC, ``id`` ASC tiebreaker). Cursor pagination (T4.2): pass
       ``after=<created_at>`` to walk forward past the limit.

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
     - Set the Information Accretion (IA) file URL on an ontology snapshot.
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

   * -
     - **Benchmark**
     -
   * - ``GET``
     - ``/benchmark/embeddings``
     - List embedding configs with persisted display metadata.
   * - ``GET``
     - ``/benchmark/matrix``
     - Per-embedding / per-stage Fmax matrix across all evaluation results.

   * -
     - **Datasets**
     -
   * - ``POST``
     - ``/datasets``
     - Enqueue an ``export_research_dataset`` job.
   * - ``GET``
     - ``/datasets``
     - List registered re-ranker datasets.
   * - ``GET``
     - ``/datasets/{id_or_name}``
     - Get a dataset by id or name.

   * -
     - **Plugin Registry**
     -
   * - ``GET``
     - ``/backends``
     - List installed embedding-backend plugins.
   * - ``GET``
     - ``/sources``
     - List installed annotation-source plugins.
   * - ``GET``
     - ``/runners``
     - List installed experiment-runner plugins.

   * -
     - **Reranker Models**
     -
   * - ``POST``
     - ``/reranker-models/import``
     - Import a lab-trained booster (multipart).
   * - ``POST``
     - ``/reranker-models/import-by-reference``
     - Import a booster already uploaded to the artifact store (JSON).

   * -
     - **Stack**
     -
   * - ``GET``
     - ``/stack``
     - Return the eight-repo PROTEA stack registry.
   * - ``GET``
     - ``/stack/pulls``
     - Aggregate open pull requests across every repo in the stack.

   * -
     - **Experiment Runs**
     -
   * - ``POST``
     - ``/experiment-runs``
     - Create an ``ExperimentRun`` (T4.7). Body: ``name`` required +
       optional narrative trio + status + JSONB / tags.
   * - ``GET``
     - ``/experiment-runs``
     - List experiment runs newest-first; filter by ``status`` (T4.8).
       Cursor pagination (T4.2): pass ``after=<created_at>`` from the
       previous page's last row.
   * - ``GET``
     - ``/experiment-runs/{run_id}``
     - Retrieve one experiment run.
   * - ``PATCH``
     - ``/experiment-runs/{run_id}``
     - Partial update (T4.9). Status transitions stamp
       ``started_at`` / ``finished_at`` idempotently.
   * - ``DELETE``
     - ``/experiment-runs/{run_id}``
     - Delete an experiment run (returns 204).

Request body for ``POST /jobs``
--------------------------------

The ``operation`` and ``queue_name`` fields are required. ``payload`` is
passed verbatim to the operation's ``execute`` method after Pydantic
validation; its schema depends on the operation. ``meta`` is stored on
the ``Job`` row and never interpreted by the API. ``description`` and
``tags`` are optional D11 narrative fields surfaced on the
``GET /jobs`` and ``GET /jobs/{id}`` responses; they let any caller
attach human intent and ad-hoc grouping tokens at submission time
without round-tripping through a separate metadata endpoint.

.. code-block:: json

   {
     "operation": "insert_proteins",
     "queue_name": "protea.jobs",
     "payload": {
       "search_criteria": "reviewed:true AND organism_id:9606"
     },
     "meta": {},
     "description": "Backfill reviewed Swiss-Prot for benchmark-v1",
     "tags": ["ablation", "benchmark-v1"]
   }

Common payload examples by operation:

.. code-block:: json

   { "operation": "fetch_uniprot_metadata",  "queue_name": "protea.jobs",
     "payload": { "search_criteria": "reviewed:true AND organism_id:9606" } }

.. code-block:: json

   { "operation": "compute_embeddings", "queue_name": "protea.embeddings",
     "payload": { "embedding_config_id": "<uuid>", "sequences_per_job": 64 } }

.. code-block:: json

   { "operation": "predict_go_terms", "queue_name": "protea.predictions",
     "payload": {
       "embedding_config_id": "<uuid>",
       "annotation_set_id": "<uuid>",
       "ontology_snapshot_id": "<uuid>",
       "query_set_id": "<uuid>",
       "limit_per_entry": 5
     }
   }

.. seealso::

   - :doc:`/architecture/operations` — every operation referenced in a
     payload, with field-level documentation.
   - :doc:`/appendix/howto_guides` — concrete ``curl`` recipes that submit
     each endpoint end-to-end.
   - :doc:`/architecture/job_lifecycle` — how the API turns a request into a
     persistent ``Job`` row and a queue message.
