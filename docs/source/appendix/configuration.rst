Configuration Reference
========================

PROTEA loads its configuration from two sources, merged in this order
(later entries win):

1. ``protea/config/system.yaml`` — file-based defaults
2. Environment variables — runtime overrides

YAML structure
--------------

.. code-block:: yaml

   database:
     url: postgresql+psycopg://user:pass@host:5432/dbname

   queue:
     amqp_url: amqp://guest:guest@localhost:5672/

   storage:
     artifacts_dir: storage/evaluation_artifacts   # cafaeval output
     backend: local                                 # "local" | "minio"
     root: storage/artifacts                        # local backend root
     minio:
       endpoint: localhost:9000
       bucket: protea
       access_key: minioadmin
       secret_key: minioadmin
       secure: false                                # true for HTTPS

   admin:
     token: protea-admin

Only ``database.url`` and ``queue.amqp_url`` are strictly required; the
``storage``, ``admin`` sections have working defaults. The file is loaded
by ``protea.infrastructure.settings.load_settings(project_root)`` at
startup.

The ``storage`` block drives the ``ArtifactStore`` abstraction described
in :doc:`/reference/infrastructure`. With ``backend: local`` (default)
all blobs land under ``storage/artifacts/`` on the API host. Setting
``backend: minio`` activates the S3-compatible path — requires the
``[storage]`` extra (``pip install 'protea[storage]'``) and a running
MinIO instance (see ``docker compose --profile storage up``). Paths
under ``storage.*`` are resolved relative to the project root when not
absolute.

Environment variable overrides
------------------------------

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Variable
     - Description
   * - ``PROTEA_DB_URL``
     - Overrides ``database.url``. Must be a valid SQLAlchemy connection
       string using the ``postgresql+psycopg`` driver.
   * - ``PROTEA_AMQP_URL``
     - Overrides ``queue.amqp_url``. Standard AMQP URL format.
   * - ``PROTEA_ARTIFACTS_DIR``
     - Overrides ``storage.artifacts_dir`` (the ``cafaeval`` artefacts
       directory used by ``run_cafa_evaluation``).
   * - ``PROTEA_STORAGE_BACKEND``
     - Overrides ``storage.backend`` — ``local`` (default) or ``minio``.
   * - ``PROTEA_STORAGE_ROOT``
     - Overrides ``storage.root`` (local backend root directory).
   * - ``PROTEA_MINIO_ENDPOINT``
     - Overrides ``storage.minio.endpoint`` (e.g. ``localhost:9000``).
   * - ``PROTEA_MINIO_BUCKET``
     - Overrides ``storage.minio.bucket``.
   * - ``PROTEA_MINIO_ACCESS_KEY``
     - Overrides ``storage.minio.access_key``.
   * - ``PROTEA_MINIO_SECRET_KEY``
     - Overrides ``storage.minio.secret_key``.
   * - ``PROTEA_MINIO_SECURE``
     - Overrides ``storage.minio.secure`` — truthy enables HTTPS.
   * - ``PROTEA_ADMIN_TOKEN``
     - Overrides ``admin.token``.
   * - ``PROTEA_REF_CACHE_DIR``
     - Directory for the on-disk KNN reference cache (embedding +
       annotation matrices keyed by ``(embedding_config_id,
       annotation_set_id)``). Defaults to ``data/ref_cache``. Read by
       :mod:`protea.core.disk_cache`.
   * - ``PROTEA_PCA_ARTIFACTS_DIR``
     - Directory for per-PLM PCA projection states (``.npz``) used to
       pre-compute the ``emb_pca`` feature family. Defaults to
       ``protea/artifacts/pca``. Read by :mod:`protea.core.pca_cache`.
   * - ``PROTEA_GITHUB_TOKEN``
     - GitHub token used by ``GET /stack/pulls`` to lift the
       unauthenticated 60 req/h rate limit to 5000 req/h. ``GITHUB_TOKEN``
       and ``GH_TOKEN`` are honoured as fallbacks (in that order). Any
       value is accepted as long as the GitHub REST API recognises it.
   * - ``PROTEA_METHOD_NUMPY_QUERY_CHUNK``
     - Per-chunk query count for the numpy KNN backend (forwarded to
       ``protea-method``). PROTEA auto-syncs this from
       ``OperationTuning.numpy_query_chunk`` (set via
       ``PROTEA_TUNING__OPERATION__NUMPY_QUERY_CHUNK`` or
       ``system.yaml``); set the env var directly only as an escape
       hatch — it short-circuits the tuning sync.

Frontend
--------

.. code-block:: bash

   # apps/web/.env.local
   NEXT_PUBLIC_API_URL=http://127.0.0.1:8000

This is the only configuration the Next.js frontend needs. It is injected
at build time by Next.js and embedded in the client bundle.

Integration test environment variables
---------------------------------------

The Docker-based integration test fixture is controlled by:

.. list-table::
   :header-rows: 1
   :widths: 30 10 60

   * - Variable
     - Default
     - Description
   * - ``PROTEA_PG_IMAGE``
     - ``pgvector/pgvector:pg16``
     - Docker image for the ephemeral Postgres container.
   * - ``PROTEA_PG_USER``
     - ``protea``
     - Database user.
   * - ``PROTEA_PG_PASSWORD``
     - ``protea``
     - Database password.
   * - ``PROTEA_PG_DB``
     - ``protea_test``
     - Database name.
   * - ``PROTEA_PG_PORT``
     - ``15432``
     - Host port mapped to container port 5432.
   * - ``PROTEA_PG_TIMEOUT``
     - ``30``
     - Seconds to wait for Postgres readiness.

RabbitMQ management
-------------------

The RabbitMQ management UI is available at http://localhost:15672 (default
credentials ``guest`` / ``guest``). The ten PROTEA queues are:

.. list-table::
   :header-rows: 1

   * - Queue
     - Consumer
     - Operations
   * - ``protea.ping``
     - QueueConsumer
     - ``ping``
   * - ``protea.jobs``
     - QueueConsumer
     - ``insert_proteins``, ``fetch_uniprot_metadata``, ``load_ontology_snapshot``,
       ``load_goa_annotations``, ``load_quickgo_annotations``,
       ``generate_evaluation_set``
   * - ``protea.training``
     - QueueConsumer
     - ``export_research_dataset`` (serialised; GPU/RAM-intensive)
   * - ``protea.embeddings``
     - QueueConsumer
     - ``compute_embeddings`` coordinator (serialised, one at a time)
   * - ``protea.embeddings.batch``
     - OperationConsumer
     - ``compute_embeddings_batch`` — GPU inference (ephemeral)
   * - ``protea.embeddings.write``
     - OperationConsumer
     - ``store_embeddings`` — bulk pgvector insert (ephemeral)
   * - ``protea.predictions``
     - QueueConsumer
     - ``predict_go_terms`` coordinator
   * - ``protea.predictions.batch``
     - OperationConsumer
     - ``predict_go_terms_batch`` — KNN + GO transfer (ephemeral)
   * - ``protea.predictions.write``
     - OperationConsumer
     - ``store_predictions`` — bulk GOPrediction insert (ephemeral)
   * - ``protea.evaluations``
     - QueueConsumer
     - ``run_cafa_evaluation``

Queues are declared at worker startup and survive broker restarts.

Tuning settings
---------------

PROTEA exposes throughput, retry policy and boundary limits through
``protea.config.tuning.TuningSettings`` (pydantic). Values are
resolved per call (defaults < ``tuning:`` section in
``protea/config/system.yaml`` < env vars).

Env var convention: ``PROTEA_TUNING__<group>__<field>``. Double
underscore is the path separator (matches pydantic-settings'
``env_nested_delimiter``) so it never collides with single
underscores inside field names.

Categories are derived from ``docs/CONFIG_INVENTORY.md`` (T-CONF.1
of master plan v3) and migrated incrementally in T-CONF.2.

QueueTuning
~~~~~~~~~~~

RabbitMQ publisher and consumer policy.

.. list-table::
   :widths: 30 12 58
   :header-rows: 1

   * - Field
     - Default
     - Purpose
   * - ``publisher_max_attempts``
     - 12
     - Reintentos máximos al publicar a RabbitMQ. 12 attempts cubren ~4 min de broker downtime con backoff exponencial cap a 30s.
   * - ``publisher_base_delay``
     - 1.0
     - Backoff inicial publisher en segundos. Multiplica x2 por intento.
   * - ``oom_max_retries``
     - 5
     - Reintentos al hit CUDA OOM en GPU worker.
   * - ``oom_base_delay``
     - 5
     - Backoff inicial OOM en segundos.
   * - ``oom_max_delay``
     - 300
     - Cap del backoff OOM en segundos (5 min).

YAML excerpt::

   tuning:
     queue:
       publisher_max_attempts: 12
       oom_max_retries: 5

Env override example::

   PROTEA_TUNING__QUEUE__PUBLISHER_MAX_ATTEMPTS=20

WorkerTuning
~~~~~~~~~~~~

Pool sizes, in-process caches, reaper timeouts, HTTP cache TTL.

.. list-table::
   :widths: 32 12 56
   :header-rows: 1

   * - Field
     - Default
     - Purpose
   * - ``db_pool_size``
     - 20
     - SQLAlchemy connection pool size.
   * - ``db_pool_max_overflow``
     - 40
     - Conexiones extra permitidas durante picos.
   * - ``db_pool_recycle_seconds``
     - 3600
     - Reciclar conexiones tras N segundos.
   * - ``model_cache_max``
     - 1
     - Modelos PLM en cache por proceso de embeddings.
   * - ``ref_cache_max``
     - 1
     - Reference data sets en cache por proceso predict.
   * - ``reaper_main_timeout_seconds``
     - 86400
     - Timeout duro antes de marcar jobs FAILED en producción (24h).
   * - ``reaper_default_timeout_seconds``
     - 3600
     - Default constructor de StaleJobReaper.
   * - ``reaper_stall_seconds``
     - 1800
     - Tiempo sin JobEvent antes de considerar un job stalled.
   * - ``api_cache_default_ttl_seconds``
     - 300.0
     - TTL default cache HTTP.

OperationTuning
~~~~~~~~~~~~~~~

Module-level chunk and batch sizes used inside operations.

.. list-table::
   :widths: 28 12 60
   :header-rows: 1

   * - Field
     - Default
     - Purpose
   * - ``annotation_chunk_size``
     - 10_000
     - Filas por chunk al cargar/iterar anotaciones.
   * - ``stream_chunk_size``
     - 2_000
     - Chunk size streaming PyArrow / SQLAlchemy yield_per.
   * - ``store_chunk_size``
     - 10_000
     - Filas por chunk al publicar predictions a la cola store.
   * - ``numpy_query_chunk``
     - 500
     - Query chunk size para KNN numpy backend (caps memoria de la matriz de distancias).

HTTP retry policy and per-source timeouts (UniProt, GOA, QuickGO,
ontology) live inside the respective pydantic payloads
(``InsertProteinsPayload``, ``LoadGoaAnnotationsPayload``, etc.) by
design: callers pick them per-job rather than as global infra
defaults.

APILimits
~~~~~~~~~

HTTP boundary limits enforced at the FastAPI router layer.

.. list-table::
   :widths: 26 14 60
   :header-rows: 1

   * - Field
     - Default
     - Purpose
   * - ``max_fasta_bytes``
     - 52428800 (50 MB)
     - Tope upload FASTA en bytes. Aplica a ``annotate`` y ``query_sets``.
   * - ``max_comment_length``
     - 500
     - Caracteres máximos por comentario en /support.
   * - ``recent_limit``
     - 20
     - Items devueltos por defecto en /support/recent.
   * - ``page_limit``
     - 100
     - Page size hard cap para list endpoints de soporte.

Config-exempt: research methodology constants
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following constants are **deliberately not** in TuningSettings
because changing them would shift the canonical numbers reported
in the thesis and papers:

- ``EMBEDDING_PCA_DIM = 16`` (``core/reranker.py``): part of the
  feature schema contract that ``protea-contracts`` will own; it
  gates compatibility with trained boosters.
- ``N_THRESHOLDS = 101`` (``core/metrics.py``): CAFA Fmax sweep
  granularity. Changing it produces non-comparable Fmax numbers.

Structural exempt
~~~~~~~~~~~~~~~~~

Format-spec positional indices live in code (e.g. GAF column indices
in ``core/operations/load_goa_annotations.py``). They are not
configurable because doing so would mean PROTEA stops reading the
GAF format.
