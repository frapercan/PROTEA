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
credentials ``guest`` / ``guest``). The seven PROTEA queues are:

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
       ``compute_embeddings`` (coordinator), ``predict_go_terms`` (coordinator),
       ``generate_evaluation_set``, ``run_cafa_evaluation``,
       ``train_reranker``, ``train_reranker_auto``
   * - ``protea.embeddings``
     - QueueConsumer
     - ``compute_embeddings`` coordinator (serialised, one at a time)
   * - ``protea.embeddings.batch``
     - OperationConsumer
     - ``compute_embeddings_batch`` — GPU inference (ephemeral)
   * - ``protea.embeddings.write``
     - OperationConsumer
     - ``store_embeddings`` — bulk pgvector insert (ephemeral)
   * - ``protea.predictions.batch``
     - OperationConsumer
     - ``predict_go_terms_batch`` — KNN + GO transfer (ephemeral)
   * - ``protea.predictions.write``
     - OperationConsumer
     - ``store_predictions`` — bulk GOPrediction insert (ephemeral)

Queues are declared at worker startup and survive broker restarts.
