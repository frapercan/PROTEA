Infrastructure
==============

.. contents:: On this page
   :local:
   :depth: 2

The ``protea.infrastructure`` package implements the persistence and messaging
layer. It is the only package that imports SQLAlchemy, psycopg2, or aio-pika
directly. All other layers interact with the database through the session
factory and with the queue through the publisher interface.

Settings
--------

``protea.infrastructure.settings`` loads configuration from
``protea/config/system.yaml`` and applies environment variable overrides.
The two mandatory settings are ``db_url`` (PostgreSQL connection string) and
``amqp_url`` (RabbitMQ connection string). Both can be overridden at runtime
via ``PROTEA_DB_URL`` and ``PROTEA_AMQP_URL`` environment variables, which
takes precedence over the YAML file. This makes the same configuration file
usable across local development, CI, and production deployments.

.. automodule:: protea.infrastructure.settings
   :members:
   :undoc-members:
   :show-inheritance:

Session management
------------------

``session_scope()`` is the single entry point for all database access in
PROTEA. It is a context manager that commits on normal exit and rolls back
on any exception, then always closes the session. Workers open and close
sessions explicitly rather than relying on this context manager for
long-lived operations, but it is used throughout the API layer and in tests.

The ``build_session_factory()`` function creates a SQLAlchemy ``sessionmaker``
bound to the given database URL. It is called once at application startup and
stored on ``app.state.session_factory``, keeping the router free of global
state.

.. automodule:: protea.infrastructure.session
   :members:
   :undoc-members:
   :show-inheritance:

ORM models
----------

All models use SQLAlchemy 2.x declarative style with ``Mapped[]`` type
annotations. The schema is managed by Alembic; migrations are generated via
``alembic revision --autogenerate`` and stored under ``alembic/versions/``.

**Job and JobEvent**

``Job`` is the central entity of the job queue. It implements a five-state
machine (``QUEUED → RUNNING → SUCCEEDED | FAILED``, or ``QUEUED →
CANCELLED``). The ``parent_job_id`` foreign key links batch child jobs to
their coordinator parent. ``payload`` and ``meta`` are PostgreSQL JSONB
columns, allowing arbitrary structured data without schema migrations for
new operation types. ``progress_current`` and ``progress_total`` are updated
atomically by write workers to drive the frontend progress bar.

``JobEvent`` is an append-only audit log: rows are written by the ``emit``
callback during execution and are never updated or deleted. The frontend
renders them as a chronological event timeline.

.. automodule:: protea.infrastructure.orm.models.job
   :members:
   :undoc-members:
   :show-inheritance:

**Protein and Sequence**

``Sequence`` stores unique amino-acid strings, deduplicated by MD5 hash.
Multiple ``Protein`` rows (canonical accessions and isoforms) may reference
the same ``Sequence`` row, preventing redundant embedding computation for
sequences that appear under different accessions.

``Protein`` stores one row per UniProt accession, including isoforms
(``<canonical>-<n>``). The ``canonical_accession`` field groups isoforms
together, and the view-only relationship to ``ProteinUniProtMetadata`` is
joined on this field rather than a foreign key, so metadata rows are not
duplicated for each isoform.

.. automodule:: protea.infrastructure.orm.models.protein.protein
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: protea.infrastructure.orm.models.protein.protein_metadata
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: protea.infrastructure.orm.models.sequence.sequence
   :members:
   :undoc-members:
   :show-inheritance:

**GO Ontology**

``OntologySnapshot`` records one complete GO OBO release, versioned by the
``obo_version`` string from the OBO file header. The unique constraint on
``obo_version`` makes repeated imports idempotent. ``GOTerm`` stores one row
per term per snapshot; ``GOTermRelationship`` stores the directed edges of
the GO DAG with their relation types (``is_a``, ``part_of``, ``regulates``,
etc.). The ``/annotations/snapshots/{id}/subgraph`` endpoint uses BFS over
these edges to return ancestor subgraphs for a given set of GO term IDs.

.. automodule:: protea.infrastructure.orm.models.annotation.ontology_snapshot
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: protea.infrastructure.orm.models.annotation.go_term
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: protea.infrastructure.orm.models.annotation.go_term_relationship
   :members:
   :undoc-members:
   :show-inheritance:

**Annotation Sets**

``AnnotationSet`` groups a batch of protein GO annotations by source
(``goa`` or ``quickgo``) and ontology snapshot version. This design allows
side-by-side comparison of annotation sets from different sources or dates
and ties every prediction result to a specific, versioned annotation input.
``ProteinGOAnnotation`` stores all GAF/QuickGO evidence fields verbatim:
qualifier, evidence code, assigned-by, database reference, with/from, and
annotation date.

.. automodule:: protea.infrastructure.orm.models.annotation.annotation_set
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: protea.infrastructure.orm.models.annotation.protein_go_annotation
   :members:
   :undoc-members:
   :show-inheritance:

**Evaluation Sets**

``EvaluationSet`` stores the CAFA-style temporal holdout delta between two
annotation sets (old → new). Contains summary statistics (NK/LK/PK protein and
annotation counts) in a JSONB ``stats`` column. ``EvaluationResult`` stores the
output of running ``cafaeval`` against a prediction set: per-namespace Fmax,
precision, recall, τ, and coverage for NK, LK, and PK settings.

.. automodule:: protea.infrastructure.orm.models.annotation.evaluation_set
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: protea.infrastructure.orm.models.annotation.evaluation_result
   :members:
   :undoc-members:
   :show-inheritance:

**Embeddings**

``EmbeddingConfig`` defines a reproducible embedding recipe: model identifier,
layer selection, pooling strategy, normalisation flags, and chunking
parameters. Its UUID primary key is stable; changing any parameter creates
a new configuration row. Both ``SequenceEmbedding`` rows and ``PredictionSet``
rows reference the same ``EmbeddingConfig``, guaranteeing that query and
reference embeddings are always comparable.

``SequenceEmbedding`` stores a pgvector ``VECTOR`` for each
(sequence, config, chunk) triple. When chunking is disabled the chunk index
is 0 and the end index is NULL. pgvector is used for storage only; nearest-
neighbour queries are performed in Python via ``protea.core.knn_search``.

.. automodule:: protea.infrastructure.orm.models.embedding.embedding_config
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: protea.infrastructure.orm.models.embedding.sequence_embedding
   :members:
   :undoc-members:
   :show-inheritance:

**Predictions**

``PredictionSet`` is the result container for one run of
``predict_go_terms``. It links the query set, embedding configuration,
annotation set, and ontology snapshot used, making every prediction set
fully reproducible. ``GOPrediction`` stores one row per (query protein,
GO term, reference protein) triple. The 14 optional feature-engineering
columns (alignment statistics and taxonomy fields) and 5 re-ranker aggregate
features (``vote_count``, ``k_position``, ``go_term_frequency``,
``ref_annotation_density``, ``neighbor_distance_std``) are ``NULL`` unless the
corresponding flags were set in the prediction payload.

.. automodule:: protea.infrastructure.orm.models.embedding.prediction_set
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: protea.infrastructure.orm.models.embedding.go_prediction
   :members:
   :undoc-members:
   :show-inheritance:

**Re-ranker Models**

``RerankerModel`` stores a trained LightGBM re-ranker for re-scoring GO
term predictions. The booster itself can live either **inline** in the
legacy ``model_data`` (``Text``, nullable) column or **by reference** in
``artifact_uri`` (``String(512)``) resolved through an ``ArtifactStore``.
New rows registered via ``scripts/register_reranker.py`` from a
``protea-reranker-lab`` run always use the artifact-backed path.

Provenance columns travel with every artifact-backed row:
``feature_schema_sha`` (``String(16)``, load-bearing at inference time),
``embedding_config_id`` / ``ontology_snapshot_id`` (FKs, both
``SET NULL``), ``producer_version`` (``String(64)``), ``producer_git_sha``
(``String(40)``) and ``spec_yaml`` (``Text``). ``metrics`` and
``feature_importance`` remain JSONB.

.. automodule:: protea.infrastructure.orm.models.embedding.reranker_model
   :members:
   :undoc-members:
   :show-inheritance:

**Scoring Configurations**

``ScoringConfig`` defines a set of feature weights and parameters for scoring
GO predictions. Each config is a named, immutable recipe that can be applied
to any prediction set to produce a composite score per prediction row.

.. automodule:: protea.infrastructure.orm.models.embedding.scoring_config
   :members:
   :undoc-members:
   :show-inheritance:

**Datasets**

``Dataset`` is the durable handle for a frozen re-ranker training dataset
published to the artifact store. One row per ``export_research_dataset``
run that completes successfully; ``protea-reranker-lab``'s
``pull_dataset.py`` resolves either a UUID or a human ``name`` against
this table to fetch the exact ``train.parquet`` / ``eval.parquet`` /
``manifest.json`` triple that produced a given booster.

Storage is backend-agnostic: ``train_uri`` / ``eval_uri`` /
``manifest_uri`` are opaque URIs (``file://…`` for the local backend,
``s3://bucket/key`` for MinIO) resolved through the
:class:`ArtifactStore` interface — callers never need to know which
backend is active. Two content fingerprints provide drift detection:
``schema_sha`` (16-char) records the feature-set version (must match
the booster's ``feature_schema_sha`` at inference time) and
``manifest_sha`` (64-char) is the sha256 of the serialized manifest
bytes. Provenance lives in ``producer_version`` / ``producer_git_sha``
so any registered booster can be traced back to the PROTEA HEAD that
emitted its dataset.

.. automodule:: protea.infrastructure.orm.models.embedding.dataset
   :members:
   :undoc-members:
   :show-inheritance:

**Support Entries**

``SupportEntry`` stores community feedback: a thumbs-up with an optional
comment. Used by the ``/support`` router.

.. automodule:: protea.infrastructure.orm.models.support_entry
   :members:
   :undoc-members:
   :show-inheritance:

**Query Sets**

``QuerySet`` represents a user-uploaded FASTA dataset. ``QuerySetEntry``
stores one row per FASTA entry, preserving the original accession header
and linking to the deduplicated ``Sequence`` row. If the amino-acid string
already exists in the database, the existing ``Sequence`` row is reused,
avoiding redundant embedding computation.

.. automodule:: protea.infrastructure.orm.models.query.query_set
   :members:
   :undoc-members:
   :show-inheritance:

**Visitor Events**

``VisitorEvent`` is the append-only log used by the Grafana
"unique visitors" dashboard. One row is written per HTTP GET to a
non-asset path. The schema deliberately omits IP addresses: each row
stores only ``visitor_hash`` — the first 16 hex chars of
``sha256(daily_salt || client_ip)`` where ``daily_salt`` is a 32-byte
random value held in process memory and rotated every calendar day.
Once the day rolls over the salt is gone, so cross-day correlation of a
visitor becomes cryptographically infeasible. This is the same
no-PII model used by Plausible and Fathom.

.. automodule:: protea.infrastructure.orm.models.visitor_event
   :members:
   :undoc-members:
   :show-inheritance:

Logging
-------

``protea.infrastructure.logging`` provides structured JSON logging via a
custom ``JSONFormatter``. The ``configure_logging()`` function sets up the
root logger with either JSON or plain text output, used by worker processes
and the API server.

.. automodule:: protea.infrastructure.logging
   :members:
   :undoc-members:
   :show-inheritance:

Artifact storage
----------------

``protea.infrastructure.storage`` defines the ``ArtifactStore`` Protocol
and its two concrete backends. It is the single surface for writing and
reading large produced blobs (re-ranker boosters, frozen datasets) and
is kept strictly separate from the evaluation-artifacts directory
consumed by ``run_cafa_evaluation``.

``ArtifactStore`` is a ``typing.Protocol`` with four methods:

- ``put(key: str, src: Path | bytes) -> str`` — store a blob under
  ``key`` and return its URI.
- ``get(key: str) -> bytes`` — fetch raw bytes stored at ``key``.
- ``url(key: str) -> str`` — return the backend-specific URI for
  ``key`` without performing I/O.
- ``exists(key: str) -> bool`` — check whether ``key`` is present.

URIs are always persisted verbatim in the database so consumers can
resolve them without knowing the concrete backend:

- ``LocalFsArtifactStore`` emits ``file:///absolute/path/...`` URIs.
- ``MinioArtifactStore`` emits ``s3://<bucket>/<key>`` URIs.

The ``MinioArtifactStore`` client is imported lazily so PROTEA can be
installed without the ``minio`` package — the constructor raises a
clear ``ImportError`` pointing at the ``[storage]`` extra when the
dependency is missing.

``get_artifact_store(settings)`` is the entry point used by every
operation that needs to write a blob. It reads
``settings.storage_backend`` (``"local"`` or ``"minio"``) and returns
the appropriate instance. If MinIO is selected but the endpoint or
credentials are incomplete, or the server is unreachable at
construction time, the factory logs a warning and returns a
``LocalFsArtifactStore`` rooted at ``settings.storage_root`` (or
``settings.artifacts_dir`` as a fallback). This prevents missing
optional infrastructure from crashing the stack in development.

.. automodule:: protea.infrastructure.storage
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: protea.infrastructure.storage.local
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: protea.infrastructure.storage.minio_store
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: protea.infrastructure.storage.factory
   :members:
   :undoc-members:
   :show-inheritance:

Queue
-----

The queue layer provides two classes: ``QueueConsumer`` and
``OperationConsumer``.

``QueueConsumer`` reads a job UUID from a RabbitMQ queue and delegates to
``BaseWorker.handle_job()``. It is used for queues where every message
corresponds to a tracked ``Job`` row: ``protea.ping``, ``protea.jobs``, and
``protea.embeddings``.

``OperationConsumer`` reads a raw serialised operation payload from the
queue and executes it directly, without creating a ``Job`` row. It is used
for high-throughput batch queues (``protea.embeddings.batch``,
``protea.embeddings.write``, ``protea.predictions.batch``,
``protea.predictions.write``) where creating thousands of child rows would
cause queue bloat. Progress is tracked at the parent job level only, via
the atomic ``progress_current`` increment.

The ``publisher`` module provides ``publish_job()`` and ``publish_operation()``
helpers. Both are called by ``BaseWorker`` after the DB commit (not before),
guaranteeing that workers always find the DB row before they try to claim it.

.. automodule:: protea.infrastructure.queue.publisher
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: protea.infrastructure.queue.consumer
   :members:
   :undoc-members:
   :show-inheritance:

.. seealso::

   - :doc:`/architecture/data_model` — the conceptual model behind every
     ORM class above.
   - :doc:`workers` — how ``BaseWorker`` and the consumer classes use the
     publisher and session helpers documented here.
   - :doc:`/adr/005-thread-local-rabbitmq-connections` — why the publisher
     reuses one connection per thread.
