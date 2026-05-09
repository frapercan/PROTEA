Core
====

.. contents:: On this page
   :local:
   :depth: 2

The ``protea.core`` package contains all domain logic. It has no dependency
on the infrastructure layer: operations receive an open SQLAlchemy session
and an ``emit`` callback, but they do not manage connections, queues, or
transactions themselves. This strict boundary makes every operation
independently testable and trivially substitutable.

Contracts
---------

The contracts module defines the interfaces that every operation must satisfy
and the shared types used across the entire codebase.

``Operation`` is a structural Protocol — any class that exposes a ``name``
string and an ``execute(session, payload, *, emit)`` method conforms to it,
without needing to inherit from a base class. ``ProteaPayload`` is the
immutable, strictly-typed Pydantic base class for all operation payloads:
strict mode prevents silent type coercion, and frozen configuration prevents
accidental mutation after validation. ``OperationResult`` is the return value
of every ``execute`` call; its ``deferred`` flag tells ``BaseWorker`` that
completion will be signalled by child workers rather than immediately.
``RetryLaterError`` is raised when a shared resource (e.g. the GPU) is
occupied — ``BaseWorker`` catches it, resets the job to ``QUEUED``, and
re-publishes the message after a configurable delay.

.. automodule:: protea.core.contracts.operation
   :members:
   :undoc-members:
   :show-inheritance:

``OperationRegistry`` is a simple dict-backed mapping from operation name
strings to instances. Workers resolve the correct operation at message
dispatch time; new operations are registered at process startup in
``scripts/worker.py`` without modifying any worker code.

.. automodule:: protea.core.contracts.registry
   :members:
   :undoc-members:
   :show-inheritance:

``parent_progress`` exposes the shared
``_update_parent_progress`` helper used by every coordinator
operation (``compute_embeddings``, ``predict_go_terms``) to advance
the parent job's progress as child workers finish their batches.
Extracted to its own module in F0 (T0.7) to remove duplicated copies
across coordinators.

.. automodule:: protea.core.contracts.parent_progress
   :members:
   :undoc-members:
   :show-inheritance:

Retry middleware
----------------

``protea.core.retry`` exposes ``with_retry``, a wrapper function used
by ``BaseWorker`` to run the execute session against transient
database errors (deadlocks, connection drops, serialisation
failures) and brief network blips. Exponential backoff with jitter;
all knobs (``max_attempts``, ``base_delay``, ``max_delay``,
``jitter_ratio``, ``predicate``, ``on_retry``) are bundled in a
``RetryPolicy`` frozen dataclass passed via the ``policy`` keyword
argument (T-CONTEXTS, PR #237). ``BaseWorker`` instantiates a fixed
policy at call site (``RetryPolicy(max_attempts=3, base_delay=1.0,
max_delay=10.0, jitter_ratio=0.3)``); there is no global
``TuningSettings`` field for these values. Added as part of F0
(T0.3) of the master plan v3.

.. automodule:: protea.core.retry
   :members:
   :undoc-members:
   :show-inheritance:

Operation catalogue
-------------------

``protea.core.operation_catalog`` builds the singleton
``OperationRegistry`` that workers consult at message dispatch. The
public function ``build_operation_registry()`` instantiates each
operation class and registers it under its canonical name. Adding a
new operation is a one-line edit here plus a new module under
``protea/core/operations/``.

.. automodule:: protea.core.operation_catalog
   :members:
   :undoc-members:
   :show-inheritance:

Plugin discovery
----------------

``protea.core.plugins`` centralises ``importlib.metadata.entry_points``
discovery for every PROTEA plugin group (``protea.backends``,
``protea.sources``, ``protea.runners``). ``discover_plugins(group)``
returns a cached ``{name: plugin}`` map and hard-errors with
``RuntimeError`` if a plugin's ``name`` attribute drifts from its
entry-point name. ``reset_plugin_cache`` is a test-only seam for
suites that install/uninstall plugins between cases. Added in T2A.5
for backend dispatch and generalised in T2A.8 (PR #240).

.. automodule:: protea.core.plugins
   :members:
   :undoc-members:
   :show-inheritance:

Experiment runners
------------------

``protea.core.runners`` adapts the generic plugin discovery to the
``protea.runners`` group. ``resolve_runner(name)`` maps an identifier
(``"knn"`` / ``"baseline"`` / ``"lightgbm"``) to a runner plugin
instance implementing the ``protea_contracts.ExperimentRunner``
interface; unknown names raise ``ValueError`` listing the discovered
set. PROTEA does not yet dispatch to runners at inference time (the
active KNN + reranker path stays in ``PredictGOTermsBatchOperation``
until F2C of master plan v3 hoists the inference core into a shared
package). The adapter exists so ``GET /v1/registry/runners`` has a
stable resolver and future code has a one-line entry. Closes T2A.8
(PR #240).

.. automodule:: protea.core.runners
   :members:
   :undoc-members:
   :show-inheritance:

Utilities
---------

``protea.core.utils`` provides three shared utilities used across multiple
operations.

``utcnow()`` returns a timezone-aware UTC datetime, avoiding the common
mistake of calling ``datetime.utcnow()`` which returns a naive object.
``chunks(seq, n)`` splits any sequence into fixed-size chunks, used by
coordinator operations to partition work into batches. ``UniProtHttpMixin``
encapsulates all retry logic for the UniProt REST API: exponential backoff
with jitter, ``Retry-After`` header parsing, and cursor extraction for
paginated endpoints. It is mixed into ``InsertProteinsOperation`` and
``FetchUniProtMetadataOperation``.

.. automodule:: protea.core.utils
   :members:
   :undoc-members:
   :show-inheritance:

KNN search
----------

``protea.core.knn_search`` provides the nearest-neighbour search layer used
during GO term prediction. The single public entry point is ``search_knn()``,
which dispatches to one of two backends based on the ``backend`` parameter.

The **numpy** backend computes exact cosine or L2 distances via matrix
multiplication. It requires no additional dependencies and is the default.
For cosine distance, query and reference matrices are L2-normalised and the
distance is computed as :math:`D = 1 - \cos(\theta) \in [0, 2]`. This is
:math:`O(NQ)` and is appropriate for reference sets up to approximately
100 000 proteins when embeddings fit in RAM as float16.

The **faiss** backend wraps the FAISS library and supports three index
types: ``Flat`` (exact), ``IVFFlat`` (approximate, Voronoi partitioning),
and ``HNSW`` (approximate, hierarchical graph). ``IVFFlat`` is recommended
for datasets above 100 000 vectors: it restricts search to the ``nprobe``
nearest Voronoi cells, reducing query time from :math:`O(N)` to approximately
:math:`O(\sqrt{N})` with negligible recall loss at default settings.

.. important::
   KNN search is **never** performed at the database layer. pgvector index
   types (HNSW, IVFFlat) are not used. All search happens in Python after
   loading reference embeddings into a numpy array. See :ref:`knn-constraint`
   in the how-to guides.

.. automodule:: protea.core.knn_search
   :members:
   :undoc-members:
   :show-inheritance:

Feature engineering
-------------------

``protea.core.feature_engineering`` enriches each query–reference pair in a
prediction result with sequence-level and phylogenetic signals. These features
are opt-in: they are computed only when ``compute_alignments=true`` and/or
``compute_taxonomy=true`` are set in the prediction payload.

**Pairwise alignment** is computed via the ``parasail`` library using the
BLOSUM62 substitution matrix with gap-open/extend penalties of 10/1. Both
global (Needleman–Wunsch) and local (Smith–Waterman) alignments are run for
each pair, producing identity, similarity, raw score, gap percentage, and
alignment length for each. These metrics capture sequence similarity beyond
what the embedding distance alone encodes, which is especially valuable for
distant homologues where embedding geometry may be unreliable.

**Taxonomic distance** is computed via ``ete3`` and the NCBI taxonomy tree
(local SQLite, downloaded on first use). For each (query, reference) pair
where taxonomy IDs are available from UniProt metadata, PROTEA finds the
lowest common ancestor and computes the edge count through it. Results are
cached with an LRU cache keyed by taxon-ID pair to avoid redundant tree
traversals across a batch.

.. automodule:: protea.core.feature_engineering
   :members:
   :undoc-members:
   :show-inheritance:

Re-ranker
---------

``protea.core.reranker`` implements a LightGBM binary classifier that
re-scores GO term predictions using 20 numeric features (embedding distance,
NW/SW alignment metrics, sequence lengths, taxonomic distance and common
ancestors, and 5 aggregate re-ranker signals) plus 3 categorical features
(qualifier, evidence code, taxonomic relation). The full feature list is
documented in :ref:`train_reranker <train-reranker-operation>`.

The module provides:

- ``prepare_dataset(df)`` — extracts and coerces feature columns. Numeric
  columns are coerced with ``errors="coerce"`` (invalid strings become
  ``NaN``); categorical columns are converted to pandas ``category`` dtype,
  which LightGBM consumes directly without manual label encoding.
- ``train(df)`` — stratified positive/negative split with early-stopping on a
  held-out validation set (default 20 %). Returns a ``TrainResult`` with the
  Booster, validation metrics (AUC, logloss, precision, recall, F1 at the
  0.5 threshold), the best boosting iteration, and gain-based feature
  importance.
- ``predict(model, df)`` — returns probability scores in ``[0, 1]``.
- ``model_to_string()`` / ``model_from_string()`` — serialization for DB
  storage in the ``RerankerModel`` table.
- ``load_training_tsv()`` — parses a training data TSV as produced by the
  ``/scoring/prediction-sets/{id}/training-data.tsv`` endpoint.

.. note::

   ``load_reranker`` / ``apply_reranker`` / ``infer_active_feature_families``
   were originally split into a sibling ``protea.core.reranking`` module;
   they were folded back into ``protea.core.reranker`` to remove a naming
   trap (``reranker`` vs ``reranking`` were impossible to grep apart).
   This module is now the single inference-side surface.

.. automodule:: protea.core.reranker
   :members:
   :undoc-members:
   :show-inheritance:

Parquet export (``protea.core.parquet_export``)
------------------------------------------------

``protea.core.parquet_export`` consolidates per-split, per-category
parquet shards produced by the KNN + feature pipeline into the frozen
dataset layout consumed by ``protea-reranker-lab``: exactly
``train.parquet``, ``eval.parquet`` and ``manifest.json`` under a single
directory. The manifest follows ``ManifestV1`` (schema version ``v2``)
and records PROTEA's ``producer_version`` + ``producer_git_sha``.

The single public function ``export_reranker_parquets(...)`` is shared
by two callers:

- ``training_dump_helpers._dump_frozen_dataset`` — thin wrapper that
  uses this helper to emit the dataset alongside a training-data dump.
- ``ExportResearchDatasetOperation`` — stand-alone operation that only
  materialises and publishes the dataset, without running LightGBM.

When ``store`` is provided, the three consolidated files are
additionally uploaded under ``key_prefix`` using the ``ArtifactStore``
interface, and the returned dict includes ``train_uri`` / ``eval_uri``
/ ``manifest_uri``.

.. automodule:: protea.core.parquet_export
   :members:
   :undoc-members:
   :show-inheritance:

Scoring
-------

``protea.core.scoring`` implements the scoring engine that applies weighted
formulas to GO predictions. A ``ScoringConfig`` defines a set of weights for
each feature column (embedding distance, alignment metrics, taxonomy, re-ranker
features). The engine computes a composite score per prediction row and can
stream scored results as TSV or compute CAFA-style metrics (Fmax, AUC-PR)
against an evaluation set.

.. automodule:: protea.core.scoring
   :members:
   :undoc-members:
   :show-inheritance:

Metrics
-------

``protea.core.metrics`` implements CAFA-style precision-recall evaluation.
Provides functions for computing Fmax (maximum F-measure over all thresholds),
weighted precision/recall, and coverage for a set of predictions against
ground-truth annotations.

.. automodule:: protea.core.metrics
   :members:
   :undoc-members:
   :show-inheritance:

Evidence codes
--------------

``protea.core.evidence_codes`` provides mappings between ECO (Evidence and
Conclusion Ontology) identifiers and GO evidence codes used in GAF files.
Used by the QuickGO annotation loader to resolve ECO IDs to standard
three-letter evidence codes.

.. automodule:: protea.core.evidence_codes
   :members:
   :undoc-members:
   :show-inheritance:

Evaluation
----------

``protea.core.evaluation`` implements the CAFA5 evaluation protocol for
computing the ground-truth delta between two annotation snapshots.

The module's central data structure is ``EvaluationData``, a frozen dataclass
that holds the NK, LK, PK, known, and pk_known annotation dictionaries.
Each dictionary maps a protein accession to a set of GO term IDs.

``EvaluationData`` fields:

- ``nk`` — delta annotations for No-Knowledge proteins (no prior annotations
  in any namespace at t0).
- ``lk`` — delta annotations for Limited-Knowledge proteins (had annotations
  in some namespaces but gained new terms in a previously empty namespace).
- ``pk`` — novel annotations for Partial-Knowledge proteins (gained new terms
  in a namespace where they already had annotations).
- ``pk_known`` — old experimental annotations for PK proteins in the
  relevant namespaces; passed as ``-known`` to ``cafaeval`` to exclude them
  from scoring.
- ``known`` — all old experimental annotations flattened across namespaces;
  available for download via the reference endpoint.

The public entry point is ``compute_evaluation_data(session,
old_annotation_set_id, new_annotation_set_id, ontology_snapshot_id)``.
It loads the GO DAG for NOT-propagation, builds a per-namespace annotation
map for both old and new sets, and classifies each (protein, namespace) pair
into NK, LK, or PK. The same protein can appear in multiple categories across
different namespaces simultaneously (e.g., LK in CCO and PK in BPO).

.. automodule:: protea.core.evaluation
   :members:
   :undoc-members:
   :show-inheritance:

Provenance
----------

``protea.core.provenance`` provides ``capture_provenance(extra=None)``,
a side-effect-free runtime snapshot for jobs / experiments / artefacts
to carry an audit trail without DB or network probes. Returns a fresh
``dict[str, Any]`` with auto-keys ``protea_version`` (from
``importlib.metadata``), ``protea_git_sha`` (delegates to
``parquet_export.resolve_protea_git_sha``), ``python_version``,
``platform``, ``hostname``, and ``captured_at`` (ISO-8601 UTC). Any
caller-supplied ``extra`` mapping is overlaid last, so callers always
win on key collisions.

Every probe is wrapped: missing distribution metadata, a non-git
checkout, or an absent ``git`` binary all degrade to ``None`` rather
than raising. Added in T3.11 of master plan v3.2 §24 Fase 4.

.. automodule:: protea.core.provenance
   :members:
   :undoc-members:
   :show-inheritance:

Operations
----------

PROTEA ships fifteen registered operation instances at worker startup
via ``protea.core.operation_catalog.build_operation_registry``: eleven
job-backed (reachable through ``POST /jobs``) plus four ephemeral
consumers (dispatched internally by the ``compute_embeddings`` and
``predict_go_terms`` coordinators — see :doc:`/architecture/operations`
for that taxonomy). Each operation is a class that implements the
``Operation`` protocol: a ``name`` string and an ``execute`` method.
Operations are stateless with respect to infrastructure — they receive a
session and emit structured events, but do not open connections or manage
transactions. The eleven job-backed entries are documented below; the
four ephemeral siblings (``compute_embeddings_batch``,
``store_embeddings``, ``predict_go_terms_batch``,
``store_predictions``) live in :doc:`/architecture/operations`.

**ping**
   Smoke-test operation. Returns immediately with a success result.
   Used to verify end-to-end connectivity between the API, RabbitMQ,
   and worker processes.

.. automodule:: protea.core.operations.ping
   :members:
   :undoc-members:
   :show-inheritance:

**insert_proteins**
   Fetches protein sequences from the UniProt REST API using cursor-based
   FASTA streaming. Sequences are deduplicated by MD5 hash before upsert;
   proteins are upserted by accession. Exponential backoff with jitter and
   ``Retry-After`` header handling are provided by ``UniProtHttpMixin``.
   Isoforms are parsed and stored separately, sharing the canonical sequence
   where the amino-acid string is identical.

.. automodule:: protea.core.operations.insert_proteins
   :members:
   :undoc-members:
   :show-inheritance:

**fetch_uniprot_metadata**
   Downloads TSV functional annotation data from UniProt and upserts
   ``ProteinUniProtMetadata`` rows keyed by canonical accession. Fields
   include functional description, EC numbers, pathway membership, and
   kinetics. Isoforms inherit metadata through the ``canonical_accession``
   join — no duplicate rows are created.

.. automodule:: protea.core.operations.fetch_uniprot_metadata
   :members:
   :undoc-members:
   :show-inheritance:

**load_ontology_snapshot**
   Downloads a GO OBO file and populates ``OntologySnapshot``, ``GOTerm``,
   and ``GOTermRelationship`` rows. The ``obo_version`` field carries a unique
   constraint so that re-importing the same release is idempotent. If a
   snapshot already exists but its relationships are missing, they are
   backfilled automatically.

.. automodule:: protea.core.operations.load_ontology_snapshot
   :members:
   :undoc-members:
   :show-inheritance:

**load_goa_annotations**
   Bulk-loads a GAF (Gene Association Format) file. Annotations are filtered
   against canonical accessions present in the database, avoiding orphaned
   foreign keys. Each batch is committed independently to bound transaction
   size.

.. automodule:: protea.core.operations.load_goa_annotations
   :members:
   :undoc-members:
   :show-inheritance:

**load_quickgo_annotations**
   Streams GO annotations from the QuickGO bulk download API (paginated TSV).
   Supports optional ECO→GO evidence code mapping and per-page commits.
   Filters out annotations whose accessions are not already in the database.

.. automodule:: protea.core.operations.load_quickgo_annotations
   :members:
   :undoc-members:
   :show-inheritance:

**compute_embeddings**
   Coordinator operation that partitions the target sequence set into batches
   and dispatches one ``compute_embeddings_batch`` message per batch to
   ``protea.embeddings.batch``. The coordinator serialises on the
   ``protea.embeddings`` queue (one at a time) to prevent concurrent model
   loads from exhausting GPU memory. Batch and write workers scale
   independently. Returns ``deferred=True`` — the parent job is closed by
   the last write worker.

.. automodule:: protea.core.operations.compute_embeddings
   :members:
   :undoc-members:
   :show-inheritance:

**predict_go_terms**
   Coordinator operation that loads reference embeddings into a process-level
   float16 cache, partitions the query set into batches, and dispatches one
   ``predict_go_terms_batch`` message per batch to
   ``protea.predictions.batch``. Feature engineering (alignments, taxonomy)
   is opt-in via payload flags. Returns ``deferred=True`` — the parent job
   is closed by the last write worker.

.. automodule:: protea.core.operations.predict_go_terms
   :members:
   :undoc-members:
   :show-inheritance:

**generate_evaluation_set**
   Computes the NK/LK/PK evaluation delta between two annotation sets using
   the CAFA5 protocol (experimental evidence only, NOT-propagation through the
   GO DAG, per-namespace classification). Stores an ``EvaluationSet`` row with
   summary statistics. Ground-truth files are generated on-demand by the
   download endpoints.

.. automodule:: protea.core.operations.generate_evaluation_set
   :members:
   :undoc-members:
   :show-inheritance:

**run_cafa_evaluation**
   Runs ``cafaeval`` for NK, LK, and PK settings against a given prediction
   set. Downloads the OBO file, writes ground-truth and prediction TSVs, calls
   ``cafa_eval()`` three times (NK and LK without ``-known``, PK with
   ``pk_known_terms.tsv`` as ``-known``), and persists an ``EvaluationResult``
   row with per-namespace Fmax, precision, recall, τ, and coverage.

.. automodule:: protea.core.operations.run_cafa_evaluation
   :members:
   :undoc-members:
   :show-inheritance:

**export_research_dataset**
   Publishes the frozen re-ranker dataset (``train.parquet`` /
   ``eval.parquet`` / ``manifest.json``) consumed by
   ``protea-reranker-lab``. Runs the KNN + feature-generation pipeline
   via ``TrainRerankerAutoOperation`` in ``dump_only`` mode and uploads
   the resulting artefacts through the configured ``ArtifactStore``
   (local FS by default, MinIO when the ``storage`` compose profile is
   active). Manifest records PROTEA's ``producer_version`` /
   ``producer_git_sha`` for full traceability from lab runs back to
   PROTEA HEAD.

.. automodule:: protea.core.operations.export_research_dataset
   :members:
   :undoc-members:
   :show-inheritance:

Training-dump helpers
---------------------

``protea.core.training_dump_helpers`` is the home of the KNN /
feature-generation helpers that were extracted in F0 (T0.6) when
``protea.core.operations.train_reranker`` was deleted. The module is
deliberately not an operation — it is reused in-process by
:class:`ExportResearchDatasetOperation` to materialise ``train`` and
``eval`` shards before the ``parquet_export`` consolidation pass.
LightGBM training itself lives in
`protea-reranker-lab <https://github.com/frapercan/protea-reranker-lab>`_,
which consumes the published ``Dataset`` rows produced by
``export_research_dataset``.

.. automodule:: protea.core.training_dump_helpers
   :members:
   :undoc-members:
   :show-inheritance:

Internal helpers
----------------

These modules are imported by the operations and the feature
engineering layer; they are documented here for completeness but are
not part of the public API.

- ``protea.core.anc2vec_embeddings`` — anc2vec ancestry embeddings for
  GO terms, used as features by the re-ranker (see ADR D19 for the
  GeOKG replacement candidate).
- ``protea.core.annotation_intern`` — string interning helper for
  reducing memory pressure when loading large annotation sets.
- ``protea.core.disk_cache`` — generic on-disk cache with TTL used by
  the KNN reference loader and the PCA cache.
- ``protea.core.feature_enricher`` — orchestrator that combines
  alignment, taxonomy and anc2vec features into a single
  per-candidate row.
- ``protea.core.pca_cache`` — per-PLM PCA projection cache, used to
  pre-compute the ``emb_pca`` feature family.

.. automodule:: protea.core.anc2vec_embeddings
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: protea.core.annotation_intern
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: protea.core.disk_cache
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: protea.core.feature_enricher
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: protea.core.pca_cache
   :members:
   :undoc-members:
   :show-inheritance:

.. seealso::

   - :doc:`/architecture/operations` — narrative documentation for every
     operation listed above, with payload examples and execution flow.
   - :doc:`infrastructure` — the ORM models that ``protea.core`` reads and
     writes.
   - :doc:`/appendix/howto_guides` — task-oriented recipes that exercise
     these modules end-to-end.
