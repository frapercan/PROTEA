Core
====

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
re-scores GO term predictions using 19 numeric features (embedding distance,
NW/SW alignment metrics, sequence lengths, taxonomic distance, and 5
aggregate re-ranker signals) plus 3 categorical features (qualifier,
evidence code, taxonomic relation).

The module provides:

- ``prepare_dataset(df)`` — extracts and coerces feature columns.
- ``train(df)`` — stratified train/val split with ``is_unbalance=True``,
  returns a ``TrainResult`` with the model, validation metrics (AUC,
  logloss, precision, recall, F1), and feature importance.
- ``predict(model, df)`` — returns probability scores [0, 1].
- ``model_to_string()`` / ``model_from_string()`` — serialization for DB
  storage in the ``RerankerModel`` table.
- ``load_training_tsv()`` — parses a training data TSV as produced by the
  ``/scoring/prediction-sets/{id}/training-data.tsv`` endpoint.

.. automodule:: protea.core.reranker
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

Operations
----------

PROTEA ships sixteen registered operation instances at worker startup in
``scripts/worker.py``. Each operation is a class that implements the
``Operation`` protocol: a ``name`` string and an ``execute`` method.
Operations are stateless with respect to infrastructure — they receive a
session and emit structured events, but do not open connections or manage
transactions.

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

**train_reranker**
   Trains a LightGBM binary classifier re-ranker from a PredictionSet +
   EvaluationSet pair. Uses temporal holdout labels and 22 features (embedding
   distance, alignment metrics, taxonomy, aggregate signals). Stores the
   serialized model, validation metrics, and feature importance in a
   ``RerankerModel`` row. ``TrainRerankerAutoOperation`` is a convenience
   variant that auto-selects training parameters.

.. automodule:: protea.core.operations.train_reranker
   :members:
   :undoc-members:
   :show-inheritance:
