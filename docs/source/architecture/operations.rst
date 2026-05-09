Operations
==========

.. contents:: On this page
   :local:
   :depth: 2

An **Operation** is the fundamental unit of domain logic in PROTEA. Every task
that a worker can execute — from a health-check ping to a full UniProt ingest —
is encapsulated in a class that satisfies the ``Operation`` protocol.

.. seealso::

   - :doc:`/appendix/howto_guides` — task-oriented recipes that submit each
     of these operations through the HTTP API with concrete payloads.
   - :doc:`/architecture/data_model` — the ORM tables that operations read
     from and write to (``Sequence``, ``Protein``, ``GOTerm``, ``AnnotationSet``,
     ``EmbeddingConfig``, ``GOPrediction``, ``EvaluationSet``…).
   - :doc:`/architecture/job_lifecycle` — how the worker layer dispatches
     these operations and tracks their progress.

The Operation protocol
----------------------

.. code-block:: python

   class Operation(Protocol):
       name: str

       def execute(
           self,
           session: Session,
           payload: dict[str, Any],
           *,
           emit: EmitFn,
       ) -> OperationResult: ...

``name``
   A stable string identifier used to route jobs. Must be unique across all
   registered operations and must match the ``operation`` field in the ``Job``
   row.

``execute``
   Receives an open SQLAlchemy session, a raw ``dict`` payload (validated
   internally), and an ``emit`` callback. Returns an ``OperationResult``.
   Must not manage sessions, queue connections, or threads.

``EmitFn``
   Type alias for ``Callable[[str, str | None, dict[str, Any], Level], None]``.
   Calling ``emit(event, message, fields, level)`` writes a ``JobEvent`` row
   in real time, visible on the frontend timeline.

``OperationResult``
   Frozen dataclass with four fields: ``result`` (stored in ``Job.meta``),
   optional ``progress_current`` / ``progress_total`` written back to
   the ``Job`` row for the progress bar, and ``deferred`` (bool) which tells
   ``BaseWorker`` that job completion will be signalled by child workers
   rather than immediately.

Payload validation
------------------

Every operation defines a **payload** class that extends ``ProteaPayload``:

.. code-block:: python

   class ProteaPayload(BaseModel, frozen=True):
       model_config = ConfigDict(strict=True)

``ProteaPayload`` is an immutable, strictly-typed Pydantic v2 base. Strict
mode prevents silent coercions (``"yes"`` is not a valid ``bool``). Each
operation calls ``MyPayload.model_validate(payload)`` at the top of
``execute()`` — validation errors surface as ``FAILED`` jobs with a clear
error message, before any DB writes occur.

Shared HTTP / retry behaviour
------------------------------

Both ``insert_proteins`` and ``fetch_uniprot_metadata`` implement an identical
resilience strategy against the UniProt REST API:

- **Cursor-based pagination** — the ``link`` response header carries the next
  cursor token. Iteration stops when no ``rel="next"`` link is present.
- **Exponential backoff with jitter** — on retriable errors (``429``, ``5xx``,
  network exceptions), the wait time is
  ``min(base × 2^(attempt-1), max) + uniform(0, jitter)``.
- **Retry-After header** — if UniProt returns a ``429`` with a ``Retry-After``
  header, that duration (capped at ``backoff_max_seconds``) is used directly.
- **max_retries** — after this many attempts the exception is re-raised,
  transitioning the job to ``FAILED``.

Every retry is logged via ``emit("http.retry", ...)`` so the frontend timeline
always shows when and why a delay occurred.

insert_proteins
---------------

**Operation name:** ``insert_proteins`` — queue: ``protea.jobs``

| **How to invoke this:** see *Submit a job via the API* in :doc:`/appendix/howto_guides`.
| **Tables touched:** ``Protein`` and ``Sequence`` (see :doc:`/architecture/data_model`).

Fetches protein sequences from the UniProt REST API in FASTA format and
upserts them into the ``protein`` and ``sequence`` tables.

Payload fields
~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 25 10 65

   * - Field
     - Default
     - Description
   * - ``search_criteria``
     - *(required)*
     - Raw UniProt query string. Example: ``reviewed:true AND organism_id:9606``
   * - ``page_size``
     - ``500``
     - Results per page (1 – ∞). Larger values reduce round-trips.
   * - ``total_limit``
     - ``null``
     - Stop after this many records (useful for testing).
   * - ``timeout_seconds``
     - ``60``
     - HTTP request timeout per page.
   * - ``include_isoforms``
     - ``true``
     - Append ``includeIsoform=true`` to the UniProt query.
   * - ``compressed``
     - ``false``
     - Request gzip-compressed responses.
   * - ``max_retries``
     - ``6``
     - Maximum retry attempts per page before raising.
   * - ``backoff_base_seconds``
     - ``0.8``
     - Exponential backoff base (seconds).
   * - ``backoff_max_seconds``
     - ``20.0``
     - Maximum wait between retries (seconds).
   * - ``jitter_seconds``
     - ``0.4``
     - Random jitter added to each backoff wait.
   * - ``user_agent``
     - *PROTEA/insert_proteins ...*
     - ``User-Agent`` header sent to UniProt.

Execution flow
~~~~~~~~~~~~~~

.. code-block:: text

   1. validate payload (Pydantic)
   2. for each FASTA page from UniProt:
      a. parse FASTA → list of records (accession, sequence, metadata)
      b. compute MD5 hash per sequence
      c. bulk-load existing Sequence rows by hash (chunks of 5 000)
      d. INSERT missing Sequence rows → obtain IDs
      e. bulk-load existing Protein rows by accession (chunks of 5 000)
      f. INSERT new Protein rows / conservative UPDATE existing rows
      g. session.flush() — no commit per page (commit on job success)
      h. emit("insert_proteins.page_done", ...)
   3. if total_limit reached → emit warning + break
   4. emit("insert_proteins.done", ...)
   5. return OperationResult(result={counts and timing})

Sequence deduplication
~~~~~~~~~~~~~~~~~~~~~~~

``Sequence`` rows are keyed by ``sequence_hash`` (MD5 of the amino-acid
string). Many ``Protein`` rows can point to the same ``Sequence`` row —
``sequence_id`` is deliberately non-unique. This eliminates redundant storage
for identical sequences across species or isoforms.

Conservative protein update
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For proteins that already exist, ``insert_proteins`` applies a
*fill-in-blanks* policy: it only overwrites a field if the current DB value
is ``None`` or empty. Existing non-null values are never overwritten. This
prevents a re-ingestion from degrading data that was enriched by a later step.

Isoform handling
~~~~~~~~~~~~~~~~

Accessions of the form ``<canonical>-<n>`` are parsed by
``Protein.parse_isoform()``. Both the isoform accession and the canonical
accession are stored. ``is_canonical = False`` for isoforms;
``isoform_index`` stores the numeric suffix.

fetch_uniprot_metadata
-----------------------

**Operation name:** ``fetch_uniprot_metadata`` — queue: ``protea.jobs``

| **How to invoke this:** see *Fetch UniProt metadata for existing proteins* in :doc:`/appendix/howto_guides`.
| **Tables touched:** ``ProteinUniProtMetadata`` (see :doc:`/architecture/data_model`).

Fetches functional annotations from the UniProt REST API in TSV format and
upserts ``ProteinUniProtMetadata`` rows, one per canonical accession.

Payload fields
~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 25 10 65

   * - Field
     - Default
     - Description
   * - ``search_criteria``
     - *(required)*
     - Raw UniProt query string.
   * - ``page_size``
     - ``500``
     - Results per TSV page.
   * - ``total_limit``
     - ``null``
     - Stop after this many rows.
   * - ``timeout_seconds``
     - ``60``
     - HTTP timeout per page.
   * - ``compressed``
     - ``true``
     - Request gzip-compressed TSV.
   * - ``max_retries``
     - ``6``
     - Maximum retry attempts.
   * - ``backoff_base_seconds``
     - ``0.8``
     - Backoff base (seconds).
   * - ``backoff_max_seconds``
     - ``20.0``
     - Maximum wait (seconds).
   * - ``jitter_seconds``
     - ``0.4``
     - Jitter added to backoff.
   * - ``commit_every_page``
     - ``true``
     - Commit after each page (reduces memory pressure on large ingests).
   * - ``update_protein_core``
     - ``true``
     - Backfill ``reviewed``, ``organism``, ``gene_name``, ``length`` on
       existing ``Protein`` rows if those fields are currently ``null``.
   * - ``user_agent``
     - *PROTEA/fetch_uniprot_metadata ...*
     - ``User-Agent`` header.

TSV field mapping
~~~~~~~~~~~~~~~~~

The operation requests 25 TSV fields from UniProt and maps them to
``ProteinUniProtMetadata`` columns:

.. list-table::
   :header-rows: 1

   * - DB column
     - UniProt TSV header
   * - ``function_cc``
     - Function [CC]
   * - ``catalytic_activity``
     - Catalytic activity
   * - ``ec_number``
     - EC number
   * - ``pathway``
     - Pathway
   * - ``kinetics``
     - Kinetics
   * - ``absorption``
     - Absorption
   * - ``active_site``
     - Active site
   * - ``binding_site``
     - Binding site
   * - ``cofactor``
     - Cofactor
   * - ``dna_binding``
     - DNA binding
   * - ``activity_regulation``
     - Activity regulation
   * - ``ph_dependence``
     - pH dependence
   * - ``redox_potential``
     - Redox potential
   * - ``rhea_id``
     - Rhea ID
   * - ``site``
     - Site
   * - ``temperature_dependence``
     - Temperature dependence
   * - ``keywords``
     - Keywords
   * - ``features``
     - Features

Isoform scoping
~~~~~~~~~~~~~~~

Because ``ProteinUniProtMetadata`` is keyed by ``canonical_accession``, all
isoforms of a protein share a single metadata record. The canonical accession
is resolved via ``Protein.parse_isoform(accession)[0]`` for each row in the
TSV response.

load_ontology_snapshot
----------------------

**Operation name:** ``load_ontology_snapshot`` — queue: ``protea.jobs``

| **How to invoke this:** see *Load a GO ontology snapshot* in :doc:`/appendix/howto_guides`.
| **Tables touched:** ``OntologySnapshot``, ``GOTerm``, ``GOTermRelationship`` (see *GO ontology* in :doc:`/architecture/data_model`).

Downloads a GO OBO file and populates ``OntologySnapshot`` + ``GOTerm`` +
``GOTermRelationship`` rows. Idempotent: if a snapshot with the same
``obo_version`` already exists and its relationships are present, the
operation returns immediately without writing anything.

Payload fields
~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 25 10 65

   * - Field
     - Default
     - Description
   * - ``obo_url``
     - *(required)*
     - Direct HTTP(S) URL to the ``.obo`` file (e.g. the EBI GO release).
   * - ``timeout_seconds``
     - ``120``
     - HTTP download timeout in seconds.
   * - ``force_relationships``
     - ``false``
     - Re-insert relationships even if the snapshot already exists with relationships.

Execution flow
~~~~~~~~~~~~~~

.. code-block:: text

   1. validate payload
   2. download OBO text (HTTP GET, single request)
   3. extract ``data-version`` header → obo_version
   4. check DB for existing OntologySnapshot with that obo_version
      a. exists + has relationships → skip (idempotent)
      b. exists + no relationships → backfill relationships only
      c. does not exist → full insert
   5. parse OBO stanzas → GOTerm rows (aspect mapped from namespace)
   6. session.add_all(go_terms)
   7. parse relationship edges → GOTermRelationship rows
   8. session.add_all(relationships)
   9. emit("load_ontology_snapshot.done", ...)
   10. return OperationResult(result={snapshot_id, term_count, rel_count})

load_goa_annotations
---------------------

**Operation name:** ``load_goa_annotations`` — queue: ``protea.jobs``

| **How to invoke this:** see *Load GOA annotations* in :doc:`/appendix/howto_guides`.
| **Tables touched:** ``AnnotationSet``, ``ProteinGOAnnotation`` (see *Annotation sets* in :doc:`/architecture/data_model`).

Streams a GOA GAF 2.2 file (plain or gzip) and bulk-inserts
``AnnotationSet`` + ``ProteinGOAnnotation`` rows. Only accessions already
present in the ``protein`` table are retained; all others are silently
skipped.

Payload fields
~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 25 10 65

   * - Field
     - Default
     - Description
   * - ``ontology_snapshot_id``
     - *(required)*
     - UUID of the ``OntologySnapshot`` row these annotations belong to.
   * - ``gaf_url``
     - *(required)*
     - HTTP(S) URL to the GAF file (plain or ``.gz``).
   * - ``source_version``
     - *(required)*
     - Human-readable version label stored in ``AnnotationSet.source_version``.
   * - ``page_size``
     - ``10000``
     - Lines buffered per commit cycle.
   * - ``timeout_seconds``
     - ``300``
     - HTTP stream timeout.
   * - ``commit_every_page``
     - ``true``
     - Commit after each page to bound memory use (recommended for large GAFs).
   * - ``total_limit``
     - ``null``
     - Stop after this many annotation rows (for testing).

Execution flow
~~~~~~~~~~~~~~

.. code-block:: text

   1. validate payload; resolve OntologySnapshot and canonical accession set
   2. create AnnotationSet row (source="goa")
   3. stream GAF lines:
      a. skip comment lines (starting with "!")
      b. parse 15-column tab-separated record
      c. filter against canonical accessions — skip unknown
      d. resolve go_term_id from go_id; skip if term unknown in snapshot
      e. buffer ProteinGOAnnotation rows
      f. flush + commit every page_size rows
   4. emit("load_goa_annotations.done", ...)
   5. return OperationResult(result={annotation_set_id, inserted, skipped})

load_quickgo_annotations
-------------------------

**Operation name:** ``load_quickgo_annotations`` — queue: ``protea.jobs``

| **How to invoke this:** see *Load QuickGO annotations* in :doc:`/appendix/howto_guides`.
| **Tables touched:** ``AnnotationSet``, ``ProteinGOAnnotation`` (see *Annotation sets* in :doc:`/architecture/data_model`).

Streams GO annotations from the QuickGO bulk download TSV API. Proteins
are determined by the canonical accessions already in the DB — no external
accession list is needed. Supports optional ECO ID → evidence code mapping,
taxon filtering, and aspect filtering.

Payload fields
~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 30 10 60

   * - Field
     - Default
     - Description
   * - ``ontology_snapshot_id``
     - *(required)*
     - UUID of the ``OntologySnapshot`` row.
   * - ``source_version``
     - *(required)*
     - Version label for ``AnnotationSet.source_version``.
   * - ``quickgo_base_url``
     - *EBI QuickGO*
     - Base URL for the QuickGO download endpoint.
   * - ``gene_product_ids``
     - ``null``
     - Explicit accession filter; ``null`` = use DB accessions.
   * - ``use_db_accessions``
     - ``true``
     - Pull the accession filter from the ``protein`` table.
   * - ``eco_mapping_url``
     - ``null``
     - URL to a GAF-ECO mapping file for evidence code resolution.
   * - ``page_size``
     - ``10000``
     - Rows buffered per commit.
   * - ``commit_every_page``
     - ``true``
     - Commit after each page.
   * - ``total_limit``
     - ``null``
     - Row cap (for testing).
   * - ``gene_product_batch_size``
     - ``200``
     - Accessions per QuickGO API request when using ``use_db_accessions``.

compute_embeddings
------------------

**Operation name:** ``compute_embeddings`` — queue: ``protea.embeddings``
(coordinator, serialised; one at a time via ``RetryLaterError`` if GPU busy)

| **How to invoke this:** see *Compute sequence embeddings* in :doc:`/appendix/howto_guides`.
| **Tables touched:** ``EmbeddingConfig``, ``SequenceEmbedding`` (see *Embeddings* in :doc:`/architecture/data_model`).
| **Lifecycle pattern:** parent-child coordinator with deferred completion — see :doc:`/architecture/job_lifecycle`.

Coordinator operation: determines which sequences need embeddings and fans
out ``ComputeEmbeddingsBatchOperation`` messages to ``protea.embeddings.batch``.
Does **not** run GPU inference directly; returns ``OperationResult(deferred=True)``
immediately after publishing batch messages.

Payload fields
~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 30 10 60

   * - Field
     - Default
     - Description
   * - ``embedding_config_id``
     - *(required)*
     - UUID of the ``EmbeddingConfig`` that defines model, layers, pooling.
   * - ``accessions``
     - ``null``
     - List of UniProt accessions to embed; ``null`` = embed all proteins.
   * - ``query_set_id``
     - ``null``
     - UUID of a ``QuerySet`` (alternative to ``accessions``).
   * - ``sequences_per_job``
     - ``64``
     - Sequences per batch message. Tune to GPU memory.
   * - ``device``
     - ``"cuda"``
     - Device for batch workers (``"cuda"`` or ``"cpu"``).
   * - ``skip_existing``
     - ``true``
     - Skip sequences that already have an embedding for this config.
   * - ``batch_size``
     - ``1``
     - Model forward-pass batch size inside each batch worker. Default of
       ``1`` is deliberate: the largest supported backend
       (``prot_t5_xl_uniref50`` at ``max_length=2048``) OOMs on a 12 GB
       GPU with anything higher. Callers running smaller models on roomier
       GPUs can raise this explicitly.

Execution flow
~~~~~~~~~~~~~~

.. code-block:: text

   1. resolve embedding config; raise RetryLaterError(delay=60s) if another
      embedding job is RUNNING (GPU exclusive lock)
   2. resolve sequence IDs from accessions or query_set_id
   3. if skip_existing: filter out sequence IDs that already have embeddings
   4. partition sequence IDs into batches of sequences_per_job
   5. publish N ComputeEmbeddingsBatch messages to protea.embeddings.batch
   6. publish N StoreEmbeddings slots to protea.embeddings.write
   7. update Job.progress_total = N
   8. return OperationResult(deferred=True, result={"batches": N})

Batch and write workers
~~~~~~~~~~~~~~~~~~~~~~~

The ``compute_embeddings_batch`` (OperationConsumer on ``protea.embeddings.batch``)
runs GPU inference per batch and publishes float32 vectors to ``protea.embeddings.write``.
The ``store_embeddings`` (OperationConsumer on ``protea.embeddings.write``) bulk-inserts
``SequenceEmbedding`` rows and atomically increments ``Job.progress_current``,
closing the parent job when all batches are done.

predict_go_terms
----------------

**Operation name:** ``predict_go_terms`` — queue: ``protea.predictions``
(coordinator; fans out KNN batch workers on ``protea.predictions.batch``)

| **How to invoke this:** see *Predict GO terms* in :doc:`/appendix/howto_guides`.
| **Tables touched:** ``PredictionSet``, ``GOPrediction`` (see *Predictions* in :doc:`/architecture/data_model`).
| **Lifecycle pattern:** parent-child coordinator with deferred completion — see :doc:`/architecture/job_lifecycle`.
| **KNN backend rationale:** see :doc:`/adr/001-knn-without-pgvector`.

Coordinator operation: loads reference embeddings into a process-level cache,
partitions query proteins into batches, and fans out ``PredictGOTermsBatch``
messages to ``protea.predictions.batch``. Returns ``OperationResult(deferred=True)``.

Payload fields
~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 30 10 60

   * - Field
     - Default
     - Description
   * - ``embedding_config_id``
     - *(required)*
     - UUID of the ``EmbeddingConfig`` used for both query and reference embeddings.
   * - ``annotation_set_id``
     - *(required)*
     - UUID of the ``AnnotationSet`` supplying GO labels for reference proteins.
   * - ``ontology_snapshot_id``
     - *(required)*
     - UUID of the ``OntologySnapshot`` used to resolve GO terms.
   * - ``query_accessions``
     - ``null``
     - List of query protein accessions; ``null`` = use all proteins.
   * - ``query_set_id``
     - ``null``
     - UUID of a ``QuerySet`` (alternative to ``query_accessions``).
   * - ``limit_per_entry``
     - ``5``
     - Maximum GO predictions per query protein.
   * - ``distance_threshold``
     - ``null``
     - Discard neighbors beyond this distance; ``null`` = no threshold.
   * - ``batch_size``
     - ``1024``
     - Query proteins per batch message.
   * - ``search_backend``
     - ``"numpy"``
     - KNN backend: ``"numpy"`` (brute-force) or ``"faiss"``.
   * - ``metric``
     - ``"cosine"``
     - Distance metric (``"cosine"`` or ``"l2"``).
   * - ``faiss_index_type``
     - ``"Flat"``
     - FAISS index type: ``"Flat"``, ``"IVFFlat"``, or ``"HNSW"``.
   * - ``compute_alignments``
     - ``true``
     - Compute NW + SW pairwise alignments (parasail) for each prediction.
   * - ``compute_taxonomy``
     - ``true``
     - Compute taxonomic distance (ete3 NCBITaxa) for each prediction.
   * - ``compute_reranker_features``
     - ``true``
     - Compute 5 aggregate re-ranker features per prediction: ``vote_count``,
       ``k_position``, ``go_term_frequency``, ``ref_annotation_density``, and
       ``neighbor_distance_std``.
   * - ``compute_v6_features``
     - ``false``
     - Compute the 25-column v6 feature family: 6 anc2vec ancestry signals,
       3 taxonomic-voter aggregates, 16 PCA-projected embedding columns. PCA
       state is fit once per ``EmbeddingConfig`` and persisted; enabling this
       flag adds those 25 columns to every ``GOPrediction`` row.
   * - ``expand_votes_to_ancestors``
     - ``false``
     - Synthesise the ``is_a`` / ``part_of`` ancestor closure of every leaf
       candidate as additional records. Matches the candidate distribution
       a lab booster sees when trained with ancestor expansion enabled.
   * - ``aspect_separated_knn``
     - ``true``
     - Build three separate KNN indices, one per GO aspect (BPO/MFO/CCO),
       so every query receives candidates in every aspect even when its
       nearest neighbours in a unified index happen to be annotated only
       in one or two. A common cause of BPO recall ceilings.
   * - ``reranker_model_id``
     - ``null``
     - Optional UUID of a registered ``RerankerModel`` (typically produced
       by ``protea-reranker-lab`` and registered via
       ``scripts/register_reranker.py``). When set, the coordinator looks
       up ``artifact_uri`` and ``feature_schema_sha``, validates both are
       present (raises ``ValueError`` otherwise), and emits a
       ``predict_go_terms.reranker_bound`` event. The snapshotted
       ``artifact_uri`` / ``feature_schema_sha`` are then propagated into
       every batch payload so workers do not have to re-query the row.

Reranker validation and fallback
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When ``reranker_model_id`` is set, each batch worker runs
``_apply_reranker_if_aligned`` after ``prediction_dicts`` has been built.
The flow is:

1. Compute ``live_sha = compute_feature_schema_sha(active_families)`` from
   the live ``compute_alignments`` / ``compute_taxonomy`` / ``compute_v6_features``
   flags.
2. If ``live_sha != reranker_feature_schema_sha`` the worker emits
   ``reranker.schema_mismatch`` (level ``error``) and returns stats with
   ``applied=False``. The batch continues using the KNN ordering — **no
   crash**, and no partial scoring.
3. On match, the worker attaches the GOTerm aspect to each dict, calls
   :func:`protea.core.reranking.apply_reranker`, and writes the
   ``reranker_score`` field into every prediction dict in memory.

``reranker_score`` is **in-memory only** — ``GOPrediction`` does not yet
have a column for it, so the score is surfaced through the
``predict_go_terms_batch.done`` event (nested ``reranker`` block with
applied/skipped/mean_score/etc.) but is not persisted.

.. note::
   The lab contract is imported lazily in the batch worker. If the
   production image ships without ``protea_reranker_lab`` installed the
   worker emits ``reranker.skipped`` with ``reason=contracts_unavailable``
   and proceeds with KNN ordering.

Reference cache
~~~~~~~~~~~~~~~

Reference embeddings are loaded once per (``embedding_config_id``,
``annotation_set_id``) pair and stored as a process-level float16 numpy
array (max 1 entry, LRU-evicted on config change). This avoids reloading
hundreds of thousands of vectors for every batch.

Batch and write workers
~~~~~~~~~~~~~~~~~~~~~~~

The ``predict_go_terms_batch`` (OperationConsumer on ``protea.predictions.batch``)
runs KNN search and GO transfer per batch. The ``store_predictions``
(OperationConsumer on ``protea.predictions.write``) bulk-inserts
``GOPrediction`` rows and atomically increments ``Job.progress_current``.

ping
----

**Operation name:** ``ping`` — queue: ``protea.ping``

Smoke-test operation. Accepts no required payload fields, emits a single
``ping.pong`` event, and returns immediately. Used to verify end-to-end
connectivity of the job queue without touching the protein data tables.

generate_evaluation_set
-----------------------

**Operation name:** ``generate_evaluation_set`` — queue: ``protea.jobs``

Computes the CAFA-style evaluation delta between two ``AnnotationSet`` rows
(old → new) and stores an ``EvaluationSet`` row with summary statistics.
Follows the CAFA5 evaluation protocol exactly.

Payload fields
~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 35 10 55

   * - Field
     - Default
     - Description
   * - ``old_annotation_set_id``
     - *(required)*
     - UUID of the older annotation set (t0, used as reference).
   * - ``new_annotation_set_id``
     - *(required)*
     - UUID of the newer annotation set (t1, ground truth source).

Both annotation sets must share the same ``ontology_snapshot_id``; the
operation raises ``ValueError`` otherwise.

Execution flow
~~~~~~~~~~~~~~

.. code-block:: text

   1. validate payload; load AnnotationSet rows; assert same ontology snapshot
   2. call compute_evaluation_data() — see protea.core.evaluation:
      a. load GO DAG children map (is_a / part_of only)
      b. build NOT-propagation exclusion set (negated terms + GO descendants)
      c. load experimental annotations for old and new sets, filtered by
         exclusion set and evidence codes (EXP, IDA, IMP, IGI, IEP, IPI, …)
      d. classify per (protein, namespace) into NK / LK / PK:
         - NK: protein had NO annotations in any namespace at t0
         - LK: protein had annotations in some namespaces but not in namespace S;
               novel terms in S are LK ground truth
         - PK: protein had annotations in namespace S at t0 and gained new terms
               in S; novel terms are PK ground truth, old terms go to pk_known
   3. emit stats (delta_proteins, nk/lk/pk counts and annotations)
   4. INSERT EvaluationSet row with stats stored in JSONB
   5. return OperationResult(result={evaluation_set_id, ...stats})

NK/LK/PK classification note
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Classification is per **(protein, namespace)**, not globally per protein.
A protein that had MFO and BPO annotations at t0 and gains new CCO and BPO
annotations at t1 will be **LK** in CCO (no prior CCO annotations) and **PK**
in BPO (had prior BPO annotations) simultaneously. This matches the CAFA5
evaluation protocol.

Ground truth files are computed on-demand by the download endpoints (not
stored in the DB) using the same ``compute_evaluation_data`` logic.

run_cafa_evaluation
-------------------

**Operation name:** ``run_cafa_evaluation`` — queue: ``protea.evaluations``

| **How to invoke this:** see *Run a CAFA evaluation* in :doc:`/appendix/howto_guides`.
| **Tables touched:** ``EvaluationSet``, ``EvaluationResult`` (see *Evaluation* in :doc:`/architecture/data_model`).
| **Protocol:** :doc:`/architecture/evaluation` for the NK/LK/PK semantics.

Runs the ``cafaeval`` evaluator against NK, LK, and PK ground-truth settings
for a given ``EvaluationSet`` × ``PredictionSet`` pair. Persists an
``EvaluationResult`` row with per-namespace Fmax, precision, recall, τ, and
coverage for each setting.

Payload fields
~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 30 10 60

   * - Field
     - Default
     - Description
   * - ``evaluation_set_id``
     - *(required)*
     - UUID of the ``EvaluationSet`` to evaluate against.
   * - ``prediction_set_id``
     - *(required)*
     - UUID of the ``PredictionSet`` containing predictions to score.
   * - ``max_distance``
     - ``null``
     - Discard predictions with cosine distance above this threshold before
       scoring (range 0 – 2). ``null`` = no threshold.
   * - ``artifacts_dir``
     - ``null``
     - Filesystem path where cafaeval output files (PR curves, TSVs) are
       written. ``null`` = artifacts are written to a temp directory and
       discarded after the job.

Execution flow
~~~~~~~~~~~~~~

.. code-block:: text

   1. load EvaluationSet + PredictionSet + OntologySnapshot from DB
   2. compute_evaluation_data() — same delta as generate_evaluation_set
   3. download OBO file from snapshot.obo_url to a temp directory
   4. write ground-truth TSVs: gt_NK.tsv, gt_LK.tsv, gt_PK.tsv,
      known_terms.tsv, pk_known_terms.tsv
   5. write predictions in CAFA format (protein \\t go_id \\t score),
      score = max(0, 1 - distance), filtered to delta proteins only
   6. session.commit() — releases DB connection before cafaeval forks workers
   7. for each setting in (NK, LK, PK):
      a. NK pass: cafa_eval(obo, predictions/, gt_NK.tsv)
      b. LK pass: cafa_eval(obo, predictions/, gt_LK.tsv)
      c. PK pass: cafa_eval(obo, predictions/, gt_PK.tsv, exclude=pk_known_terms.tsv)
      d. parse per-namespace Fmax / precision / recall / τ / coverage
      e. if artifacts_dir: write cafaeval PR curves and metric TSVs
   8. INSERT EvaluationResult row with results dict (NK → {BPO, MFO, CCO})
   9. return OperationResult(result={evaluation_result_id, results})

PK known-terms
~~~~~~~~~~~~~~

For the PK pass, ``pk_known_terms.tsv`` is passed to ``cafa_eval`` as the
``-known`` (``exclude``) argument. This file contains all experimental
annotations from the *old* snapshot for PK proteins in the relevant namespace.
``cafaeval`` uses this to exclude known annotations from scoring — methods
that simply repeat prior annotations receive no credit for PK predictions.

SIGTERM handling
~~~~~~~~~~~~~~~~

``cafaeval`` uses Python ``multiprocessing.Pool`` internally. Before each
``cafa_eval()`` call, the operation temporarily resets ``SIGTERM`` and
``SIGINT`` handlers to defaults so that forked pool workers can be terminated
cleanly. The original handlers are restored afterwards.

.. _train-reranker-operation:

train_reranker
--------------

.. deprecated:: contract-first-lab-integration

   LightGBM training has been decoupled from PROTEA and now lives in
   `protea-reranker-lab <https://github.com/frapercan/protea-reranker-lab>`_.
   ``TrainRerankerOperation`` and ``TrainRerankerAutoOperation`` are
   **no longer registered** in the ``OperationRegistry`` — a ``POST /jobs``
   request with ``operation_name: train_reranker`` or ``train_reranker_auto``
   will be rejected. The classes remain importable as internal helpers
   that :ref:`export-research-dataset-operation` reuses in-process to run
   the shared KNN + feature-generation pipeline in ``dump_only`` mode.

   The narrative below (feature matrix, LightGBM hyperparameters,
   validation metrics) is retained as the canonical specification of
   the booster that the lab trains — the lab consumes the exact same
   feature schema PROTEA produces. For the live end-to-end flow, see
   :ref:`export-research-dataset-operation` below and the
   ``/reranker-models/import`` HTTP surface.

**Operation name:** ``train_reranker`` — *internal helper, not queued*

| **How to invoke this:** no longer invocable via ``/jobs``. See
  :ref:`export-research-dataset-operation` and the ``POST /datasets``
  endpoint; booster training runs in ``protea-reranker-lab``.
| **Tables touched:** ``RerankerModel`` (written by ``/reranker-models/import``, not by this helper directly).
| **Evaluation context:** :doc:`/architecture/evaluation` and :doc:`/results`.

Trains a LightGBM binary classifier that re-scores GO term predictions on top
of the embedding-based KNN baseline. The input is a
``(PredictionSet, EvaluationSet)`` pair: predictions supply the feature matrix
(stored in ``GOPrediction`` columns populated at prediction time through
``compute_alignments=True`` and ``compute_taxonomy=True``), the evaluation set
supplies the ground-truth delta used to derive binary labels.

The design follows the principle of keeping the training signal
*interpretable*: the re-ranker is not an end-to-end deep network over raw
embeddings, but a gradient-boosted decision tree over hand-engineered features
that each have a well-known meaning (alignment identity, taxonomic distance,
neighbour agreement). Feature importance can therefore be inspected directly
and used to drive the iterative training loop documented in :doc:`../results`.

Payload fields
~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 30 10 60

   * - Field
     - Default
     - Description
   * - ``prediction_set_id``
     - *(required)*
     - UUID of the ``PredictionSet`` to use as training data.
   * - ``evaluation_set_id``
     - *(required)*
     - UUID of the ``EvaluationSet`` providing ground-truth labels.

Feature matrix
~~~~~~~~~~~~~~

The feature set is defined statically in
:data:`protea.core.reranker.NUMERIC_FEATURES` and
:data:`protea.core.reranker.CATEGORICAL_FEATURES`. It comprises **20 numeric**
and **3 categorical** columns, grouped by origin:

.. list-table:: Numeric features (20)
   :header-rows: 1
   :widths: 25 15 60

   * - Feature
     - Group
     - Description
   * - ``distance``
     - KNN
     - Cosine or L2 distance from query to nearest-neighbour reference.
       Lower = closer in embedding space.
   * - ``identity_nw``
     - Alignment
     - Needleman–Wunsch global identity percentage (parasail, BLOSUM62).
   * - ``similarity_nw``
     - Alignment
     - NW similarity percentage (identical + positive-scoring substitutions).
   * - ``alignment_score_nw``
     - Alignment
     - Raw NW alignment score.
   * - ``gaps_pct_nw``
     - Alignment
     - Fraction of aligned positions that are gaps in the NW alignment.
   * - ``alignment_length_nw``
     - Alignment
     - Length of the NW alignment (number of columns).
   * - ``identity_sw``
     - Alignment
     - Smith–Waterman local identity percentage.
   * - ``similarity_sw``
     - Alignment
     - SW similarity percentage.
   * - ``alignment_score_sw``
     - Alignment
     - Raw SW alignment score.
   * - ``gaps_pct_sw``
     - Alignment
     - Gap fraction in the SW alignment.
   * - ``alignment_length_sw``
     - Alignment
     - Length of the SW alignment.
   * - ``length_query``
     - Length
     - Residue count of the query protein sequence.
   * - ``length_ref``
     - Length
     - Residue count of the matched reference sequence.
   * - ``taxonomic_distance``
     - Taxonomy
     - Distance between query and reference in the NCBI taxonomy tree
       (ete3 ``NCBITaxa``).
   * - ``taxonomic_common_ancestors``
     - Taxonomy
     - Number of common ancestors shared between query and reference taxa.
   * - ``vote_count``
     - Aggregate
     - Number of top-``k`` neighbours that transfer the same GO term to the
       query (higher = stronger consensus).
   * - ``k_position``
     - Aggregate
     - Rank of the first neighbour supporting the term (1 = top hit).
   * - ``go_term_frequency``
     - Aggregate
     - Frequency of the GO term in the entire reference set (prior).
   * - ``ref_annotation_density``
     - Aggregate
     - Average number of GO annotations per reference protein in the batch.
   * - ``neighbor_distance_std``
     - Aggregate
     - Standard deviation of KNN distances for the query
       (low = tight cluster, high = uncertain).

.. list-table:: Categorical features (3)
   :header-rows: 1
   :widths: 25 75

   * - Feature
     - Description
   * - ``qualifier``
     - GO annotation qualifier propagated from the reference
       (e.g. ``enables``, ``involved_in``, ``part_of``). ``NOT``-qualified
       references are already filtered out upstream by the evaluation
       pipeline.
   * - ``evidence_code``
     - GO evidence code of the reference annotation (EXP, IDA, IMP, …, or
       electronic IEA). Acts as a prior on annotation reliability.
   * - ``taxonomic_relation``
     - Coarse taxonomic relation between query and reference
       (``same_species``, ``same_genus``, ``same_family``, …, ``unrelated``).

All three categorical features are converted to pandas ``category`` dtype in
:func:`protea.core.reranker.prepare_dataset`; LightGBM consumes them directly,
so no manual label encoding is required. Missing values in numeric columns
are left as ``NaN`` — LightGBM handles them natively by learning an optimal
direction at each split.

Training protocol
~~~~~~~~~~~~~~~~~

The training loop is implemented in :func:`protea.core.reranker.train` and
proceeds as follows:

1. **Label derivation.** Each row of the training DataFrame carries a binary
   ``label`` column: ``1`` if the predicted ``(protein, go_term)`` pair is
   present in the NK ∪ LK ∪ PK ground truth of the associated
   ``EvaluationSet``, ``0`` otherwise. The label is computed upstream by the
   ``/scoring/prediction-sets/{id}/training-data.tsv`` endpoint.

2. **Stratified split.** Positives and negatives are split *independently*
   into train and validation fractions (default ``val_fraction = 0.2``) so
   that the positive rate is preserved on both sides. Shuffling uses a fixed
   seed (default ``42``) for reproducibility.

3. **Optional class-balance control.** Because the raw training set is
   extremely imbalanced (positive rate typically ≤ 1 %), the caller can
   pass ``neg_pos_ratio`` to subsample negatives independently on the train
   and validation splits. The default is ``None`` (keep all negatives); the
   results chapter documents the ratios used for each re-ranker iteration.

4. **Optional per-sample weighting.** When ``sample_weight`` is provided
   (for example, the Information Accretion of each GO term), the weights
   are attached to both the training and validation datasets so that
   high-weight samples contribute more to the LightGBM loss. This lets the
   re-ranker be trained directly against the IA-weighted Fmax metric used
   in :doc:`evaluation`.

5. **LightGBM training.** The Booster is trained with the default
   hyperparameters below, merged with any overrides passed in the ``params``
   argument. Early stopping is enabled on the validation split.

.. list-table:: LightGBM default hyperparameters
   :header-rows: 1
   :widths: 30 30 40

   * - Parameter
     - Default
     - Notes
   * - ``objective``
     - ``binary``
     - Binary classification.
   * - ``metric``
     - ``["binary_logloss", "auc"]``
     - Both are reported; early stopping uses the first metric.
   * - ``boosting_type``
     - ``gbdt``
     - Standard gradient-boosted decision trees.
   * - ``num_leaves``
     - ``31``
     - LightGBM default.
   * - ``learning_rate``
     - ``0.01``
     - Low LR + early stopping; see ``num_boost_round``.
   * - ``feature_fraction``
     - ``0.8``
     - Column sub-sampling per tree (reduces overfitting).
   * - ``bagging_fraction``
     - ``0.8``
     - Row sub-sampling per boosting round.
   * - ``bagging_freq``
     - ``5``
     - Apply bagging every 5 iterations.
   * - ``seed``
     - ``42``
     - Fixed for reproducibility.
   * - ``num_boost_round``
     - ``1000``
     - Maximum boosting iterations.
   * - ``early_stopping_rounds``
     - ``50``
     - Stop if validation logloss does not improve for 50 rounds.

Validation metrics
~~~~~~~~~~~~~~~~~~

After training, the validation set is scored and the following metrics are
written to ``RerankerModel.metrics`` as a JSONB dict:

- ``best_iteration`` — boosting round at which early stopping triggered.
- ``val_auc`` — ROC-AUC on the validation split.
- ``val_logloss`` — binary logloss at the best iteration.
- ``val_precision`` / ``val_recall`` / ``val_f1`` — computed at the
  ``p ≥ 0.5`` decision threshold. These are reported for quick inspection
  but are **not** the primary objective: the re-ranker is ultimately scored
  through the full CAFA evaluation pipeline (:doc:`evaluation`), which uses
  IA-weighted Fmax per ``(category, namespace)`` cell.
- ``train_samples`` / ``val_samples`` — row counts after any negative
  subsampling.
- ``positive_rate`` — fraction of rows with ``label == 1`` *before* any
  subsampling.

In addition, :func:`~protea.core.reranker.train` returns a
gain-based feature-importance dictionary (``feature_name → total gain``),
which is persisted in ``RerankerModel.feature_importance``. This is the
signal used in :doc:`../results` to drive the v1 → v2 → v3 iterations of the
re-ranker.

.. note::
   Cross-validation is **not** performed inside the training operation.
   The temporal-holdout re-ranker design uses 13 historical GOA splits
   (releases 160 through 220) as independent training folds; the
   cross-validation loop is driven at a higher level by
   ``scripts/run_experiments.py``, which invokes ``train_reranker`` once per
   split. See :doc:`../results` for the aggregate numbers.

train_reranker_auto
~~~~~~~~~~~~~~~~~~~

**Operation name:** ``train_reranker_auto`` — *internal helper, not queued*

.. note::

   Formerly a convenience variant that auto-selected the most recent
   ``PredictionSet`` and ``EvaluationSet`` and forwarded them to
   ``train_reranker``. Like ``train_reranker`` it is **no longer
   registered** in the ``OperationRegistry``. The class survives only
   as the in-process engine that ``ExportResearchDatasetOperation``
   drives in ``dump_only`` mode to produce the frozen parquet triple
   consumed by the lab.

.. _export-research-dataset-operation:

export_research_dataset
------------------------

**Operation name:** ``export_research_dataset`` — queue: ``protea.training``

| **How to invoke this:** see *Registering a reranker from protea-reranker-lab* in :doc:`/appendix/howto_guides`.
| **Tables touched:** read-only over existing ``PredictionSet`` / feature data. Writes blobs via the configured ``ArtifactStore`` (no ORM writes).

Runs the same KNN + feature-generation pipeline as ``train_reranker_auto``
but **skips LightGBM training entirely** and publishes the resulting
``train.parquet`` / ``eval.parquet`` / ``manifest.json`` triple through the
configured :class:`~protea.infrastructure.storage.ArtifactStore`. The
produced dataset is the canonical input consumed by
``protea-reranker-lab`` for offline re-ranker training.

The manifest embeds ``schema_version="v2"``, the PROTEA
``producer_version`` (``protea.__version__``) and ``producer_git_sha`` so
any lab run can be traced back to the exact PROTEA HEAD that produced
its training data.

Payload fields
~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 30 10 60

   * - Field
     - Default
     - Description
   * - ``embedding_config_id``
     - *(required)*
     - UUID of the ``EmbeddingConfig`` used for KNN.
   * - ``ontology_snapshot_id``
     - *(required)*
     - UUID of the ``OntologySnapshot`` used to resolve GO terms.
   * - ``train_versions``
     - *(required)*
     - List of historical annotation-set versions forming the training
       snapshot pairs (must contain at least 2 entries; sorted
       ascending).
   * - ``test_versions``
     - *(required)*
     - List of annotation-set versions reserved for evaluation
       (non-empty; sorted ascending).
   * - ``annotation_source``
     - ``"goa"``
     - Annotation source tag recorded in the manifest.
   * - ``output_name``
     - *(required)*
     - Human label for the published dataset. Used as ``name`` in the
       manifest and as the store key prefix ``datasets/<output_name>/``.
   * - ``k``
     - ``5``
     - KNN neighbour count.
   * - ``search_backend``
     - ``"faiss"``
     - KNN backend (``"numpy"`` or ``"faiss"``).
   * - ``compute_alignments``
     - ``false``
     - Include NW alignment and length feature families.
   * - ``compute_taxonomy``
     - ``false``
     - Include the taxonomy-pair feature family.
   * - ``expand_votes_to_ancestors``
     - ``false``
     - Propagate neighbour votes up the GO DAG before feature
       materialisation.
   * - ``use_embedding_pca``
     - ``false``
     - Include embedding PCA and v6-era feature families (enables
       ``anc2vec_*``, ``emb_pca``, ``taxonomy_voters``, ``go_context``).

Execution flow
~~~~~~~~~~~~~~

.. code-block:: text

   1. validate payload (Pydantic v2, strict mode)
   2. load Settings; resolve ArtifactStore via get_artifact_store(settings)
   3. run train_reranker_auto in dump-only mode against a temp dir
      (per_cell training_scope, no booster trained)
   4. for each of train.parquet / eval.parquet / manifest.json:
        store.put("datasets/<output_name>/<file>", path)
   5. emit export_research_dataset.published {backend, key_prefix, files}
   6. return OperationResult(result={
        output_name, key_prefix, storage_backend,
        train_uri, eval_uri, manifest_uri, ...auto_result
      })

Emitted events
~~~~~~~~~~~~~~

The operation relays the underlying ``train_reranker_auto`` events
under its own namespace (``train_reranker_auto.*`` →
``export_research_dataset.*``) and additionally emits:

- ``export_research_dataset.started`` — payload echoed back with
  resolved backend.
- ``export_research_dataset.rows_written`` — n_train_rows / n_eval_rows
  per shard once the parquets are materialised.
- ``export_research_dataset.published`` — ``{backend, key_prefix, files}``
  when the three artefacts have been uploaded successfully.
- ``export_research_dataset.completed`` — final summary including the
  three resulting URIs (``file://…`` or ``s3://bucket/key``).

Ephemeral consumer operations
-----------------------------

Four operations run as :class:`OperationConsumer` workers on the high-throughput
batch queues. They differ from the operations documented above in three ways:

1. **No ``Job`` row.** The coordinator publishes payloads directly to the queue;
   the consumer executes them without creating a child ``Job`` row in the DB.
   Progress is tracked by atomically incrementing
   ``Job.progress_current`` on the *parent* job.
2. **No HTTP endpoint.** These operations cannot be submitted through
   ``POST /jobs``. They are only reachable through the fan-out logic of the
   coordinator operations ``compute_embeddings`` and ``predict_go_terms``.
3. **Payload is the full message.** Because there is no ``Job`` row to read
   from, the full :class:`ProteaPayload` is serialised into the AMQP body.

They still implement the ``Operation`` protocol and are registered alongside
the other eleven in ``protea/core/operation_catalog.py``. Bringing the
total to **15 registered operations** (11 job-backed + 4 ephemeral).

compute_embeddings_batch
~~~~~~~~~~~~~~~~~~~~~~~~

**Queue:** ``protea.embeddings.batch`` — **consumer:** ``OperationConsumer``

Runs the protein language model forward pass for one batch of sequences.
Loads the model on first use, caches it at process level, performs GPU
inference (ESM-2, ESM-C, ProtT5/ProstT5, or Ankh base/large), pools
residue-level representations according to the ``EmbeddingConfig`` strategy,
converts to float32, and publishes a ``StoreEmbeddings`` message to
``protea.embeddings.write``.

Backends are selected via ``EmbeddingConfig.model_backend``:

* ``esm``   — HuggingFace ``EsmModel`` (ESM-2 family); single-sequence forward.
* ``esm3c`` — ESM SDK ``ESMC`` (ESM3c family); FP16 on GPU, no external tokenizer.
* ``t5``    — HuggingFace ``T5EncoderModel`` (ProstT5, ``prot_t5_xl_uniref50``…);
  the ``<AA2fold>`` prefix is auto-injected when the model name contains
  ``prostt5``.
* ``ankh``  — HuggingFace ``T5EncoderModel`` loaded via ``AutoTokenizer``
  (``ElnaggarLab/ankh-base``, ``ElnaggarLab/ankh-large``). Shares the batched
  T5 pipeline but with two mandatory deviations from the ProstT5 path,
  both verified end-to-end with real weights on 2026-04-10:

  1. **bfloat16 on CUDA, never float16.** Ankh was pre-trained on TPU in
     bfloat16 and its T5 LayerNorm overflows to ``NaN`` under FP16 on every
     forward pass. The loader pins ``torch_dtype=torch.bfloat16`` on GPU
     and ``torch.float32`` on CPU.
  2. **Char-level tokenisation with** ``is_split_into_words=True``. Ankh's
     SentencePiece vocabulary treats literal spaces as ``<unk>``, so the
     ProstT5-style ``" ".join(seq)`` path emits ~50 % ``<unk>`` tokens and
     destroys the embedding. ``_embed_ankh`` instead passes
     ``[list(seq) for seq in batch]`` with ``is_split_into_words=True``
     (verified 0 ``<unk>``). The ``<AA2fold>`` prefix is never injected.
* ``auto``  — falls back to ``esm``.

Residue-tensor convention
^^^^^^^^^^^^^^^^^^^^^^^^^

Every backend strips **all** special tokens before pooling and chunking, so
the residue tensor has exactly ``N`` rows where ``N`` is the amino-acid
length of the input sequence. This makes ``chunk_index_s`` and
``chunk_index_e`` mean the same thing on every backend: indices into the
amino-acid sequence, not into a backend-specific token tensor.

.. list-table::
   :header-rows: 1
   :widths: 15 50 15

   * - Backend
     - Tokens stripped from residue slice
     - residue_len
   * - ``esm``
     - CLS (position 0) + EOS (last position)
     - ``N``
   * - ``esm3c``
     - BOS (position 0) + EOS (last position)
     - ``N``
   * - ``t5`` (``prot_t5_xl_uniref50``)
     - EOS (last position)
     - ``N``
   * - ``t5`` (ProstT5)
     - ``<AA2fold>`` (position 0) + EOS (last position)
     - ``N``
   * - ``ankh``
     - EOS (last position)
     - ``N``

This convention was unified on 2026-04-10. Before that date, the T5 family
kept the trailing EOS and ProstT5 additionally kept the leading
``<AA2fold>`` prefix token, so chunk indices on those backends were offset
by 0–2 positions relative to the amino-acid sequence. Embeddings computed
under the old convention are not directly comparable with new ones and
must be recomputed.

A single GPU worker is typically sufficient because each batch already
saturates the device; the coordinator serialises dispatch with
``RetryLaterError(delay=60s)`` when another embedding job is ``RUNNING``.

store_embeddings
~~~~~~~~~~~~~~~~

**Queue:** ``protea.embeddings.write`` — **consumer:** ``OperationConsumer``

Bulk-inserts ``SequenceEmbedding`` rows (one per chunk) into the pgvector
``VECTOR`` column, using PostgreSQL ``INSERT ... ON CONFLICT DO NOTHING`` to
tolerate re-runs. After each successful insert it atomically increments
``Job.progress_current`` on the parent ``compute_embeddings`` job and closes
the job when ``progress_current == progress_total``.

This stage is deliberately decoupled from GPU inference so that slow disk
writes never block the GPU, and so the write worker can be scaled
independently.

predict_go_terms_batch
~~~~~~~~~~~~~~~~~~~~~~

**Queue:** ``protea.predictions.batch`` — **consumer:** ``OperationConsumer``

Runs the KNN + GO transfer pipeline for one batch of query proteins:

1. Loads query embeddings from the DB.
2. Resolves the reference cache (process-level float16 array; loaded from disk
   at ``data/ref_cache/<embedding_config>__<annotation_set>_*.npy`` if present,
   otherwise streamed from PostgreSQL in chunks of 2 000 rows and persisted).
3. Performs per-aspect KNN search via numpy or FAISS (``Flat``, ``IVFFlat``,
   ``HNSW``) using the configured metric (``cosine`` or ``l2``).
4. Transfers GO terms from neighbours to the query, computing predicted
   distances, optional alignment features (NW/SW via ``parasail``), optional
   taxonomic features (``ete3`` NCBITaxa), and optional re-ranker aggregate
   features.
5. Publishes a ``StorePredictions`` message to ``protea.predictions.write``.

GPU is not required — KNN search runs on CPU unless a FAISS GPU index is
configured at process startup.

store_predictions
~~~~~~~~~~~~~~~~~

**Queue:** ``protea.predictions.write`` — **consumer:** ``OperationConsumer``

Bulk-inserts ``GOPrediction`` rows (one row per predicted ``(protein, go_term)``
pair) and atomically increments ``Job.progress_current`` on the parent
``predict_go_terms`` job. Uses ``INSERT ... ON CONFLICT DO NOTHING`` so that
re-delivered messages are a no-op.

All feature-engineering columns declared in :class:`GOPrediction`
(``identity_nw``, ``similarity_sw``, ``taxonomic_distance``, etc.) are written
in the same insert when they are present in the payload.

Registering a new operation
----------------------------

1. Create a module under ``protea/core/operations/``.
2. Define a payload class extending ``ProteaPayload``.
3. Implement the ``Operation`` protocol (``name`` attribute + ``execute``).
4. Register the instance in the worker startup script:

   .. code-block:: python

      registry.register(MyOperation())

5. Route jobs to the correct queue by setting ``queue_name`` in the ``POST /jobs``
   request body.

No changes to ``BaseWorker``, the API, or the infrastructure layer are required.
