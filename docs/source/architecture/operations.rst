Operations
==========

An **Operation** is the fundamental unit of domain logic in PROTEA. Every task
that a worker can execute — from a health-check ping to a full UniProt ingest —
is encapsulated in a class that satisfies the ``Operation`` protocol.

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
   Frozen dataclass with three fields: ``result`` (stored in ``Job.meta``),
   and optional ``progress_current`` / ``progress_total`` written back to
   the ``Job`` row for the progress bar.

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
     - ``8``
     - Model forward-pass batch size inside each batch worker.

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

**Operation name:** ``predict_go_terms`` — queue: ``protea.jobs``
(coordinator; fans out KNN batch workers)

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
     - ``false``
     - Compute NW + SW pairwise alignments (parasail) for each prediction.
   * - ``compute_taxonomy``
     - ``false``
     - Compute taxonomic distance (ete3 NCBITaxa) for each prediction.

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
