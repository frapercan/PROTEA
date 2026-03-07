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
