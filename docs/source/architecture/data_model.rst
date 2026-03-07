Data Model
==========

All models use SQLAlchemy 2.x declarative style with ``Mapped[]`` type annotations.
The schema is managed by Alembic.

Protein and sequence deduplication
------------------------------------

.. code-block:: text

   ┌──────────────────────────┐        ┌────────────────────────┐
   │         Protein          │        │        Sequence        │
   │──────────────────────────│  N→1   │────────────────────────│
   │ accession (PK)           │───────▶│ id (PK, autoincrement) │
   │ canonical_accession      │        │ sequence (Text)        │
   │ is_canonical             │        │ sequence_hash (MD5)    │
   │ isoform_index            │        └────────────────────────┘
   │ entry_name               │
   │ reviewed                 │        ┌──────────────────────────────┐
   │ taxonomy_id              │  N→1   │   ProteinUniProtMetadata     │
   │ organism                 │───────▶│──────────────────────────────│
   │ gene_name                │ (view) │ canonical_accession (PK)     │
   │ length                   │        │ function_cc, ec_number, ...  │
   │ sequence_id (FK)         │        └──────────────────────────────┘
   └──────────────────────────┘

**Sequence**
   Stores unique amino-acid sequences, deduplicated by MD5 hash (``sequence_hash``).
   Many ``Protein`` rows can reference the same ``Sequence`` — ``sequence_id`` is
   deliberately non-unique.

**Protein**
   One row per UniProt accession, including isoforms (``<canonical>-<n>``).
   Isoforms share the same ``canonical_accession`` and are differentiated by
   ``is_canonical`` and ``isoform_index``. The relationship to
   ``ProteinUniProtMetadata`` is view-only (no foreign key), joined by
   ``canonical_accession``.

**ProteinUniProtMetadata**
   One row per canonical accession. Stores raw UniProt functional annotations
   (functional description, EC numbers, pathways, kinetics, etc.) as ``Text`` fields.
   Isoforms inherit metadata via the ``canonical_accession`` join.

   Fields mapped from the UniProt TSV response:

   .. list-table::
      :header-rows: 1

      * - Column
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
      * - ``keywords``
        - Keywords
      * - *(+ 12 more)*
        -

Job queue
---------

.. code-block:: text

   ┌────────────────────────────┐    1→N    ┌──────────────────────────┐
   │           Job              │──────────▶│         JobEvent         │
   │────────────────────────────│           │──────────────────────────│
   │ id (UUID, PK)              │           │ id (BigInt, PK)          │
   │ operation                  │           │ job_id (FK)              │
   │ queue_name                 │           │ event (str)              │
   │ status (enum)              │           │ message (str, nullable)  │
   │ payload (JSONB)            │           │ fields (JSONB)           │
   │ meta (JSONB)               │           │ level (info/warn/error)  │
   │ progress_current           │           │ ts (timestamp)           │
   │ progress_total             │           └──────────────────────────┘
   │ error_code                 │
   │ error_message              │
   │ created_at / started_at /  │
   │ finished_at                │
   └────────────────────────────┘

**Job**
   Central entity of the job queue. The ``payload`` JSONB field contains operation-specific
   parameters validated at execution time against the corresponding ``ProteaPayload`` model.
   ``meta`` stores the final ``OperationResult.result`` dict on completion.

**JobEvent**
   Append-only audit log. Written by the ``emit`` callback during execution. The frontend
   renders these as a chronological timeline. Events are never updated or deleted.

Status enum
-----------

.. list-table::
   :header-rows: 1

   * - Value
     - Meaning
   * - ``queued``
     - Created, waiting in RabbitMQ
   * - ``running``
     - Worker has claimed the job
   * - ``succeeded``
     - Operation completed successfully
   * - ``failed``
     - Operation raised an exception
   * - ``cancelled``
     - Cancelled via API before or during execution
