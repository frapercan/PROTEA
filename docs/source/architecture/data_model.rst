Data Model
==========

All models use SQLAlchemy 2.x declarative style with ``Mapped[]`` type annotations.
The schema is managed by Alembic (22 migrations to date).

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

GO ontology
-----------

.. code-block:: text

   ┌──────────────────────────┐     1→N    ┌────────────────────────┐
   │    OntologySnapshot      │──────────▶│        GOTerm          │
   │──────────────────────────│           │────────────────────────│
   │ id (UUID, PK)            │           │ id (PK)                │
   │ obo_url                  │           │ go_id (e.g. GO:0003674)│
   │ obo_version              │           │ name                   │
   │ loaded_at                │           │ aspect (F/P/C)         │
   └──────────────────────────┘           │ definition             │
                                          │ is_obsolete            │
                                          │ ontology_snapshot_id   │
                                          └──────────┬─────────────┘
                                                     │
                                          ┌──────────▼─────────────┐
                                          │  GOTermRelationship    │
                                          │────────────────────────│
                                          │ child_go_term_id (FK)  │
                                          │ parent_go_term_id (FK) │
                                          │ relation_type          │
                                          │ ontology_snapshot_id   │
                                          └────────────────────────┘

**OntologySnapshot**
   One row per loaded OBO file release, versioned by ``obo_version`` (unique constraint).
   Idempotent load: if a snapshot already exists with its relationships, it is skipped.
   If relationships are missing they are backfilled automatically.

**GOTerm**
   One row per GO term per snapshot. ``aspect`` is one of ``F`` (molecular function),
   ``P`` (biological process), or ``C`` (cellular component).

**GOTermRelationship**
   Directed edge in the GO DAG. ``relation_type`` is one of ``is_a``, ``part_of``,
   ``regulates``, ``positively_regulates``, ``negatively_regulates``.
   Used by ``GET /annotations/snapshots/{id}/subgraph`` for BFS ancestor traversal.

Annotation sets
---------------

.. code-block:: text

   ┌──────────────────────┐     1→N    ┌────────────────────────────────┐
   │    AnnotationSet     │──────────▶│     ProteinGOAnnotation        │
   │──────────────────────│           │────────────────────────────────│
   │ id (UUID, PK)        │           │ id (PK)                        │
   │ source (goa/quickgo) │           │ protein_accession              │
   │ source_version       │           │ go_term_id (FK → GOTerm)       │
   │ ontology_snapshot_id │           │ annotation_set_id (FK)         │
   │ job_id               │           │ qualifier                      │
   │ created_at           │           │ evidence_code                  │
   │ meta (JSONB)         │           │ assigned_by                    │
   └──────────────────────┘           │ db_reference                   │
                                      │ with_from                      │
                                      │ annotation_date                │
                                      └────────────────────────────────┘

**AnnotationSet**
   Groups a batch of protein GO annotations by source (``goa`` or ``quickgo``)
   and ontology snapshot version.

**ProteinGOAnnotation**
   One row per (protein, GO term, annotation set) triple. Stores all GAF/QuickGO
   evidence fields verbatim.

Embeddings
----------

.. code-block:: text

   ┌──────────────────────────┐     1→N    ┌──────────────────────────────┐
   │    EmbeddingConfig       │──────────▶│      SequenceEmbedding       │
   │──────────────────────────│           │──────────────────────────────│
   │ id (UUID, PK)            │           │ id (PK)                      │
   │ model_name               │           │ sequence_id (FK)             │
   │ model_backend            │           │ embedding_config_id (FK)     │
   │ layer_indices            │           │ embedding (VECTOR)           │
   │ layer_agg                │           │ chunk_index_s (int)          │
   │ pooling                  │           │ chunk_index_e (int, nullable)│
   │ normalize                │           └──────────────────────────────┘
   │ normalize_residues       │
   │ max_length               │
   │ use_chunking             │
   │ chunk_size               │
   │ chunk_overlap            │
   │ description              │
   │ created_at               │
   └──────────────────────────┘

**EmbeddingConfig**
   Defines a reproducible embedding recipe (model, layer selection, pooling strategy,
   chunking). Referenced by both ``SequenceEmbedding`` rows and ``PredictionSet`` rows
   to ensure query and reference embeddings are always comparable.

**SequenceEmbedding**
   Stores a pgvector VECTOR for one (sequence, config, chunk) triple.
   When chunking is disabled: ``chunk_index_s=0``, ``chunk_index_e=NULL``.
   When chunking is enabled: each chunk is a separate row with its own start/end indices.

   .. note::
      KNN search is **never** performed at the DB layer. Embeddings are loaded into
      numpy arrays and searched via ``protea.core.knn_search`` using numpy or FAISS.

Query sets
----------

.. code-block:: text

   ┌──────────────────────┐     1→N    ┌──────────────────────────────┐
   │      QuerySet        │──────────▶│       QuerySetEntry          │
   │──────────────────────│           │──────────────────────────────│
   │ id (UUID, PK)        │           │ id (PK)                      │
   │ name                 │           │ query_set_id (FK)            │
   │ description          │           │ accession (original header)  │
   │ created_at           │           │ sequence_id (FK → Sequence)  │
   └──────────────────────┘           └──────────────────────────────┘

**QuerySet**
   User-uploaded FASTA dataset for custom prediction queries. Created via
   ``POST /query-sets`` (multipart upload).

**QuerySetEntry**
   One row per FASTA entry. Preserves the original accession header from the FASTA file
   and links to the deduplicated ``Sequence`` row (reuses existing sequences if the
   amino-acid string is already in the DB).

Predictions
-----------

.. code-block:: text

   ┌──────────────────────────────┐     1→N    ┌───────────────────────────────────┐
   │        PredictionSet         │──────────▶│          GOPrediction             │
   │──────────────────────────────│           │───────────────────────────────────│
   │ id (UUID, PK)                │           │ id (PK)                           │
   │ embedding_config_id (FK)     │           │ prediction_set_id (FK)            │
   │ annotation_set_id (FK)       │           │ protein_accession (query)         │
   │ ontology_snapshot_id (FK)    │           │ go_term_id (FK)                   │
   │ query_set_id (FK, nullable)  │           │ distance (cosine/L2)              │
   │ limit_per_entry              │           │ ref_protein_accession             │
   │ distance_threshold           │           │ qualifier, evidence_code          │
   │ created_at                   │           │ ── alignment (NW) ──              │
   └──────────────────────────────┘           │ identity_nw, similarity_nw        │
                                              │ alignment_score_nw                │
                                              │ gaps_pct_nw, alignment_length_nw  │
                                              │ ── alignment (SW) ──              │
                                              │ identity_sw, similarity_sw        │
                                              │ alignment_score_sw                │
                                              │ gaps_pct_sw, alignment_length_sw  │
                                              │ ── lengths ──                     │
                                              │ length_query, length_ref          │
                                              │ ── taxonomy ──                    │
                                              │ query_taxonomy_id                 │
                                              │ ref_taxonomy_id                   │
                                              │ taxonomic_lca                     │
                                              │ taxonomic_distance                │
                                              │ taxonomic_common_ancestors        │
                                              │ taxonomic_relation                │
                                              └───────────────────────────────────┘

**PredictionSet**
   Groups all GO predictions for one run of ``predict_go_terms``. References the
   ``EmbeddingConfig``, ``AnnotationSet``, and ``OntologySnapshot`` used.
   Optionally linked to a ``QuerySet`` when predictions were run from a FASTA upload.

**GOPrediction**
   One row per (query protein, GO term, reference protein) triple. The alignment and
   taxonomy columns are ``NULL`` unless ``compute_alignments=true`` and/or
   ``compute_taxonomy=true`` were set in the prediction payload. Five additional
   re-ranker features (``vote_count``, ``k_position``, ``go_term_frequency``,
   ``ref_annotation_density``, ``neighbor_distance_std``) are populated when
   ``compute_reranker_features=true``.

**RerankerModel**
   Stores a trained LightGBM binary classifier. References the ``PredictionSet``
   and ``EvaluationSet`` used for training. Contains the serialized model string,
   validation metrics (JSONB), and feature importance (JSONB).

**ScoringConfig**
   Defines a named scoring recipe: a set of feature weights and parameters
   that can be applied to any prediction set. Immutable once created.

Evaluation
----------

.. code-block:: text

   ┌──────────────────────────────┐     1→N    ┌───────────────────────────────────┐
   │       EvaluationSet          │──────────▶│        EvaluationResult           │
   │──────────────────────────────│           │───────────────────────────────────│
   │ id (UUID, PK)                │           │ id (UUID, PK)                     │
   │ old_annotation_set_id (FK)   │           │ evaluation_set_id (FK)            │
   │ new_annotation_set_id (FK)   │           │ prediction_set_id (FK)            │
   │ ontology_snapshot_id (FK)    │           │ scoring_config_id (FK, nullable)  │
   │ stats (JSONB)                │           │ reranker_model_id (FK, nullable)  │
   │ job_id (FK)                  │           │ results (JSONB)                   │
   │ created_at                   │           │ max_distance (Float, nullable)    │
   └──────────────────────────────┘           │ job_id (FK)                       │
                                              │ created_at                        │
                                              └───────────────────────────────────┘

**EvaluationSet**
   Stores the CAFA-style temporal holdout delta between two annotation sets
   (old → new). The ``stats`` JSONB column contains NK/LK/PK protein and
   annotation counts, delta protein count, and per-namespace breakdowns.

**EvaluationResult**
   Stores the output of running ``cafaeval`` against a prediction set for
   a given evaluation set. The ``results`` JSONB column contains per-category
   (NK/LK/PK) per-namespace (BPO/MFO/CCO) Fmax, precision, recall, τ, and
   coverage. Optionally references a ``ScoringConfig`` or ``RerankerModel``
   when predictions were scored or re-ranked before evaluation.

Support
-------

.. code-block:: text

   ┌──────────────────────────┐
   │      SupportEntry        │
   │──────────────────────────│
   │ id (UUID, PK)            │
   │ comment (Text, nullable) │
   │ created_at               │
   └──────────────────────────┘

**SupportEntry**
   Community feedback: a thumbs-up with an optional comment (max 500 chars).

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
   │ parent_job_id (FK, null)   │           │ fields (JSONB)           │
   │ payload (JSONB)            │           │ level (info/warn/error)  │
   │ meta (JSONB)               │           │ ts (timestamp)           │
   │ progress_current           │           └──────────────────────────┘
   │ progress_total             │
   │ error_code                 │
   │ error_message              │
   │ created_at / started_at /  │
   │ finished_at                │
   └────────────────────────────┘

**Job**
   Central entity of the job queue. ``parent_job_id`` links child batch jobs to their
   coordinator parent (used in distributed pipelines). ``progress_current`` /
   ``progress_total`` track batch completion for progress bars.

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
