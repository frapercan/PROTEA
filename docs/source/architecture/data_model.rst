Data Model
==========

.. contents:: On this page
   :local:
   :depth: 2

All models use SQLAlchemy 2.x declarative style with ``Mapped[]`` type annotations.
The schema is managed by Alembic (35 migrations to date).

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
   Many ``Protein`` rows can reference the same ``Sequence``; ``sequence_id`` is
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

   Valid values for ``model_backend`` are ``esm`` (HuggingFace ``EsmModel`` /
   ESM-2), ``esm3c`` (ESM SDK ``ESMC``), ``t5`` (``T5EncoderModel`` for ProstT5
   and ``prot_t5_xl_uniref50``), ``ankh`` (``T5EncoderModel`` for
   ``ElnaggarLab/ankh-base`` / ``ankh-large``, loaded via ``AutoTokenizer``,
   forced to ``bfloat16`` on CUDA because Ankh overflows to ``NaN`` under
   FP16, and tokenised char-by-char with ``is_split_into_words=True`` because
   its SentencePiece vocabulary maps literal spaces to ``<unk>``; the
   ``<AA2fold>`` prefix is never injected), and ``auto`` (alias for ``esm``).

**SequenceEmbedding**
   Stores a pgvector VECTOR for one (sequence, config, chunk) triple.
   When chunking is disabled: ``chunk_index_s=0``, ``chunk_index_e=NULL``.
   When chunking is enabled: each chunk is a separate row with its own start/end indices.

   ``chunk_index_s`` and ``chunk_index_e`` are **amino-acid positions** on
   every backend. The embedding operation strips all special tokens
   (CLS/BOS/EOS and ProstT5's ``<AA2fold>`` prefix) from the residue tensor
   before chunking, so ``chunk_index_s=0`` always refers to the first amino
   acid and ``chunk_index_e`` equals the amino-acid length for the final
   chunk of a full-length sequence. This convention was unified on
   2026-04-10; embeddings computed before that date used backend-specific
   slicing and must be recomputed to be directly comparable.

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
   ``compute_taxonomy=true`` were set in the prediction payload (both default
   to ``True``).

   When ``compute_reranker_features=true`` (also the default) the legacy
   re-ranker aggregate columns are populated: ``vote_count``,
   ``k_position``, ``go_term_frequency``, ``ref_annotation_density``,
   ``neighbor_distance_std``, ``neighbor_vote_fraction``,
   ``neighbor_min_distance``, ``neighbor_mean_distance``: eight fields in
   total.

   When ``compute_v6_features=true`` (opt-in, default ``False``) the v6
   feature family is materialised on top: 6 anc2vec columns
   (``anc2vec_neighbor_cos`` / ``anc2vec_neighbor_maxcos`` /
   ``anc2vec_has_emb`` / ``anc2vec_query_known_cos`` /
   ``anc2vec_query_known_maxcos`` / ``anc2vec_query_known_count``), 3
   tax-voter aggregates (``tax_voters_same_frac``,
   ``tax_voters_close_frac``, ``tax_voters_mean_common_ancestors``) and
   16 PCA-projected embedding columns (``emb_pca_query_0`` …
   ``emb_pca_query_15``): 25 additional fields. PCA state is fit once
   per ``EmbeddingConfig`` and cached on disk
   (``PROTEA_PCA_ARTIFACTS_DIR``).

**RerankerModel**
   Stores a trained LightGBM binary (or LambdaRank) re-ranker. References the
   ``PredictionSet`` and ``EvaluationSet`` used for training (both
   ``SET NULL`` on delete). Two storage modes coexist:

   - **Inline (legacy).** ``model_data`` (``Text``, now nullable) holds the
     serialized booster string. Rows created before the 2026-04 integration
     with ``protea-reranker-lab`` use this path.
   - **Artifact-backed (preferred).** ``artifact_uri`` (``String(512)``)
     points at a ``file://`` or ``s3://`` URI resolved by the
     ``ArtifactStore``. Rows inserted via ``scripts/register_reranker.py``
     always use this path and leave ``model_data`` NULL.

   Additional provenance columns record the lab run that produced the model:

   - ``feature_schema_sha`` (``String(16)``): 12-hex-char fingerprint
     of the feature families the booster was trained on, computed via
     ``protea_reranker_lab.contracts.compute_feature_schema_sha``.
     **Load-bearing at inference time**: ``predict_go_terms`` refuses to
     apply a booster whose ``feature_schema_sha`` does not match the live
     feature set, falling back to KNN ordering rather than scoring with
     NaN-filled columns.
   - ``embedding_config_id`` / ``ontology_snapshot_id`` (FKs, both
     ``SET NULL``): the embedding recipe and ontology release the
     booster was trained against.
   - ``producer_version`` (``String(64)``) / ``producer_git_sha``
     (``String(40)``): PROTEA ``__version__`` and HEAD sha at export
     time, recorded in the dataset manifest and propagated here.
   - ``spec_yaml`` (``Text``): the full ``ExperimentSpec`` YAML used to
     drive the lab training run, for reproducibility.

**ScoringConfig**
   Defines a named scoring recipe: a set of feature weights and parameters
   that can be applied to any prediction set. Immutable once created.

Re-ranker datasets
------------------

.. code-block:: text

   ┌──────────────────────────────────┐
   │             Dataset              │
   │──────────────────────────────────│
   │ id (UUID, PK)                    │
   │ name (str, unique)               │
   │ operation (export_research_…)    │
   │ job_id (FK, nullable)            │
   │ ── storage layout ──             │
   │ storage_backend (local|minio)    │
   │ key_prefix                       │
   │ train_uri / eval_uri             │
   │ manifest_uri                     │
   │ ── content fingerprints ──       │
   │ schema_sha (16-char)             │
   │ manifest_sha (sha256)            │
   │ n_train_rows / n_eval_rows       │
   │ ── dump parameters ──            │
   │ k                                │
   │ annotation_source                │
   │ embedding_config_id (FK, null)   │
   │ ontology_snapshot_id (FK, null)  │
   │ train_snapshot_pairs (JSONB)     │
   │ eval_snapshot_pair               │
   │ ── reproducibility ──            │
   │ producer_version                 │
   │ producer_git_sha                 │
   │ meta (JSONB)                     │
   │ created_at                       │
   └──────────────────────────────────┘

**Dataset**
   The durable handle for a frozen re-ranker dataset published to the
   artifact store by ``export_research_dataset``. One row per successful
   export run; ``protea-reranker-lab``'s ``pull_dataset.py`` resolves
   either ``id`` or ``name`` against this table to download the exact
   ``train.parquet`` / ``eval.parquet`` / ``manifest.json`` triple that
   trained a given booster. ``schema_sha`` must equal the booster's
   ``feature_schema_sha`` at inference (load-bearing); ``manifest_sha``
   detects content drift across re-emissions.

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

Visitor events
--------------

.. code-block:: text

   ┌──────────────────────────────┐
   │        VisitorEvent          │
   │──────────────────────────────│
   │ id (BigInt, PK)              │
   │ day (Date, indexed)          │
   │ visitor_hash (16-char)       │
   │ path                         │
   │ method                       │
   │ status (int)                 │
   │ created_at                   │
   └──────────────────────────────┘

**VisitorEvent**
   Append-only log that powers the Grafana "unique visitors" dashboard.
   The schema deliberately omits IP addresses: ``visitor_hash`` is the
   first 16 hex chars of ``sha256(daily_salt || client_ip)``, where
   ``daily_salt`` is a 32-byte random value held in process memory and
   regenerated every calendar day. Once the day rolls over the salt is
   gone, so cross-day correlation becomes cryptographically infeasible
   (the same no-PII model used by Plausible and Fathom). The ``(day,
   visitor_hash)`` index drives unique-visitor counts per day; the
   ``path`` index drives top-page reports.

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
   │ description (Text, null)   │
   │ findings (Text, null)      │
   │ tags (ARRAY[Text])         │
   │ created_at / started_at /  │
   │ finished_at                │
   └────────────────────────────┘

**Job**
   Central entity of the job queue. ``parent_job_id`` links child batch jobs to their
   coordinator parent (used in distributed pipelines). ``progress_current`` /
   ``progress_total`` track batch completion for progress bars.

   The three narrative columns (``description``, ``findings``, ``tags``)
   were added in T3.9 / D11 to give every run a place for human context:
   ``description`` carries the pre-execution intent supplied at
   ``POST /jobs`` time, ``findings`` is a free-form post-completion note
   the operator (or a downstream tool) can write back via the existing
   PATCH path, and ``tags`` is a free-form ``Text[]`` (default ``[]``)
   for ad-hoc grouping. None of the three are interpreted by
   ``BaseWorker``; they are operator metadata only.

**JobEvent**
   Append-only audit log. Written by the ``emit`` callback during execution. The frontend
   renders these as a chronological timeline. Events are never updated or deleted.

**JobComment**
   Human-authored note thread attached to a ``Job`` (T3.10 / D11).
   One-to-many: ``job_id`` UUID FK to ``job(id)`` with ``ON DELETE
   CASCADE``, ``author`` Text nullable, ``body`` Text required,
   ``created_at`` Timestamptz default ``now()``. Indexed by
   ``(job_id, created_at)`` for the natural read pattern. Comments
   are written through ``POST /jobs/{job_id}/comments`` and read
   chronologically through ``GET /jobs/{job_id}/comments``; they
   complement ``JobEvent`` (which is machine-emitted) by giving
   curators / operators a place for free-form annotations.

Experiment runs
---------------

.. code-block:: text

   ┌──────────────────────────────┐
   │       ExperimentRun          │
   │──────────────────────────────│
   │ id (UUID, PK)                │
   │ name (Text, UNIQUE)          │
   │ description (Text, null)     │
   │ hypothesis (Text, null)      │
   │ findings (Text, null)        │
   │ status (enum)                │
   │ config (JSONB)               │
   │ provenance (JSONB)           │
   │ tags (ARRAY[Text])           │
   │ created_at                   │
   │ started_at (null)            │
   │ finished_at (null)           │
   └──────────────────────────────┘

**ExperimentRun**
   Per-research-run narrative + provenance anchor (T3.8 / Fase 4).
   The row that F-EXP campaigns and the F8b Experiments page hang
   their per-run metadata off of: a single ``ExperimentRun``
   typically aggregates multiple ``Job`` / ``EvaluationResult`` /
   ``RerankerModel`` rows under one human ``name``. The narrative
   trio (``description`` / ``hypothesis`` / ``findings``) mirrors
   ``Job``'s D11 columns; ``config`` and ``provenance`` are JSONB
   bags suitable for the ``capture_provenance`` snapshot from
   :mod:`protea.core.provenance`. Status enum is
   ``planned`` → ``running`` → ``done`` (or ``abandoned``); planned
   rather than queued because experiments often live as drafts
   before any compute kicks off, and ``abandoned`` rather than
   ``failed`` because a research run can be stopped without a hard
   error. Linkage to sibling rows lives in follow-up tasks
   T4.7-T4.9.

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

.. seealso::

   - :doc:`operations`: every operation lists the tables it touches.
   - :doc:`/reference/infrastructure`: the SQLAlchemy ``Mapped[]`` classes
     behind every table on this page.
   - :doc:`/adr/006-sequence-deduplication-by-md5`: why the
     ``Sequence`` ↔ ``Protein`` split exists.
   - :doc:`/adr/001-knn-without-pgvector`: why ``SequenceEmbedding`` uses
     pgvector for storage but not for search.
