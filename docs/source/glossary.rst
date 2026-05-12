Glossary
========

A reference list of acronyms, domain terms, and PROTEA-specific concepts that
appear throughout the documentation. Use Sphinx's ``:term:`` role to link to
any entry from another page.

.. glossary::
   :sorted:

   ADR
      **Architecture Decision Record.** A short document that records a
      significant architectural choice, its context, and the trade-offs
      considered. PROTEA stores ADRs under ``docs/source/adr/`` following
      the MADR template.

   Ankh
      A transformer-based protein language model trained by Elnaggar et al.
      PROTEA can route embedding jobs to an Ankh backend via
      ``protea-backends``.

   AnnotationSet
      A batch of ``ProteinGOAnnotation`` rows grouped by source
      (``goa`` or ``quickgo``) and tied to one :term:`OntologySnapshot`.
      Two annotation sets from different sources or dates can coexist and
      be compared.

   artifact store
      The abstraction (``ArtifactStore``) that decouples PROTEA from a
      specific storage medium. The local backend resolves ``file://`` URIs
      under a project-relative directory; the :term:`MinIO` backend resolves
      ``s3://bucket/key`` URIs. The backend is selected by
      ``PROTEA_STORAGE_BACKEND``.

   audit trail
      The sequence of ``JobEvent`` rows that PROTEA writes on every state
      transition of a ``Job``. Together they record when a job was claimed,
      what the operation emitted, and the final outcome (or error code).

   bench-v1-K5-v226-lineage
      The named lineage of re-ranker training runs that use
      ``K=5`` KNN neighbours, annotation source version 226, and the
      ``bench-v1`` feature schema. Used as a reproducibility handle in the
      thesis evaluation chapter.

   BPO
      **Biological Process Ontology.** One of the three namespaces of the
      :term:`GO`. BPO terms describe the multi-step molecular events that a
      gene product participates in (e.g. cell division, signal transduction).

   CAFA
      **Critical Assessment of protein Function Annotation.** A biennial
      community benchmark that evaluates computational methods for
      predicting GO term annotations from protein sequences. PROTEA was
      built around the CAFA evaluation protocol; the ``cafaeval`` fork used
      by PROTEA is at ``cafaeval-protea``.

   canonical accession
      The UniProt accession that identifies the reference isoform of a
      protein (e.g. ``P04637``). PROTEA groups isoforms under their
      canonical accession and uses it as the join key across tables.

   CCO
      **Cellular Component Ontology.** One of the three namespaces of the
      :term:`GO`. CCO terms describe locations in or around the cell where
      a gene product is active (e.g. nucleus, mitochondria, cytoplasm).

   Coordinator
      An :term:`Operation` that does not perform the heavy work itself but
      partitions it into batches and publishes child messages to a downstream
      queue. Coordinators return ``OperationResult(deferred=True)`` so the
      parent ``Job`` row remains in ``RUNNING`` until the last child finishes.
      ``compute_embeddings`` and ``predict_go_terms`` are coordinators.

   Dataset
      An ORM model that represents a frozen snapshot of training and
      evaluation data produced by ``export_research_dataset``. Identified by
      a unique name; stores ``train_uri``, ``eval_uri``, ``manifest_uri``,
      content fingerprints (``schema_sha``, ``manifest_sha``), and producer
      provenance. Consumed by ``protea-reranker-lab``.

   Deferred
      An ``OperationResult`` flag that tells ``BaseWorker`` *not* to
      transition the parent ``Job`` to ``SUCCEEDED`` when ``execute()``
      returns. Used by coordinators that hand work off to child workers
      tracked through atomic progress counters.

   deploy-keeper
      The long-running process (or worktree task) responsible for keeping
      the PROTEA dev stack alive via an ngrok tunnel between sessions.
      See :doc:`/runbooks/index` for recovery procedures.

   DLQ
      **Dead-Letter Queue.** A RabbitMQ queue that receives messages that
      could not be delivered or were rejected after all retries. In PROTEA,
      the DLQ for each pipeline queue has a corresponding :term:`DLX`
      exchange.

   DLX
      **Dead-Letter Exchange.** The RabbitMQ exchange that routes rejected or
      expired messages to the corresponding :term:`DLQ`. Configured per
      queue via the ``x-dead-letter-exchange`` argument.

   dual-write
      A migration strategy where code writes the same data to both an old and
      a new column simultaneously. PROTEA used dual-write during the
      ``GOPrediction`` feature-column migration to JSONB so that both the
      legacy flat columns and the new JSONB field remained consistent until
      the old columns were dropped.

   ECO
      **Evidence and Conclusion Ontology.** Identifies how a GO annotation
      was derived (experimental, computational, author statement, etc.).
      QuickGO returns ECO IDs which PROTEA optionally maps to GAF-style
      evidence codes.

   EmbeddingConfig
      An immutable record of all parameters that affect the geometry of an
      embedding (model, chunking, pooling). Identified by a UUID; changing
      any field produces a new configuration. Every ``SequenceEmbedding``
      and every ``GOPrediction`` carries the UUID it was computed against.

   ESM-2
      A family of protein language models from Meta AI trained on UniRef50.
      PROTEA can route embedding jobs to an ESM-2 backend via
      ``protea-backends``. Available sizes range from 8M to 15B parameters.

   ESM-C
      **ESM-Cambrian.** The successor family to :term:`ESM-2`, also from Meta
      AI, with improved scaling and training protocol. PROTEA primarily uses
      ``ESMC 300M`` to produce 960-dimensional sequence embeddings.

   EvaluationSet
      The CAFA-style temporal-holdout split derived from a (t0, t1) pair of
      annotation sets. Stores per-protein :term:`NK`/:term:`LK`/:term:`PK`
      classifications per namespace. Consumed by ``run_cafa_evaluation`` and
      the re-ranker training pipeline.

   evidence code
      A short code (e.g. ``EXP``, ``IEA``, ``ISS``) in GAF format that
      indicates how a GO annotation was supported. Stored in
      ``ProteinGOAnnotation.evidence_code``. Experimental codes
      (``EXP``, ``IDA``, ``IMP``, ``IGI``, ``IEP``) are considered
      high-confidence; ``IEA`` (inferred from electronic annotation) is
      excluded in many benchmark evaluations.

   FAISS
      Facebook AI Similarity Search. The approximate nearest-neighbour
      library used by PROTEA's prediction pipeline at scale (IVFFlat index).

   FASTA
      A plain-text bioinformatics format in which each record starts with a
      ``>`` header line followed by the raw amino-acid or nucleotide sequence.
      PROTEA ingests protein sequences from UniProt in FASTA format via
      ``insert_proteins``.

   FeatureRegistry
      The central registry that maps feature names to their extraction
      functions for re-ranker training and inference. Validated at startup
      using a ``schema_sha`` fingerprint so that model and pipeline cannot
      silently diverge.

   Fmax
      The maximum F-measure achieved over all decision thresholds for a
      binary or multi-label classifier. The headline metric reported by
      ``cafaeval`` and the primary measure used in :doc:`results`.

   GAF
      **Gene Association File.** The plain-text tabular format used by
      UniProt-GOA to publish protein → GO term annotations. PROTEA's
      ``load_goa_annotations`` operation streams GAF 2.2 files.

   GO
      **Gene Ontology.** A structured controlled vocabulary that describes
      gene-product attributes across three orthogonal aspects: Molecular
      Function (:term:`MFO`), Biological Process (:term:`BPO`), and Cellular
      Component (:term:`CCO`). Terms are organised as a directed acyclic
      graph; a more specific term is a child of every more general term that
      subsumes it. PROTEA stores a complete release as one
      :term:`OntologySnapshot`.

   GOA
      **Gene Ontology Annotation** project at EBI. Publishes high-volume
      protein → GO term assignments derived from experiments, sequence
      curation, and electronic annotation.

   GOPrediction
      An ORM row recording a single candidate GO term produced by the KNN
      prediction pipeline for a specific protein within a
      :term:`PredictionSet`. Stores the raw KNN score and optional
      re-ranker feature columns (as JSONB).

   golden parquet gate
      A CI gate that compares key statistics of a newly produced
      ``train.parquet`` / ``eval.parquet`` against a previously approved
      reference. Fails the build if row counts, column schema, or feature
      distributions drift beyond tolerance.

   GO term
      A node in the :term:`GO` graph, identified by a stable accession
      (e.g. ``GO:0003674``). Each term belongs to exactly one namespace
      (:term:`BPO`, :term:`MFO`, or :term:`CCO`).

   IA
      **Information Accretion.** A weighting scheme used in CAFA evaluation
      that down-weights uninformative GO terms and rewards prediction of rare,
      specific terms. PROTEA's evaluation uses the IA weights from the CAFA6
      benchmark.

   isoform
      A protein variant produced from the same gene by alternative splicing or
      other post-transcriptional events. UniProt identifies isoforms as
      ``<canonical_accession>-<n>`` (e.g. ``P04637-2``). PROTEA stores each
      isoform as a separate ``Protein`` row grouped under
      ``canonical_accession``.

   Job
      An ORM row representing a user-visible unit of work submitted to PROTEA
      via ``POST /jobs``. A ``Job`` progresses through states
      (``QUEUED``, ``RUNNING``, ``SUCCEEDED``, ``FAILED``) tracked by a
      state machine in ``BaseWorker``. ``payload``, ``meta``, and ``fields``
      are PostgreSQL :term:`JSONB`.

   JobEvent
      A structured log row appended by every state transition or
      ``emit()`` call during a Job's lifetime. Provides the
      :term:`audit trail` without requiring a separate logging service.

   JSONB
      The PostgreSQL binary JSON column type. PROTEA uses JSONB for
      ``Job.payload``, ``Job.meta``, ``Job.fields``, and
      ``GOPrediction.features`` to allow schema evolution without migrations
      on those columns.

   JWT
      **JSON Web Token.** A compact, URL-safe token format used for
      stateless authentication. PROTEA API endpoints optionally validate
      JWTs issued by a configured identity provider.

   KNN
      **K-Nearest Neighbours.** The core prediction algorithm in PROTEA:
      for each query protein embedding, the ``K`` most similar reference
      embeddings are retrieved, and their GO annotations are transferred
      with a score proportional to embedding similarity. KNN search runs in
      Python via NumPy or :term:`FAISS` (never via pgvector SQL at scale).

   LAFA
      **Large-scale Annotation with Functional Annotators.** The PROTEA
      inference layer (implemented in ``protea-method``) that wraps the KNN
      prediction + re-ranker scoring pipeline into a single callable for
      CAFA submission and benchmark evaluation.

   LCP
      **Largest Contentful Paint.** A Core Web Vitals metric that measures
      the time from page load until the largest visible content element is
      rendered. Tracked in the PROTEA Next.js frontend performance budget.

   leakage-free temporal holdout
      The practice of constructing training and evaluation sets from
      annotation snapshots taken at strictly different time points, ensuring
      that no ground-truth label from the evaluation period is visible during
      training. Enforced by ``generate_evaluation_set`` via the
      ``(t0, t1)`` snapshot-pair design.

   LK
      **Limited-Knowledge** evaluation category. The query protein had
      annotations in some namespaces at t0 but not the one under evaluation.

   manage.sh
      The shell script under ``scripts/manage.sh`` that starts, stops,
      scales, and inspects the long-running processes that make up the
      PROTEA dev stack: the FastAPI server, the ten RabbitMQ queue
      workers (one per queue), the stale-job reaper (a periodic
      database scanner, not a queue consumer), and the Next.js
      frontend. The reference for everyday operations.

   MD5
      The 128-bit message digest algorithm used by PROTEA to deduplicate
      protein sequences. Identical amino-acid strings produce the same MD5
      hash and therefore share a single ``Sequence`` row, regardless of
      which UniProt accession submitted them.

   Method Object
      A design pattern where a complex algorithm is encapsulated in a class
      that carries all the parameters and state needed for one invocation.
      PROTEA uses this pattern for :term:`Operation` implementations so that
      each unit of domain logic is independently testable.

   MFO
      **Molecular Function Ontology.** One of the three namespaces of the
      :term:`GO`. MFO terms describe the biochemical activities of a gene
      product at the molecular level (e.g. kinase activity, DNA binding).

   MIL
      **Multiple-Instance Learning.** A weakly supervised learning framework
      where a label is assigned to a bag of instances rather than to each
      instance individually. Referenced in the thesis related-work chapter as
      one family of protein annotation approaches.

   MinIO
      An S3-compatible object storage server. PROTEA's :term:`artifact store`
      uses MinIO in production for storing re-ranker datasets and booster
      files. Configured via ``PROTEA_MINIO_ENDPOINT`` and related env vars.

   NK
      **No-Knowledge** evaluation category. The query protein had **no**
      experimental annotations in the older snapshot for the namespace under
      evaluation. The hardest of the three CAFA categories.

   ngrok tunnel
      A reverse-proxy tunnel that exposes a local PROTEA instance to the
      internet over a stable URL. Used in the ``deploy-keeper`` pattern so
      that remote collaborators or CAFA submission scripts can reach the API
      without a public server. See :doc:`/runbooks/index` for setup.

   OBO
      **Open Biological and Biomedical Ontology.** A plain-text serialisation
      format for ontologies. PROTEA's ``load_ontology_snapshot`` operation
      parses the GO OBO file to populate :term:`OntologySnapshot` and
      :term:`GO term` rows.

   OOM
      **Out-Of-Memory.** A failure mode where a process exceeds available RAM
      or GPU memory. The embedding worker OOM runbook (see
      :doc:`/runbooks/index`) describes how to diagnose and recover from GPU
      OOM during batch embedding jobs.

   OntologySnapshot
      One full GO release stored in PROTEA, versioned by ``obo_version``
      from the OBO header. Every prediction is permanently linked to the
      snapshot it was produced against, which is what makes the pipeline
      reproducible.

   Operation
      The fundamental unit of domain logic in PROTEA. Any class implementing
      ``name: str`` and ``execute(session, payload, *, emit) -> OperationResult``.
      See :doc:`/architecture/operations`.

   OperationConsumer
      The worker class that handles fire-and-forget batch tasks. The payload
      is carried inline in the message rather than referenced by a UUID, no
      child ``Job`` row is created, and progress is reported via an atomic
      increment on the parent job's counter.

   OperationRegistry
      A dict-backed registry (``contracts/registry.py``) that maps operation
      names to their implementing classes. Operations are registered at
      startup; ``BaseWorker`` resolves them by name at dispatch time.

   ORM
      **Object-Relational Mapper.** The layer that maps Python classes to
      database tables. PROTEA uses SQLAlchemy 2 ORM models defined under
      ``protea/infrastructure/orm/models/``.

   OTel
      **OpenTelemetry.** A vendor-neutral observability framework for traces,
      metrics, and logs. Referenced in the PROTEA observability roadmap.

   Parameter Object
      A pattern where many related arguments are grouped into a single
      structured object (typically a dataclass or Pydantic model) rather than
      passed individually. PROTEA uses parameter objects for operation
      payloads validated by Pydantic at the API boundary.

   pgvector
      A PostgreSQL extension that adds a ``VECTOR`` column type. PROTEA uses
      it solely for **storing** embedding vectors; nearest-neighbour queries
      run in Python (NumPy or FAISS), not via SQL. See
      :doc:`/adr/001-knn-without-pgvector`.

   PK
      **Partial-Knowledge** evaluation category. The query protein already
      had annotations in the namespace under evaluation; new ones were added
      between t0 and t1.

   PLM
      **Protein Language Model.** A transformer neural network pre-trained on
      large protein sequence databases (analogous to NLP language models).
      PROTEA supports :term:`ESM-2`, :term:`ESM-C`, :term:`ProtT5`,
      :term:`ProstT5`, and :term:`Ankh` via swappable backends.

   PredictionSet
      The result container for a prediction job. Links a query set, an
      :term:`EmbeddingConfig`, an :term:`AnnotationSet`, and an
      :term:`OntologySnapshot`. Holds many ``GOPrediction`` rows.

   PROTEA
      **PROtein funcTional Embedding-based Annotation.** The main annotation
      platform developed as part of the doctoral thesis of Francisco Miguel
      Perez Canales. PROTEA consolidates the PIS and FANTASIA codebases into
      a clean, modular architecture covering ingestion, embedding, KNN
      prediction, re-ranking, and CAFA evaluation.

   Protein
      An ORM row representing one UniProt accession (including isoforms).
      Multiple ``Protein`` rows share the same ``Sequence`` row when their
      amino-acid strings are identical. Grouped by :term:`canonical accession`.

   ProtT5
      A protein language model in the T5 encoder-decoder family, pre-trained
      by Elnaggar et al. on BFD and UniRef50. Produces per-residue and
      per-protein representations. PROTEA can route embedding jobs to a
      ProtT5 backend.

   ProstT5
      A compact variant of :term:`ProtT5` further fine-tuned for protein
      structure-sequence translation tasks. Supported as an embedding backend
      in PROTEA via ``protea-backends``.

   QueueConsumer
      The worker class that handles user-visible jobs backed by a ``Job`` row.
      Implements the two-session lifecycle (claim → execute) and writes
      ``JobEvent`` rows on every state transition. See
      :doc:`/architecture/job_lifecycle`.

   qualifier
      A GO annotation attribute that modifies the relationship between a
      protein and a GO term (e.g. ``NOT``, ``contributes_to``,
      ``colocalizes_with``). Stored in ``ProteinGOAnnotation.qualifier``.

   release-please
      A Google-maintained tool that automates SemVer changelog and GitHub
      release creation based on Conventional Commits. Used in the PROTEA
      release workflow.

   RerankerModel
      An ORM row that registers a trained LightGBM booster for inference
      within PROTEA. Stores the booster either inline (``model_data``,
      legacy) or by reference (``artifact_uri``, preferred). The
      ``feature_schema_sha`` field is load-bearing: inference refuses to
      score with a booster whose schema fingerprint differs from the live
      pipeline.

   RetryLaterError
      A sentinel exception that an :term:`Operation` can raise when a shared
      resource (e.g. the GPU) is temporarily unavailable. ``BaseWorker``
      catches it, resets the job to ``QUEUED``, and re-publishes the message
      after the requested delay. Used to serialise embedding coordinators
      against a single-GPU host.

   schema_sha
      A deterministic SHA-256 fingerprint of the feature schema produced by
      ``FeatureRegistry``. Written into every ``Dataset`` row and every
      ``RerankerModel`` row. At inference time PROTEA compares the live
      pipeline's fingerprint against the booster's recorded fingerprint and
      refuses to score if they differ, preventing silent drift. See the
      schema-sha-v2 backfill runbook in :doc:`/runbooks/index`.

   SemVer
      **Semantic Versioning.** The ``MAJOR.MINOR.PATCH`` versioning scheme
      used by PROTEA and all plugin repos. Managed by :term:`release-please`
      via Conventional Commits.

   Sequence
      An ORM row representing a unique amino-acid string, deduplicated by
      :term:`MD5` hash. Multiple :term:`Protein` rows can reference the same
      ``Sequence``.

   smell budget
      A project-level cap on the number of code-smell findings tolerated in
      the codebase. CI enforces the budget; a ``smell-check`` step fails if
      new smells are introduced without a corresponding budget increment in
      the config file.

   study_v9
      The named version of the re-ranker feature study that defines the
      feature set used for the CAFA 6 submission. Used as a reproducibility
      handle in the thesis.

   t0
      The older annotation snapshot in a temporal-holdout split. Functions as
      the *reference set*: a method may use any annotation present at t0 as
      ground truth for transfer.

   t1
      The newer annotation snapshot in a temporal-holdout split. Annotations
      that exist at t1 but not at t0 form the evaluation ground truth.

   TTFB
      **Time to First Byte.** An HTTP performance metric measuring the delay
      between sending a request and receiving the first byte of the response.
      Tracked in the PROTEA API performance budget.

   TSV
      **Tab-Separated Values.** A plain-text tabular format. PROTEA's
      ``fetch_uniprot_metadata`` operation streams TSV data from the UniProt
      REST API; QuickGO bulk downloads are also in TSV format.

   two-session worker pattern
      The ``BaseWorker`` design where job claim and job execution use
      **separate** SQLAlchemy sessions: the first session transitions
      ``QUEUED → RUNNING`` and flushes ``job.started``; the second session
      runs the operation and transitions to ``SUCCEEDED`` or ``FAILED``. This
      ensures that a crash during execution cannot roll back the claim, and
      the audit trail is always consistent.

   AMQP
      **Advanced Message Queuing Protocol.** The wire protocol used by
      RabbitMQ. PROTEA workers connect to the broker via the ``aio_pika``
      async client. The broker URL is configured through
      ``PROTEA_AMQP_URL`` (e.g. ``amqp://user:pass@localhost:5672/``).

   anc2vec
      A graph-embedding approach that represents :term:`GO` terms as dense
      vectors by training on the ontology graph structure (ancestor
      relationships). Provides an alternative to pure sequence-based
      representations for GO-term similarity; referenced in
      :doc:`/related_work` as a graph-based baseline.

   async job queue
      The combination of RabbitMQ (message broker) and PostgreSQL (job-state
      store) that PROTEA uses to schedule and track long-running operations.
      A client submits work via ``POST /jobs``; a :term:`QueueConsumer` worker
      picks the message off the queue, claims the :term:`Job` row, and
      transitions it through the state machine.

   AuPRC
      **Area under the Precision-Recall Curve.** A secondary evaluation metric
      reported by ``cafaeval`` alongside :term:`Fmax`. AuPRC summarises
      classifier performance across all decision thresholds; higher values
      indicate better calibration as well as peak performance.

   BLASTP
      The protein-vs-protein variant of the BLAST sequence-alignment suite.
      Used as a homology-transfer baseline in CAFA benchmarks and in related
      works that annotate query sequences by copying GO terms from
      high-scoring database hits. PROTEA does not invoke BLASTP directly;
      it serves as a comparison point in :doc:`/related_work`.

   chunk pooling
      The strategy used to aggregate per-residue embeddings produced by a
      :term:`PLM` into a single fixed-length vector for each protein. PROTEA
      supports mean pooling over all residue tokens; the pooling strategy is
      recorded in :term:`EmbeddingConfig` and must be consistent between
      index construction and query time.

   DIAMOND
      A fast protein-sequence aligner that achieves BLASTP-level sensitivity
      at orders-of-magnitude higher throughput. Referenced in
      :doc:`/related_work` as a homology-transfer baseline; not invoked
      directly by PROTEA.

   embedding backend
      A plugin (from the ``protea-backends`` repo) that encapsulates the
      connection to a specific :term:`PLM` inference service and returns
      per-residue or per-protein vectors. Backends are selected by model
      name at job submission time and are hot-swappable without changing the
      prediction pipeline. Current backends include :term:`ESM-2`,
      :term:`ESM-C`, :term:`ProtT5`, :term:`ProstT5`, and :term:`Ankh`.

   ESM-3
      The third-generation ESM model family from EvolutionaryScale. ESM-3
      is a multi-modal model that jointly reasons over sequence, structure,
      and function. Noted in :doc:`/related_work` as a frontier PLM;
      PROTEA targets its compact variant :term:`ESM-3c` as a future backend.

   ESM-3c
      The compact (300M-parameter) variant of :term:`ESM-3`. Produces
      sequence-level embeddings compatible with the PROTEA KNN pipeline.
      Supported as an embedding backend via ``protea-backends``.

   EvaluationResult
      An ORM row that stores the per-aspect and combined Fmax,
      :term:`AuPRC`, and coverage metrics produced by
      ``run_cafa_evaluation`` for a given :term:`PredictionSet` against a
      frozen :term:`EvaluationSet`. Multiple runs against the same
      prediction set (e.g. with different thresholds) create separate rows.

   evaluation runner
      A runner plugin (from ``protea-runners``) that submits a
      ``run_cafa_evaluation`` job and waits for the :term:`EvaluationResult`
      rows to be available. Used by the experiment automation layer to
      benchmark a :term:`PredictionSet` without manual API calls.

   EvidenceFilter
      A configuration that restricts which GO annotations are used as the
      reference set for KNN transfer. Typically expressed as a set of
      allowed evidence codes (e.g. excluding :term:`IEA`). Applied during
      annotation loading or at KNN scoring time to control annotation quality.

   IC
      **Information Content.** A measure of the specificity of a :term:`GO
      term`, computed as the negative log-frequency of that term in a
      reference annotation corpus. High-IC terms are rare and specific; the
      IC of a term is used to weight predictions in :term:`IA`-weighted
      evaluation and to compute :term:`Smin`.

   IEA
      **Inferred from Electronic Annotation.** A GO evidence code assigned
      when annotation was derived computationally (e.g. from sequence
      similarity or keyword mapping) rather than from manual experiment.
      IEA annotations are typically excluded from CAFA benchmark ground
      truth because they may reflect the method being evaluated.

   integration runner
      A runner plugin (from ``protea-runners``) that chains multiple
      PROTEA operations in a single experiment pass: protein ingestion,
      embedding computation, KNN prediction, and optionally re-ranker
      scoring. Used by experiment automation scripts to drive end-to-end
      runs without manual job submission.

   NetGO
      A network-based protein function prediction method that integrates
      sequence features with protein-protein interaction networks. Cited
      in :doc:`/related_work` as a strong baseline that PROTEA is compared
      against in CAFA 6 evaluation results.

   plugin registry dispatch
      The mechanism by which PROTEA resolves an operation name (a string
      from the job payload) to a concrete :term:`Operation` implementation
      at runtime via :term:`OperationRegistry`. The registry is populated
      through Python ``entry_points`` declared in each plugin
      ``pyproject.toml``, allowing new operations to be added without
      modifying the core package.

   RabbitMQ
      The open-source message broker that carries all PROTEA operation
      messages between the API and the queue workers. Each pipeline stage
      has a dedicated queue (e.g. ``protea.embeddings``,
      ``protea.predictions``). Supports :term:`DLQ` / :term:`DLX` for
      failed-message routing and consumer acknowledgements for
      at-least-once delivery.

   Redis
      An in-memory data structure store. PROTEA uses Redis as the backing
      store for the FastAPI rate-limiter (via ``slowapi``) and as an
      optional cache layer for frequent read-heavy endpoints. Not used for
      the primary job queue (see :term:`RabbitMQ`).

   reranker_cache
      The local directory (``~/Thesis2/storage/reranker_cache/``) that
      stores downloaded re-ranker datasets and booster artefacts pulled
      from the :term:`artifact store`. Used by ``protea-reranker-lab``
      scripts so that large files are not re-downloaded between training
      runs.

   Smin
      The minimum semantic distance metric used in some CAFA evaluation
      variants as a complement to :term:`Fmax`. Smin is derived from the
      :term:`IC`-weighted Hamming distance between the predicted and
      true annotation vectors across the ontology graph; lower Smin
      indicates better performance.
