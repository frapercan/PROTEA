Glossary
========

A reference list of acronyms, domain terms, and PROTEA-specific concepts that
appear throughout the documentation. Use Sphinx's ``:term:`` role to link to
any entry from another page.

.. glossary::
   :sorted:

   GO
      **Gene Ontology.** A structured controlled vocabulary that describes
      gene-product attributes across three orthogonal aspects: Molecular
      Function (MFO), Biological Process (BPO), and Cellular Component (CCO).
      Terms are organised as a directed acyclic graph; a more specific term
      is a child of every more general term that subsumes it. PROTEA stores a
      complete release as one :term:`OntologySnapshot`.

   GAF
      **Gene Association File.** The plain-text tabular format used by
      UniProt-GOA to publish protein → GO term annotations. PROTEA's
      ``load_goa_annotations`` operation streams GAF 2.2 files.

   GOA
      **Gene Ontology Annotation** project at EBI. Publishes high-volume
      protein → GO term assignments derived from experiments, sequence
      curation, and electronic annotation.

   ECO
      **Evidence and Conclusion Ontology.** Identifies how a GO annotation
      was derived (experimental, computational, author statement, etc.).
      QuickGO returns ECO IDs which PROTEA optionally maps to GAF-style
      evidence codes.

   IA
      **Information Accretion.** A weighting scheme used in CAFA evaluation
      that down-weights uninformative GO terms and rewards prediction of rare,
      specific terms. PROTEA's evaluation uses the IA weights from the CAFA6
      benchmark.

   Fmax
      The maximum F-measure achieved over all decision thresholds for a
      binary or multi-label classifier. The headline metric reported by
      ``cafaeval`` and the primary measure used in :doc:`results`.

   NK
      **No-Knowledge** evaluation category. The query protein had **no**
      experimental annotations in the older snapshot for the namespace under
      evaluation. The hardest of the three CAFA categories.

   LK
      **Limited-Knowledge** evaluation category. The query protein had
      annotations in some namespaces at t0 but not the one under evaluation.

   PK
      **Partial-Knowledge** evaluation category. The query protein already
      had annotations in the namespace under evaluation; new ones were added
      between t0 and t1.

   t0
      The older annotation snapshot in a temporal-holdout split. Functions as
      the *reference set*: a method may use any annotation present at t0 as
      ground truth for transfer.

   t1
      The newer annotation snapshot in a temporal-holdout split. Annotations
      that exist at t1 but not at t0 form the evaluation ground truth.

   Operation
      The fundamental unit of domain logic in PROTEA. Any class implementing
      ``name: str`` and ``execute(session, payload, *, emit) -> OperationResult``.
      See :doc:`/architecture/operations`.

   Coordinator
      An :term:`Operation` that does not perform the heavy work itself but
      partitions it into batches and publishes child messages to a downstream
      queue. Coordinators return ``OperationResult(deferred=True)`` so the
      parent ``Job`` row remains in ``RUNNING`` until the last child finishes.
      ``compute_embeddings`` and ``predict_go_terms`` are coordinators.

   Deferred
      An ``OperationResult`` flag that tells ``BaseWorker`` *not* to
      transition the parent ``Job`` to ``SUCCEEDED`` when ``execute()``
      returns. Used by coordinators that hand work off to child workers
      tracked through atomic progress counters.

   QueueConsumer
      The worker class that handles user-visible jobs backed by a ``Job`` row.
      Implements the two-session lifecycle (claim → execute) and writes
      ``JobEvent`` rows on every state transition. See
      :doc:`/architecture/job_lifecycle`.

   OperationConsumer
      The worker class that handles fire-and-forget batch tasks. The payload
      is carried inline in the message rather than referenced by a UUID, no
      child ``Job`` row is created, and progress is reported via an atomic
      increment on the parent job's counter.

   RetryLaterError
      A sentinel exception that an :term:`Operation` can raise when a shared
      resource (e.g.\ the GPU) is temporarily unavailable. ``BaseWorker``
      catches it, resets the job to ``QUEUED``, and re-publishes the message
      after the requested delay. Used to serialise embedding coordinators
      against a single-GPU host.

   pgvector
      A PostgreSQL extension that adds a ``VECTOR`` column type. PROTEA uses
      it solely for **storing** embedding vectors; nearest-neighbour queries
      run in Python (NumPy or FAISS), not via SQL. See
      :doc:`/adr/001-knn-without-pgvector`.

   FAISS
      Facebook AI Similarity Search. The approximate nearest-neighbour
      library used by PROTEA's prediction pipeline at scale (IVFFlat index).

   ESM
      **Evolutionary Scale Modeling.** A family of protein language models
      from Meta AI. PROTEA primarily uses ``ESMC 300M`` to produce
      960-dimensional sequence embeddings.

   OntologySnapshot
      One full GO release stored in PROTEA, versioned by ``obo_version``
      from the OBO header. Every prediction is permanently linked to the
      snapshot it was produced against, which is what makes the pipeline
      reproducible.

   AnnotationSet
      A batch of ``ProteinGOAnnotation`` rows grouped by source
      (``goa`` or ``quickgo``) and tied to one :term:`OntologySnapshot`.
      Two annotation sets from different sources or dates can coexist and
      be compared.

   EmbeddingConfig
      An immutable record of all parameters that affect the geometry of an
      embedding (model, chunking, pooling). Identified by a UUID; changing
      any field produces a new configuration. Every ``SequenceEmbedding``
      and every ``GOPrediction`` carries the UUID it was computed against.

   PredictionSet
      The result container for a prediction job. Links a query set, an
      :term:`EmbeddingConfig`, an :term:`AnnotationSet`, and an
      :term:`OntologySnapshot`. Holds many ``GOPrediction`` rows.

   EvaluationSet
      The CAFA-style temporal-holdout split derived from a (t0, t1) pair of
      annotation sets. Stores per-protein NK/LK/PK classifications per
      namespace. Consumed by ``run_cafa_evaluation`` and the re-ranker
      training pipeline.

   manage.sh
      The shell script under ``scripts/manage.sh`` that starts, stops,
      scales, and inspects the long-running processes that make up the
      PROTEA dev stack: the FastAPI server, the ten RabbitMQ queue
      workers (one per queue), the stale-job reaper (a periodic
      database scanner, not a queue consumer), and the Next.js
      frontend. The reference for everyday operations.
