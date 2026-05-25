Architecture
============

This section describes the runtime architecture of PROTEA: its components,
data model, job lifecycle, and extension points. Each page focuses on one
concern and links to the others where they intersect.

:doc:`system_overview`
   The four horizontal layers (presentation, API, worker, data), the ten
   RabbitMQ queues that connect them, and how a typical request flows through
   the stack from FASTA upload to stored prediction.

:doc:`job_lifecycle`
   The two-session ``BaseWorker`` pattern, parent-child coordinator jobs,
   ``RetryLaterError`` for serialised resources, atomic progress counters, and
   the soft-cancellation contract.

:doc:`data_model`
   The relational schema in five logical groups (sequences and proteins,
   ontology and annotations, embeddings, predictions, query sets and jobs)
   with the deduplication and versioning rules that make every prediction
   reproducible.

:doc:`operations`
   The ``Operation`` protocol that unifies every unit of domain logic, the
   ``OperationRegistry``, and reference documentation for every operation
   shipped with PROTEA (ingestion, embeddings, predictions, evaluation).

:doc:`evaluation`
   The CAFA temporal-holdout protocol, the NK/LK/PK classification, and the
   end-to-end evaluation workflow used to produce the figures in
   :doc:`/results`.

:doc:`orchestration`
   How PROTEA relates to the rest of the working tree: the satellite
   repositories, the optional ``agent-farm`` orchestration system, and
   the contract surface (HTTP API + artefact store) the platform exposes
   for automated consumption.

:doc:`auth`
   Four-role authentication system (guest/researcher/operator/admin) shipped in
   FARM-AUTH.1-11 (ADR D37). Human email+password login, API-key programmatic
   access, session revocation, per-user quota, optional SMTP, and audit log.

Architecture Decision Records
-----------------------------

The pages above describe **what** the architecture looks like today. The
:doc:`/adr/index` records explain **why** each major decision was taken:
the constraint, the rejected alternatives, and the trade-off that closed
the question.

.. list-table::
   :header-rows: 1
   :widths: 8 52 40

   * - ADR
     - Decision
     - Problem it solves
   * - :doc:`001 </adr/001-knn-without-pgvector>`
     - KNN on CPU, not pgvector or GPU
     - pgvector does not scale to 500K+ vectors; the GPU must stay free for
       embedding inference
   * - :doc:`002 </adr/002-two-session-worker-pattern>`
     - Two-session worker pattern
     - A mid-operation crash used to leave the job invisible to monitoring
   * - :doc:`003 </adr/003-queue-consumer-vs-operation-consumer>`
     - ``QueueConsumer`` vs. ``OperationConsumer``
     - Thousands of batch jobs per pipeline flooded the ``jobs`` table
   * - :doc:`004 </adr/004-dead-letter-queue-and-retry-strategy>`
     - Dead-letter queue and retry strategy
     - Failed messages were silently lost; retries without backoff amplified
       failures
   * - :doc:`005 </adr/005-thread-local-rabbitmq-connections>`
     - Reusable RabbitMQ connections
     - A coordinator dispatching 500 batches opened 500 TCP connections
   * - :doc:`006 </adr/006-sequence-deduplication-by-md5>`
     - Sequence deduplication by MD5
     - Tens of thousands of duplicate Swiss-Prot sequences wasted GPU hours

The full ADR index lives at :doc:`/adr/index`.

.. toctree::
   :maxdepth: 2
   :hidden:

   system_overview
   job_lifecycle
   data_model
   operations
   evaluation
   orchestration
   auth
   /adr/index
