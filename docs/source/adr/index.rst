Architecture Decision Records
=============================

Design decisions that are not obvious from reading the code.  Each ADR
documents **why** a decision was made, not just what — the code already
shows the what.

Decisions are grouped by system layer:

.. list-table::
   :header-rows: 1
   :widths: 10 50 40

   * - ADR
     - Decision
     - Problem it solves
   * - 001
     - :doc:`KNN on CPU, not pgvector or GPU <001-knn-without-pgvector>`
     - pgvector does not scale to 500K+ vectors; GPU must be reserved for inference
   * - 006
     - :doc:`Sequence deduplication by MD5 <006-sequence-deduplication-by-md5>`
     - 30K duplicate sequences in Swiss-Prot waste hours of GPU time
   * - 002
     - :doc:`Two-session worker pattern <002-two-session-worker-pattern>`
     - A mid-operation crash left the job invisible to monitoring
   * - 003
     - :doc:`Two types of consumer <003-queue-consumer-vs-operation-consumer>`
     - Thousands of batch jobs per pipeline flooded the jobs table
   * - 004
     - :doc:`Dead letter queue and retries <004-dead-letter-queue-and-retry-strategy>`
     - Failed messages were lost; retries without backoff amplified failures
   * - 005
     - :doc:`Reusable RabbitMQ connections <005-thread-local-rabbitmq-connections>`
     - A coordinator dispatching 500 batches opened 500 TCP connections

.. toctree::
   :maxdepth: 1
   :hidden:

   001-knn-without-pgvector
   002-two-session-worker-pattern
   003-queue-consumer-vs-operation-consumer
   004-dead-letter-queue-and-retry-strategy
   005-thread-local-rabbitmq-connections
   006-sequence-deduplication-by-md5
