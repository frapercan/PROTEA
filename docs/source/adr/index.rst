Architecture Decision Records
=============================

Design decisions that are not obvious from reading the code. Each ADR
documents **why** a decision was made, not just what. The code already
shows the what.

ADRs come in two layers:

- **Implementation decisions** (numbered ``001``-``008``): runtime,
  data model and operational choices discovered while building
  PROTEA. They explain trade-offs of concrete code paths (KNN
  algorithm choice, queue topology, deduplication strategy, retries,
  etc.).
- **Strategic decisions** (``D1``-``D30``): plan-level decisions
  taken in the master plan v3 (2026-05-05). They drive the structure
  of the project, the deployment story, and the thesis writing
  cadence.

Implementation decisions
------------------------

.. list-table::
   :header-rows: 1
   :widths: 8 50 42

   * - ADR
     - Decision
     - Problem it solves
   * - 001
     - :doc:`KNN on CPU, not pgvector or GPU <001-knn-without-pgvector>`
     - pgvector does not scale to 500K+ vectors; GPU must be reserved for inference
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
   * - 006
     - :doc:`Sequence deduplication by MD5 <006-sequence-deduplication-by-md5>`
     - 30K duplicate sequences in Swiss-Prot waste hours of GPU time
   * - 007
     - :doc:`Contract-first integration with protea-reranker-lab <007-contract-first-lab-integration>`
     - Re-ranker iteration cadence would contaminate the production dependency tree
   * - 008
     - :doc:`PK coverage fix in cafaeval fork <008-cafaeval-pk-coverage-fix>`
     - Upstream cafaeval reports coverage > 1 in PK; precision is under-divided

Strategic decisions
-------------------

Decisions taken in the master plan v3 (2026-05-05). Statuses:
*Accepted*, *Pending* (gate opens at the indicated phase), *Deferred*
(scheduled later in the timeline) or *Obsolete* (superseded by a
later revision).

.. list-table::
   :header-rows: 1
   :widths: 6 38 12 44

   * - ID
     - Decision
     - Status
     - Phase / Gate
   * - D1
     - :doc:`Project structure (7 code repos) <D01-project-structure>`
     - Accepted
     - F0 (closed); enacted F0-F2
   * - D2
     - :doc:`export_research_dataset in protea-core <D02-export-research-dataset-location>`
     - Accepted
     - F1
   * - D3
     - :doc:`GOPrediction.features as JSONB <D03-goprediction-features-jsonb>`
     - Accepted
     - F3
   * - D4
     - :doc:`API versioning <D04-api-versioning>`
     - Pending
     - gate at F4
   * - D5
     - :doc:`Front-end in protea-core <D05-frontend-in-core>`
     - Accepted
     - F1
   * - D6
     - :doc:`Authentication strategy <D06-authentication>`
     - Pending
     - gate at F5
   * - D7
     - :doc:`Observability stack <D07-observability-stack>`
     - Pending
     - gate at F-OPS
   * - D8
     - :doc:`UI component library <D08-ui-components>`
     - Accepted
     - F8a
   * - D9
     - :doc:`OBSOLETE: lab as runtime dependency <D09-obsolete-lab-runtime-dep>`
     - Obsolete
     - superseded by D1
   * - D10
     - :doc:`schema_sha v2 migration <D10-schema-sha-v2>`
     - Pending
     - T1.6 (requires_human)
   * - D11
     - :doc:`Job narrative model <D11-job-narrative-model>`
     - Accepted
     - F3
   * - D12
     - :doc:`F-EXP as QA reproduction <D12-fexp-qa-reproduction>`
     - Accepted
     - F-EXP
   * - D13
     - :doc:`Early UI track parallel to F2 <D13-early-ui-track>`
     - Accepted
     - F8a / F8b
   * - D14
     - :doc:`Plugin granularity (deferred) <D14-plugin-granularity>`
     - Deferred
     - F9 post-defense
   * - D15
     - :doc:`protea-method shipping channels <D15-protea-method-shipping>`
     - Accepted
     - F-OPS
   * - D16
     - :doc:`Thesis repository location <D16-thesis-location>`
     - Accepted
     - F0
   * - D17
     - :doc:`OBSOLETE: thesis template choice <D17-obsolete-thesis-template>`
     - Obsolete
     - n/a
   * - D18
     - :doc:`Thesis writing model <D18-thesis-writing-model>`
     - Accepted
     - F-THESIS
   * - D19
     - :doc:`F-RESEARCH targets <D19-fresearch-targets>`
     - Accepted
     - F-RESEARCH
   * - D20
     - :doc:`Co-supervisor review cadence <D20-supervisors-cadence>`
     - Accepted
     - F-THESIS
   * - D21
     - :doc:`Thesis writing parallel from F0 <D21-thesis-track-parallel>`
     - Accepted
     - F-THESIS
   * - D22
     - :doc:`Thesis as research diary <D22-thesis-research-diary>`
     - Accepted
     - F-THESIS
   * - D23
     - :doc:`LAFA submission strategy <D23-lafa-submission>`
     - Accepted
     - F-LAFA
   * - D24
     - :doc:`Hardcoded params externalisation (T-CONF) <D24-hardcoded-params>`
     - Accepted
     - F0 (closed)
   * - D25
     - :doc:`HPC operation mode <D25-hpc-mode>`
     - Pending
     - gate at F-OPS
   * - D26
     - :doc:`Container runtime: OCI plus Apptainer <D26-container-runtime>`
     - Accepted
     - F-OPS
   * - D27
     - :doc:`Image registry <D27-image-registry>`
     - Pending
     - gate at F-OPS
   * - D28
     - :doc:`Secrets management <D28-secrets-management>`
     - Pending
     - gate at F-OPS
   * - D29
     - :doc:`Release pipeline <D29-release-pipeline>`
     - Pending
     - gate at F-OPS
   * - D30
     - :doc:`Insights appendix <D30-insights-appendix>`
     - Accepted
     - F7
   * - D31
     - :doc:`T2B.5 Method Object reframe (sub-cluster granularity) <D31-t2b5-method-object-reframe>`
     - Accepted
     - F2C / §24

.. toctree::
   :maxdepth: 1
   :hidden:

   001-knn-without-pgvector
   002-two-session-worker-pattern
   003-queue-consumer-vs-operation-consumer
   004-dead-letter-queue-and-retry-strategy
   005-thread-local-rabbitmq-connections
   006-sequence-deduplication-by-md5
   007-contract-first-lab-integration
   008-cafaeval-pk-coverage-fix
   D01-project-structure
   D02-export-research-dataset-location
   D03-goprediction-features-jsonb
   D04-api-versioning
   D05-frontend-in-core
   D06-authentication
   D07-observability-stack
   D08-ui-components
   D09-obsolete-lab-runtime-dep
   D10-schema-sha-v2
   D11-job-narrative-model
   D12-fexp-qa-reproduction
   D13-early-ui-track
   D14-plugin-granularity
   D15-protea-method-shipping
   D16-thesis-location
   D17-obsolete-thesis-template
   D18-thesis-writing-model
   D19-fresearch-targets
   D20-supervisors-cadence
   D21-thesis-track-parallel
   D22-thesis-research-diary
   D23-lafa-submission
   D24-hardcoded-params
   D25-hpc-mode
   D26-container-runtime
   D27-image-registry
   D28-secrets-management
   D29-release-pipeline
   D30-insights-appendix
   D31-t2b5-method-object-reframe
