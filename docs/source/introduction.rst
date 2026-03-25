Introduction
============

The legacy coupling problem
----------------------------

The **Protein Information System (PIS)** and **FANTASIA** established foundational infrastructure
for protein data ingestion and functional annotation at scale. However, both systems share a
structural limitation: their workers conflate multiple concerns into single classes.

A typical PIS/FANTASIA worker manages its own database session, connects directly to the message
broker, orchestrates task sequencing, *and* executes domain logic — all in the same class. This
coupling produces code that is difficult to unit-test (because all infrastructure must be mocked
at once), hard to extend (because adding a new operation requires understanding the entire
execution context), and fragile under failure (because a queue disconnect or DB error can leave
jobs in ambiguous states with no audit trail).

The PROTEA approach
-------------------

PROTEA is architected around a deliberate separation of three layers:

**Infrastructure layer** (``protea/infrastructure/``)
   Manages database sessions, connection factories, configuration loading, and the RabbitMQ
   transport. This layer knows nothing about domain logic.

**Execution layer** (``protea/workers/``)
   Orchestrates the job lifecycle: claiming a job, dispatching it to the correct operation,
   and recording the outcome. The ``BaseWorker`` uses two independent sessions by design —
   one to claim (QUEUED → RUNNING) and one to execute — ensuring that even a mid-execution
   crash leaves the DB in a consistent, inspectable state.

**Domain layer** (``protea/core/``)
   Pure domain logic. Each ``Operation`` receives an open session and an ``emit`` callback;
   it returns an ``OperationResult``. Operations do not manage sessions, queues, or HTTP
   routing. They are individually testable with a mocked session and a noop emit function.

An incremental migration
-------------------------

The goal of PROTEA is not a complete rewrite. PIS tables (``protein``, ``sequence``,
``protein_uniprot_metadata``) and FANTASIA computation workflows are progressively migrated
into this architecture as new capabilities are added. Each migration step must preserve or
improve computational efficiency and must not introduce regressions in the data model.

Current capabilities
---------------------

PROTEA currently provides sixteen registered operations spanning the full protein
functional annotation pipeline:

- **Data ingestion** — ``insert_proteins``, ``fetch_uniprot_metadata``,
  ``load_ontology_snapshot``, ``load_goa_annotations``, ``load_quickgo_annotations``
- **Embedding computation** — ``compute_embeddings`` (coordinator),
  ``compute_embeddings_batch``, ``store_embeddings``
- **GO term prediction** — ``predict_go_terms`` (coordinator),
  ``predict_go_terms_batch``, ``store_predictions``
- **Evaluation** — ``generate_evaluation_set``, ``run_cafa_evaluation``
- **Re-ranking** — ``train_reranker``, ``train_reranker_auto``
- **Diagnostics** — ``ping``

A scoring engine applies weighted formulas or trained LightGBM re-rankers to
prediction sets. A one-click ``/annotate`` endpoint automates the entire workflow
from FASTA upload to GO term prediction.

.. admonition:: Design principle
   :class: note

   New operations are added by implementing the ``Operation`` protocol and registering them
   at worker startup. No changes to the infrastructure or execution layers are required.
