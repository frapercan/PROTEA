ADR-002: Two-session worker pattern
====================================

:Date: 2025-12-20
:Author: frapercan

The problem
-----------

A worker executes operations that can run for hours (compute_embeddings,
load_goa_annotations).  If the operation fails mid-way, we need the job
to remain marked as ``RUNNING`` in the database so monitoring can detect it.

With a single database session, a rollback on error also reverts the
``QUEUED -> RUNNING`` transition.  The job silently goes back to ``QUEUED``
and nobody notices the failure until the reaper catches it an hour later.

What we do
----------

``BaseWorker.handle_job(job_id)`` opens **two independent sessions**:

1. **Claim session** — changes the job to ``RUNNING``, records
   ``started_at`` and the ``job.started`` event, and **commits immediately**.
   From this point the job is visible as running.

2. **Execute session** — runs the operation.  On success: ``SUCCEEDED``.
   On failure: ``FAILED`` with ``error_code`` and ``error_message``.
   A rollback here does not affect the claim.

Trade-offs
----------

- Two round-trips to DB per job — irrelevant when the operation takes
  minutes.
- RabbitMQ delivers each message to a single consumer (``prefetch=1``),
  so there is no real race condition between workers for the same job.

Rejected
--------

- **Savepoints** inside a long transaction: hold locks and bloat the
  PostgreSQL WAL.
- **Optimistic locking** with a version column: does not solve the
  requirement that the claim must be visible before execution starts.
