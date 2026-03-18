ADR-003: Two types of consumer
===============================

:Date: 2026-01-10
:Author: frapercan

The problem
-----------

Distributed pipelines (``compute_embeddings``, ``predict_go_terms``) split
work into hundreds of batches.  If each batch had its own ``Job`` row in
the DB:

- The ``jobs`` table fills with thousands of rows per prediction run,
  making it impossible to see real user-facing jobs.
- Each batch pays the cost of the two-session pattern (2 round-trips),
  which for 2-8s batches is more overhead than useful work.

What we do
----------

Two consumers coexist:

**QueueConsumer** — for user-facing jobs with full lifecycle tracking:

- Receives ``{"job_id": "<uuid>"}`` and delegates to
  ``BaseWorker.handle_job()``.
- Used by: ``protea.ping``, ``protea.jobs``, ``protea.embeddings``.

**OperationConsumer** — for ephemeral batches with no individual DB row:

- Receives ``{"operation": "...", "job_id": "<parent>", "payload": {...}}``.
- Executes the operation in a single session, ack/nack, done.
- Progress is reported by incrementing ``progress_current`` on the
  **parent job**.
- Events are written to the parent's log with the ``child.`` prefix.
- Used by: ``protea.embeddings.batch``, ``protea.embeddings.write``,
  ``protea.predictions.batch``, ``protea.predictions.write``.

From the outside, the user sees a single job (the coordinator) with a
progress bar that advances.  Batches are invisible.

Trade-offs
----------

- Two code paths for consuming messages, but both are short (~100 lines)
  and share infrastructure (DLQ, registry, emit).
- If a batch fails and goes to the DLQ, there is no individual retry
  counter — just the dead message for inspection.

Rejected
--------

- **Job with** ``is_batch=True`` **flag**: still creates thousands of DB
  rows.
- **Fire-and-forget** without tracking: operators lose visibility into
  progress and failures.
