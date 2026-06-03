Export Coordinator
==================

.. contents:: On this page
   :local:
   :depth: 2

The export coordinator (``export_coordinator`` operation, module
``protea/core/operations/export_minijobs/export_coordinator.py``) is the
entry point for the minijob-based dataset export pipeline. It runs on the
``protea.training`` queue and either fans out per-snapshot KNN minijobs
(when the env gate is active) or delegates directly to the legacy monolithic
``ExportResearchDatasetOperation``.

.. rubric:: Environment gate

The entire minijob path is guarded by a single environment variable:

.. list-table::
   :header-rows: 1
   :widths: 40 20 40

   * - Variable
     - Default
     - Behaviour
   * - ``PROTEA_EXPORT_MINIJOBS``
     - ``"0"``
     - ``"0"`` delegates to the legacy monolithic path; ``"1"`` activates
       the coordinator fan-out.

When ``PROTEA_EXPORT_MINIJOBS=0`` (the default), the coordinator emits a
``export_coordinator.legacy_delegate`` info event and runs
``ExportResearchDatasetOperation`` in-process without touching the minijob
queues. No behaviour change is visible to callers.

.. rubric:: Coordinator lifecycle

When ``PROTEA_EXPORT_MINIJOBS=1`` the coordinator executes the following
steps:

.. code-block:: text

   1. Validate payload (ExportCoordinatorPayload, Pydantic strict mode).
   2. Pre-flight check: reject unsupported search_backend values immediately
      (fast-fail, see below).
   3. Emit export_coordinator.dispatching with the full dispatch plan.
   4. Partition snapshot versions:
      - One export_knn_batch message per training version
        (len(train_versions) messages).
      - One export_knn_batch message per test version
        (len(test_versions) messages).
   5. Publish all messages to protea.training.knn-batch.
   6. Return OperationResult(deferred=True, progress_total=n_minijobs).
      The coordinator Job stays RUNNING until all children complete.

Each ``export_knn_batch`` child runs on ``protea.training.knn-batch`` as an
``OperationConsumer`` (no DB ``Job`` row of its own). When a child
completes successfully it calls
``protea.core.contracts.parent_progress.update_parent_progress``, which
atomically increments the parent ``Job.progress_current``. The parent
transitions to ``SUCCEEDED`` when the last child increments the counter to
``progress_total``.

.. rubric:: annotation_set_id auto-derivation

When a ``POST /v1/datasets`` request targets the ``export_coordinator``
path (i.e. ``PROTEA_EXPORT_MINIJOBS=1``), the API router automatically
resolves ``annotation_set_id`` from the caller-supplied
``(annotation_source, max(train_versions))`` pair before enqueueing the
job. Callers do not supply ``annotation_set_id`` in the HTTP body; the
router queries the ``AnnotationSet`` table and injects the UUID into the
job payload. If no matching ``AnnotationSet`` row exists, the endpoint
returns HTTP 422 immediately and the job is never created.

The resolution rule is::

    annotation_set_id = SELECT id FROM annotation_set
                        WHERE source = annotation_source
                          AND source_version = str(max(train_versions))
                        LIMIT 1

.. rubric:: Accepted search backends

Three backends are supported end-to-end in the minijob path:

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Backend
     - Notes
   * - ``numpy``
     - Brute-force cosine search over a float32 numpy matrix. Safe on any
       host; no additional dependencies.
   * - ``faiss``
     - FAISS flat index (CPU). Requires ``faiss-cpu`` in the environment.
       Default for production export runs.
   * - ``torch``
     - GPU-accelerated KNN via ``protea_method.knn_search``. Re-enabled in
       protea-method commit 5dd737d (PR #564) alongside the ``cu128`` wheel
       default in ``scripts/install_gpu_torch.sh``. Requires a CUDA-capable
       host and the GPU torch wheel (see :ref:`gpu-torch-install`).

Any other value is rejected by the pre-flight check before a single child
is published.

.. rubric:: Fast-fail semantics

Invalid payload values that would cause every child to fail
non-retryably are caught by a pre-flight check inside ``_dispatch_minijobs``
before the first message is published. Today the only pre-flight guard is
``search_backend`` validation.

When the pre-flight rejects the payload the coordinator:

1. Emits ``export_coordinator.failed`` (level ``error``) with the invalid
   value and the list of accepted backends.
2. Raises ``ValueError``.
3. ``BaseWorker`` catches the exception and transitions the parent ``Job``
   to ``FAILED`` immediately.

No child messages are published, no ``RUNNING`` rows are orphaned, and the
error is visible on the job timeline within seconds.

The fast-fail pattern was introduced after the 2026-05-26 incident in which
five coordinator jobs stayed ``RUNNING`` for over an hour while every child
raised ``ValueError("Unknown search backend: 'torch'")`` before the torch
backend was re-enabled.

.. rubric:: Aggregate failure semantics

For failures that slip through the pre-flight (transient errors,
partial outages, or race conditions), the consumer calls
``protea.core.contracts.parent_failure.report_child_failure`` to accumulate
the failure on the parent ``Job.meta``. The aggregation policy is:

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Field in ``parent.meta``
     - Meaning
   * - ``failed_children_count``
     - Running count of failed children, incremented atomically per call.
   * - ``dispatched_total``
     - Total children dispatched (from the coordinator result).
   * - ``top_error_messages``
     - Up to 3 distinct error messages (insertion order, duplicates dropped)
       for post-mortem analysis.

The parent transitions to ``FAILED`` when
``failed_children_count >= dispatched_total`` OR
``failed_children_count / dispatched_total >= failure_ratio``
(default ``failure_ratio=1.0``). Once the parent leaves ``RUNNING``
the helper is a no-op (safe to call repeatedly).

.. rubric:: Child event taxonomy

.. list-table::
   :header-rows: 1
   :widths: 45 55

   * - Event name
     - Emitted by / meaning
   * - ``export_coordinator.dispatching``
     - Coordinator: fan-out plan announced (output_name, coordinator_job_id,
       train/test versions, n_minijobs).
   * - ``export_coordinator.failed``
     - Coordinator pre-flight: invalid payload; no children published.
   * - ``export_coordinator.legacy_delegate``
     - Coordinator: PROTEA_EXPORT_MINIJOBS=0, delegating to monolithic path.
   * - ``child.export_knn_batch.start``
     - KNN batch child: starting KNN search for one snapshot version.
   * - ``child.export_knn_batch.noop``
     - KNN batch child: snapshot version already processed (idempotency skip).
   * - ``child.export_knn_batch.done``
     - KNN batch child: KNN pairs written for one snapshot version.
   * - ``child.pair_knn_done``
     - KNN batch child: one (train/eval) pair completed; triggers
       ``update_parent_progress`` increment.
   * - ``child.failed``
     - Any minijob child: non-retryable error; triggers
       ``report_child_failure`` aggregation on parent.

.. rubric:: Queue topology

.. code-block:: text

   protea.training      <- export_coordinator (Job-backed, coordinator)
   protea.training.knn-batch  <- export_knn_batch  (OperationConsumer, no Job row)
   protea.training.features   <- export_features_batch (future stage)
   protea.training.write      <- export write stage (future stage)

The ``features`` and ``write`` stages are wired in the queue topology but
not yet consumed by the minijob path; the current minijob implementation
covers the KNN partition only. Full end-to-end minijob migration is tracked
separately.

.. seealso::

   - :doc:`job_lifecycle`: two-session worker pattern and deferred completion.
   - :doc:`multistage-pipeline`: shared coordinator / fan-out / collect
     contract (``MultiStagePayload``, ``StageArtifactStore``,
     ``PipelineStage``, ``Coordinator``).
   - :doc:`operations`: ``export_research_dataset`` monolithic path
     (legacy delegate target).
