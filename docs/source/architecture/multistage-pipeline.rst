Multi-Stage Pipeline Contract
=============================

.. contents:: On this page
   :local:
   :depth: 2

Three production pipelines in PROTEA share the same coordinator-fan-out-collect
pattern but each grew its own boilerplate independently:

* ``compute_embeddings`` (coordinator + batch + write)
* ``predict_go_terms`` (coordinator + batch + write)
* ``export_minijobs`` (coordinator + knn-batch + features-batch + write)

The multi-stage contract (``protea/core/contracts/multistage.py``,
introduced in F-MULTISTAGE-COHERENCE.1) defines the shared abstractions
so future migrations can converge those pipelines onto a single auditable
pattern without changing any on-the-wire message shapes.


.. rubric:: Contract classes

``MultiStagePayload``
~~~~~~~~~~~~~~~~~~~~~

Every *batch* stage payload inherits from this base instead of
``ProteaPayload`` directly.  The base adds a single required field:

.. code-block:: python

   class MultiStagePayload(ProteaPayload, frozen=True):
       coordinator_job_id: str  # UUID of the parent coordinator Job row

Child operations use ``coordinator_job_id`` to call
:func:`~protea.core.contracts.parent_progress.update_parent_progress`,
which atomically increments the parent's progress counter and marks it
SUCCEEDED when the last child finishes.

``StageArtifactStore``
~~~~~~~~~~~~~~~~~~~~~~

A thin facade over :class:`~protea.infrastructure.storage.ArtifactStore`
that encodes the canonical path convention::

    temp/<pipeline>/<coordinator_job_id>/<stage>/<key>

This eliminates ad-hoc path construction scattered across the three real
pipeline implementations.  The three main methods are:

.. code-block:: python

   store.write_intermediate(stage, key, data) -> str   # returns URI
   store.read_intermediate(uri) -> bytes
   store.exists_intermediate(stage, key) -> bool

``publish_next_stage``
~~~~~~~~~~~~~~~~~~~~~~

A helper that serialises a Pydantic payload and decides whether to inline
it or upload it to the artifact store:

.. code-block:: python

   queue, body = publish_next_stage(
       "protea.embeddings.batch",
       batch_payload,
       artifact_store=stage_store,
       size_threshold_kb=64.0,
   )
   # Returns (queue_name, {"_artifact_uri": "..."}) when payload > 64 KB
   # Returns (queue_name, {...full payload...}) otherwise

The returned pair is ready for appending to
``OperationResult.publish_operations``.

``PipelineStage``
~~~~~~~~~~~~~~~~~

Abstract base class for *batch* and *write* stage operations.

Concrete subclasses must set ``name``, ``description``, ``stage_name``,
and implement ``_execute_stage``.  The ``execute`` wrapper emits
consistent ``<stage_name>.start`` / ``<stage_name>.done`` events:

.. code-block:: python

   class MyBatchOp(PipelineStage):
       name = "my_batch"
       description = "Processes a single batch."
       stage_name = "my_batch"

       def _execute_stage(self, session, payload, *, emit):
           ...
           return OperationResult(result={"processed": n})

``Coordinator``
~~~~~~~~~~~~~~~

Abstract base class for coordinator (fan-out) operations.

Concrete subclasses must set ``name``, ``description``, and implement
``partition`` which returns a list of ``(queue, body)`` pairs.
``dispatch_partition_with_progress`` builds the deferred
``OperationResult`` automatically:

.. code-block:: python

   class MyCoordinator(Coordinator):
       name = "my_coordinator"
       description = "Partitions work and fans out to batch workers."

       def partition(self, session, payload, *, parent_job_id, emit):
           return [
               ("my.batch.queue", {"coordinator_job_id": str(parent_job_id), ...})
               for batch in batches
           ]

       def execute(self, session, payload, *, emit):
           parent_job_id = UUID(payload["_job_id"])
           return self.dispatch_partition_with_progress(
               session, payload, parent_job_id=parent_job_id, emit=emit,
               extra_result={"items": total_items},
           )


.. rubric:: Prospective consumers

The three existing pipelines keep their current per-pipeline boilerplate
and continue to pass their tests untouched.  Migration is tracked in
separate slices:

.. list-table::
   :header-rows: 1
   :widths: 35 15 50

   * - Pipeline
     - Slice
     - Status
   * - ``compute_embeddings``
     - F-MULTISTAGE-COHERENCE.2
     - pending migration
   * - ``predict_go_terms``
     - F-MULTISTAGE-COHERENCE.3
     - pending migration
   * - ``export_minijobs``
     - F-MULTISTAGE-COHERENCE.4
     - pending migration (depends on F-EXPORT-MINIJOB.4)


.. rubric:: Path convention

All intermediate artefacts produced by a multi-stage pipeline are stored
under::

    temp/<pipeline>/<coordinator_job_id>/<stage>/<key>

For the local-FS backend this resolves to a file under the configured
``PROTEA_LOCAL_STORAGE_ROOT``.  For MinIO it becomes an object key in the
configured bucket.  The ``StageArtifactStore`` facade handles both; callers
never construct paths manually.
