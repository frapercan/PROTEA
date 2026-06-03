Embedding Worker OOM
====================

``ComputeEmbeddingsBatchOperation`` (``protea/core/operations/compute_embeddings.py``)
runs inside the ``protea.embeddings.batch`` queue worker and performs
GPU forward passes for protein language model inference. When the batch
size or sequence length exceeds available VRAM, PyTorch raises a
``torch.cuda.OutOfMemoryError``. The ``OperationConsumer``
(``protea/infrastructure/queue/consumer.py``) catches the error,
flushes the GPU cache, and republishes the message with an incremented
``x-oom-retry`` header. After ``oom_max_retries`` attempts (default 5)
the message is dead-lettered.

OOM retry policy defaults (``QueueTuning`` in ``protea/config/tuning.py``):

- ``oom_max_retries`` = 5 retries
- ``oom_base_delay`` = 5 s (exponential: 5 / 10 / 20 / 40 / 80 s)
- ``oom_max_delay`` = 300 s cap
- Total wait budget before dead-letter: approx. 155 s

Symptoms
--------

- Worker logs on ``protea.embeddings.batch`` show repeated lines of the
  form::

      CUDA OOM: backing off 20s (retry 3/5). operation=compute_embeddings_batch

  followed eventually by::

      CUDA OOM retries exhausted — dead-lettering. operation=compute_embeddings_batch retries=5

- The parent ``compute_embeddings`` job is stuck in RUNNING with no
  forward progress. ``GET /jobs/{id}/events`` returns
  ``"event": "child.cuda_oom_dead_letter"`` with no subsequent
  ``child.*`` activity.

- ``protea.dead-letter`` accumulates messages in the RabbitMQ management
  UI (``http://localhost:15672``).

- ``GET /jobs?status=running`` shows a ``compute_embeddings`` job whose
  ``progress_current`` has not advanced in more than the OOM wait budget.

- ``nvidia-smi`` reports 100% VRAM utilisation on the GPU worker node
  just before the crash:

  .. code-block:: bash

      watch -n1 nvidia-smi

Diagnosis
---------

1. **Confirm the failure class from worker logs:**

   .. code-block:: bash

       bash scripts/manage.sh logs embeddings-batch

   Look for ``OutOfMemoryError`` or ``CUDA OOM retries exhausted``.

2. **Check available VRAM vs. model requirements during a live job:**

   .. code-block:: bash

       nvidia-smi --query-gpu=name,memory.total,memory.used,memory.free \
           --format=csv,noheader,nounits

   A ``prot_t5_xl_uniref50`` model at ``max_length=2048`` and fp32
   requires roughly 11-12 GB of VRAM with ``batch_size=1``. ESM-2 650M
   requires roughly 3-4 GB at ``batch_size=8``.

3. **Retrieve the batch payload from the DLQ to read the batch_size:**

   .. code-block:: bash

       curl -s -u guest:guest \
           -X POST http://localhost:15672/api/queues/%2F/protea.dead-letter/get \
           -H "Content-Type: application/json" \
           -d '{"count":5,"ackmode":"ack_requeue_true","encoding":"auto","truncate":50000}' \
           | python3 -m json.tool

   The message body contains ``"operation": "compute_embeddings_batch"``,
   ``"batch_size": <N>``, and ``"embedding_config_id": "<uuid>"``.

4. **Correlate batch_size and model_name via the EmbeddingConfig:**

   .. code-block:: bash

       # Replace <config-uuid> with the value from the DLQ payload.
       curl -s http://localhost:8000/embedding-configs/<config-uuid> \
           | python3 -m json.tool | grep -E '"model_name|model_backend|max_length"'

5. **Inspect the parent job's event log for the full retry sequence:**

   .. code-block:: bash

       curl -s http://localhost:8000/jobs/<job-uuid>/events \
           | python3 -m json.tool

Fix
---

**Immediate: reduce batch_size for the new job**

The ``batch_size`` field on the coordinator payload controls the number of
sequences passed to each GPU forward pass inside
``ComputeEmbeddingsBatchOperation._infer_all``. Cancel the stuck job and
resubmit with a lower ``batch_size``:

.. code-block:: bash

    # Cancel the stuck coordinator job.
    curl -s -X POST http://localhost:8000/jobs/<job-uuid>/cancel

    # Resubmit with batch_size=1 (safe default for large models).
    curl -s -X POST http://localhost:8000/jobs \
        -H "Content-Type: application/json" \
        -d '{
          "operation": "compute_embeddings",
          "payload": {
            "embedding_config_id": "<config-uuid>",
            "batch_size": 1,
            "device": "cuda"
          }
        }' | python3 -m json.tool

The coordinator passes ``batch_size`` through to every child batch message
(see ``build_batch_dispatch_messages`` in
``protea/core/operations/_compute_embeddings_helpers.py``). ``batch_size=1``
is the safe floor and the explicit default for large T5 models (see
docstring on ``ComputeEmbeddingsPayload``).

**FP32 to FP16: load the model in half precision**

For ESM-C (``esm3c`` backend), the model is already cast to FP16 by the
backend plugin before any forward pass. For other backends, the backend
plugin's ``load_model`` call controls the dtype. If the relevant backend
plugin in ``protea-backends`` exposes a ``dtype`` or ``fp16`` parameter,
set it in the ``EmbeddingConfig`` before submitting the job.

**Purge DLQ messages for the cancelled job**

After cancelling, remove the dead-lettered batch messages so they do not
re-fire:

.. code-block:: bash

    # Purge the entire DLQ (destructive — verify first with a peek).
    curl -s -u guest:guest \
        -X DELETE http://localhost:15672/api/queues/%2F/protea.dead-letter/contents

**Restart the batch worker**

If the worker process is stuck (GPU memory not released after the crash),
restart it:

.. code-block:: bash

    bash scripts/manage.sh stop
    bash scripts/manage.sh status    # confirm all workers stopped
    bash scripts/manage.sh start     # restart full stack

Prevention
----------

**Per-backend memory budget assertion**

Add a startup assertion in the backend plugin's ``load_model`` that
compares the model's parameter count (MB) against
``torch.cuda.get_device_properties(device).total_memory``. Log a
``WARNING`` when the ratio exceeds a safe threshold so operators see the
risk before the first OOM.

**Tune retry knobs for the deployment target**

The OOM retry policy can be tightened to fail faster (avoiding 155 s of
silent retries) or loosened for intermittent pressure spikes:

.. code-block:: bash

    # Reduce retries to 2 for dev machines with limited VRAM.
    export PROTEA_TUNING__QUEUE__OOM_MAX_RETRIES=2
    export PROTEA_TUNING__QUEUE__OOM_MAX_DELAY=60

    # Or set in protea/config/system.yaml under the tuning: section:
    # tuning:
    #   queue:
    #     oom_max_retries: 2
    #     oom_max_delay: 60

**Monitor VRAM utilisation**

Add a Prometheus gauge for ``nvidia-smi`` VRAM utilisation and alert when
it exceeds 90% during an active embedding job. PROTEA's monitoring
configuration lives in ``docker-compose.monitoring.yml``.
