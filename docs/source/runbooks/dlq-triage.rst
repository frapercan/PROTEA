DLQ Triage
==========

PROTEA declares one dead-letter exchange (``protea.dlx``) and one dead-letter
queue (``protea.dead-letter``). Every durable queue is wired to forward
undeliverable messages to this exchange via the ``x-dead-letter-exchange``
argument. Messages land in the DLQ when:

- An ``OperationConsumer`` (``protea/infrastructure/queue/consumer.py``) nacks
  a message with ``requeue=False`` (invalid JSON, unregistered operation, or
  non-OOM exception with the default ``requeue_on_failure=False`` option).
- CUDA OOM retries are exhausted (after up to 5 attempts by default;
  configurable via ``PROTEA_TUNING__QUEUE__OOM_MAX_RETRIES``).
- A message body cannot be parsed as valid JSON.

The DLQ never drains automatically. It accumulates until an operator
inspects and resolves the root cause.

Symptoms
--------

- RabbitMQ management UI (``http://localhost:15672``, guest/guest) shows
  ``protea.dead-letter`` with a non-zero message count.
- A parent job is stuck in RUNNING with no forward progress and its events
  log contains ``child.failed`` or ``child.cuda_oom_dead_letter`` entries.
- ``GET /jobs/{id}/events`` returns ``"event": "child.cuda_oom_dead_letter"``
  or ``"event": "child.failed"`` with no subsequent ``child.*`` activity.
- Worker logs on the relevant queue (e.g. ``logs/worker-embeddings-batch-1.log``)
  show ``Operation failed`` or ``CUDA OOM retries exhausted`` lines.

Diagnosis
---------

1. **Check DLQ depth via RabbitMQ management API:**

   .. code-block:: bash

      curl -s -u guest:guest \
          http://localhost:15672/api/queues/%2F/protea.dead-letter \
          | python3 -m json.tool | grep '"messages"'

2. **Inspect DLQ message content without consuming it** (peek via the HTTP
   API; this does not remove the message):

   .. code-block:: bash

      curl -s -u guest:guest \
          -X POST http://localhost:15672/api/queues/%2F/protea.dead-letter/get \
          -H "Content-Type: application/json" \
          -d '{"count":10,"ackmode":"ack_requeue_true","encoding":"auto","truncate":50000}' \
          | python3 -m json.tool

   The ``x-death`` header in each message shows the original queue name,
   the reason (``rejected`` or ``expired``), and the death count.

3. **Correlate with the parent job:** The message body contains
   ``"job_id": "<uuid>"`` (parent job) and ``"operation": "<name>"``.
   Use the ``job_id`` to retrieve the full event log:

   .. code-block:: bash

      curl -s http://localhost:8000/jobs/<job-uuid>/events \
          | python3 -m json.tool

4. **Determine the failure class from worker logs.** For batch queues:

   .. code-block:: bash

      # Embeddings batch worker
      bash scripts/manage.sh logs embeddings-batch
      # Predictions batch worker
      bash scripts/manage.sh logs predictions-batch

5. **Check whether OOM retries were exhausted:**

   .. code-block:: bash

      grep "CUDA OOM retries exhausted\|dead-lettering" logs/worker-embeddings-batch-1.log

Fix
---

1. **Drain and discard dead-lettered messages** (use only after you have
   identified the root cause and confirmed the messages are not recoverable):

   .. code-block:: bash

      # Purge the entire DLQ
      curl -s -u guest:guest \
          -X DELETE http://localhost:15672/api/queues/%2F/protea.dead-letter/contents \
          -H "Content-Type: application/json" \
          -d '{}'

2. **Re-queue a message back to the original queue** (if the root cause is
   fixed and the message is retryable). There is no built-in shovel in
   the dev stack; use the management UI:

   a. Open ``http://localhost:15672`` > Queues > ``protea.dead-letter``.
   b. Use the ``Get Messages`` form to peek and copy the message body.
   c. Use the ``Publish Message`` form on the target queue (e.g.
      ``protea.embeddings.batch``) to re-publish.

   Alternatively, re-submit the parent job via the API, which will
   re-dispatch all batch messages:

   .. code-block:: bash

      # Cancel the stuck parent job
      curl -s -X POST http://localhost:8000/jobs/<parent-uuid>/cancel

      # Re-submit the same operation with the same payload
      curl -s -X POST http://localhost:8000/jobs \
          -H "Content-Type: application/json" \
          -d '{"operation":"compute_embeddings","payload":{...}}'

3. **For CUDA OOM dead-letters:** reduce batch size in the payload
   (``batch_size`` field in ``compute_embeddings_batch`` / ``predict_go_terms_batch``
   messages) or increase ``PROTEA_TUNING__QUEUE__OOM_MAX_RETRIES`` to allow
   more backoff cycles before giving up.

4. **For schema / parse errors** (``Unparseable operation message`` in logs):
   the message is malformed and cannot be recovered. Purge from DLQ and
   fix the publisher code that generated it.

5. **Mark the parent job FAILED** once the dead-lettered children confirm
   the job cannot complete:

   .. code-block:: sql

      UPDATE job
      SET status       = 'failed',
          finished_at   = now(),
          error_code    = 'ChildDeadLettered',
          error_message = 'One or more child batch messages were dead-lettered'
      WHERE id = '<parent-uuid>'
        AND status = 'running';

6. **Verify the DLQ is drained:**

   .. code-block:: bash

      curl -s -u guest:guest \
          http://localhost:15672/api/queues/%2F/protea.dead-letter \
          | python3 -m json.tool | grep '"messages"'
      # Expect: "messages": 0

Prevention
----------

- Set up a RabbitMQ alert on the ``protea.dead-letter`` queue depth. A count
  greater than zero warrants immediate investigation.
- Tune ``PROTEA_TUNING__QUEUE__OOM_MAX_RETRIES`` and ``OOM_BASE_DELAY`` before
  running large GPU inference batches to give the GPU time to recover between
  retries.
- For GPU OOM specifically, reduce ``embedding_batch_size`` /
  ``prediction_batch_size`` in the operation payload rather than increasing
  retries indefinitely.
- Related source: ``protea/infrastructure/queue/consumer.py``
  (``_DLQ_NAME``, ``_handle_cuda_oom``, ``_handle_general_failure``).
