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

The depth is not the measurement
--------------------------------

**A large DLQ is not evidence that work was lost, and the depth cannot tell you
whether it was.** Measured on 2026-11-08, when the queue held **18,816**
messages: of the first 500 peeked through ``GET /v1/admin/dlq/summary``, 496
were ``compute_embeddings_batch`` dead-lettered from ``protea.embeddings.batch``
in the ``1w`` age bucket, and 4 were ``job_dispatch`` from ``protea.jobs`` a
month old. Nothing in those numbers says whether a vector is missing.

What settles it is **completeness per configuration**, which costs one indexed
count each:

.. code-block:: sql

   SELECT left(c.id::text, 8), coalesce(c.display_name, c.model_name),
          count(e.sequence_id) AS rows
     FROM embedding_config c
     LEFT JOIN sequence_embedding e ON e.embedding_config_id = c.id
    GROUP BY c.id, c.display_name, c.model_name
   HAVING count(e.sequence_id) > 0
    ORDER BY count(e.sequence_id) DESC;

On that date it answered:

.. code-block:: text

   d4b4c057  learned-code:mean:sfjepa-world-holes-topk:cut227:e7ba37da  684.048
   34446081  learned-code:mean:sfjepa-world-holes-topk:cut211:e7ba37da  684.048
   e7ba37da  ankh_base                                                 684.048
   f3a7fe16  Ankh-base, full length, mean plus max                     683.887

684,048 is not a round number taken from somewhere: it is the whole sequence
population, 629,895 sequences carrying one accession plus 54,153 shared ones.
**Three of the four configurations are complete**, and the fourth is short by
exactly the 161 sequences over 8,259 residues that the eager Ankh route cannot
compute on the node's card, which has its own fix.

So those 18,816 messages are copies of batches that DID get in, most plausibly
from a relaunch with ``skip_existing`` after the three ``compute_embeddings``
runs of 2026-10-28 and 2026-10-29 that failed or were cancelled with their
counters near zero. They occupy a queue and nothing else.

Read the conclusion the right way round. The reason to check completeness is
that the opposite answer is the dangerous one: a database error dead-letters a
batch while its job stays RUNNING with no error, and the parent's counter can
advance on a redelivered write, so **a job can report success over work that
never landed**. The queue depth looks the same in both cases. Only the row count
against the population distinguishes them.

Two limits of the summary, so the next reader does not over-read it: the age is
a BUCKET, so ``1w`` does not separate 2026-11-01 from 2026-11-05, and the
endpoint peeks a sample rather than the whole queue. Neither changes the
conclusion, and neither is worth inspecting 18,816 messages to refine.

Purging is a separate decision and belongs to the researcher:
``POST /v1/admin/dlq/purge``, never as a cleanup step.

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
