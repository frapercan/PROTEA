ADR-D53: A laboratory method runs as a platform job, from an image pinned by id
===============================================================================

:Status: Proposed
:Date: 2026-11-03
:Author: Francisco Miguel Pérez Canales
:Phase: platform consolidation (the lab's research enters the platform)

Context: the lab's best method reaches the platform as a file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The laboratory's best method (the reader over the world model) is fitted outside the
platform and ships as a container image plus a data bundle. Its predictions have entered
the platform as TSV files written elsewhere and loaded by ``ingest_method_predictions``.
That operation checks the file (aspects, unknown terms, depth), but it cannot say which
queries the method saw or which image produced it, and the import policy agreed on
2026-10-28 admits laboratory artifacts (models, embeddings), not results computed outside
the platform.

The image follows one contract: ``--cut C --query_file F --output O.tsv``, with ``O.json``
beside the TSV recording the method's name, the lab commit, the models and their hashes,
and the hashes of the query file it read and of the TSV it wrote. The node with the card
already holds the image and the bundle.

Decision
~~~~~~~~

A new operation, ``run_lab_method``, runs the image on the node and enqueues the
ingestion.

1. **It is a job on a new queue, ``protea.methods``, consumed by the node with the card.**
   Not an operation message: ``OperationConsumer`` executes on the AMQP thread and
   acknowledges at the end, so a run of hours would lose its connection and be delivered
   again: the broker closes it after two missed heartbeats of 600 s, so an operation message
   has 1,200 s. ``QueueConsumer`` pumps the connection every 5 s and renews the job's lease
   while the operation works, and the operation emits an event every ten minutes while the
   container runs and while its output is hashed and uploaded. Past the reaper's hard limit
   a job stays alive while it has emitted within 1,800 s, so a 12 h run keeps a margin of
   three.
2. **The queries come from the store by key and sha256.** The FASTA is the one
   ``export_evaluation_targets`` publishes for an evaluation set, so the method and the
   evaluator score one population by construction.
3. **The image is pinned by its content id.** The payload names a tag, for the record, and
   the 64 hex id. The tag must resolve on the node to that id, and the container starts
   from the id, so a tag moved between the dispatch and the run changes nothing.
4. **The container is confined.** No network, the bundle mounted read only where the image
   expects it, a memory ceiling (40 GB by default) and a wall-clock limit (12 h by
   default). Its name is derived from the job, so a retried job removes what an
   interrupted attempt left running before it starts a second run on the card. The
   payload names the bundle, so the node bounds it: ``bundle_path`` must lie inside the
   node's ``operation.method_bundle_root`` once its symlinks are resolved, the resolved
   path is the one mounted, and a node that sets no root starts no run. Both sides of the
   mount must be absolute, normalized paths other than ``/``, with no colon, which
   ``podman`` would read as the end of the path. Without the bound a payload could mount
   any folder of the node into a container whose output the platform stores.
5. **The card is checked, not locked.** The job table is asked first, through the guard
   the embedding and residue passes share (``_card_guard``): while one of them has batches
   in flight, or another method run is RUNNING, the job raises ``RetryLaterError`` and
   comes back ten minutes later, and a pass dispatched during a method run waits for it
   the same way. Then the card's free memory, which also sees a process the job table
   does not know: when it is less than the payload asks for, the job comes back the same
   way. The operation does not stop or pause other consumers.
6. **The outputs are stored by content.** The TSV and the JSON go to the store under keys
   that carry their hash (``method-runs/<name>/cut<C>/predictions-<hash16>.tsv``), and so
   does the container's log, on success and on failure: when a run dies after hours it is
   the only account of why.
7. **The method's record is required and checked.** The JSON must state the method's name,
   the cut, the hash of the query file and the hash of the TSV, and each must equal what
   the platform gave or measured. A run that leaves no record, or a record that leaves one
   of the four out, is refused before anything is stored for the ingestion: the record is
   the evidence of what ran. (In the ingestion the record stays what it was, a block of
   provenance, not a gate.)
8. **The ingestion is a child job.** ``ingest_method_predictions`` is enqueued on
   ``protea.methods.write`` with ``parent_job_id`` set. Its ``method`` block is the
   method's record plus ``platform_run`` (job, node, image, image id, the bundle path it
   mounted, query file and its hash, seconds), and every gate of the ingestion still
   applies.

Before the card is spent the arm is checked as the evaluator will check it: the
configuration's backend is ``ingested-method``, it declares the cut the payload asks the
image for, and the window the predictions go into starts at or after that cut.

What this does not decide
~~~~~~~~~~~~~~~~~~~~~~~~~

* **Which method runs first** was left to the author, because the cut-211 recipe needs its
  readers retrained (they were deleted on 2026-10-30) and the cut-230 recipe reads releases
  from 228 on. He chose cut 230 on 2026-11-03 ("3. OK" to that recommendation): the
  deployed model, on the platform's 230 to 235 window, once the corpus pass has ended and
  this operation is deployed. The permission covers that run.
* **Whether the platform rebuilds the method's inputs itself.** The bundle is a laboratory
  artifact. This operation records which bundle and which image ran; it does not reproduce
  them.
* **A card lock shared by every consumer of the node.** The guard makes a method run and
  the passes wait for each other when they start; it is a check at the start, not a lock.
  ``apply_learned_encoder``, which uses a card inline, does not ask it, and a method run
  the reaper failed holds no claim in the job table (its container, if its worker died
  without stopping it, shows only in the card's free memory).

The scratch folder where a run writes its queries, its output and its log is the node's
``operation.method_scratch_dir`` tuning. A method's TSV can be several GB, and in a tmpfs it
would sit in memory beside the database.

Consequences
~~~~~~~~~~~~

* The node needs a worker on ``protea.methods``. The worker declares the queue, with the
  dead letter exchange, when it starts, so it starts before the first dispatch, and the
  node sets ``operation.method_bundle_root`` (and ``operation.method_scratch_dir``, on disk)
  before it.
* A method's result becomes a platform job with its inputs pinned: the query file and its
  hash, the image and its id, and the bundle path are in the job's payload, the bundle
  path the node resolved and mounted is in ``platform_run``, and the outputs are in the
  store by hash.
* The ingestion's provenance block gains ``platform_run``. It is a record, not a gate, as
  the rest of the block is.

Alternatives considered
~~~~~~~~~~~~~~~~~~~~~~~

* **A coordinator plus an operation message on the card queue**
  (``protea.embeddings.batch``), as the embedding passes do. Rejected: the message would
  run for hours on the AMQP thread.
* **Importing the file the laboratory produces by hand.** This is what the operation
  replaces.
* **Running the image by its tag.** A tag can move between the dispatch and the run.

Related: ADR-D44 (the platform's own container, going out to LAFA) and ADR-D51 (the
per-residue codes, the next input such a method reads).
