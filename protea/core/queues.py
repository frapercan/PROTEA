"""Every broker name PROTEA uses, written once (ADR-D60, decision 6).

A queue is named by the work it carries and what that work needs, never by a
machine (ADR-D60, decision 4). The operation specs in
:mod:`protea.core.operation_catalog` declare their queues from these constants,
and so does every code path that publishes to a queue by name.
``tests/test_queue_names_have_one_home.py`` fails when a queue name is written
as a string literal anywhere else in ``protea/`` or ``scripts/``.

The module imports nothing, so a node without the database stack can read it.
"""

from __future__ import annotations

#: The smoke test's own queue (``ping``).
PING = "protea.ping"
#: The general job queue: loaders, declarations and the jobs no other queue takes.
JOBS = "protea.jobs"
#: The embedding coordinators, which fan their batches out to :data:`EMBEDDINGS_BATCH`.
EMBEDDINGS = "protea.embeddings"
#: The card's batches: one message is one forward pass.
EMBEDDINGS_BATCH = "protea.embeddings.batch"
#: The embedding and residue-code writes, on the host that owns the database.
EMBEDDINGS_WRITE = "protea.embeddings.write"
#: The node's own upload of the residue-code shards it spooled (ADR-D51).
EMBEDDINGS_UPLOAD = "protea.embeddings.upload"
#: The prediction coordinator, which fans its batches out to :data:`PREDICTIONS_BATCH`.
PREDICTIONS = "protea.predictions"
#: Nearest-neighbour search and GO transfer, one batch per message.
PREDICTIONS_BATCH = "protea.predictions.batch"
#: The bulk prediction write.
PREDICTIONS_WRITE = "protea.predictions.write"
#: A laboratory method's container, on the node with the card (ADR-D53).
METHODS = "protea.methods"
#: The ingestion of a method's predictions, on the server (ADR-D53).
METHODS_WRITE = "protea.methods.write"
#: Evaluations, kept apart so a long one never blocks :data:`JOBS`.
EVALUATIONS = "protea.evaluations"
#: The dataset exports, one at a time.
TRAINING = "protea.training"
#: The export minijobs: nearest neighbours per batch.
TRAINING_KNN_BATCH = "protea.training.knn-batch"
#: The export minijobs: features per batch.
TRAINING_FEATURES = "protea.training.features"
#: The export minijobs: the assembly of every shard.
TRAINING_WRITE = "protea.training.write"

#: The queues an operation spec may name.
WORK_QUEUES: frozenset[str] = frozenset(
    {
        PING,
        JOBS,
        EMBEDDINGS,
        EMBEDDINGS_BATCH,
        EMBEDDINGS_WRITE,
        EMBEDDINGS_UPLOAD,
        PREDICTIONS,
        PREDICTIONS_BATCH,
        PREDICTIONS_WRITE,
        METHODS,
        METHODS_WRITE,
        EVALUATIONS,
        TRAINING,
        TRAINING_KNN_BATCH,
        TRAINING_FEATURES,
        TRAINING_WRITE,
    }
)

#: Where a rejected message goes: the exchange every work queue names as its
#: dead-letter exchange, and the one queue bound to it.
DEAD_LETTER_EXCHANGE = "protea.dlx"
DEAD_LETTER_QUEUE = "protea.dead-letter"
