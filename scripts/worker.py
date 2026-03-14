# scripts/worker.py
"""
Continuous queue worker. Consumes jobs from RabbitMQ and executes them.

Usage:
    poetry run python scripts/worker.py
    poetry run python scripts/worker.py --queue protea.jobs
"""
from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path

from protea.core.contracts.registry import OperationRegistry
from protea.core.operations.compute_embeddings import (
    ComputeEmbeddingsBatchOperation,
    ComputeEmbeddingsOperation,
    StoreEmbeddingsOperation,
)
from protea.core.operations.fetch_uniprot_metadata import FetchUniProtMetadataOperation
from protea.core.operations.generate_evaluation_set import GenerateEvaluationSetOperation
from protea.core.operations.insert_proteins import InsertProteinsOperation
from protea.core.operations.load_goa_annotations import LoadGOAAnnotationsOperation
from protea.core.operations.load_ontology_snapshot import LoadOntologySnapshotOperation
from protea.core.operations.load_quickgo_annotations import LoadQuickGOAnnotationsOperation
from protea.core.operations.ping import PingOperation
from protea.core.operations.predict_go_terms import (
    PredictGOTermsBatchOperation,
    PredictGOTermsOperation,
    StorePredictionsOperation,
)
from protea.core.operations.run_cafa_evaluation import RunCafaEvaluationOperation
from protea.infrastructure.queue.consumer import OperationConsumer, QueueConsumer
from protea.infrastructure.session import build_session_factory
from protea.infrastructure.settings import load_settings
from protea.workers.base_worker import BaseWorker, WorkerConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
# Suppress pika's verbose connection lifecycle messages
logging.getLogger("pika").setLevel(logging.WARNING)


def main() -> None:
    parser = argparse.ArgumentParser(description="PROTEA queue worker")
    parser.add_argument("--queue", default="protea.jobs", help="Queue name to consume")
    parser.add_argument("--requeue-on-failure", action="store_true")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    settings = load_settings(project_root)

    factory = build_session_factory(settings.db_url)

    registry = OperationRegistry()
    registry.register(PingOperation())
    registry.register(InsertProteinsOperation())
    registry.register(FetchUniProtMetadataOperation())
    registry.register(LoadOntologySnapshotOperation())
    registry.register(LoadQuickGOAnnotationsOperation())
    registry.register(LoadGOAAnnotationsOperation())
    registry.register(GenerateEvaluationSetOperation())
    registry.register(RunCafaEvaluationOperation())
    registry.register(ComputeEmbeddingsOperation())
    registry.register(ComputeEmbeddingsBatchOperation())
    registry.register(StoreEmbeddingsOperation())
    registry.register(PredictGOTermsOperation())
    registry.register(PredictGOTermsBatchOperation())
    registry.register(StorePredictionsOperation())

    # Queues that carry ephemeral operation messages (no DB Job row per message)
    # use OperationConsumer.  All other queues use the standard QueueConsumer.
    _OPERATION_QUEUES = {
        "protea.embeddings.batch",
        "protea.embeddings.write",
        "protea.predictions.batch",
        "protea.predictions.write",
    }

    if args.queue in _OPERATION_QUEUES:
        consumer: QueueConsumer | OperationConsumer = OperationConsumer(
            amqp_url=settings.amqp_url,
            queue_name=args.queue,
            registry=registry,
            session_factory=factory,
            requeue_on_failure=args.requeue_on_failure,
        )
    else:
        worker = BaseWorker(factory, registry, WorkerConfig(worker_name="queue-worker"), amqp_url=settings.amqp_url)
        consumer = QueueConsumer(
            amqp_url=settings.amqp_url,
            queue_name=args.queue,
            worker=worker,
            requeue_on_failure=args.requeue_on_failure,
        )

    logging.info("Worker started. queue=%s", args.queue)
    while True:
        try:
            consumer.run()
        except KeyboardInterrupt:
            logging.info("Worker stopped.")
            break
        except Exception as exc:
            logging.error("Consumer crashed: %s — reconnecting in 5s", exc)
            time.sleep(5)


if __name__ == "__main__":
    main()
