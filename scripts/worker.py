# scripts/worker.py
"""
Continuous queue worker. Consumes jobs from RabbitMQ and executes them.

Usage:
    poetry run python scripts/worker.py
    poetry run python scripts/worker.py --queue protea.jobs
"""
from __future__ import annotations

import argparse
from pathlib import Path

from protea.core.contracts.registry import OperationRegistry
from protea.core.operations.fetch_uniprot_metadata import FetchUniProtMetadataOperation
from protea.core.operations.insert_proteins import InsertProteinsOperation
from protea.core.operations.ping import PingOperation
from protea.infrastructure.queue.consumer import QueueConsumer
from protea.infrastructure.settings import load_settings
from protea.infrastructure.session import build_session_factory
from protea.workers.base_worker import BaseWorker, WorkerConfig


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

    worker = BaseWorker(factory, registry, WorkerConfig(worker_name="queue-worker"))

    consumer = QueueConsumer(
        amqp_url=settings.amqp_url,
        queue_name=args.queue,
        worker=worker,
        requeue_on_failure=args.requeue_on_failure,
    )

    print(f"Worker started. Listening on queue: {args.queue}")
    print("Press Ctrl+C to stop.")
    consumer.run()


if __name__ == "__main__":
    main()
