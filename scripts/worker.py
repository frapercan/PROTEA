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

from protea.core.operation_catalog import build_operation_registry
from protea.infrastructure.queue.consumer import OperationConsumer, QueueConsumer
from protea.infrastructure.session import build_session_factory
from protea.infrastructure.settings import load_settings
from protea.workers.base_worker import BaseWorker, WorkerConfig
from protea.workers.stale_job_reaper import StaleJobReaper


def main() -> None:
    parser = argparse.ArgumentParser(description="PROTEA queue worker")
    parser.add_argument("--queue", default="protea.jobs", help="Queue name to consume")
    parser.add_argument("--requeue-on-failure", action="store_true")
    parser.add_argument(
        "--log-format",
        choices=["json", "text"],
        default="json",
        help="Log output format (default: json)",
    )
    args = parser.parse_args()

    from protea.infrastructure.logging import configure_logging

    configure_logging(json=(args.log_format == "json"))
    # Suppress pika's verbose connection lifecycle messages
    logging.getLogger("pika").setLevel(logging.WARNING)

    project_root = Path(__file__).resolve().parents[1]
    settings = load_settings(project_root)

    factory = build_session_factory(settings.db_url)

    registry = build_operation_registry()

    # Queues that carry ephemeral operation messages (no DB Job row per message)
    # use OperationConsumer.  All other queues use the standard QueueConsumer.
    _OPERATION_QUEUES = {
        "protea.embeddings.batch",
        "protea.embeddings.write",
        "protea.predictions.batch",
        "protea.predictions.write",
    }

    # Special mode: stale job reaper (no queue, just periodic DB check).
    if args.queue == "reaper":
        # 24h hard timeout + 30min stall window by default. Earlier value
        # (6h) killed predict_go_terms coords that waited in the batch FIFO
        # behind other coords; with only one predictions.batch worker the
        # last ones in a 23-job batch routinely sat past 6h even though
        # work was progressing upstream.
        # Both numbers configurable via WorkerTuning (PROTEA_TUNING__WORKER__
        # REAPER_MAIN_TIMEOUT_SECONDS and ..._STALL_SECONDS).
        from protea.config.tuning import get_tuning

        worker_settings = get_tuning().worker
        reaper = StaleJobReaper(
            factory,
            timeout_seconds=worker_settings.reaper_main_timeout_seconds,
            stall_seconds=worker_settings.reaper_stall_seconds,
        )
        logging.info(
            "Stale job reaper started. timeout=%ds stall=%ds interval=60s",
            worker_settings.reaper_main_timeout_seconds,
            worker_settings.reaper_stall_seconds,
        )
        reaper.run(interval_seconds=60)
        return

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

    # Pre-warm taxonomy DB for prediction workers that may need it.
    if args.queue in ("protea.predictions.batch", "protea.jobs", "protea.training"):
        try:
            from protea.core.feature_engineering import warmup_taxonomy_db

            warmup_taxonomy_db()
        except Exception as exc:
            logging.warning("Taxonomy DB warmup skipped: %s", exc)

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
