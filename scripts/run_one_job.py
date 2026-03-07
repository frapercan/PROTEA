# scripts/run_one_job.py
from __future__ import annotations

import sys
from pathlib import Path
from uuid import UUID

from sqlalchemy import text

from protea.core.contracts.registry import OperationRegistry
from protea.core.operations.fetch_uniprot_metadata import FetchUniProtMetadataOperation
from protea.core.operations.insert_proteins import InsertProteinsOperation
from protea.core.operations.ping import PingOperation
from protea.infrastructure.session import build_session_factory
from protea.infrastructure.settings import load_settings
from protea.workers.base_worker import BaseWorker, WorkerConfig


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: poetry run python scripts/run_one_job.py <job_id_uuid>")
        return 2

    job_id = UUID(sys.argv[1])

    project_root = Path(__file__).resolve().parents[1]
    settings = load_settings(project_root)

    print("DB_URL:", settings.db_url)

    factory = build_session_factory(settings.db_url)

    # Pre-check: job row
    with factory() as s:
        row = s.execute(
            text("select id::text, status::text, operation from job where id = :id"),
            {"id": str(job_id)},
        ).fetchone()
        print("JOB_ROW:", row)

    registry = OperationRegistry()
    registry.register(PingOperation())
    registry.register(InsertProteinsOperation())
    registry.register(FetchUniProtMetadataOperation())

    worker = BaseWorker(factory, registry, WorkerConfig(worker_name="manual"))
    worker.handle_job(job_id)

    # Post-check
    with factory() as s:
        row2 = s.execute(
            text("select id::text, status::text, started_at, finished_at, error_code, error_message from job where id = :id"),
            {"id": str(job_id)},
        ).fetchone()
        print("JOB_AFTER:", row2)

        ev_count = s.execute(
            text("select count(*) from job_event where job_id = :id"),
            {"id": str(job_id)},
        ).scalar_one()
        print("JOB_EVENT_COUNT:", ev_count)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
