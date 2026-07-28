from __future__ import annotations

import uuid
from collections.abc import Iterable
from collections.abc import Sequence as Seq
from datetime import UTC, datetime
from typing import Any


def utcnow() -> datetime:
    """Return the current UTC datetime (timezone-aware)."""
    return datetime.now(UTC)


def chunks(seq: Seq[Any], n: int) -> Iterable[Seq[Any]]:
    """Yield successive n-sized chunks from seq."""
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def job_id_from_payload(payload: Any) -> uuid.UUID | None:
    """Resolve the owning Job's UUID from a worker-enhanced payload.

    ``BaseWorker`` injects the claimed job's id as ``_job_id`` into every
    operation payload (``base_worker.handle_job``). Threading it onto the
    persisted result row turns an otherwise-orphan artifact (``job_id=None``)
    into one with full provenance back to its triggering job: the same
    discipline ``export_research_dataset`` already applies to its ``Dataset``
    row. Returns ``None`` when the payload carries no ``_job_id`` (e.g. a
    direct ``op.execute`` call in tests or a non-job-backed dispatch).
    """
    if not isinstance(payload, dict):
        return None
    raw = payload.get("_job_id")
    return uuid.UUID(str(raw)) if raw else None
