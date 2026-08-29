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


def contract_payload(payload: Any) -> Any:
    """The payload without the keys the transport added, ready to validate.

    ``base_worker`` hands every operation ``{**job.payload, "_job_id": ...}``
    so the operation can find its own job row. That key is delivery metadata,
    not part of any contract, and no model declares it: pydantic treats a
    leading underscore as a private attribute, so it CANNOT be declared even
    if we wanted to.

    That was invisible while the base payload ignored undeclared keys. It is
    not invisible now: the contract forbids them, which is the whole point of
    forbidding them, and a payload validated with ``_job_id`` still on it
    raises. Stripping it here keeps the guard pointed at what it is for, which
    is a key nobody meant to send, rather than at the one the worker sends on
    purpose.

    Only underscore-prefixed keys are removed. Anything else the caller did
    not declare still raises, which is the behaviour worth having: it is how
    a worker running older code than its dispatcher stops doing the wrong work
    quietly.
    """
    if not isinstance(payload, dict):
        return payload
    return {k: v for k, v in payload.items() if not str(k).startswith("_")}
