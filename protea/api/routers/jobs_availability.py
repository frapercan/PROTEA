"""GPU pipeline availability signal (FIX-ANNOTATE-BANNER-ACCURACY).

The annotation form's "GPU busy" banner must reflect GENUINELY active GPU
work only. A DB Job row in QUEUED/RUNNING is not enough: a stale/zombie
RUNNING job (whose worker died and whose lease has expired) leaves a row
behind with no live consumer and no RabbitMQ message, which would
otherwise falsely block the form forever.

This router exposes one read-only endpoint, ``GET /jobs/gpu-availability``,
that gates ``busy`` on real liveness (a fresh ``leased_until`` in the
future) plus genuinely queued work. It is mounted before the main jobs
router so the static ``/jobs/gpu-availability`` path resolves ahead of the
``/jobs/{job_id}`` catch-all.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy import or_
from sqlalchemy.orm import Session, sessionmaker

from protea.api.deps import get_session_factory
from protea.core.contracts.parent_progress import in_flight
from protea.core.utils import utcnow
from protea.infrastructure.orm.models.job import Job, JobEvent, JobStatus
from protea.infrastructure.session import session_scope

# Operations that occupy the shared GPU pipeline. Only these gate the
# public availability signal. The coordinators (``compute_embeddings`` /
# ``predict_go_terms`` / ``export_research_dataset``) carry DB Job rows
# whose lease a live worker renews every heartbeat; the GPU batch
# consumers run as OperationConsumers with no Job row, so coordinator
# liveness is the truthful proxy for "real GPU work".
_GPU_OPERATIONS: frozenset[str] = frozenset(
    {
        "compute_embeddings",
        "compute_embeddings_batch",
        "predict_go_terms",
        "predict_go_terms_batch",
        "export_research_dataset",
        # 2026-11-03: the residue passes use the same card and were missing, so the
        # banner said "free" through a whole corpus pass.
        "encode_residue_vocabulary",
        "encode_residue_vocabulary_batch",
        "encode_residue_sparse",
        "encode_residue_sparse_batch",
        # A method's container holds the node's card for hours (ADR-D53); a RUNNING run
        # keeps a live lease, so it counts as fresh like any leased job.
        "run_lab_method",
    }
)

router = APIRouter(prefix="/jobs", tags=["jobs"])

# GPU presence does not change during a process lifetime, so probe torch once
# and memoize. Imported lazily to keep torch off the API import path until the
# first availability call.
_GPU_PRESENT: bool | None = None


def _gpu_present() -> bool:
    """Return whether a CUDA GPU is visible to this host's torch build."""
    global _GPU_PRESENT
    if _GPU_PRESENT is None:
        try:
            import torch

            _GPU_PRESENT = bool(torch.cuda.is_available())
        except Exception:
            _GPU_PRESENT = False
    return _GPU_PRESENT


class GpuAvailability(BaseModel):
    """Truthful busy/free signal for the shared GPU pipeline.

    ``busy`` is true only when there is a freshly-leased RUNNING
    coordinator (``leased_until`` in the future) or genuinely QUEUED work
    for a GPU operation. Stale RUNNING rows are reported in
    ``running_stale`` for observability but never set ``busy``.
    """

    busy: bool = Field(
        ...,
        description=(
            "True when real GPU work is in flight (fresh-leased RUNNING "
            "coordinator or queued GPU work). Stale jobs never set this."
        ),
    )
    running_fresh: int = Field(
        ..., description="Count of RUNNING GPU jobs with a live (future) lease."
    )
    queued: int = Field(..., description="Count of QUEUED GPU jobs awaiting a worker.")
    running_stale: int = Field(
        ...,
        description=(
            "Count of RUNNING GPU jobs whose lease has expired and that are not "
            "waiting for dispatched batches (zombie rows; ignored for the busy signal)."
        ),
    )
    running_dispatched: int = Field(
        default=0,
        description=(
            "Count of coordinators waiting for batches they dispatched: RUNNING with no "
            "live lease after a job.dispatched event (a waiting coordinator clears its "
            "lease on purpose), or failed by the reaper while their batches still arrive "
            "(an event within the reaper's liveness window). "
            "They set busy."
        ),
    )
    gpu_present: bool = Field(
        default=True,
        description=(
            "Whether the API host's torch sees a CUDA GPU. It says nothing about the GPU "
            "node's card, which runs the embedding batches and is not visible from the "
            "API host."
        ),
    )
    active_operation: str | None = Field(
        default=None,
        description="Operation name of the representative live GPU job, if any.",
    )
    progress_current: int | None = Field(
        default=None, description="Progress counter of the representative live job."
    )
    progress_total: int | None = Field(
        default=None, description="Progress total of the representative live job."
    )


def _classify(active: list[Job], dispatched: set[Any]) -> GpuAvailability:
    """Bucket active GPU jobs into fresh, waiting, queued and stale; pick a representative.

    A coordinator that waits for its batches clears its lease on purpose (``deferred``) and
    emits ``job.dispatched``; it is in flight until its last batch is stored, so it counts
    as busy even with no lease, as does one the reaper failed while its batches still
    arrive. A RUNNING row with an expired lease and no such event is a zombie.
    """
    now = utcnow()
    queued = running_fresh = running_dispatched = running_stale = 0
    rank = {"fresh": 3, "dispatched": 2, "queued": 1}
    representative: Job | None = None
    best = 0
    for j in active:
        if j.status == JobStatus.QUEUED:
            kind = "queued"
            queued += 1
        elif j.status == JobStatus.RUNNING and j.leased_until is not None and j.leased_until > now:
            kind = "fresh"
            running_fresh += 1
        elif j.status == JobStatus.FAILED or j.id in dispatched:
            kind = "dispatched"
            running_dispatched += 1
        else:
            running_stale += 1
            continue
        if rank[kind] > best:
            representative, best = j, rank[kind]

    busy = running_fresh > 0 or queued > 0 or running_dispatched > 0
    rep = representative if busy else None
    return GpuAvailability(
        busy=busy,
        running_fresh=running_fresh,
        queued=queued,
        running_stale=running_stale,
        running_dispatched=running_dispatched,
        gpu_present=_gpu_present(),
        active_operation=rep.operation if rep else None,
        progress_current=rep.progress_current if rep else None,
        progress_total=rep.progress_total if rep else None,
    )


@router.get(
    "/gpu-availability",
    summary="GPU pipeline availability (truthful busy/free signal)",
    response_model=GpuAvailability,
)
def gpu_availability(
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> GpuAvailability:
    """Report whether the shared GPU pipeline is genuinely busy.

    Powers the annotation form's "GPU busy" banner. Unlike a raw
    ``GET /jobs?status=running`` scan, this gates on real liveness: a
    RUNNING job only counts when its ``leased_until`` is still in the
    future (a live worker renews the lease every heartbeat). Expired-lease
    RUNNING rows are zombies (dead worker) and are excluded from ``busy``
    so the form is not blocked by stale state. A coordinator waiting for the
    batches it dispatched has no lease on purpose and still counts: it holds
    the card until its last batch is stored.
    """
    with session_scope(factory) as session:
        active = (
            session.query(Job)
            .filter(
                Job.operation.in_(_GPU_OPERATIONS),
                or_(Job.status == JobStatus.QUEUED, in_flight(utcnow())),
            )
            .all()
        )
        ids = [j.id for j in active]
        dispatched = (
            {
                job_id
                for (job_id,) in session.query(JobEvent.job_id)
                .filter(JobEvent.job_id.in_(ids), JobEvent.event == "job.dispatched")
                .distinct()
            }
            if ids
            else set()
        )
        return _classify(active, dispatched)
