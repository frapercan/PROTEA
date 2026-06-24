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

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session, sessionmaker

from protea.api.deps import get_session_factory
from protea.core.utils import utcnow
from protea.infrastructure.orm.models.job import Job, JobStatus
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
            "Count of RUNNING GPU jobs whose lease has expired "
            "(zombie rows; ignored for the busy signal)."
        ),
    )
    gpu_present: bool = Field(
        default=True,
        description=(
            "Whether a CUDA GPU is available to the embedding workers. When "
            "false, embedding novel sequences falls back to CPU and is "
            "noticeably slower; the UI surfaces this so users understand why."
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


def _classify(active: list[Job]) -> GpuAvailability:
    """Bucket active GPU jobs into fresh/queued/stale and pick a representative."""
    now = utcnow()
    queued = 0
    running_fresh = 0
    running_stale = 0
    representative: Job | None = None
    for j in active:
        if j.status == JobStatus.QUEUED:
            queued += 1
            if representative is None:
                representative = j
        elif j.leased_until is not None and j.leased_until > now:
            running_fresh += 1
            # A live RUNNING job is the most informative representative;
            # prefer it over a queued one.
            if representative is None or representative.status == JobStatus.QUEUED:
                representative = j
        else:
            running_stale += 1

    busy = running_fresh > 0 or queued > 0
    rep = representative if busy else None
    return GpuAvailability(
        busy=busy,
        running_fresh=running_fresh,
        queued=queued,
        running_stale=running_stale,
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
    so the form is not blocked by stale state.
    """
    with session_scope(factory) as session:
        active = (
            session.query(Job)
            .filter(
                Job.operation.in_(_GPU_OPERATIONS),
                Job.status.in_((JobStatus.QUEUED, JobStatus.RUNNING)),
            )
            .all()
        )
        return _classify(active)
