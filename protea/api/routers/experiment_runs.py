"""ExperimentRun narrative endpoints (T4.7-T4.9 of master plan v3.2).

Surfaces the ORM created in T3.8 so the F8b Experiments page (T8b.5)
and CLI tooling can manage research-run metadata. Schema mirrors the
JSON shape exposed by the jobs router for consistency.

Endpoints
---------

* ``POST   /experiment-runs``           — create (status=planned).
* ``GET    /experiment-runs``           — list, optional status filter.
* ``GET    /experiment-runs/{id}``      — fetch one.
* ``PATCH  /experiment-runs/{id}``      — update narrative + status +
                                          provenance overlay; transitions
                                          stamp ``started_at`` /
                                          ``finished_at`` automatically.
* ``DELETE /experiment-runs/{id}``      — remove (rare; mostly drafts).

Linkage to Job / EvaluationResult / RerankerModel rows is intentionally
out of scope here — the F-EXP campaign work (T-EXP.1-T-EXP.7) defines
the join shape once it lands.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field, field_validator
from sqlalchemy.orm import Session, sessionmaker

from protea.api.deps import get_session_factory
from protea.core.utils import utcnow
from protea.infrastructure.orm.models.experiment_run import (
    ExperimentRun,
    ExperimentRunStatus,
)
from protea.infrastructure.session import session_scope

router = APIRouter(prefix="/experiment-runs", tags=["experiment-runs"])


class CreateExperimentRunRequest(BaseModel):
    """Body for ``POST /experiment-runs``.

    Carries the narrative trio (``description`` / ``hypothesis`` /
    ``findings``) plus structured ``config`` + ``provenance`` overlays
    that the F-EXP campaign tooling reads back. New rows always start in
    ``planned`` status; transitions happen via ``PATCH``.
    """

    model_config = {"extra": "forbid"}

    name: str = Field(..., min_length=1, description="Unique slug for the run.")
    description: str | None = Field(
        default=None,
        description=(
            "Human-readable intent text shown on the experiment-run "
            "detail page (D11 narrative)."
        ),
    )
    hypothesis: str | None = Field(
        default=None,
        description=(
            "What the run is trying to verify or falsify; surfaces in "
            "the F-EXP campaign view."
        ),
    )
    config: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Structured run configuration (hyperparameters, feature "
            "flags, reference dataset slugs). Free-form JSON; consumers "
            "agree on the schema per campaign."
        ),
    )
    provenance: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Provenance overlay; the server merges in baseline fields "
            "from ``capture_provenance()`` (git sha, hostname, "
            "timestamp) so the caller only needs to add domain-specific "
            "context."
        ),
    )
    tags: list[str] = Field(
        default_factory=list,
        description=(
            "Lightweight grouping tokens (e.g. ``ablation``, "
            "``benchmark-v1``) used for filtering."
        ),
    )

    @field_validator("name", mode="before")
    @classmethod
    def strip_name(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("name must be a non-empty string")
        return v.strip()


class UpdateExperimentRunRequest(BaseModel):
    """All fields optional; absent ones leave the column untouched."""

    model_config = {"extra": "forbid"}

    description: str | None = Field(
        default=None,
        description=(
            "Replace the run's narrative description text. ``None`` "
            "leaves the column untouched."
        ),
    )
    hypothesis: str | None = Field(
        default=None,
        description="Replace the hypothesis text. ``None`` leaves it untouched.",
    )
    findings: str | None = Field(
        default=None,
        description=(
            "Free-form narrative recording the run's outcome. Set when "
            "transitioning to ``done`` to capture the writeup."
        ),
    )
    status: ExperimentRunStatus | None = Field(
        default=None,
        description=(
            "Transition the run to a new status. The server stamps "
            "``started_at`` on the first move to ``running`` and "
            "``finished_at`` on the first move to ``done`` / "
            "``abandoned`` (idempotent on re-entry)."
        ),
    )
    config: dict[str, Any] | None = Field(
        default=None,
        description=(
            "Replace the structured config blob. ``None`` leaves it "
            "untouched; ``{}`` clears it."
        ),
    )
    provenance: dict[str, Any] | None = Field(
        default=None,
        description=(
            "Replace the provenance overlay. ``None`` leaves it "
            "untouched; ``{}`` clears it."
        ),
    )
    tags: list[str] | None = Field(
        default=None,
        description=(
            "Replace the tag list. ``None`` leaves it untouched; "
            "``[]`` clears it."
        ),
    )


def _serialise_experiment_run(run: ExperimentRun) -> dict[str, Any]:
    """Shape one ``ExperimentRun`` row for the API response."""
    return {
        "id": str(run.id),
        "name": run.name,
        "description": run.description,
        "hypothesis": run.hypothesis,
        "findings": run.findings,
        "status": run.status.value,
        "config": run.config,
        "provenance": run.provenance,
        "tags": list(run.tags or []),
        "created_at": run.created_at.isoformat() if run.created_at else None,
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
    }


def _stamp_status_transition(run: ExperimentRun, new_status: ExperimentRunStatus) -> None:
    """Bump ``started_at`` / ``finished_at`` per status transition rules.

    Idempotent: re-entering a state never resets its timestamp; transitions
    only fill ``None`` slots, never overwrite. Aligned with how Job rows
    handle their ``started_at`` / ``finished_at``.
    """
    now = utcnow()
    if new_status == ExperimentRunStatus.RUNNING and run.started_at is None:
        run.started_at = now
    if new_status in (
        ExperimentRunStatus.DONE,
        ExperimentRunStatus.ABANDONED,
    ) and run.finished_at is None:
        run.finished_at = now
    run.status = new_status


@router.post("", status_code=201, summary="Create an experiment run")
def create_experiment_run(
    body: CreateExperimentRunRequest,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Create a new ``ExperimentRun`` row in ``planned`` status."""
    with session_scope(factory) as session:
        existing = (
            session.query(ExperimentRun.id)
            .filter(ExperimentRun.name == body.name)
            .first()
        )
        if existing is not None:
            raise HTTPException(
                status_code=409,
                detail=f"ExperimentRun with name {body.name!r} already exists",
            )
        run = ExperimentRun(
            name=body.name,
            description=body.description,
            hypothesis=body.hypothesis,
            config=dict(body.config),
            provenance=dict(body.provenance),
            tags=list(body.tags),
        )
        session.add(run)
        session.flush()
        return _serialise_experiment_run(run)


@router.get("", summary="List experiment runs")
def list_experiment_runs(
    status: ExperimentRunStatus | None = Query(
        default=None,
        description=(
            "Filter by ``ExperimentRunStatus`` (``planned``, "
            "``running``, ``done``, ``abandoned``)."
        ),
    ),
    limit: int = Query(
        default=50,
        ge=1,
        le=500,
        description="Max rows per page; capped at 500.",
    ),
    after: datetime | None = Query(
        default=None,
        description=(
            "Cursor for pagination (T4.2); return rows with "
            "``created_at < after`` only. Use the ``created_at`` of "
            "the last row from the previous page to walk forward."
        ),
    ),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    """Return runs newest-first, optionally filtered by status.

    Pagination is cursor-based: pass ``after=<created_at>`` to get the
    next page. Microsecond resolution on ``created_at`` keeps tie
    collisions astronomically rare.
    """
    with session_scope(factory) as session:
        q = session.query(ExperimentRun)
        if status is not None:
            q = q.filter(ExperimentRun.status == status)
        if after is not None:
            q = q.filter(ExperimentRun.created_at < after)
        rows = q.order_by(ExperimentRun.created_at.desc()).limit(limit).all()
        return [_serialise_experiment_run(r) for r in rows]


@router.get("/{run_id}", summary="Get one experiment run")
def get_experiment_run(
    run_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Fetch a single run by id."""
    with session_scope(factory) as session:
        run = session.get(ExperimentRun, run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="ExperimentRun not found")
        return _serialise_experiment_run(run)


@router.patch("/{run_id}", summary="Update an experiment run")
def update_experiment_run(
    run_id: UUID,
    body: UpdateExperimentRunRequest,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Patch narrative fields, status, config, provenance, or tags.

    Only fields explicitly present in the request body are updated;
    omitted fields leave the column untouched. Status transitions stamp
    ``started_at`` / ``finished_at`` per the rules in
    :func:`_stamp_status_transition`.
    """
    with session_scope(factory) as session:
        run = session.get(ExperimentRun, run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="ExperimentRun not found")
        data = body.model_dump(exclude_unset=True)
        if "status" in data and data["status"] is not None:
            _stamp_status_transition(run, ExperimentRunStatus(data.pop("status")))
        for key, value in data.items():
            setattr(run, key, value)
        session.flush()
        return _serialise_experiment_run(run)


@router.delete("/{run_id}", status_code=204, summary="Delete an experiment run")
def delete_experiment_run(
    run_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> None:
    """Permanently delete a run. Mostly used for cleaning up draft rows."""
    with session_scope(factory) as session:
        run = session.get(ExperimentRun, run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="ExperimentRun not found")
        session.delete(run)
