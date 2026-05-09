"""Frozen re-ranker dataset registry.

``POST /datasets`` enqueues an ``export_research_dataset`` job that runs
KNN + feature generation, publishes train/eval/manifest artefacts to the
configured artifact store (local FS or MinIO) and inserts a ``Dataset``
row once the upload completes. The row is the durable handle the lab
uses to pull the exact dump by name or id.

``GET /datasets`` and ``GET /datasets/{id_or_name}`` expose the registry
for the lab's ``pull_dataset.py`` and for UI consumers.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field, field_validator
from sqlalchemy.orm import Session, sessionmaker

from protea.api.deps import get_amqp_url, get_session_factory
from protea.core.utils import utcnow
from protea.infrastructure.orm.models.embedding.dataset import Dataset
from protea.infrastructure.orm.models.job import Job, JobEvent
from protea.infrastructure.queue.publisher import publish_job
from protea.infrastructure.session import session_scope

router = APIRouter(prefix="/datasets", tags=["datasets"])


class CreateDatasetRequest(BaseModel):
    """Body for ``POST /datasets``.

    Mirrors the ``export_research_dataset`` operation payload. The caller
    does not pick a queue: the dataset export always runs on the
    ``protea.training`` worker (serialized, GPU/RAM-intensive).
    """

    model_config = {"extra": "forbid"}

    output_name: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description=(
            "Unique slug for the dataset; becomes the artifact-store "
            "prefix and the row's ``name`` column. Must be non-empty."
        ),
    )
    embedding_config_id: str = Field(
        ...,
        min_length=1,
        description=(
            "UUID of the ``EmbeddingConfig`` that drives source "
            "embeddings (vector dim, backend, model name)."
        ),
    )
    ontology_snapshot_id: str = Field(
        ...,
        min_length=1,
        description=(
            "UUID of the ``OntologySnapshot`` anchoring the GO term "
            "graph (defines aspect / ancestor lookups)."
        ),
    )
    train_versions: list[int] = Field(
        ...,
        min_length=2,
        description=(
            "GOA snapshot versions used for training. At least two are "
            "required so the dump can build a temporal split."
        ),
    )
    test_versions: list[int] = Field(
        ...,
        min_length=1,
        description=(
            "GOA snapshot versions used for evaluation; typically a "
            "single recent release."
        ),
    )
    annotation_source: str = Field(
        default="goa",
        description=(
            "``goa`` (default, UniProt-GOA) or ``quickgo``. Controls "
            "which annotation table the dump pulls from."
        ),
    )
    k: int = Field(
        default=5,
        gt=0,
        description="KNN neighbour count per query protein.",
    )
    search_backend: str = Field(
        default="faiss",
        description=(
            "``faiss`` (CPU-IVF, default) or ``numpy`` (exact brute "
            "force). Both return identical neighbours within "
            "numerical noise."
        ),
    )
    compute_alignments: bool = Field(
        default=False,
        description=(
            "When true, materialise pairwise alignment features "
            "(parasail). Slow but feeds the reranker."
        ),
    )
    compute_taxonomy: bool = Field(
        default=False,
        description=(
            "When true, attach taxonomy-distance features per "
            "neighbour pair."
        ),
    )
    expand_votes_to_ancestors: bool = Field(
        default=False,
        description=(
            "Legacy knob; ancestor-expanded ground-truth is now "
            "reconciled inside the eval-set itself."
        ),
    )
    use_embedding_pca: bool = Field(
        default=False,
        description=(
            "When true, project embeddings through the cached PCA "
            "before KNN (smaller index, slight Fmax loss)."
        ),
    )

    @field_validator("output_name", "embedding_config_id", "ontology_snapshot_id", mode="before")
    @classmethod
    def _strip(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()


def _dataset_to_dict(d: Dataset) -> dict[str, Any]:
    return {
        "id": str(d.id),
        "name": d.name,
        "operation": d.operation,
        "job_id": str(d.job_id) if d.job_id else None,
        "storage_backend": d.storage_backend,
        "key_prefix": d.key_prefix,
        "train_uri": d.train_uri,
        "eval_uri": d.eval_uri,
        "manifest_uri": d.manifest_uri,
        "schema_sha": d.schema_sha,
        "manifest_sha": d.manifest_sha,
        "n_train_rows": d.n_train_rows,
        "n_eval_rows": d.n_eval_rows,
        "k": d.k,
        "annotation_source": d.annotation_source,
        "embedding_config_id": str(d.embedding_config_id) if d.embedding_config_id else None,
        "ontology_snapshot_id": str(d.ontology_snapshot_id) if d.ontology_snapshot_id else None,
        "train_snapshot_pairs": d.train_snapshot_pairs,
        "eval_snapshot_pair": d.eval_snapshot_pair,
        "producer_version": d.producer_version,
        "producer_git_sha": d.producer_git_sha,
        "meta": d.meta,
        "created_at": d.created_at.isoformat() if d.created_at else None,
    }


@router.post("", summary="Enqueue a dataset export job")
def create_dataset(
    body: CreateDatasetRequest,
    factory: sessionmaker[Session] = Depends(get_session_factory),
    amqp_url: str = Depends(get_amqp_url),
) -> dict[str, Any]:
    """Enqueue an ``export_research_dataset`` job.

    Returns ``{job_id}``. Poll ``GET /jobs/{job_id}`` for status; once the
    job is ``SUCCEEDED``, ``GET /datasets/{name}`` returns the registered
    row with its artifact URIs.
    """
    # Up-front conflict check — saves a whole KNN run if the name is taken.
    with session_scope(factory) as session:
        existing = (
            session.query(Dataset.id).filter(Dataset.name == body.output_name).first()
        )
        if existing is not None:
            raise HTTPException(
                status_code=409,
                detail=f"Dataset {body.output_name!r} already exists",
            )

        queue_name = "protea.training"
        payload = body.model_dump()
        job = Job(
            operation="export_research_dataset",
            queue_name=queue_name,
            payload=payload,
            meta={"created_at_iso": utcnow().isoformat()},
        )
        session.add(job)
        session.flush()
        job_id = job.id
        session.add(
            JobEvent(
                job_id=job_id,
                event="job.created",
                fields={
                    "operation": "export_research_dataset",
                    "queue": queue_name,
                    "output_name": body.output_name,
                },
            )
        )

    publish_job(amqp_url, queue_name, job_id)
    return {"job_id": str(job_id), "queue": queue_name, "status": "queued"}


@router.get("", summary="List registered datasets")
def list_datasets(
    name_like: str | None = Query(default=None, description="Substring filter on name"),
    embedding_config_id: UUID | None = Query(
        default=None,
        description=(
            "Filter datasets to those derived from one specific "
            "``embedding_config`` UUID."
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
            "Cursor for pagination (T4.2): return rows with "
            "``created_at < after`` only. Use the ``created_at`` of "
            "the last row from the previous page to walk forward."
        ),
    ),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    """Return registered frozen datasets newest-first.

    The lab's ``pull_dataset.py`` polls this endpoint to discover dump
    artefacts produced by ``export_research_dataset``. Filters narrow by
    name substring or by source ``embedding_config_id``. Pagination is
    cursor-based (``after``) plus a hard ``limit`` ceiling.
    """
    with session_scope(factory) as session:
        q = session.query(Dataset)
        if name_like:
            q = q.filter(Dataset.name.ilike(f"%{name_like}%"))
        if embedding_config_id is not None:
            q = q.filter(Dataset.embedding_config_id == embedding_config_id)
        if after is not None:
            q = q.filter(Dataset.created_at < after)
        rows = q.order_by(Dataset.created_at.desc()).limit(limit).all()
        return [_dataset_to_dict(r) for r in rows]


@router.get("/{id_or_name}", summary="Get a dataset by id or name")
def get_dataset(
    id_or_name: str,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Resolve a dataset by UUID or by the ``name`` slug.

    Tries the UUID path first; on ``ValueError`` (non-UUID input), falls
    back to the ``name`` column. Returns ``404`` if neither resolves.
    The lab uses the name path so dump callers can refer to ``bench-v1-K5``
    without juggling UUIDs.
    """
    with session_scope(factory) as session:
        row: Dataset | None = None
        try:
            row = session.get(Dataset, UUID(id_or_name))
        except ValueError:
            pass
        if row is None:
            row = session.query(Dataset).filter(Dataset.name == id_or_name).first()
        if row is None:
            raise HTTPException(status_code=404, detail=f"Dataset {id_or_name!r} not found")
        return _dataset_to_dict(row)
