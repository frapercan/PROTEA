"""Frozen re-ranker dataset registry.

``POST /datasets`` enqueues an ``export_research_dataset`` job that runs
KNN + feature generation, publishes train/eval/manifest artefacts to the
configured artifact store (local FS or MinIO) and inserts a ``Dataset``
row once the upload completes. The row is the durable handle the lab
uses to pull the exact dump by name or id.

``POST /datasets/import-by-reference`` is the lightweight twin: it
registers a ``Dataset`` row pointing at already-staged artefacts (lab
side dump, salvage replay, or any out of band export) without running
the KNN pipeline. The lab uses this for benches it produced locally
before ``export_research_dataset`` existed, or for re-imports after a
DB wipe.

``GET /datasets`` and ``GET /datasets/{id_or_name}`` expose the registry
for the lab's ``pull_dataset.py`` and for UI consumers.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel, Field, field_validator
from sqlalchemy.orm import Session, sessionmaker

from protea.api.deps import get_amqp_url, get_session_factory, get_settings
from protea.api.rate_limit import datasets_limit, limiter
from protea.api.roles import ROLE_OPERATOR, require_role
from protea.api.routers.datasets_detail import (
    _ARTIFACT_FILENAME,
    compute_aspect_stats,
    presigned_redirect,
    stream_local_file,
)
from protea.core.schema_sha_v2 import maybe_v2
from protea.core.utils import utcnow
from protea.infrastructure.orm.models.embedding.dataset import Dataset
from protea.infrastructure.orm.models.job import Job, JobEvent
from protea.infrastructure.queue.publisher import publish_job
from protea.infrastructure.session import session_scope
from protea.infrastructure.settings import Settings

router = APIRouter(prefix="/datasets", tags=["datasets"])


class CreateDatasetRequest(BaseModel):
    """Body for ``POST /datasets``.

    Mirrors the ``export_research_dataset`` operation payload. The caller
    does not pick a queue: the dataset export always runs on the
    ``protea.training`` worker (serialized, GPU/RAM-intensive).
    """

    model_config = {
        "extra": "forbid",
        "json_schema_extra": {
            "example": {
                "output_name": "bench-v1-K5",
                "embedding_config_id": "00000000-0000-0000-0000-000000000001",
                "ontology_snapshot_id": "00000000-0000-0000-0000-000000000002",
                "train_versions": [160, 165, 170, 175],
                "test_versions": [230],
                "annotation_source": "goa",
                "k": 5,
                "search_backend": "faiss",
                "compute_alignments": True,
                "compute_taxonomy": True,
                "expand_votes_to_ancestors": False,
                "use_embedding_pca": False,
            }
        },
    }

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
            "GOA snapshot versions used for evaluation; typically a single recent release."
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
        description=("When true, attach taxonomy-distance features per neighbour pair."),
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


@router.post(
    "",
    summary="Enqueue a dataset export job",
    dependencies=[Depends(require_role(ROLE_OPERATOR))],
)
@limiter.limit(datasets_limit)
def create_dataset(
    request: Request,
    response: Response,
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
        existing = session.query(Dataset.id).filter(Dataset.name == body.output_name).first()
        if existing is not None:
            raise HTTPException(
                status_code=409,
                detail=f"Dataset {body.output_name!r} already exists",
            )

        queue_name = "protea.training"
        payload = body.model_dump()
        # When the minijob pipeline is enabled, route through the
        # coordinator so the cell is partitioned into per-pair
        # KNN/feature minijobs across protea.training.knn-batch and
        # protea.training.features. Otherwise stay on the monolithic
        # operation for back-compat.
        operation_name = (
            "export_coordinator"
            if os.environ.get("PROTEA_EXPORT_MINIJOBS", "0") == "1"
            else "export_research_dataset"
        )
        job = Job(
            operation=operation_name,
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
                    "operation": operation_name,
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
            "Filter datasets to those derived from one specific ``embedding_config`` UUID."
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


class ImportDatasetByReferenceRequest(BaseModel):
    """Body for ``POST /datasets/import-by-reference``.

    The lab calls this when the train / eval parquets and the manifest
    already live in the artifact store (filesystem dump, MinIO upload
    from a prior environment, salvage replay, etc.). PROTEA registers
    a ``Dataset`` row pointing at those URIs verbatim. No job is
    enqueued; the artefacts are not re-read or copied.

    The lab passes the fields it already has from its own ``manifest.json``
    so the registry row is content-identical to what an in-PROTEA
    ``export_research_dataset`` run would have produced.
    """

    model_config = {
        "extra": "forbid",
        "json_schema_extra": {
            "example": {
                "name": "bench-v1-K5-v226-lineage-prostt5",
                "storage_backend": "local",
                "key_prefix": "datasets/bench-v1-K5-v226-lineage-prostt5/",
                "train_uri": (
                    "file:///home/frapercan/Thesis2/repositories/"
                    "protea-reranker-lab/datasets/"
                    "bench-v1-K5-v226-lineage-prostt5/train.parquet"
                ),
                "eval_uri": (
                    "file:///home/frapercan/Thesis2/repositories/"
                    "protea-reranker-lab/datasets/"
                    "bench-v1-K5-v226-lineage-prostt5/eval.parquet"
                ),
                "manifest_uri": (
                    "file:///home/frapercan/Thesis2/repositories/"
                    "protea-reranker-lab/datasets/"
                    "bench-v1-K5-v226-lineage-prostt5/manifest.json"
                ),
                "schema_sha": "6d97a624b8a7",
                "manifest_sha": "f" * 64,
                "k": 5,
                "annotation_source": "goa",
                "n_train_rows": 24351779,
                "n_eval_rows": 1066859,
                "embedding_config_id": "c0ae5b69-d6dc-41cf-a711-1739d3d2e170",
                "ontology_snapshot_id": "35c3ad67-3002-47db-8f71-eeed69d22ad6",
                "train_snapshot_pairs": ["v220-v226"],
                "eval_snapshot_pair": "v226-v230",
                "producer_version": "0.8.0",
                "producer_git_sha": "059db1907c5208a965238e8e6682184fb83537be",
                "external_source": "protea-reranker-lab@059db19",
                "force": False,
            }
        },
    }

    name: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description=(
            "Unique slug for the Dataset row. Matches the ``name`` "
            "value the lab uses in its own manifest, so callers can "
            "look the row up by name afterwards."
        ),
    )
    storage_backend: str = Field(
        default="local",
        description=(
            "Backend identifier matching the URIs (``local`` for "
            "``file://`` paths, ``minio`` for ``s3://`` keys). "
            "Recorded verbatim on the row; PROTEA does not validate "
            "the URI scheme against this value."
        ),
    )
    key_prefix: str = Field(
        ...,
        min_length=1,
        max_length=512,
        description=(
            "Artifact store prefix that contains the parquets + "
            "manifest (e.g. ``datasets/<name>/``). Used by the lab to "
            "rebuild URIs after a backend swap."
        ),
    )
    train_uri: str | None = Field(
        default=None,
        max_length=1024,
        description="URI of the training parquet (nullable for eval only dumps).",
    )
    eval_uri: str | None = Field(
        default=None,
        max_length=1024,
        description="URI of the evaluation parquet (nullable for train only dumps).",
    )
    manifest_uri: str = Field(
        ...,
        min_length=1,
        max_length=1024,
        description="URI of the manifest.json (required).",
    )
    schema_sha: str = Field(
        ...,
        min_length=1,
        max_length=16,
        description=(
            "Feature set fingerprint copied verbatim from the lab's "
            "manifest. Boosters trained against this dataset must "
            "match on inference."
        ),
    )
    manifest_sha: str | None = Field(
        default=None,
        max_length=64,
        description=(
            "sha256 hex of the serialized manifest.json bytes. "
            "Nullable for legacy dumps that did not record it; the "
            "lab computes it client side for new dumps."
        ),
    )
    k: int = Field(..., gt=0, description="KNN neighbour count baked into the dump.")
    annotation_source: str = Field(
        default="goa",
        description="``goa`` or ``quickgo``; matches the lab's manifest.",
    )
    n_train_rows: int = Field(
        default=0,
        ge=0,
        description="Row count of the training split as captured in the producer's manifest.",
    )
    n_eval_rows: int = Field(
        default=0,
        ge=0,
        description="Row count of the evaluation split as captured in the producer's manifest.",
    )
    embedding_config_id: str | None = Field(
        default=None,
        description=(
            "UUID of the ``EmbeddingConfig`` the dump used. NULL when "
            "the FK cannot be resolved in this PROTEA instance (DB "
            "reset, different deployment); the dataset row still "
            "registers but loses the back reference."
        ),
    )
    ontology_snapshot_id: str | None = Field(
        default=None,
        description="UUID of the ``OntologySnapshot``; nullable like ``embedding_config_id``.",
    )
    train_snapshot_pairs: list[str] = Field(
        default_factory=list,
        description=(
            "List of ``v{old}-v{new}`` strings, one per training "
            "shard. Empty list is allowed for eval only dumps."
        ),
    )
    eval_snapshot_pair: str | None = Field(
        default=None,
        max_length=64,
        description="``v{old}-v{new}`` window of the eval parquet, if present.",
    )
    producer_version: str | None = Field(
        default=None,
        max_length=64,
        description="Version label of the producer that emitted the dump (e.g. lab semver tag).",
    )
    producer_git_sha: str | None = Field(
        default=None,
        max_length=40,
        description="Git SHA of the producer (lab or external pipeline) that emitted the dump.",
    )
    external_source: str | None = Field(
        default=None,
        description=(
            "Free form provenance tag (e.g. "
            "``protea-reranker-lab@<git-sha>``) recorded in the "
            "``meta`` JSONB column. Mirrors the reranker-models "
            "import-by-reference contract."
        ),
    )
    meta: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Free form extension hook. Merged with "
            "``{'imported_by_reference': True, 'external_source': …}`` "
            "so a downstream audit can tell registry rows produced "
            "via this endpoint apart from in-PROTEA exports."
        ),
    )
    force: bool = Field(
        default=False,
        description=(
            "When ``True``, overwrite any existing Dataset row with "
            "the same ``name``. Default ``False`` rejects duplicates "
            "with 409."
        ),
    )

    @field_validator(
        "name",
        "storage_backend",
        "key_prefix",
        "manifest_uri",
        "schema_sha",
        "annotation_source",
        mode="before",
    )
    @classmethod
    def _strip(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()

    @field_validator("embedding_config_id", "ontology_snapshot_id", mode="before")
    @classmethod
    def _strip_optional_uuid(cls, v: str | None) -> str | None:
        if v is None:
            return None
        if not isinstance(v, str) or not v.strip():
            return None
        return v.strip()


def _resolve_optional_uuid(
    session: Session,
    raw_id: str | None,
    pk_column: Any,
) -> uuid.UUID | None:
    """Resolve an optional UUID FK against ``pk_column``.

    Mirrors the helper in :mod:`protea.api.routers.reranker_models`:
    callers may pass a UUID for an entity that no longer exists in
    this PROTEA instance (after a DB wipe, or from a different
    deployment). NULL the column rather than 500'ing so the dataset
    row can still register; the lab will reattach the FK later via
    a maintenance script if needed.
    """
    if not raw_id:
        return None
    try:
        candidate = uuid.UUID(raw_id)
    except (ValueError, TypeError) as exc:
        raise HTTPException(
            status_code=422,
            detail=f"invalid UUID for FK column: {raw_id!r}",
        ) from exc
    found = session.query(pk_column).filter(pk_column == candidate).first()
    return candidate if found is not None else None


def _evict_existing_dataset_or_409(session: Session, name: str, force: bool) -> None:
    """Delete an existing ``Dataset`` row when ``force=True``, else 409."""
    existing = session.query(Dataset).filter(Dataset.name == name).first()
    if existing is None:
        return
    if not force:
        raise HTTPException(
            status_code=409,
            detail=f"Dataset name={name!r} already exists (id={existing.id})",
        )
    session.delete(existing)
    session.flush()


def _build_imported_dataset(
    session: Session,
    body: ImportDatasetByReferenceRequest,
) -> Dataset:
    """Resolve FKs + merge meta + construct the Dataset row.

    Split out of the route handler to keep the handler under the
    smell-check method LOC ceiling. Pure construction: the caller
    flushes and grabs the id.
    """
    from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
    from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig

    embedding_config_uuid = _resolve_optional_uuid(
        session, body.embedding_config_id, EmbeddingConfig.id
    )
    ontology_snapshot_uuid = _resolve_optional_uuid(
        session, body.ontology_snapshot_id, OntologySnapshot.id
    )

    merged_meta: dict[str, Any] = dict(body.meta or {})
    merged_meta.setdefault("imported_by_reference", True)
    if body.external_source:
        merged_meta.setdefault("external_source", body.external_source)

    return Dataset(
        name=body.name,
        operation="import_by_reference",
        job_id=None,
        storage_backend=body.storage_backend,
        key_prefix=body.key_prefix,
        train_uri=body.train_uri,
        eval_uri=body.eval_uri,
        manifest_uri=body.manifest_uri,
        schema_sha=body.schema_sha,
        schema_sha_v2=maybe_v2(body.schema_sha),
        manifest_sha=body.manifest_sha,
        n_train_rows=body.n_train_rows,
        n_eval_rows=body.n_eval_rows,
        k=body.k,
        annotation_source=body.annotation_source,
        embedding_config_id=embedding_config_uuid,
        ontology_snapshot_id=ontology_snapshot_uuid,
        train_snapshot_pairs=list(body.train_snapshot_pairs),
        eval_snapshot_pair=body.eval_snapshot_pair,
        producer_version=body.producer_version,
        producer_git_sha=body.producer_git_sha,
        meta=merged_meta,
    )


@router.post(
    "/import-by-reference",
    status_code=201,
    summary="Register an already-staged dataset",
    dependencies=[Depends(require_role(ROLE_OPERATOR))],
)
def import_dataset_by_reference(
    body: ImportDatasetByReferenceRequest,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Register a ``Dataset`` row whose artefacts are already staged.

    The lab uploads (or simply leaves on disk) the parquets and
    ``manifest.json`` itself and posts the URIs + manifest fields
    here. PROTEA persists a Dataset row pointing at those URIs without
    re-reading the artefacts. Useful for benches the lab produced
    before ``export_research_dataset`` existed, for replays after a
    DB wipe, and for the ``bench-v1-K5-v226-lineage-prostt5`` LB.1 bootstrap.

    The optional ``embedding_config_id`` and ``ontology_snapshot_id``
    are resolved against the local DB and NULL'd when missing, so the
    insert never fails on a stale FK. ``schema_sha_v2`` is dual-written
    when the ``PROTEA_SCHEMA_SHA_V2_WRITE_ENABLED`` flag is on (T1.6).

    Returns ``201`` with the row's id + name. ``409`` on a duplicate
    ``name`` unless ``force=true`` was passed.
    """
    with session_scope(factory) as session:
        _evict_existing_dataset_or_409(session, body.name, body.force)
        dataset = _build_imported_dataset(session, body)
        session.add(dataset)
        session.flush()
        dataset_id = dataset.id

    return {
        "id": str(dataset_id),
        "name": body.name,
        "storage_backend": body.storage_backend,
        "manifest_uri": body.manifest_uri,
        "schema_sha": body.schema_sha,
        "manifest_sha": body.manifest_sha,
    }


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


def _resolve_dataset_by_id(dataset_id: str, session: Session) -> Dataset:
    """Resolve a Dataset by UUID; raise 404 when not found."""
    try:
        row = session.get(Dataset, UUID(dataset_id))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="dataset_id must be a UUID") from exc
    if row is None:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset_id!r} not found")
    return row


@router.get(
    "/{dataset_id}/stats",
    summary="Aspect-level statistics for a dataset",
)
def get_dataset_stats(
    dataset_id: str,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Return per-aspect protein / GO-term / annotation counts for a dataset.

    Reads from ``dataset.meta['aspect_stats']`` when present (populated
    by backfill or a previous call). On a cache miss the counts are
    computed live and written back. See :mod:`datasets_detail` for the
    implementation.
    """
    with session_scope(factory) as session:
        row = _resolve_dataset_by_id(dataset_id, session)
        return compute_aspect_stats(row, session)


@router.get(
    "/{dataset_id}/download",
    summary="Download a dataset artifact (presigned URL or streamed)",
    dependencies=[Depends(require_role(ROLE_OPERATOR))],
)
def download_dataset_artifact(
    dataset_id: str,
    artifact: str = Query(..., description="One of: train, eval, manifest"),
    factory: sessionmaker[Session] = Depends(get_session_factory),
    settings: Settings = Depends(get_settings),
) -> Response:
    """Mint a presigned download URL (MinIO) or stream the file (local).

    For ``storage_backend=minio`` the endpoint 302-redirects to a
    15-minute presigned GET URL. For ``storage_backend=local`` the
    artifact bytes are streamed inline. See :mod:`datasets_detail`.
    """
    if artifact not in _ARTIFACT_FILENAME:
        raise HTTPException(
            status_code=422,
            detail=f"artifact must be one of: {', '.join(sorted(_ARTIFACT_FILENAME))}",
        )
    filename = _ARTIFACT_FILENAME[artifact]

    with session_scope(factory) as session:
        row = _resolve_dataset_by_id(dataset_id, session)

        if artifact == "train":
            uri = row.train_uri
        elif artifact == "eval":
            uri = row.eval_uri
        else:
            uri = row.manifest_uri

        if not uri:
            raise HTTPException(
                status_code=404,
                detail=f"Dataset {dataset_id!r} has no {artifact!r} artifact registered",
            )

        backend = (row.storage_backend or "local").lower()

    if backend == "minio":
        return presigned_redirect(uri, filename, settings)
    return stream_local_file(uri, filename)
