"""One-click protein annotation endpoint.

Accepts a FASTA file (or raw text), auto-selects the best available
embedding config, annotation set, and ontology snapshot, creates a
QuerySet, and kicks off ``compute_embeddings``.  Returns all the IDs the
frontend needs to chain ``predict_go_terms`` once embeddings finish.
"""

from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, UploadFile
from pydantic import BaseModel, Field
from sqlalchemy import exists
from sqlalchemy.orm import Session, sessionmaker
from starlette.datastructures import UploadFile as StarletteUploadFile

from protea.api.auth.anon_quota import require_anon_quota
from protea.api.cache import cached
from protea.api.deps import get_amqp_url, get_session_factory
from protea.api.routers.query_sets import _parse_fasta
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
from protea.infrastructure.orm.models.job import Job, JobEvent
from protea.infrastructure.orm.models.query.query_set import QuerySet, QuerySetEntry
from protea.infrastructure.orm.models.sequence.sequence import Sequence
from protea.infrastructure.queue.publisher import publish_job
from protea.infrastructure.session import session_scope

router = APIRouter(prefix="/annotate", tags=["annotate"])

# Cache key for the resolved "smallest config that has embeddings" id. The
# answer only changes when a new PLM is embedded, so a short TTL keeps the
# quick-annotation path fast (no per-request 5.8M-row scan) without going
# stale in a way users notice.
_BEST_CONFIG_CACHE_KEY = "annotate:best_embedding_config_id"

# Default embedding recipe (ESM-2 650M, last layer, mean pooling).
_DEFAULT_CONFIG = {
    "model_name": "facebook/esm2_t33_650M_UR50D",
    "model_backend": "esm",
    "layer_indices": [0],
    "layer_agg": "mean",
    "pooling": "mean",
    "normalize_residues": False,
    "normalize": True,
    "max_length": 1022,
    "use_chunking": False,
    "chunk_size": 512,
    "chunk_overlap": 0,
}


def _best_embedding_config(session: Session) -> EmbeddingConfig | None:
    """Pick the smallest model that already has embeddings; the quick-annotation
    path is latency-sensitive, so a 300M PLM beats a 3B one for the default.

    Walks configs smallest-first and EXISTS-probes ``sequence_embedding``
    (indexed on ``embedding_config_id``) one config at a time, stopping at the
    first that has any embedding. This is O(configs) cheap index lookups
    instead of a GROUP BY count over all 5.8M embedding rows."""
    configs = (
        session.query(EmbeddingConfig)
        .order_by(EmbeddingConfig.param_count.asc().nulls_last())
        .all()
    )
    if not configs:
        return None
    for config in configs:
        has_embeddings = session.query(
            exists().where(SequenceEmbedding.embedding_config_id == config.id)
        ).scalar()
        if has_embeddings:
            return config
    return configs[0]


def _best_embedding_config_id_cached(
    factory: sessionmaker[Session], ttl: float
) -> uuid.UUID | None:
    """Return the cached id of the smallest config that has embeddings.

    Caches only the id (a uuid), never a session-bound ORM object, so the
    value is safe to reuse across requests. ``None`` means no config exists
    yet (caller creates the default). Returns ``None`` without caching so a
    later default-create is picked up promptly."""

    def _produce() -> uuid.UUID | None:
        with session_scope(factory) as session:
            config = _best_embedding_config(session)
            return config.id if config is not None else None

    config_id = cached(_BEST_CONFIG_CACHE_KEY, ttl, _produce, serve_stale_on_error=True)
    if config_id is None:
        invalidate_best_config_cache()
    return config_id


def invalidate_best_config_cache() -> None:
    """Drop the cached best-config id (call after embedding a new PLM)."""
    from protea.api.cache import invalidate

    invalidate(_BEST_CONFIG_CACHE_KEY)


def _newest_annotation_set(session: Session) -> AnnotationSet | None:
    return session.query(AnnotationSet).order_by(AnnotationSet.created_at.desc()).first()


def _newest_ontology_snapshot(session: Session) -> OntologySnapshot | None:
    return session.query(OntologySnapshot).order_by(OntologySnapshot.loaded_at.desc()).first()


class AnnotateFormOptions(BaseModel):
    """User-controllable feature flags for the quick-annotation endpoint.

    These fields map 1:1 to the ``predict_go_terms`` coordinator payload so
    the frontend can expose them directly without an intermediate translation.
    """

    compute_reranker_features: bool = Field(
        default=True,
        description=(
            "Compute the full reranker feature bundle: lineage, anc2vec, "
            "anc2vec_query, emb_pca, annotation_meta. "
            "Disable to skip these families and reduce compute time."
        ),
    )
    # NOTE: use_embedding_pca is ONLY supported in the export_research_dataset
    # (datasets.py) path and is NOT accepted by predict_go_terms. Do not add
    # it here; adding it would silently be ignored by the coordinator payload.


@router.post("", summary="Annotate proteins from FASTA", dependencies=[Depends(require_anon_quota)])
async def annotate(
    request: Request,
    factory: sessionmaker[Session] = Depends(get_session_factory),
    amqp_url: str = Depends(get_amqp_url),
) -> dict[str, Any]:
    """One-click annotation: upload FASTA, auto-select best method, run pipeline.

    Accepts either an uploaded FASTA ``file`` **or** raw ``fasta_text``.
    Creates a QuerySet, picks the best embedding config (or creates the
    default ESM-2 650M config), and queues a ``compute_embeddings`` job.

    Returns the IDs the frontend needs to monitor progress and chain
    ``predict_go_terms`` once embeddings are ready.

    ``compute_reranker_features`` controls whether the reranker feature
    families (lineage, anc2vec, anc2vec_query, emb_pca, annotation_meta) are
    included in the downstream ``predict_go_terms`` job. Default: ``True``.
    """
    file, fasta_text, name, compute_reranker_features = await _parse_annotate_request(request)

    content = await _read_fasta_content(file, fasta_text)
    records = _parse_and_dedup_records(content)

    from protea.config.tuning import get_tuning

    tuning = get_tuning()
    ttl = tuning.worker.api_cache_default_ttl_seconds
    cached_config_id = _best_embedding_config_id_cached(factory, ttl)

    with session_scope(factory) as session:
        query_set_id = _upsert_query_set(session, name, records)
        config_id, annotation_set_id, ontology_snapshot_id, reranker_id = (
            _resolve_dispatch_resources(session, cached_config_id)
        )
        embed_job_id = _enqueue_embed_job(session, config_id, query_set_id)

    publish_job(amqp_url, "protea.embeddings", embed_job_id)

    predict_payload = _predict_payload(
        config_id, annotation_set_id, ontology_snapshot_id, query_set_id, compute_reranker_features
    )
    return {
        "query_set_id": str(query_set_id),
        "embedding_config_id": str(config_id),
        "annotation_set_id": str(annotation_set_id),
        "ontology_snapshot_id": str(ontology_snapshot_id),
        "embedding_job_id": str(embed_job_id),
        "predict_payload": predict_payload,
        "reranker_id": str(reranker_id) if reranker_id else None,
        "sequence_count": len(records),
    }


async def _parse_annotate_request(
    request: Request,
) -> tuple[UploadFile | None, str | None, str, bool]:
    """Parse and validate the multipart request body for /annotate.

    Returns (file, fasta_text, name, compute_reranker_features).
    Enforces the configured multipart size limit before FastAPI auto-parsing
    would reject it with a generic 400, allowing _read_fasta_content to run
    and produce a more helpful 413 message if the FASTA itself is too large.
    """
    from protea.config.tuning import get_tuning

    tuning = get_tuning()
    max_part = tuning.api.max_fasta_bytes + (1 << 20)
    try:
        form = await request.form(max_part_size=max_part)
    except Exception as exc:
        raise HTTPException(
            status_code=413,
            detail=f"Upload exceeds the {tuning.api.max_fasta_bytes // (1024 * 1024)} MB limit",
        ) from exc

    # request.form() yields starlette's UploadFile; fastapi.UploadFile is a
    # subclass, so check the base class to recognise the file part.
    raw_file = form.get("file")
    file = raw_file if isinstance(raw_file, StarletteUploadFile) else None
    raw_text = form.get("fasta_text")
    fasta_text = raw_text if isinstance(raw_text, str) else None
    raw_name = form.get("name")
    name = raw_name if isinstance(raw_name, str) and raw_name else "Quick annotation"
    raw_crf = form.get("compute_reranker_features")
    compute_reranker_features = (
        True
        if raw_crf is None
        else str(raw_crf).strip().lower() not in ("false", "0", "no", "off")
    )
    return file, fasta_text, name, compute_reranker_features


def _predict_payload(
    config_id: uuid.UUID,
    annotation_set_id: uuid.UUID,
    ontology_snapshot_id: uuid.UUID,
    query_set_id: uuid.UUID,
    compute_reranker_features: bool,
) -> dict[str, Any]:
    """Build the predict_go_terms chaining payload returned by /annotate."""
    return {
        "embedding_config_id": str(config_id),
        "annotation_set_id": str(annotation_set_id),
        "ontology_snapshot_id": str(ontology_snapshot_id),
        "query_set_id": str(query_set_id),
        "search_backend": "numpy",
        "aspect_separated_knn": True,
        "compute_alignments": True,
        "compute_taxonomy": True,
        "compute_reranker_features": compute_reranker_features,
    }


async def _read_fasta_content(file: UploadFile | None, fasta_text: str | None) -> str:
    """Resolve the FASTA payload: prefer the uploaded file, fall back to ``fasta_text``.

    Enforces the configured byte cap (HTTP 413) and validates UTF-8
    (HTTP 422). Raises 422 when neither input is provided.
    """
    from protea.config.tuning import get_tuning

    max_bytes = get_tuning().api.max_fasta_bytes
    if file is not None:
        raw = await file.read()
        if len(raw) > max_bytes:
            raise HTTPException(
                status_code=413,
                detail=f"FASTA file exceeds {max_bytes // (1024 * 1024)} MB limit",
            )
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError:
            raise HTTPException(
                status_code=422, detail="FASTA file must be UTF-8 encoded"
            ) from None
    if fasta_text:
        if len(fasta_text.encode("utf-8")) > max_bytes:
            raise HTTPException(
                status_code=413,
                detail=f"FASTA text exceeds {max_bytes // (1024 * 1024)} MB limit",
            )
        return fasta_text
    raise HTTPException(status_code=422, detail="Provide a FASTA file or fasta_text")


def _parse_and_dedup_records(content: str) -> list[tuple[str, str, str]]:
    """Parse the FASTA payload and reject duplicate accessions (HTTP 422)."""
    records = _parse_fasta(content)
    if not records:
        raise HTTPException(status_code=422, detail="No valid sequences found in the FASTA input")
    seen: set[str] = set()
    for acc, _, _ in records:
        if acc in seen:
            raise HTTPException(status_code=422, detail=f"Duplicate accession: '{acc}'")
        seen.add(acc)
    return records


def _upsert_query_set(session: Session, name: str, records: list[tuple[str, str, str]]) -> Any:
    """Upsert ``Sequence`` rows for the FASTA records and create a ``QuerySet``
    with one ``QuerySetEntry`` per record. Returns the new ``QuerySet`` id."""
    hash_to_seq_id: dict[str, int] = {}
    hashes = [Sequence.compute_hash(seq) for _, seq, _ in records]
    existing = (
        session.query(Sequence.sequence_hash, Sequence.id)
        .filter(Sequence.sequence_hash.in_(hashes))
        .all()
    )
    for h, sid in existing:
        hash_to_seq_id[h] = sid
    for (_, seq, _), h in zip(records, hashes, strict=False):
        if h not in hash_to_seq_id:
            new_seq = Sequence(sequence=seq, sequence_hash=h)
            session.add(new_seq)
            session.flush()
            hash_to_seq_id[h] = new_seq.id

    qs = QuerySet(name=name, description="Created via quick annotation")
    session.add(qs)
    session.flush()
    session.add_all(
        [
            QuerySetEntry(query_set_id=qs.id, sequence_id=hash_to_seq_id[h], accession=acc)
            for (acc, _, _), h in zip(records, hashes, strict=False)
        ]
    )
    session.flush()
    return qs.id


def _resolve_dispatch_resources(
    session: Session, cached_config_id: uuid.UUID | None = None
) -> tuple[Any, Any, Any, Any]:
    """Pick the best embedding config (creating the default ESM-2 if none exists),
    the newest AnnotationSet + OntologySnapshot, and the latest RerankerModel.

    ``cached_config_id`` is the TTL-cached id of the smallest config that has
    embeddings; when supplied (and still present), it skips the per-request
    config scan. ``None`` means resolve fresh (and create the default if the
    DB is empty).

    Raises HTTP 409 when no annotation set or ontology snapshot is loaded yet.
    Returns ``(config_id, annotation_set_id, ontology_snapshot_id, reranker_id)``;
    ``reranker_id`` is ``None`` when no RerankerModel rows exist."""
    config = None
    if cached_config_id is not None:
        config = session.get(EmbeddingConfig, cached_config_id)
        if config is None:
            invalidate_best_config_cache()
    if config is None:
        config = _best_embedding_config(session)
    if config is None:
        config = EmbeddingConfig(**_DEFAULT_CONFIG)
        session.add(config)
        session.flush()
        invalidate_best_config_cache()
    ann = _newest_annotation_set(session)
    if ann is None:
        raise HTTPException(
            status_code=409,
            detail="No annotation sets available. Load GO annotations first.",
        )
    snap = _newest_ontology_snapshot(session)
    if snap is None:
        raise HTTPException(
            status_code=409,
            detail="No ontology snapshots available. Load a GO ontology first.",
        )
    best_reranker = session.query(RerankerModel).order_by(RerankerModel.created_at.desc()).first()
    reranker_id = best_reranker.id if best_reranker else None
    return config.id, ann.id, snap.id, reranker_id


def _enqueue_embed_job(session: Session, config_id: Any, query_set_id: Any) -> Any:
    """Insert a ``compute_embeddings`` Job row + its ``job.created`` JobEvent.

    Returns the new job id; the AMQP publish happens after the session commits."""
    job = Job(
        operation="compute_embeddings",
        queue_name="protea.embeddings",
        payload={
            "embedding_config_id": str(config_id),
            "query_set_id": str(query_set_id),
            "device": "cuda",
            "skip_existing": True,
            "batch_size": 8,
            "sequences_per_job": 64,
        },
    )
    session.add(job)
    session.flush()
    session.add(
        JobEvent(
            job_id=job.id,
            event="job.created",
            fields={"operation": "compute_embeddings", "source": "annotate"},
        )
    )
    return job.id
