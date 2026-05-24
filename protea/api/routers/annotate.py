"""One-click protein annotation endpoint.

Accepts a FASTA file (or raw text), auto-selects the best available
embedding config, annotation set, and ontology snapshot, creates a
QuerySet, and kicks off ``compute_embeddings``.  Returns all the IDs the
frontend needs to chain ``predict_go_terms`` once embeddings finish.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Form, HTTPException, UploadFile
from pydantic import BaseModel, Field
from sqlalchemy import func
from sqlalchemy.orm import Session, sessionmaker

from protea.api.deps import get_amqp_url, get_session_factory
from protea.api.roles import ROLE_VIEWER, require_role
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
    path is latency-sensitive, so a 300M PLM beats a 3B one for the default."""
    rows = (
        session.query(
            EmbeddingConfig,
            func.count(SequenceEmbedding.id).label("cnt"),
        )
        .outerjoin(SequenceEmbedding, SequenceEmbedding.embedding_config_id == EmbeddingConfig.id)
        .group_by(EmbeddingConfig.id)
        .order_by(EmbeddingConfig.param_count.asc().nulls_last())
        .all()
    )
    if not rows:
        return None
    for config, cnt in rows:
        if cnt > 0:
            return config
    return rows[0][0]


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


@router.post("", summary="Annotate proteins from FASTA", dependencies=[Depends(require_role(ROLE_VIEWER))])
async def annotate(
    file: UploadFile | None = None,
    fasta_text: str | None = Form(None),
    name: str = Form("Quick annotation"),
    compute_reranker_features: bool = Form(
        True,
        description=(
            "Enable the full reranker feature bundle: lineage / anc2vec / "
            "anc2vec_query / emb_pca / annotation_meta. "
            "Disable to skip these families and reduce compute time."
        ),
    ),
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
    content = await _read_fasta_content(file, fasta_text)
    records = _parse_and_dedup_records(content)

    with session_scope(factory) as session:
        query_set_id = _upsert_query_set(session, name, records)
        config_id, annotation_set_id, ontology_snapshot_id, reranker_id = (
            _resolve_dispatch_resources(session)
        )
        embed_job_id = _enqueue_embed_job(session, config_id, query_set_id)

    publish_job(amqp_url, "protea.embeddings", embed_job_id)

    predict_payload: dict[str, Any] = {
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


def _resolve_dispatch_resources(session: Session) -> tuple[Any, Any, Any, Any]:
    """Pick the best embedding config (creating the default ESM-2 if none exists),
    the newest AnnotationSet + OntologySnapshot, and the latest RerankerModel.

    Raises HTTP 409 when no annotation set or ontology snapshot is loaded yet.
    Returns ``(config_id, annotation_set_id, ontology_snapshot_id, reranker_id)``;
    ``reranker_id`` is ``None`` when no RerankerModel rows exist."""
    config = _best_embedding_config(session)
    if config is None:
        config = EmbeddingConfig(**_DEFAULT_CONFIG)
        session.add(config)
        session.flush()
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
