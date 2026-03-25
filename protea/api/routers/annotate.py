"""One-click protein annotation endpoint.

Accepts a FASTA file (or raw text), auto-selects the best available
embedding config, annotation set, and ontology snapshot, creates a
QuerySet, and kicks off ``compute_embeddings``.  Returns all the IDs the
frontend needs to chain ``predict_go_terms`` once embeddings finish.
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Form, HTTPException, UploadFile
from sqlalchemy import func
from sqlalchemy.orm import Session, sessionmaker

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
    """Pick the config with the most computed embeddings (prefer ESM-2)."""
    rows = (
        session.query(
            EmbeddingConfig,
            func.count(SequenceEmbedding.id).label("cnt"),
        )
        .outerjoin(SequenceEmbedding, SequenceEmbedding.embedding_config_id == EmbeddingConfig.id)
        .group_by(EmbeddingConfig.id)
        .order_by(func.count(SequenceEmbedding.id).desc())
        .all()
    )
    if not rows:
        return None
    # Prefer a config that already has embeddings
    for config, cnt in rows:
        if cnt > 0:
            return config
    return rows[0][0]


def _newest_annotation_set(session: Session) -> AnnotationSet | None:
    return (
        session.query(AnnotationSet)
        .order_by(AnnotationSet.created_at.desc())
        .first()
    )


def _newest_ontology_snapshot(session: Session) -> OntologySnapshot | None:
    return (
        session.query(OntologySnapshot)
        .order_by(OntologySnapshot.loaded_at.desc())
        .first()
    )


@router.post("", summary="Annotate proteins from FASTA")
async def annotate(
    file: UploadFile | None = None,
    fasta_text: str | None = Form(None),
    name: str = Form("Quick annotation"),
    factory: sessionmaker[Session] = Depends(get_session_factory),
    amqp_url: str = Depends(get_amqp_url),
) -> dict[str, Any]:
    """One-click annotation: upload FASTA, auto-select best method, run pipeline.

    Accepts either an uploaded FASTA ``file`` **or** raw ``fasta_text``.
    Creates a QuerySet, picks the best embedding config (or creates the
    default ESM-2 650M config), and queues a ``compute_embeddings`` job.

    Returns the IDs the frontend needs to monitor progress and chain
    ``predict_go_terms`` once embeddings are ready.
    """
    # ── Parse FASTA ──────────────────────────────────────────────────
    _MAX_FASTA_BYTES = 50 * 1024 * 1024  # 50 MB
    if file is not None:
        raw = await file.read()
        if len(raw) > _MAX_FASTA_BYTES:
            raise HTTPException(status_code=413, detail="FASTA file exceeds 50 MB limit")
        try:
            content = raw.decode("utf-8")
        except UnicodeDecodeError:
            raise HTTPException(status_code=422, detail="FASTA file must be UTF-8 encoded") from None
    elif fasta_text:
        if len(fasta_text.encode("utf-8")) > _MAX_FASTA_BYTES:
            raise HTTPException(status_code=413, detail="FASTA text exceeds 50 MB limit")
        content = fasta_text
    else:
        raise HTTPException(status_code=422, detail="Provide a FASTA file or fasta_text")

    records = _parse_fasta(content)
    if not records:
        raise HTTPException(status_code=422, detail="No valid sequences found in the FASTA input")

    seen: set[str] = set()
    for acc, _ in records:
        if acc in seen:
            raise HTTPException(status_code=422, detail=f"Duplicate accession: '{acc}'")
        seen.add(acc)

    # ── Create QuerySet + upsert sequences ───────────────────────────
    with session_scope(factory) as session:
        # Upsert sequences
        hash_to_seq_id: dict[str, int] = {}
        hashes = [Sequence.compute_hash(seq) for _, seq in records]
        existing = (
            session.query(Sequence.sequence_hash, Sequence.id)
            .filter(Sequence.sequence_hash.in_(hashes))
            .all()
        )
        for h, sid in existing:
            hash_to_seq_id[h] = sid
        for (_, seq), h in zip(records, hashes, strict=False):
            if h not in hash_to_seq_id:
                new_seq = Sequence(sequence=seq, sequence_hash=h)
                session.add(new_seq)
                session.flush()
                hash_to_seq_id[h] = new_seq.id

        qs = QuerySet(name=name, description="Created via quick annotation")
        session.add(qs)
        session.flush()
        entries = [
            QuerySetEntry(
                query_set_id=qs.id,
                sequence_id=hash_to_seq_id[h],
                accession=acc,
            )
            for (acc, _), h in zip(records, hashes, strict=False)
        ]
        session.add_all(entries)
        session.flush()
        query_set_id = qs.id

        # ── Auto-select best resources ───────────────────────────────
        config = _best_embedding_config(session)
        if config is None:
            config = EmbeddingConfig(**_DEFAULT_CONFIG)
            session.add(config)
            session.flush()
        config_id = config.id

        ann = _newest_annotation_set(session)
        if ann is None:
            raise HTTPException(
                status_code=409,
                detail="No annotation sets available. Load GO annotations first.",
            )
        annotation_set_id = ann.id

        snap = _newest_ontology_snapshot(session)
        if snap is None:
            raise HTTPException(
                status_code=409,
                detail="No ontology snapshots available. Load a GO ontology first.",
            )
        ontology_snapshot_id = snap.id

        # ── Check for trained reranker ────────────────────────────────
        best_reranker = (
            session.query(RerankerModel)
            .order_by(RerankerModel.created_at.desc())
            .first()
        )
        reranker_id = best_reranker.id if best_reranker else None

        # ── Create compute_embeddings job ────────────────────────────
        embed_payload = {
            "embedding_config_id": str(config_id),
            "query_set_id": str(query_set_id),
            "device": "cuda",
            "skip_existing": True,
            "batch_size": 8,
            "sequences_per_job": 64,
        }
        job = Job(
            operation="compute_embeddings",
            queue_name="protea.embeddings",
            payload=embed_payload,
        )
        session.add(job)
        session.flush()
        embed_job_id = job.id
        session.add(
            JobEvent(
                job_id=embed_job_id,
                event="job.created",
                fields={"operation": "compute_embeddings", "source": "annotate"},
            )
        )

    publish_job(amqp_url, "protea.embeddings", embed_job_id)

    # Build the predict payload the frontend will POST when embeddings finish.
    predict_payload: dict[str, Any] = {
        "embedding_config_id": str(config_id),
        "annotation_set_id": str(annotation_set_id),
        "ontology_snapshot_id": str(ontology_snapshot_id),
        "query_set_id": str(query_set_id),
        "search_backend": "numpy",
        "aspect_separated_knn": True,
        "compute_alignments": True,
        "compute_taxonomy": True,
        "compute_reranker_features": True,
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
