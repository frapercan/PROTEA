from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session, sessionmaker
from starlette.requests import Request

from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.job import Job, JobEvent
from protea.infrastructure.queue.publisher import publish_job
from protea.infrastructure.session import session_scope

router = APIRouter(prefix="/embeddings", tags=["embeddings"])

_JOBS_QUEUE = "protea.jobs"

_VALID_BACKENDS = {"esm", "esm3c", "t5", "auto"}
_VALID_LAYER_AGG = {"mean", "last", "concat"}
_VALID_POOLING = {"mean", "max", "cls", "mean_max"}


def get_session_factory(request: Request) -> sessionmaker[Session]:
    factory = getattr(request.app.state, "session_factory", None)
    if factory is None:
        raise RuntimeError("app.state.session_factory is not set")
    return factory  # type: ignore[no-any-return]


def get_amqp_url(request: Request) -> str:
    url = getattr(request.app.state, "amqp_url", None)
    if url is None:
        raise RuntimeError("app.state.amqp_url is not set")
    return url  # type: ignore[no-any-return]


def _validate_embedding_config_body(body: dict[str, Any]) -> dict[str, Any]:
    errors: list[str] = []

    model_name = body.get("model_name")
    if not isinstance(model_name, str) or not model_name.strip():
        errors.append("model_name must be a non-empty string")

    model_backend = body.get("model_backend")
    if model_backend not in _VALID_BACKENDS:
        errors.append(f"model_backend must be one of {sorted(_VALID_BACKENDS)}")

    layer_indices = body.get("layer_indices")
    if (
        not isinstance(layer_indices, list)
        or len(layer_indices) == 0
        or not all(isinstance(i, int) for i in layer_indices)
    ):
        errors.append("layer_indices must be a non-empty list of ints")

    layer_agg = body.get("layer_agg")
    if layer_agg not in _VALID_LAYER_AGG:
        errors.append(f"layer_agg must be one of {sorted(_VALID_LAYER_AGG)}")

    pooling = body.get("pooling")
    if pooling not in _VALID_POOLING:
        errors.append(f"pooling must be one of {sorted(_VALID_POOLING)}")

    normalize_residues = body.get("normalize_residues", False)
    if not isinstance(normalize_residues, bool):
        errors.append("normalize_residues must be a boolean")

    normalize = body.get("normalize", True)
    if not isinstance(normalize, bool):
        errors.append("normalize must be a boolean")

    max_length = body.get("max_length", 1022)
    if not isinstance(max_length, int) or max_length <= 0:
        errors.append("max_length must be a positive integer")

    use_chunking = body.get("use_chunking", False)
    if not isinstance(use_chunking, bool):
        errors.append("use_chunking must be a boolean")

    chunk_size = body.get("chunk_size", 512)
    if not isinstance(chunk_size, int) or chunk_size <= 0:
        errors.append("chunk_size must be a positive integer")

    chunk_overlap = body.get("chunk_overlap", 0)
    if not isinstance(chunk_overlap, int) or chunk_overlap < 0:
        errors.append("chunk_overlap must be a non-negative integer")

    description = body.get("description", None)
    if description is not None and not isinstance(description, str):
        errors.append("description must be a string or null")

    # Cross-field: overlap must be strictly less than chunk_size
    if (
        isinstance(chunk_size, int) and isinstance(chunk_overlap, int)
        and chunk_overlap >= chunk_size
    ):
        errors.append(
            f"chunk_overlap ({chunk_overlap}) must be strictly less than chunk_size ({chunk_size})"
        )

    if errors:
        raise HTTPException(status_code=422, detail=errors)

    return {
        "model_name": model_name,
        "model_backend": model_backend,
        "layer_indices": layer_indices,
        "layer_agg": layer_agg,
        "pooling": pooling,
        "normalize_residues": normalize_residues,
        "normalize": normalize,
        "max_length": max_length,
        "use_chunking": use_chunking,
        "chunk_size": chunk_size,
        "chunk_overlap": chunk_overlap,
        "description": description,
    }


def _config_to_dict(c: EmbeddingConfig, embedding_count: int | None = None) -> dict[str, Any]:
    d: dict[str, Any] = {
        "id": str(c.id),
        "model_name": c.model_name,
        "model_backend": c.model_backend,
        "layer_indices": c.layer_indices,
        "layer_agg": c.layer_agg,
        "pooling": c.pooling,
        "normalize_residues": c.normalize_residues,
        "normalize": c.normalize,
        "max_length": c.max_length,
        "use_chunking": c.use_chunking,
        "chunk_size": c.chunk_size,
        "chunk_overlap": c.chunk_overlap,
        "description": c.description,
        "created_at": c.created_at.isoformat(),
    }
    if embedding_count is not None:
        d["embedding_count"] = embedding_count
    return d


# ── Embedding Configs ─────────────────────────────────────────────────────────

@router.get("/configs")
def list_embedding_configs(
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    with session_scope(factory) as session:
        rows = (
            session.query(EmbeddingConfig)
            .order_by(EmbeddingConfig.created_at.desc())
            .all()
        )
        counts = {
            config_id: cnt
            for config_id, cnt in session.query(
                SequenceEmbedding.embedding_config_id,
                func.count(SequenceEmbedding.id),
            ).group_by(SequenceEmbedding.embedding_config_id).all()
        }
        return [_config_to_dict(c, embedding_count=counts.get(c.id, 0)) for c in rows]


@router.post("/configs")
def create_embedding_config(
    body: dict[str, Any],
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    validated = _validate_embedding_config_body(body)

    with session_scope(factory) as session:
        config = EmbeddingConfig(**validated)
        session.add(config)
        session.flush()
        result = _config_to_dict(config)

    return result


@router.get("/configs/{config_id}")
def get_embedding_config(
    config_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    with session_scope(factory) as session:
        c = session.get(EmbeddingConfig, config_id)
        if c is None:
            raise HTTPException(status_code=404, detail="EmbeddingConfig not found")

        embedding_count = (
            session.query(func.count(SequenceEmbedding.id))
            .filter(SequenceEmbedding.embedding_config_id == config_id)
            .scalar()
        )

        return _config_to_dict(c, embedding_count=embedding_count)


@router.delete("/configs/{config_id}")
def delete_embedding_config(
    config_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    with session_scope(factory) as session:
        c = session.get(EmbeddingConfig, config_id)
        if c is None:
            raise HTTPException(status_code=404, detail="EmbeddingConfig not found")

        # 1) Delete GOPredictions for all PredictionSets linked to this config
        #    (PredictionSet → GOPrediction is CASCADE, but bulk-delete bypasses ORM)
        pred_set_ids = [
            row[0]
            for row in session.query(PredictionSet.id)
            .filter(PredictionSet.embedding_config_id == config_id)
            .all()
        ]
        deleted_predictions = 0
        if pred_set_ids:
            deleted_predictions = (
                session.query(GOPrediction)
                .filter(GOPrediction.prediction_set_id.in_(pred_set_ids))
                .delete(synchronize_session=False)
            )

        # 2) Delete PredictionSets
        deleted_prediction_sets = (
            session.query(PredictionSet)
            .filter(PredictionSet.embedding_config_id == config_id)
            .delete(synchronize_session=False)
        )

        # 3) Delete SequenceEmbeddings
        deleted_embeddings = (
            session.query(SequenceEmbedding)
            .filter(SequenceEmbedding.embedding_config_id == config_id)
            .delete(synchronize_session=False)
        )

        # 4) Delete the config itself
        session.delete(c)

    return {
        "deleted": str(config_id),
        "embeddings_deleted": deleted_embeddings,
        "prediction_sets_deleted": deleted_prediction_sets,
        "predictions_deleted": deleted_predictions,
    }


# ── Predict ───────────────────────────────────────────────────────────────────

@router.post("/predict")
def predict_go_terms(
    body: dict[str, Any],
    factory: sessionmaker[Session] = Depends(get_session_factory),
    amqp_url: str = Depends(get_amqp_url),
) -> dict[str, Any]:
    def _parse_uuid(key: str) -> UUID:
        raw = body.get(key)
        try:
            return UUID(str(raw))
        except (ValueError, AttributeError):
            raise HTTPException(status_code=422, detail=f"{key} must be a valid UUID")

    config_id = _parse_uuid("embedding_config_id")
    annotation_set_id = _parse_uuid("annotation_set_id")
    ontology_snapshot_id = _parse_uuid("ontology_snapshot_id")

    with session_scope(factory) as session:
        if session.get(EmbeddingConfig, config_id) is None:
            raise HTTPException(status_code=404, detail="EmbeddingConfig not found")
        if session.get(AnnotationSet, annotation_set_id) is None:
            raise HTTPException(status_code=404, detail="AnnotationSet not found")
        if session.get(OntologySnapshot, ontology_snapshot_id) is None:
            raise HTTPException(status_code=404, detail="OntologySnapshot not found")

        job = Job(operation="predict_go_terms", queue_name=_JOBS_QUEUE, payload=body)
        session.add(job)
        session.flush()
        job_id = job.id
        session.add(JobEvent(
            job_id=job_id,
            event="job.created",
            fields={"operation": "predict_go_terms", "queue": _JOBS_QUEUE},
        ))

    publish_job(amqp_url, _JOBS_QUEUE, job_id)
    return {"id": str(job_id), "status": "queued"}


# ── Prediction Sets ───────────────────────────────────────────────────────────

@router.get("/prediction-sets")
def list_prediction_sets(
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    with session_scope(factory) as session:
        rows = (
            session.query(PredictionSet)
            .order_by(PredictionSet.created_at.desc())
            .limit(100)
            .all()
        )
        result = []
        for ps in rows:
            prediction_count = (
                session.query(func.count(GOPrediction.id))
                .filter(GOPrediction.prediction_set_id == ps.id)
                .scalar()
            )
            result.append({
                "id": str(ps.id),
                "embedding_config_id": str(ps.embedding_config_id),
                "annotation_set_id": str(ps.annotation_set_id),
                "ontology_snapshot_id": str(ps.ontology_snapshot_id),
                "query_set_id": str(ps.query_set_id) if ps.query_set_id else None,
                "limit_per_entry": ps.limit_per_entry,
                "distance_threshold": ps.distance_threshold,
                "created_at": ps.created_at.isoformat(),
                "prediction_count": prediction_count,
            })
        return result


@router.get("/prediction-sets/{set_id}")
def get_prediction_set(
    set_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    with session_scope(factory) as session:
        ps = session.get(PredictionSet, set_id)
        if ps is None:
            raise HTTPException(status_code=404, detail="PredictionSet not found")

        prediction_count = (
            session.query(func.count(GOPrediction.id))
            .filter(GOPrediction.prediction_set_id == set_id)
            .scalar()
        )

        per_protein = (
            session.query(GOPrediction.protein_accession, func.count(GOPrediction.id))
            .filter(GOPrediction.prediction_set_id == set_id)
            .group_by(GOPrediction.protein_accession)
            .all()
        )

        return {
            "id": str(ps.id),
            "embedding_config_id": str(ps.embedding_config_id),
            "annotation_set_id": str(ps.annotation_set_id),
            "ontology_snapshot_id": str(ps.ontology_snapshot_id),
            "query_set_id": str(ps.query_set_id) if ps.query_set_id else None,
            "limit_per_entry": ps.limit_per_entry,
            "distance_threshold": ps.distance_threshold,
            "created_at": ps.created_at.isoformat(),
            "prediction_count": prediction_count,
            "per_protein_counts": {acc: cnt for acc, cnt in per_protein},
        }


@router.get("/prediction-sets/{set_id}/proteins")
def list_prediction_set_proteins(
    set_id: UUID,
    search: str | None = None,
    limit: int = 50,
    offset: int = 0,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    with session_scope(factory) as session:
        ps = session.get(PredictionSet, set_id)
        if ps is None:
            raise HTTPException(status_code=404, detail="PredictionSet not found")

        from protea.infrastructure.orm.models.protein.protein import Protein
        from protea.infrastructure.orm.models.annotation.protein_go_annotation import ProteinGOAnnotation

        q = (
            session.query(
                GOPrediction.protein_accession,
                func.count(GOPrediction.id).label("go_count"),
                func.min(GOPrediction.distance).label("min_distance"),
            )
            .filter(GOPrediction.prediction_set_id == set_id)
            .group_by(GOPrediction.protein_accession)
        )
        if search:
            q = q.filter(GOPrediction.protein_accession.ilike(f"%{search}%"))

        total = q.count()
        rows = q.order_by(GOPrediction.protein_accession).offset(offset).limit(limit).all()

        accessions = [r[0] for r in rows]
        protein_map = {
            p.accession: p
            for p in session.query(Protein).filter(Protein.accession.in_(accessions)).all()
        }

        ann_counts: dict[str, int] = {}
        match_counts: dict[str, int] = {}
        if accessions:
            ann_counts = {
                acc: cnt
                for acc, cnt in session.query(
                    ProteinGOAnnotation.protein_accession,
                    func.count(ProteinGOAnnotation.id),
                )
                .filter(
                    ProteinGOAnnotation.protein_accession.in_(accessions),
                    ProteinGOAnnotation.annotation_set_id == ps.annotation_set_id,
                )
                .group_by(ProteinGOAnnotation.protein_accession)
                .all()
            }

            match_counts = {
                acc: cnt
                for acc, cnt in session.query(
                    GOPrediction.protein_accession,
                    func.count(func.distinct(GOPrediction.go_term_id)),
                )
                .join(
                    ProteinGOAnnotation,
                    (ProteinGOAnnotation.go_term_id == GOPrediction.go_term_id)
                    & (ProteinGOAnnotation.protein_accession == GOPrediction.protein_accession)
                    & (ProteinGOAnnotation.annotation_set_id == ps.annotation_set_id),
                )
                .filter(
                    GOPrediction.prediction_set_id == set_id,
                    GOPrediction.protein_accession.in_(accessions),
                )
                .group_by(GOPrediction.protein_accession)
                .all()
            }

        return {
            "total": total,
            "offset": offset,
            "limit": limit,
            "items": [
                {
                    "accession": acc,
                    "go_count": go_count,
                    "min_distance": round(min_dist, 4) if min_dist is not None else None,
                    "annotation_count": ann_counts.get(acc, 0),
                    "match_count": match_counts.get(acc, 0),
                    "in_db": acc in protein_map,
                }
                for acc, go_count, min_dist in rows
            ],
        }


@router.get("/prediction-sets/{set_id}/proteins/{accession}")
def get_protein_predictions(
    set_id: UUID,
    accession: str,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    with session_scope(factory) as session:
        from protea.infrastructure.orm.models.annotation.go_term import GOTerm

        ps = session.get(PredictionSet, set_id)
        if ps is None:
            raise HTTPException(status_code=404, detail="PredictionSet not found")

        rows = (
            session.query(GOPrediction, GOTerm)
            .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
            .filter(
                GOPrediction.prediction_set_id == set_id,
                GOPrediction.protein_accession == accession,
            )
            .order_by(GOPrediction.distance)
            .all()
        )

        return [
            {
                "go_id": gt.go_id,
                "name": gt.name,
                "aspect": gt.aspect,
                "distance": round(pred.distance, 4),
                "ref_protein_accession": pred.ref_protein_accession,
                "qualifier": pred.qualifier,
                "evidence_code": pred.evidence_code,
            }
            for pred, gt in rows
        ]


@router.get("/prediction-sets/{set_id}/go-terms")
def get_go_term_distribution(
    set_id: UUID,
    limit: int = 50,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    with session_scope(factory) as session:
        from protea.infrastructure.orm.models.annotation.go_term import GOTerm

        ps = session.get(PredictionSet, set_id)
        if ps is None:
            raise HTTPException(status_code=404, detail="PredictionSet not found")

        rows = (
            session.query(
                GOTerm.go_id,
                GOTerm.name,
                GOTerm.aspect,
                func.count(GOPrediction.id).label("count"),
            )
            .join(GOPrediction, GOPrediction.go_term_id == GOTerm.id)
            .filter(GOPrediction.prediction_set_id == set_id)
            .group_by(GOTerm.go_id, GOTerm.name, GOTerm.aspect)
            .order_by(func.count(GOPrediction.id).desc())
            .limit(limit)
            .all()
        )

        by_aspect: dict[str, list] = {"F": [], "P": [], "C": [], "other": []}
        for go_id, name, aspect, count in rows:
            entry = {"go_id": go_id, "name": name, "count": count}
            by_aspect.get(aspect or "other", by_aspect["other"]).append(entry)

        aspect_counts = (
            session.query(GOTerm.aspect, func.count(GOPrediction.id))
            .join(GOPrediction, GOPrediction.go_term_id == GOTerm.id)
            .filter(GOPrediction.prediction_set_id == set_id)
            .group_by(GOTerm.aspect)
            .all()
        )

        return {
            "by_aspect": by_aspect,
            "aspect_totals": {asp or "other": cnt for asp, cnt in aspect_counts},
            "top_terms": [
                {"go_id": go_id, "name": name, "aspect": aspect, "count": count}
                for go_id, name, aspect, count in rows
            ],
        }


@router.delete("/prediction-sets/{set_id}")
def delete_prediction_set(
    set_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    with session_scope(factory) as session:
        ps = session.get(PredictionSet, set_id)
        if ps is None:
            raise HTTPException(status_code=404, detail="PredictionSet not found")

        deleted_predictions = (
            session.query(GOPrediction)
            .filter(GOPrediction.prediction_set_id == set_id)
            .delete(synchronize_session=False)
        )
        session.delete(ps)

    return {"deleted": str(set_id), "predictions_deleted": deleted_predictions}
