from __future__ import annotations

import csv
import io
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import func
from sqlalchemy.orm import Session, sessionmaker
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
from protea.api.deps import get_amqp_url, get_session_factory
from protea.infrastructure.orm.models.job import Job, JobEvent
from protea.infrastructure.queue.publisher import publish_job
from protea.infrastructure.session import session_scope

router = APIRouter(prefix="/embeddings", tags=["embeddings"])

_JOBS_QUEUE = "protea.jobs"

_VALID_BACKENDS = {"esm", "esm3c", "t5", "auto"}
_VALID_LAYER_AGG = {"mean", "last", "concat"}
_VALID_POOLING = {"mean", "max", "cls", "mean_max"}


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
        isinstance(chunk_size, int)
        and isinstance(chunk_overlap, int)
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


@router.get("/configs", summary="List embedding configs")
def list_embedding_configs(
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    """List all embedding configurations with their stored embedding counts, newest first."""
    with session_scope(factory) as session:
        rows = session.query(EmbeddingConfig).order_by(EmbeddingConfig.created_at.desc()).all()
        counts = {
            config_id: cnt
            for config_id, cnt in session.query(
                SequenceEmbedding.embedding_config_id,
                func.count(SequenceEmbedding.id),
            )
            .group_by(SequenceEmbedding.embedding_config_id)
            .all()
        }
        return [_config_to_dict(c, embedding_count=counts.get(c.id, 0)) for c in rows]


@router.post("/configs", summary="Create an embedding config")
def create_embedding_config(
    body: dict[str, Any],
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Create a new EmbeddingConfig that defines the model, layer selection, pooling strategy, and chunking.

    This config is referenced by `compute_embeddings` jobs and `predict_go_terms` jobs to ensure
    query and reference embeddings were produced under identical settings.
    """
    validated = _validate_embedding_config_body(body)

    with session_scope(factory) as session:
        config = EmbeddingConfig(**validated)
        session.add(config)
        session.flush()
        result = _config_to_dict(config)

    return result


@router.get("/configs/{config_id}", summary="Get embedding config details")
def get_embedding_config(
    config_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Retrieve a single EmbeddingConfig with its total stored embedding count."""
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


@router.delete("/configs/{config_id}", summary="Delete an embedding config")
def delete_embedding_config(
    config_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Delete an EmbeddingConfig and cascade-delete all linked embeddings, prediction sets, and predictions."""
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


@router.post("/predict", summary="Trigger GO term prediction")
def predict_go_terms(
    body: dict[str, Any],
    factory: sessionmaker[Session] = Depends(get_session_factory),
    amqp_url: str = Depends(get_amqp_url),
) -> dict[str, Any]:
    """Queue a `predict_go_terms` job that runs KNN-based GO term transfer.

    The coordinator partitions query proteins into batches, each dispatched to
    `protea.predictions.batch` workers for KNN search (numpy or FAISS) + GO annotation transfer.
    Results are written to a new `PredictionSet` via `protea.predictions.write` workers.

    Required body fields: `embedding_config_id`, `annotation_set_id`, `ontology_snapshot_id`.
    Optional: `query_set_id` (FASTA upload), `limit_per_entry`, `distance_threshold`,
    `batch_size`, `search_backend`, `compute_alignments`, `compute_taxonomy`,
    `aspect_separated_knn` (bool, default false — builds one KNN index per GO aspect to
    guarantee BPO/MFO/CCO coverage even when unified nearest neighbours carry only one aspect).
    """

    def _parse_uuid(key: str) -> UUID:
        raw = body.get(key)
        try:
            return UUID(str(raw))
        except (ValueError, AttributeError):
            raise HTTPException(status_code=422, detail=f"{key} must be a valid UUID") from None

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
        session.add(
            JobEvent(
                job_id=job_id,
                event="job.created",
                fields={"operation": "predict_go_terms", "queue": _JOBS_QUEUE},
            )
        )

    publish_job(amqp_url, _JOBS_QUEUE, job_id)
    return {"id": str(job_id), "status": "queued"}


# ── Prediction Sets ───────────────────────────────────────────────────────────


@router.get("/prediction-sets", summary="List prediction sets")
def list_prediction_sets(
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    """List the 100 most recent prediction sets with their GO prediction counts."""
    with session_scope(factory) as session:
        # Single query with a correlated subquery for counts (avoids N+1).
        count_subq = (
            session.query(func.count(GOPrediction.id))
            .filter(GOPrediction.prediction_set_id == PredictionSet.id)
            .correlate(PredictionSet)
            .scalar_subquery()
        )
        rows = (
            session.query(
                PredictionSet, EmbeddingConfig, AnnotationSet, OntologySnapshot, count_subq
            )
            .join(EmbeddingConfig, PredictionSet.embedding_config_id == EmbeddingConfig.id)
            .join(AnnotationSet, PredictionSet.annotation_set_id == AnnotationSet.id)
            .join(OntologySnapshot, PredictionSet.ontology_snapshot_id == OntologySnapshot.id)
            .order_by(PredictionSet.created_at.desc())
            .limit(100)
            .all()
        )
        result = []
        for ps, ec, ann, snap, prediction_count in rows:
            result.append(
                {
                    "id": str(ps.id),
                    "embedding_config_id": str(ps.embedding_config_id),
                    "embedding_config_name": ec.model_name,
                    "annotation_set_id": str(ps.annotation_set_id),
                    "annotation_set_label": f"{ann.source} {ann.source_version}"
                    if ann.source_version
                    else ann.source,
                    "ontology_snapshot_id": str(ps.ontology_snapshot_id),
                    "ontology_snapshot_version": snap.obo_version,
                    "query_set_id": str(ps.query_set_id) if ps.query_set_id else None,
                    "limit_per_entry": ps.limit_per_entry,
                    "distance_threshold": ps.distance_threshold,
                    "created_at": ps.created_at.isoformat(),
                    "prediction_count": prediction_count or 0,
                }
            )
        return result


@router.get("/prediction-sets/{set_id}", summary="Get prediction set details")
def get_prediction_set(
    set_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Retrieve a prediction set with total prediction count and per-protein GO term counts."""
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


@router.get("/prediction-sets/{set_id}/proteins", summary="List proteins in a prediction set")
def list_prediction_set_proteins(
    set_id: UUID,
    search: str | None = None,
    limit: int = 50,
    offset: int = 0,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Paginated list of proteins in a prediction set with their predicted GO count, minimum distance,
    known annotation count, and how many predictions match known annotations (precision proxy)."""
    with session_scope(factory) as session:
        ps = session.get(PredictionSet, set_id)
        if ps is None:
            raise HTTPException(status_code=404, detail="PredictionSet not found")

        from protea.infrastructure.orm.models.annotation.protein_go_annotation import (
            ProteinGOAnnotation,
        )
        from protea.infrastructure.orm.models.protein.protein import Protein

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


@router.get(
    "/prediction-sets/{set_id}/proteins/{accession}", summary="Get predictions for one protein"
)
def get_protein_predictions(
    set_id: UUID,
    accession: str,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    """Return all predicted GO terms for a protein in a prediction set, sorted by distance (nearest first).
    Includes GO term details plus optional alignment (NW/SW) and taxonomy fields when computed."""
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
                # Alignment — NW
                "identity_nw": pred.identity_nw,
                "similarity_nw": pred.similarity_nw,
                "alignment_score_nw": pred.alignment_score_nw,
                "gaps_pct_nw": pred.gaps_pct_nw,
                "alignment_length_nw": pred.alignment_length_nw,
                # Alignment — SW
                "identity_sw": pred.identity_sw,
                "similarity_sw": pred.similarity_sw,
                "alignment_score_sw": pred.alignment_score_sw,
                "gaps_pct_sw": pred.gaps_pct_sw,
                "alignment_length_sw": pred.alignment_length_sw,
                # Lengths
                "length_query": pred.length_query,
                "length_ref": pred.length_ref,
                # Taxonomy
                "query_taxonomy_id": pred.query_taxonomy_id,
                "ref_taxonomy_id": pred.ref_taxonomy_id,
                "taxonomic_lca": pred.taxonomic_lca,
                "taxonomic_distance": pred.taxonomic_distance,
                "taxonomic_common_ancestors": pred.taxonomic_common_ancestors,
                "taxonomic_relation": pred.taxonomic_relation,
                # Re-ranker features
                "vote_count": pred.vote_count,
                "k_position": pred.k_position,
                "go_term_frequency": pred.go_term_frequency,
                "ref_annotation_density": pred.ref_annotation_density,
                "neighbor_distance_std": pred.neighbor_distance_std,
            }
            for pred, gt in rows
        ]


@router.get(
    "/prediction-sets/{set_id}/go-terms", summary="GO term distribution in a prediction set"
)
def get_go_term_distribution(
    set_id: UUID,
    limit: int = 50,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Return the most frequently predicted GO terms grouped by aspect (F/P/C)
    and the total prediction counts per aspect."""
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


_TSV_COLUMNS = [
    "protein_accession",
    "go_id",
    "go_name",
    "go_aspect",
    "distance",
    "ref_protein_accession",
    "qualifier",
    "evidence_code",
    # NW alignment
    "identity_nw",
    "similarity_nw",
    "alignment_score_nw",
    "gaps_pct_nw",
    "alignment_length_nw",
    # SW alignment
    "identity_sw",
    "similarity_sw",
    "alignment_score_sw",
    "gaps_pct_sw",
    "alignment_length_sw",
    # Lengths
    "length_query",
    "length_ref",
    # Taxonomy
    "query_taxonomy_id",
    "ref_taxonomy_id",
    "taxonomic_lca",
    "taxonomic_distance",
    "taxonomic_common_ancestors",
    "taxonomic_relation",
    # Re-ranker features
    "vote_count",
    "k_position",
    "go_term_frequency",
    "ref_annotation_density",
    "neighbor_distance_std",
]


@router.get(
    "/prediction-sets/{set_id}/predictions.tsv",
    summary="Download predictions as TSV",
    response_class=StreamingResponse,
)
def download_predictions_tsv(
    set_id: UUID,
    accession: str | None = Query(None, description="Filter to a single query protein accession"),
    aspect: str | None = Query(None, description="Filter by GO aspect: F, P, or C"),
    max_distance: float | None = Query(
        None, description="Only include predictions with distance ≤ this value"
    ),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> StreamingResponse:
    """Stream all GO predictions for a prediction set as a tab-separated file.

    Each row is one (protein, GO term, reference protein) triple. Columns include
    embedding distance, GO term metadata, annotation fields, and optional alignment
    and taxonomy features (columns are present but empty when not computed).

    Optional filters: ``accession``, ``aspect`` (F/P/C), ``max_distance``.

    The response streams rows directly from the database — suitable for large
    prediction sets without loading everything into memory.
    """
    from protea.infrastructure.orm.models.annotation.go_term import GOTerm

    # Verify existence before starting the stream so 404 can be returned properly.
    with session_scope(factory) as _check:
        if _check.get(PredictionSet, set_id) is None:
            raise HTTPException(status_code=404, detail="PredictionSet not found")

    def _generate():
        with session_scope(factory) as session:
            buf = io.StringIO()
            writer = csv.writer(buf, delimiter="\t", lineterminator="\n")
            writer.writerow(_TSV_COLUMNS)
            yield buf.getvalue()

            q = (
                session.query(GOPrediction, GOTerm)
                .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
                .filter(GOPrediction.prediction_set_id == set_id)
            )
            if accession:
                q = q.filter(GOPrediction.protein_accession == accession)
            if aspect:
                q = q.filter(GOTerm.aspect == aspect.upper())
            if max_distance is not None:
                q = q.filter(GOPrediction.distance <= max_distance)

            q = q.order_by(GOPrediction.protein_accession, GOPrediction.distance)

            for pred, gt in q.yield_per(1000):
                buf = io.StringIO()
                writer = csv.writer(buf, delimiter="\t", lineterminator="\n")
                writer.writerow(
                    [
                        pred.protein_accession,
                        gt.go_id,
                        gt.name,
                        gt.aspect,
                        pred.distance,
                        pred.ref_protein_accession,
                        pred.qualifier or "",
                        pred.evidence_code or "",
                        _fmt(pred.identity_nw),
                        _fmt(pred.similarity_nw),
                        _fmt(pred.alignment_score_nw),
                        _fmt(pred.gaps_pct_nw),
                        _fmt(pred.alignment_length_nw),
                        _fmt(pred.identity_sw),
                        _fmt(pred.similarity_sw),
                        _fmt(pred.alignment_score_sw),
                        _fmt(pred.gaps_pct_sw),
                        _fmt(pred.alignment_length_sw),
                        pred.length_query if pred.length_query is not None else "",
                        pred.length_ref if pred.length_ref is not None else "",
                        pred.query_taxonomy_id if pred.query_taxonomy_id is not None else "",
                        pred.ref_taxonomy_id if pred.ref_taxonomy_id is not None else "",
                        pred.taxonomic_lca if pred.taxonomic_lca is not None else "",
                        pred.taxonomic_distance if pred.taxonomic_distance is not None else "",
                        pred.taxonomic_common_ancestors
                        if pred.taxonomic_common_ancestors is not None
                        else "",
                        pred.taxonomic_relation or "",
                        pred.vote_count if pred.vote_count is not None else "",
                        pred.k_position if pred.k_position is not None else "",
                        pred.go_term_frequency if pred.go_term_frequency is not None else "",
                        pred.ref_annotation_density
                        if pred.ref_annotation_density is not None
                        else "",
                        _fmt(pred.neighbor_distance_std),
                    ]
                )
                yield buf.getvalue()

    filename = f"predictions_{set_id}.tsv"
    return StreamingResponse(
        _generate(),
        media_type="text/tab-separated-values",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _fmt(v: float | None) -> str:
    """Format a nullable float for TSV output."""
    if v is None:
        return ""
    return f"{v:.6g}"


@router.get(
    "/prediction-sets/{set_id}/predictions-cafa.tsv",
    summary="Download predictions in CAFA submission format",
    response_class=StreamingResponse,
)
def download_predictions_cafa(
    set_id: UUID,
    eval_id: UUID | None = Query(
        None, description="Filter to delta proteins from this EvaluationSet."
    ),
    aspect: str | None = Query(None, description="Filter by GO aspect: F, P, or C"),
    max_distance: float | None = Query(
        None, description="Only include predictions with distance ≤ this value"
    ),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> StreamingResponse:
    """Stream predictions in CAFA format: ``protein_accession\\tgo_id\\tscore``.

    Score is computed as ``max(0.0, 1.0 - distance)`` so that closer neighbours
    receive higher confidence scores in the [0, 1] range expected by the
    CAFA evaluator.  One row per (protein, GO term) pair — duplicate GO terms
    for the same protein are deduplicated keeping the highest score (lowest distance).

    Pass ``eval_id`` to restrict output to delta proteins only (NK + LK targets),
    which is required for a valid CAFA evaluation.
    """
    from protea.core.evaluation import compute_evaluation_data
    from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
    from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
    from protea.infrastructure.orm.models.annotation.go_term import GOTerm

    with session_scope(factory) as _check:
        if _check.get(PredictionSet, set_id) is None:
            raise HTTPException(status_code=404, detail="PredictionSet not found")

        delta_proteins: set[str] | None = None
        if eval_id is not None:
            e = _check.get(EvaluationSet, eval_id)
            if e is None:
                raise HTTPException(status_code=404, detail="EvaluationSet not found")
            ann_old = _check.get(AnnotationSet, e.old_annotation_set_id)
            data = compute_evaluation_data(
                _check,
                e.old_annotation_set_id,
                e.new_annotation_set_id,
                ann_old.ontology_snapshot_id,
            )
            delta_proteins = set(data.nk) | set(data.lk)

    def _generate():
        with session_scope(factory) as session:
            # Deduplicate at the DB level: keep the lowest distance per
            # (protein_accession, go_id) pair so we never need an unbounded
            # `seen` set in Python — this preserves true streaming.
            from sqlalchemy import func as sa_func

            min_dist = (
                session.query(
                    GOPrediction.protein_accession,
                    GOPrediction.go_term_id,
                    sa_func.min(GOPrediction.distance).label("min_distance"),
                )
                .filter(GOPrediction.prediction_set_id == set_id)
            )
            if max_distance is not None:
                min_dist = min_dist.filter(GOPrediction.distance <= max_distance)
            min_dist = min_dist.group_by(
                GOPrediction.protein_accession, GOPrediction.go_term_id
            ).subquery()

            q = (
                session.query(min_dist.c.protein_accession, GOTerm.go_id, min_dist.c.min_distance)
                .join(GOTerm, min_dist.c.go_term_id == GOTerm.id)
            )
            if aspect:
                q = q.filter(GOTerm.aspect == aspect.upper())
            if delta_proteins is not None:
                q = q.filter(min_dist.c.protein_accession.in_(delta_proteins))

            q = q.order_by(min_dist.c.protein_accession, GOTerm.go_id)

            for acc, go_id, dist in q.yield_per(1000):
                score = max(0.0, 1.0 - dist)
                yield f"{acc}\t{go_id}\t{score:.4f}\n"

    filename = f"predictions_cafa_{set_id}.tsv"
    return StreamingResponse(
        _generate(),
        media_type="text/tab-separated-values",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.delete("/prediction-sets/{set_id}", summary="Delete a prediction set")
def delete_prediction_set(
    set_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Delete a prediction set and all its GOPrediction rows."""
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
