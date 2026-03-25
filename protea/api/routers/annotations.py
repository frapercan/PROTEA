from __future__ import annotations

import io
import zipfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import ValidationError
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from protea.api.deps import get_amqp_url, get_artifacts_dir, get_session_factory
from protea.core.evaluation import compute_evaluation_data
from protea.core.operations.generate_evaluation_set import GenerateEvaluationSetPayload
from protea.core.operations.load_goa_annotations import LoadGOAAnnotationsPayload
from protea.core.operations.load_ontology_snapshot import LoadOntologySnapshotPayload
from protea.core.operations.load_quickgo_annotations import LoadQuickGOAnnotationsPayload
from protea.core.operations.run_cafa_evaluation import RunCafaEvaluationPayload
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.evaluation_result import EvaluationResult
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.go_term_relationship import GOTermRelationship
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.annotation.protein_go_annotation import ProteinGOAnnotation
from protea.infrastructure.orm.models.job import Job, JobEvent
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.sequence.sequence import Sequence
from protea.infrastructure.queue.publisher import publish_job
from protea.infrastructure.session import session_scope

router = APIRouter(prefix="/annotations", tags=["annotations"])

_JOBS_QUEUE = "protea.jobs"


# ── Ontology Snapshots ────────────────────────────────────────────────────────


@router.get("/snapshots", summary="List ontology snapshots")
def list_snapshots(
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    """List all loaded GO ontology snapshots with their GO term counts, newest first."""
    with session_scope(factory) as session:
        count_sub = (
            session.query(
                GOTerm.ontology_snapshot_id,
                func.count(GOTerm.id).label("cnt"),
            )
            .group_by(GOTerm.ontology_snapshot_id)
            .subquery()
        )
        rows = (
            session.query(OntologySnapshot, count_sub.c.cnt)
            .outerjoin(count_sub, OntologySnapshot.id == count_sub.c.ontology_snapshot_id)
            .order_by(OntologySnapshot.loaded_at.desc())
            .all()
        )
        return [
            {
                "id": str(s.id),
                "obo_url": s.obo_url,
                "obo_version": s.obo_version,
                "ia_url": s.ia_url,
                "loaded_at": s.loaded_at.isoformat(),
                "go_term_count": cnt or 0,
            }
            for s, cnt in rows
        ]


@router.get("/snapshots/{snapshot_id}", summary="Get ontology snapshot details")
def get_snapshot(
    snapshot_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Retrieve a single ontology snapshot with its GO term count."""
    with session_scope(factory) as session:
        s = session.get(OntologySnapshot, snapshot_id)
        if s is None:
            raise HTTPException(status_code=404, detail="OntologySnapshot not found")

        term_count = (
            session.query(func.count(GOTerm.id))
            .filter(GOTerm.ontology_snapshot_id == snapshot_id)
            .scalar()
        )

        return {
            "id": str(s.id),
            "obo_url": s.obo_url,
            "obo_version": s.obo_version,
            "ia_url": s.ia_url,
            "loaded_at": s.loaded_at.isoformat(),
            "go_term_count": term_count,
        }


@router.patch("/snapshots/{snapshot_id}/ia-url", summary="Set IA URL on an ontology snapshot")
def set_snapshot_ia_url(
    snapshot_id: UUID,
    body: dict[str, Any],
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Associate an Information Accretion (IA) file URL with an existing ontology snapshot.

    The IA file contains per-term information-content weights (two columns:
    ``go_id``, ``ia_value``) and is published alongside each CAFA benchmark
    (e.g. ``IA_cafa6.tsv``).  Once set, ``run_cafa_evaluation`` picks it up
    automatically for every evaluation that uses this snapshot — no need to
    pass ``ia_file`` in the job payload.

    Pass ``{"ia_url": null}`` to clear the association (evaluations will fall
    back to uniform IC=1).

    This endpoint only touches ``ia_url``; the OBO file and GO term data are
    not affected.
    """
    ia_url = body.get("ia_url")
    if "ia_url" not in body:
        raise HTTPException(
            status_code=422, detail="Body must contain 'ia_url' key (string or null)"
        )

    with session_scope(factory) as session:
        s = session.get(OntologySnapshot, snapshot_id)
        if s is None:
            raise HTTPException(status_code=404, detail="OntologySnapshot not found")
        s.ia_url = ia_url or None
        session.flush()
        return {
            "id": str(s.id),
            "obo_version": s.obo_version,
            "ia_url": s.ia_url,
        }


@router.post("/snapshots/load", summary="Trigger ontology snapshot load")
def load_ontology_snapshot(
    body: dict[str, Any],
    factory: sessionmaker[Session] = Depends(get_session_factory),
    amqp_url: str = Depends(get_amqp_url),
) -> dict[str, Any]:
    """Queue a `load_ontology_snapshot` job that downloads and parses a GO OBO file.

    The job is idempotent by `obo_version`: if the snapshot already exists with relationships
    it will be skipped; if relationships are missing they will be backfilled.
    """
    try:
        LoadOntologySnapshotPayload.model_validate(body)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors()) from exc

    with session_scope(factory) as session:
        job = Job(operation="load_ontology_snapshot", queue_name=_JOBS_QUEUE, payload=body)
        session.add(job)
        session.flush()
        job_id = job.id
        session.add(
            JobEvent(
                job_id=job_id,
                event="job.created",
                fields={"operation": "load_ontology_snapshot", "queue": _JOBS_QUEUE},
            )
        )

    publish_job(amqp_url, _JOBS_QUEUE, job_id)
    return {"id": str(job_id), "status": "queued"}


# ── Annotation Sets ───────────────────────────────────────────────────────────


@router.get("/sets", summary="List annotation sets")
def list_annotation_sets(
    source: str | None = Query(default=None, description="Filter by source: `goa` or `quickgo`."),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    """List annotation sets with their annotation counts, newest first. Optionally filter by source."""
    with session_scope(factory) as session:
        count_sub = (
            session.query(
                ProteinGOAnnotation.annotation_set_id,
                func.count(ProteinGOAnnotation.id).label("cnt"),
            )
            .group_by(ProteinGOAnnotation.annotation_set_id)
            .subquery()
        )
        q = session.query(AnnotationSet, count_sub.c.cnt).outerjoin(
            count_sub, AnnotationSet.id == count_sub.c.annotation_set_id
        )
        if source is not None:
            q = q.filter(AnnotationSet.source == source)
        rows = q.order_by(AnnotationSet.created_at.desc()).all()
        return [
            {
                "id": str(a.id),
                "source": a.source,
                "source_version": a.source_version,
                "ontology_snapshot_id": str(a.ontology_snapshot_id),
                "job_id": str(a.job_id) if a.job_id else None,
                "created_at": a.created_at.isoformat(),
                "meta": a.meta,
                "annotation_count": cnt or 0,
            }
            for a, cnt in rows
        ]


@router.get("/sets/{set_id}", summary="Get annotation set details")
def get_annotation_set(
    set_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Retrieve a single annotation set with its total annotation count."""
    with session_scope(factory) as session:
        a = session.get(AnnotationSet, set_id)
        if a is None:
            raise HTTPException(status_code=404, detail="AnnotationSet not found")

        annotation_count = (
            session.query(func.count(ProteinGOAnnotation.id))
            .filter(ProteinGOAnnotation.annotation_set_id == set_id)
            .scalar()
        )

        return {
            "id": str(a.id),
            "source": a.source,
            "source_version": a.source_version,
            "ontology_snapshot_id": str(a.ontology_snapshot_id),
            "job_id": str(a.job_id) if a.job_id else None,
            "created_at": a.created_at.isoformat(),
            "meta": a.meta,
            "annotation_count": annotation_count,
        }


@router.delete("/sets/{set_id}", summary="Delete an annotation set")
def delete_annotation_set(
    set_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Delete an annotation set and all its annotations. Returns 409 if referenced by a prediction set."""
    with session_scope(factory) as session:
        a = session.get(AnnotationSet, set_id)
        if a is None:
            raise HTTPException(status_code=404, detail="AnnotationSet not found")
        annotation_count = (
            session.query(func.count(ProteinGOAnnotation.id))
            .filter(ProteinGOAnnotation.annotation_set_id == set_id)
            .scalar()
        )
        try:
            session.delete(a)
            session.flush()
        except IntegrityError:
            raise HTTPException(
                status_code=409,
                detail="This annotation set is referenced by one or more prediction sets. Delete those first.",
            ) from None
        return {"deleted": str(set_id), "annotations_deleted": annotation_count}


@router.post("/sets/load-goa", summary="Trigger GOA annotation load")
def load_goa_annotations(
    body: dict[str, Any],
    factory: sessionmaker[Session] = Depends(get_session_factory),
    amqp_url: str = Depends(get_amqp_url),
) -> dict[str, Any]:
    """Queue a `load_goa_annotations` job that streams a GAF file (gzip or plain) and upserts
    GO annotations into an AnnotationSet. Only proteins already in the DB are annotated."""
    try:
        LoadGOAAnnotationsPayload.model_validate(body)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors()) from exc

    with session_scope(factory) as session:
        job = Job(operation="load_goa_annotations", queue_name=_JOBS_QUEUE, payload=body)
        session.add(job)
        session.flush()
        job_id = job.id
        session.add(
            JobEvent(
                job_id=job_id,
                event="job.created",
                fields={"operation": "load_goa_annotations", "queue": _JOBS_QUEUE},
            )
        )

    publish_job(amqp_url, _JOBS_QUEUE, job_id)
    return {"id": str(job_id), "status": "queued"}


@router.post("/sets/load-quickgo", summary="Trigger QuickGO annotation load")
def load_quickgo_annotations(
    body: dict[str, Any],
    factory: sessionmaker[Session] = Depends(get_session_factory),
    amqp_url: str = Depends(get_amqp_url),
) -> dict[str, Any]:
    """Queue a `load_quickgo_annotations` job that streams GO annotations from the QuickGO
    bulk download API with optional taxon, aspect, and evidence code filtering."""
    try:
        LoadQuickGOAnnotationsPayload.model_validate(body)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors()) from exc

    with session_scope(factory) as session:
        job = Job(operation="load_quickgo_annotations", queue_name=_JOBS_QUEUE, payload=body)
        session.add(job)
        session.flush()
        job_id = job.id
        session.add(
            JobEvent(
                job_id=job_id,
                event="job.created",
                fields={"operation": "load_quickgo_annotations", "queue": _JOBS_QUEUE},
            )
        )

    publish_job(amqp_url, _JOBS_QUEUE, job_id)
    return {"id": str(job_id), "status": "queued"}


# ── CAFA Evaluation Sets ──────────────────────────────────────────────────────


@router.post("/evaluation-sets/generate", summary="Queue a generate_evaluation_set job")
def generate_evaluation_set(
    body: dict[str, Any],
    factory: sessionmaker[Session] = Depends(get_session_factory),
    amqp_url: str = Depends(get_amqp_url),
) -> dict[str, Any]:
    """Queue a job that computes the CAFA delta between two annotation sets.

    Applies experimental evidence filtering, NOT-qualifier propagation through
    the GO DAG, and classifies delta proteins into NK/LK.  Stats are stored in
    a new EvaluationSet row; ground-truth TSVs are streamed on demand.
    """
    try:
        GenerateEvaluationSetPayload.model_validate(body)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors()) from exc

    with session_scope(factory) as session:
        job = Job(operation="generate_evaluation_set", queue_name=_JOBS_QUEUE, payload=body)
        session.add(job)
        session.flush()
        job_id = job.id
        session.add(
            JobEvent(
                job_id=job_id,
                event="job.created",
                fields={"operation": "generate_evaluation_set", "queue": _JOBS_QUEUE},
            )
        )

    publish_job(amqp_url, _JOBS_QUEUE, job_id)
    return {"id": str(job_id), "status": "queued"}


@router.get("/evaluation-sets", summary="List evaluation sets")
def list_evaluation_sets(
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    """List all evaluation sets, newest first."""
    with session_scope(factory) as session:
        rows = session.query(EvaluationSet).order_by(EvaluationSet.created_at.desc()).all()
        return [
            {
                "id": str(e.id),
                "old_annotation_set_id": str(e.old_annotation_set_id),
                "new_annotation_set_id": str(e.new_annotation_set_id),
                "job_id": str(e.job_id) if e.job_id else None,
                "created_at": e.created_at.isoformat(),
                "stats": e.stats,
            }
            for e in rows
        ]


@router.delete("/evaluation-sets/{eval_id}", summary="Delete an evaluation set", status_code=204)
def delete_evaluation_set(
    eval_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
    artifacts_dir: Path = Depends(get_artifacts_dir),
) -> None:
    """Delete an evaluation set and all its results. Cascades to EvaluationResult rows."""
    with session_scope(factory) as session:
        e = session.get(EvaluationSet, eval_id)
        if e is None:
            raise HTTPException(status_code=404, detail="EvaluationSet not found")
        # Collect result IDs to clean up artifact dirs
        result_ids = [
            str(r.id)
            for r in session.query(EvaluationResult)
            .filter(EvaluationResult.evaluation_set_id == eval_id)
            .all()
        ]
        session.delete(e)

    import shutil

    for rid in result_ids:
        result_dir = artifacts_dir / rid
        if result_dir.exists():
            shutil.rmtree(result_dir, ignore_errors=True)


@router.get("/evaluation-sets/{eval_id}", summary="Get evaluation set details")
def get_evaluation_set(
    eval_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    with session_scope(factory) as session:
        e = session.get(EvaluationSet, eval_id)
        if e is None:
            raise HTTPException(status_code=404, detail="EvaluationSet not found")
        return {
            "id": str(e.id),
            "old_annotation_set_id": str(e.old_annotation_set_id),
            "new_annotation_set_id": str(e.new_annotation_set_id),
            "job_id": str(e.job_id) if e.job_id else None,
            "created_at": e.created_at.isoformat(),
            "stats": e.stats,
        }


def _eval_set_or_404(session: Session, eval_id: UUID) -> EvaluationSet:
    e = session.get(EvaluationSet, eval_id)
    if e is None:
        raise HTTPException(status_code=404, detail="EvaluationSet not found")
    return e


@router.get(
    "/evaluation-sets/{eval_id}/ground-truth-NK.tsv",
    response_class=StreamingResponse,
    summary="Download NK ground truth (CAFA format)",
)
def download_gt_nk(
    eval_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> StreamingResponse:
    """Download No-Knowledge ground truth: delta proteins with zero prior experimental annotations.
    Format: ``protein_accession\\tgo_id`` (no header, 2 columns).
    """
    with session_scope(factory) as session:
        e = _eval_set_or_404(session, eval_id)
        ann_old = session.get(AnnotationSet, e.old_annotation_set_id)
        data = compute_evaluation_data(
            session,
            e.old_annotation_set_id,
            e.new_annotation_set_id,
            ann_old.ontology_snapshot_id,
        )
        lines = [
            f"{protein}\t{go_id}\n"
            for protein, go_ids in sorted(data.nk.items())
            for go_id in sorted(go_ids)
        ]
    return StreamingResponse(
        iter(lines),
        media_type="text/tab-separated-values",
        headers={"Content-Disposition": 'attachment; filename="ground_truth_NK.tsv"'},
    )


@router.get(
    "/evaluation-sets/{eval_id}/ground-truth-LK.tsv",
    response_class=StreamingResponse,
    summary="Download LK ground truth (CAFA format)",
)
def download_gt_lk(
    eval_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> StreamingResponse:
    """Download Limited-Knowledge ground truth: delta proteins with prior experimental annotations.
    Format: ``protein_accession\\tgo_id`` (no header, 2 columns).
    """
    with session_scope(factory) as session:
        e = _eval_set_or_404(session, eval_id)
        ann_old = session.get(AnnotationSet, e.old_annotation_set_id)
        data = compute_evaluation_data(
            session,
            e.old_annotation_set_id,
            e.new_annotation_set_id,
            ann_old.ontology_snapshot_id,
        )
        lines = [
            f"{protein}\t{go_id}\n"
            for protein, go_ids in sorted(data.lk.items())
            for go_id in sorted(go_ids)
        ]
    return StreamingResponse(
        iter(lines),
        media_type="text/tab-separated-values",
        headers={"Content-Disposition": 'attachment; filename="ground_truth_LK.tsv"'},
    )


@router.get(
    "/evaluation-sets/{eval_id}/ground-truth-PK.tsv",
    response_class=StreamingResponse,
    summary="Download PK ground truth (CAFA format)",
)
def download_gt_pk(
    eval_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> StreamingResponse:
    """Download Partial-Knowledge ground truth: proteins that gained new terms in a
    namespace where they already had experimental annotations at t0.
    Use together with ``known-terms.tsv`` passed as ``-known`` to the CAFA evaluator.
    Format: ``protein_accession\\tgo_id`` (no header, 2 columns).
    """
    with session_scope(factory) as session:
        e = _eval_set_or_404(session, eval_id)
        ann_old = session.get(AnnotationSet, e.old_annotation_set_id)
        data = compute_evaluation_data(
            session,
            e.old_annotation_set_id,
            e.new_annotation_set_id,
            ann_old.ontology_snapshot_id,
        )
        lines = [
            f"{protein}\t{go_id}\n"
            for protein, go_ids in sorted(data.pk.items())
            for go_id in sorted(go_ids)
        ]
    return StreamingResponse(
        iter(lines),
        media_type="text/tab-separated-values",
        headers={"Content-Disposition": 'attachment; filename="ground_truth_PK.tsv"'},
    )


@router.get(
    "/evaluation-sets/{eval_id}/known-terms.tsv",
    response_class=StreamingResponse,
    summary="Download known-terms from OLD annotation set (for CAFA PK evaluation)",
)
def download_known_terms(
    eval_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> StreamingResponse:
    """Download ALL experimental annotations from the OLD set (not delta-filtered).
    Format: ``protein_accession\\tgo_id`` (no header, 2 columns).
    Pass this as ``-known`` to the CAFA evaluator to enable PK scoring.
    """
    with session_scope(factory) as session:
        e = _eval_set_or_404(session, eval_id)
        ann_old = session.get(AnnotationSet, e.old_annotation_set_id)
        data = compute_evaluation_data(
            session,
            e.old_annotation_set_id,
            e.new_annotation_set_id,
            ann_old.ontology_snapshot_id,
        )
        lines = [
            f"{protein}\t{go_id}\n"
            for protein, go_ids in sorted(data.known.items())
            for go_id in sorted(go_ids)
        ]
    return StreamingResponse(
        iter(lines),
        media_type="text/tab-separated-values",
        headers={"Content-Disposition": 'attachment; filename="known_terms.tsv"'},
    )


@router.get(
    "/evaluation-sets/{eval_id}/delta-proteins.fasta",
    response_class=StreamingResponse,
    summary="Download delta proteins as FASTA",
)
def download_delta_fasta(
    eval_id: UUID,
    category: str = Query(
        default="all", description="Which proteins to include: `nk`, `lk`, or `all` (default)."
    ),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> StreamingResponse:
    """Download the amino-acid sequences of delta proteins (NK and/or LK) as FASTA.

    Only proteins whose sequence is already stored in the database are included.
    Header format: ``>ACCESSION entry_name OS=organism OX=taxonomy_id (NK|LK)``
    """
    with session_scope(factory) as session:
        e = _eval_set_or_404(session, eval_id)
        ann_old = session.get(AnnotationSet, e.old_annotation_set_id)
        data = compute_evaluation_data(
            session,
            e.old_annotation_set_id,
            e.new_annotation_set_id,
            ann_old.ontology_snapshot_id,
        )

        # Collect requested accessions with their NK/LK/PK label
        accession_label: dict[str, str] = {}
        if category in ("nk", "all"):
            for acc in data.nk:
                accession_label[acc] = "NK"
        if category in ("lk", "all"):
            for acc in data.lk:
                accession_label[acc] = "LK"
        if category in ("pk", "all"):
            for acc in data.pk:
                accession_label.setdefault(acc, "PK")  # may also be LK in another ns

        if not accession_label:
            return StreamingResponse(
                iter([]),
                media_type="text/plain",
                headers={
                    "Content-Disposition": f'attachment; filename="delta_proteins_{category}.fasta"'
                },
            )

        # Fetch proteins + sequences in one query
        rows = (
            session.query(Protein, Sequence)
            .join(Sequence, Protein.sequence_id == Sequence.id)
            .filter(Protein.accession.in_(list(accession_label.keys())))
            .order_by(Protein.accession)
            .all()
        )

        lines: list[str] = []
        for protein, seq in rows:
            label = accession_label.get(protein.accession, "")
            parts = [protein.accession]
            if protein.entry_name:
                parts.append(protein.entry_name)
            if protein.organism:
                parts.append(f"OS={protein.organism}")
            if protein.taxonomy_id:
                parts.append(f"OX={protein.taxonomy_id}")
            parts.append(f"({label})")
            lines.append(f">{' '.join(parts)}\n")
            # Wrap sequence at 60 chars per line (standard FASTA)
            s = seq.sequence
            for i in range(0, len(s), 60):
                lines.append(s[i : i + 60] + "\n")

    return StreamingResponse(
        iter(lines),
        media_type="text/plain",
        headers={"Content-Disposition": f'attachment; filename="delta_proteins_{category}.fasta"'},
    )


# ── CAFA Evaluation Results ───────────────────────────────────────────────────


@router.post(
    "/evaluation-sets/{eval_id}/run",
    summary="Queue a run_cafa_evaluation job",
)
def run_cafa_evaluation(
    eval_id: UUID,
    body: dict[str, Any],
    factory: sessionmaker[Session] = Depends(get_session_factory),
    amqp_url: str = Depends(get_amqp_url),
    artifacts_dir: Path = Depends(get_artifacts_dir),
) -> dict[str, Any]:
    """Queue a job that runs the CAFA evaluator (NK / LK / PK) for a prediction set.

    Body must contain ``prediction_set_id`` (required) and optionally
    ``max_distance`` (float).
    """
    body = {**body, "evaluation_set_id": str(eval_id), "artifacts_dir": str(artifacts_dir)}
    try:
        RunCafaEvaluationPayload.model_validate(body)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors()) from exc

    with session_scope(factory) as session:
        if session.get(EvaluationSet, eval_id) is None:
            raise HTTPException(status_code=404, detail="EvaluationSet not found")
        job = Job(operation="run_cafa_evaluation", queue_name=_JOBS_QUEUE, payload=body)
        session.add(job)
        session.flush()
        job_id = job.id
        session.add(
            JobEvent(
                job_id=job_id,
                event="job.created",
                fields={"operation": "run_cafa_evaluation", "queue": _JOBS_QUEUE},
            )
        )

    publish_job(amqp_url, _JOBS_QUEUE, job_id)
    return {"id": str(job_id), "status": "queued"}


@router.get(
    "/evaluation-sets/{eval_id}/results/{result_id}/metrics.tsv",
    summary="Download evaluation metrics as TSV",
)
def download_evaluation_metrics(
    eval_id: UUID,
    result_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> StreamingResponse:
    with session_scope(factory) as session:
        result = session.get(EvaluationResult, result_id)
        if result is None or result.evaluation_set_id != eval_id:
            raise HTTPException(status_code=404, detail="EvaluationResult not found")

        def _rows() -> Iterator[str]:
            yield "setting\tnamespace\tfmax\tprecision\trecall\ttau\tcoverage\tn_proteins\n"
            for setting in ("NK", "LK", "PK"):
                ns_data = result.results.get(setting, {})
                for ns in ("BPO", "MFO", "CCO"):
                    m = ns_data.get(ns)
                    if m is None:
                        continue
                    yield (
                        f"{setting}\t{ns}\t{m.get('fmax', '')}\t{m.get('precision', '')}\t"
                        f"{m.get('recall', '')}\t{m.get('tau', '')}\t{m.get('coverage', '')}\t"
                        f"{m.get('n_proteins', '')}\n"
                    )

        return StreamingResponse(
            _rows(),
            media_type="text/tab-separated-values",
            headers={"Content-Disposition": f'attachment; filename="metrics_{result_id}.tsv"'},
        )


@router.get(
    "/evaluation-sets/{eval_id}/results/{result_id}/artifacts.zip",
    summary="Download all cafaeval artifacts for an evaluation result as a zip",
)
def download_evaluation_artifacts(
    eval_id: UUID,
    result_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
    artifacts_dir: Path = Depends(get_artifacts_dir),
) -> StreamingResponse:
    with session_scope(factory) as session:
        result = session.get(EvaluationResult, result_id)
        if result is None or result.evaluation_set_id != eval_id:
            raise HTTPException(status_code=404, detail="EvaluationResult not found")

    result_dir = artifacts_dir / str(result_id)
    if not result_dir.exists():
        raise HTTPException(status_code=404, detail="No artifacts found for this result")

    def _zip_stream() -> Iterator[bytes]:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            for path in sorted(result_dir.rglob("*")):
                if path.is_file():
                    zf.write(path, path.relative_to(result_dir))
        yield buf.getvalue()

    return StreamingResponse(
        _zip_stream(),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="artifacts_{result_id}.zip"'},
    )


@router.get(
    "/evaluation-sets/{eval_id}/results",
    summary="List evaluation results for an evaluation set",
)
def list_evaluation_results(
    eval_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    with session_scope(factory) as session:
        if session.get(EvaluationSet, eval_id) is None:
            raise HTTPException(status_code=404, detail="EvaluationSet not found")
        rows = (
            session.query(EvaluationResult)
            .filter(EvaluationResult.evaluation_set_id == eval_id)
            .order_by(EvaluationResult.created_at.desc())
            .all()
        )
        return [
            {
                "id": str(r.id),
                "evaluation_set_id": str(r.evaluation_set_id),
                "prediction_set_id": str(r.prediction_set_id),
                "scoring_config_id": str(r.scoring_config_id) if r.scoring_config_id else None,
                "reranker_model_id": str(r.reranker_model_id) if r.reranker_model_id else None,
                "reranker_config": r.reranker_config,
                "job_id": str(r.job_id) if r.job_id else None,
                "created_at": r.created_at.isoformat(),
                "results": r.results,
            }
            for r in rows
        ]


@router.delete(
    "/evaluation-sets/{eval_id}/results/{result_id}",
    summary="Delete an evaluation result",
    status_code=204,
)
def delete_evaluation_result(
    eval_id: UUID,
    result_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
    artifacts_dir: Path = Depends(get_artifacts_dir),
) -> None:
    with session_scope(factory) as session:
        result = session.get(EvaluationResult, result_id)
        if result is None or result.evaluation_set_id != eval_id:
            raise HTTPException(status_code=404, detail="EvaluationResult not found")
        session.delete(result)

    # Remove artifact directory if present (best-effort)
    result_dir = artifacts_dir / str(result_id)
    if result_dir.exists():
        import shutil

        shutil.rmtree(result_dir, ignore_errors=True)


# ── GO subgraph ───────────────────────────────────────────────────────────────


@router.get("/snapshots/{snapshot_id}/subgraph")
def get_go_subgraph(
    snapshot_id: UUID,
    go_ids: str = Query(..., description="Comma-separated GO IDs, e.g. GO:0003674,GO:0008150"),
    depth: int = Query(default=3, ge=1, le=6),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Return a subgraph of the GO DAG containing the requested terms and their ancestors up to ``depth`` levels."""
    with session_scope(factory) as session:
        snap = session.get(OntologySnapshot, snapshot_id)
        if snap is None:
            raise HTTPException(status_code=404, detail="OntologySnapshot not found")

        query_go_ids = {g.strip() for g in go_ids.split(",") if g.strip()}

        # Resolve initial term DB ids
        seed_terms = (
            session.query(GOTerm)
            .filter(
                GOTerm.ontology_snapshot_id == snapshot_id,
                GOTerm.go_id.in_(query_go_ids),
            )
            .all()
        )

        if not seed_terms:
            return {"nodes": [], "edges": []}

        # BFS upward through the DAG
        visited_ids: set[int] = {t.id for t in seed_terms}
        frontier: set[int] = visited_ids.copy()
        all_terms: dict[int, GOTerm] = {t.id: t for t in seed_terms}
        all_edges: list[dict[str, Any]] = []

        for _ in range(depth):
            if not frontier:
                break
            rels = (
                session.query(GOTermRelationship)
                .filter(
                    GOTermRelationship.ontology_snapshot_id == snapshot_id,
                    GOTermRelationship.child_go_term_id.in_(frontier),
                )
                .all()
            )

            parent_ids = {r.parent_go_term_id for r in rels} - visited_ids
            for r in rels:
                all_edges.append(
                    {
                        "source": r.child_go_term_id,
                        "target": r.parent_go_term_id,
                        "relation_type": r.relation_type,
                    }
                )

            if parent_ids:
                parents = session.query(GOTerm).filter(GOTerm.id.in_(parent_ids)).all()
                for p in parents:
                    all_terms[p.id] = p
                visited_ids |= parent_ids
                frontier = parent_ids
            else:
                break

        query_db_ids = {t.id for t in seed_terms}
        nodes = [
            {
                "id": t.id,
                "go_id": t.go_id,
                "name": t.name,
                "aspect": t.aspect,
                "is_query": t.id in query_db_ids,
            }
            for t in all_terms.values()
        ]
        return {"nodes": nodes, "edges": all_edges}
