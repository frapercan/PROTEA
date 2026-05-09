from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import distinct, func
from sqlalchemy.orm import Session, sessionmaker

from protea.api.cache import cached
from protea.api.deps import get_session_factory
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.protein_go_annotation import ProteinGOAnnotation
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.protein.protein_metadata import ProteinUniProtMetadata
from protea.infrastructure.session import session_scope

router = APIRouter(prefix="/proteins", tags=["proteins"])


# ── Stats ─────────────────────────────────────────────────────────────────────


@router.get("/stats", summary="Aggregate protein statistics")
def get_protein_stats(
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Return aggregate counts: total proteins, canonical vs isoforms, reviewed,
    and how many have metadata, embeddings, or GO annotations.

    Cached for 5 minutes — the DISTINCT-over-JOIN counts scan 4M–80M rows and
    take 30+ seconds to run from scratch. Counts move slowly enough that a
    5-min staleness is invisible to users.
    """

    def _compute() -> dict[str, Any]:
        with session_scope(factory) as session:
            total = session.query(func.count(Protein.accession)).scalar() or 0
            canonical = (
                session.query(func.count(Protein.accession))
                .filter(Protein.is_canonical.is_(True))
                .scalar()
                or 0
            )
            reviewed = (
                session.query(func.count(Protein.accession))
                .filter(Protein.reviewed.is_(True))
                .scalar()
                or 0
            )
            with_metadata = (
                session.query(func.count(distinct(Protein.canonical_accession)))
                .join(
                    ProteinUniProtMetadata,
                    Protein.canonical_accession == ProteinUniProtMetadata.canonical_accession,
                )
                .scalar()
                or 0
            )
            with_embeddings = (
                session.query(func.count(distinct(Protein.accession)))
                .join(
                    SequenceEmbedding,
                    Protein.sequence_id == SequenceEmbedding.sequence_id,
                )
                .scalar()
                or 0
            )
            with_go = (
                session.query(func.count(distinct(ProteinGOAnnotation.protein_accession))).scalar()
                or 0
            )
            return {
                "total": total,
                "canonical": canonical,
                "isoforms": total - canonical,
                "reviewed": reviewed,
                "unreviewed": total - reviewed,
                "with_metadata": with_metadata,
                "with_embeddings": with_embeddings,
                "with_go_annotations": with_go,
            }

    return cached("proteins:stats", 300.0, _compute)


# ── List ──────────────────────────────────────────────────────────────────────


@router.get("", summary="List proteins")
def list_proteins(
    search: str | None = Query(
        default=None,
        description="Substring filter on accession/entry/gene/organism (case-insensitive).",
    ),
    reviewed: bool | None = Query(
        default=None,
        description="Filter to UniProt-reviewed (true) or unreviewed (false). None = no filter.",
    ),
    canonical_only: bool = Query(
        default=True,
        description="When true (default), exclude protein isoforms.",
    ),
    limit: int = Query(default=50, le=500, description="Max rows per page; capped at 500."),
    offset: int = Query(
        default=0,
        ge=0,
        description="Offset into alphabetically-ordered (by accession) results.",
    ),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Paginated protein listing with optional full-text search across accession, entry name, gene name, and organism."""
    with session_scope(factory) as session:
        q = session.query(Protein)
        if canonical_only:
            q = q.filter(Protein.is_canonical.is_(True))
        if reviewed is not None:
            q = q.filter(Protein.reviewed == reviewed)
        if search:
            # Limit search length to prevent abuse; escape LIKE special chars.
            term = search[:100].replace("%", r"\%").replace("_", r"\_")
            like = f"%{term}%"
            q = q.filter(
                Protein.accession.ilike(like)
                | Protein.entry_name.ilike(like)
                | Protein.gene_name.ilike(like)
                | Protein.organism.ilike(like)
            )
        total = q.count()
        rows = q.order_by(Protein.accession).offset(offset).limit(limit).all()
        return {
            "total": total,
            "offset": offset,
            "limit": limit,
            "items": [
                {
                    "accession": p.accession,
                    "entry_name": p.entry_name,
                    "gene_name": p.gene_name,
                    "organism": p.organism,
                    "taxonomy_id": p.taxonomy_id,
                    "length": p.length,
                    "reviewed": p.reviewed,
                    "is_canonical": p.is_canonical,
                    "isoform_index": p.isoform_index,
                }
                for p in rows
            ],
        }


# ── Detail ────────────────────────────────────────────────────────────────────


def _serialise_uniprot_metadata(
    meta: ProteinUniProtMetadata | None,
) -> dict[str, Any] | None:
    """Materialise the UniProt functional-metadata payload for a protein.

    Returns ``None`` when the protein has no metadata row (Swiss-Prot
    annotation has not been ingested for this canonical accession).
    """
    if meta is None:
        return None
    return {
        "function_cc": meta.function_cc,
        "ec_number": meta.ec_number,
        "catalytic_activity": meta.catalytic_activity,
        "pathway": meta.pathway,
        "keywords": meta.keywords,
        "cofactor": meta.cofactor,
        "activity_regulation": meta.activity_regulation,
        "absorption": meta.absorption,
        "kinetics": meta.kinetics,
        "ph_dependence": meta.ph_dependence,
        "redox_potential": meta.redox_potential,
        "temperature_dependence": meta.temperature_dependence,
        "active_site": meta.active_site,
        "binding_site": meta.binding_site,
        "dna_binding": meta.dna_binding,
        "rhea_id": meta.rhea_id,
        "site": meta.site,
        "features": meta.features,
    }


def _list_isoforms(session: Session, p: Protein) -> list[str]:
    """Return non-canonical isoform accessions for a canonical protein."""
    if not p.is_canonical:
        return []
    return [
        row.accession
        for row in session.query(Protein.accession)
        .filter(
            Protein.canonical_accession == p.canonical_accession,
            Protein.is_canonical.is_(False),
        )
        .order_by(Protein.isoform_index)
        .all()
    ]


@router.get("/{accession}", summary="Get protein details")
def get_protein(
    accession: str,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Full details for one protein: core fields, UniProt functional metadata, embedding count,
    GO annotation count, and accessions of known isoforms (if canonical)."""
    with session_scope(factory) as session:
        p = session.get(Protein, accession)
        if p is None:
            raise HTTPException(status_code=404, detail="Protein not found")

        meta = (
            session.query(ProteinUniProtMetadata)
            .filter(ProteinUniProtMetadata.canonical_accession == p.canonical_accession)
            .first()
        )
        embedding_count = (
            session.query(func.count(SequenceEmbedding.id))
            .filter(SequenceEmbedding.sequence_id == p.sequence_id)
            .scalar()
            if p.sequence_id
            else 0
        )
        go_count = (
            session.query(func.count(ProteinGOAnnotation.id))
            .filter(ProteinGOAnnotation.protein_accession == accession)
            .scalar()
        )

        return {
            "accession": p.accession,
            "entry_name": p.entry_name,
            "gene_name": p.gene_name,
            "organism": p.organism,
            "taxonomy_id": p.taxonomy_id,
            "length": p.length,
            "reviewed": p.reviewed,
            "is_canonical": p.is_canonical,
            "canonical_accession": p.canonical_accession,
            "isoform_index": p.isoform_index,
            "isoforms": _list_isoforms(session, p),
            "sequence_id": p.sequence_id,
            "embedding_count": embedding_count,
            "go_annotation_count": go_count,
            "metadata": _serialise_uniprot_metadata(meta),
        }


# ── Annotations ───────────────────────────────────────────────────────────────


@router.get("/{accession}/annotations", summary="List GO annotations for a protein")
def get_protein_annotations(
    accession: str,
    annotation_set_id: str | None = Query(
        default=None,
        description=(
            "Optional UUID of an ``AnnotationSet`` to scope the "
            "lookup. When omitted, annotations from every set the "
            "protein appears in are returned. Invalid UUIDs return 422."
        ),
    ),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    """Return all GO term annotations for a protein, joined with term details and annotation set source.
    Optionally filter to a specific annotation set by UUID."""
    with session_scope(factory) as session:
        q = (
            session.query(ProteinGOAnnotation, GOTerm, AnnotationSet)
            .join(GOTerm, ProteinGOAnnotation.go_term_id == GOTerm.id)
            .join(AnnotationSet, ProteinGOAnnotation.annotation_set_id == AnnotationSet.id)
            .filter(ProteinGOAnnotation.protein_accession == accession)
        )
        if annotation_set_id:
            try:
                q = q.filter(ProteinGOAnnotation.annotation_set_id == UUID(annotation_set_id))
            except ValueError:
                raise HTTPException(status_code=422, detail="Invalid annotation_set_id") from None
        rows = q.order_by(GOTerm.aspect, GOTerm.name).all()
        return [
            {
                "go_id": gt.go_id,
                "name": gt.name,
                "aspect": gt.aspect,
                "qualifier": ann.qualifier,
                "evidence_code": ann.evidence_code,
                "assigned_by": ann.assigned_by,
                "db_reference": ann.db_reference,
                "annotation_set_id": str(ann.annotation_set_id),
                "annotation_set_source": aset.source,
                "annotation_set_version": aset.source_version,
            }
            for ann, gt, aset in rows
        ]
