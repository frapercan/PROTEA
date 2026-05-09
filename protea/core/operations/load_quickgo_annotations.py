from __future__ import annotations

import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Annotated, Any, NamedTuple

from protea_contracts import (
    EcoMappingPayload,
    QuickGoAnnotationRecord,
    QuickGoStreamPayload,
)
from pydantic import Field, field_validator
from sqlalchemy import distinct, select
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.annotation.protein_go_annotation import ProteinGOAnnotation
from protea.infrastructure.orm.models.protein.protein import Protein

PositiveInt = Annotated[int, Field(gt=0)]


class _QuickGoStoreCtx(NamedTuple):
    """Immutable per-stream context handed to ``_store_buffer`` / ``_flush_page``."""

    annotation_set_id: uuid.UUID
    protein_accessions: set[str]
    go_term_map: dict[str, int]
    eco_map: dict[str, str]


@dataclass
class _QuickGoPageTotals:
    """Mutable accumulator for the QuickGO page loop."""

    pages: int = 0
    lines: int = 0
    inserted: int = 0
    skipped: int = 0


class LoadQuickGOAnnotationsPayload(ProteaPayload, frozen=True):
    """Payload for loading GO annotations from the QuickGO bulk download endpoint.

    QuickGO returns a single streamed TSV filtered by the canonical accessions
    already present in the DB — no external accession list is needed.

    ``eco_mapping_url`` (optional) points to a GAF-ECO mapping file
    (space-separated: ``ECO:XXXXXXX  CODE``). When provided, ECO IDs are
    resolved to GO evidence codes (IDA, IEA…) before insertion. If omitted,
    the raw ECO ID is stored as-is in ``evidence_code``.
    """

    ontology_snapshot_id: str
    source_version: str
    quickgo_base_url: str = "https://www.ebi.ac.uk/QuickGO/services/annotation/downloadSearch"
    gene_product_ids: list[str] | None = None
    use_db_accessions: bool = True
    eco_mapping_url: str | None = None
    page_size: PositiveInt = 10000
    timeout_seconds: PositiveInt = 300
    commit_every_page: bool = True
    total_limit: PositiveInt | None = None
    gene_product_batch_size: PositiveInt = 200

    @field_validator("ontology_snapshot_id", "source_version", mode="before")
    @classmethod
    def must_be_non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()


class LoadQuickGOAnnotationsOperation:
    """Streams GO annotations from the QuickGO bulk download API.

    Proteins to annotate are determined by the canonical accessions already
    present in the DB — no external FASTA or accession list is needed.

    The QuickGO TSV columns used:
      GENE PRODUCT ID → protein accession
      GO TERM         → GO identifier
      QUALIFIER       → qualifier (enables, involved_in…)
      ECO ID          → mapped to evidence_code via eco_mapping_url (or stored raw)
      REFERENCE       → db_reference
      WITH/FROM       → with_from
      ASSIGNED BY     → assigned_by
      DATE            → annotation_date
    """

    name = "load_quickgo_annotations"
    description = (
        "Stream GO annotations from QuickGO's bulk download endpoint and insert "
        "ProteinGOAnnotation rows for accessions already present in the DB."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        p = payload or {}
        bits = []
        if p.get("source_version"):
            bits.append(f"source={p['source_version']}")
        if p.get("total_limit"):
            bits.append(f"limit={p['total_limit']}")
        return " · ".join(bits)

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = LoadQuickGOAnnotationsPayload.model_validate(payload)

        snapshot_id = uuid.UUID(p.ontology_snapshot_id)
        if session.get(OntologySnapshot, snapshot_id) is None:
            raise ValueError(f"OntologySnapshot {p.ontology_snapshot_id} not found")

        t0 = time.perf_counter()
        emit(
            "load_quickgo_annotations.start",
            None,
            {
                "ontology_snapshot_id": p.ontology_snapshot_id,
                "source_version": p.source_version,
            },
            "info",
        )

        canonical_accessions, protein_accessions = self._load_accessions(session, emit)
        if not canonical_accessions:
            emit("load_quickgo_annotations.no_proteins", None, {}, "warning")
            return OperationResult(result={"annotations_inserted": 0})

        effective_gp_ids = list(canonical_accessions) if p.use_db_accessions else p.gene_product_ids
        go_term_map = self._load_go_term_map(session, snapshot_id, emit)
        eco_map = self._load_eco_mapping(p, emit)
        annotation_set = self._create_annotation_set(session, p, snapshot_id, emit)
        store_ctx = _QuickGoStoreCtx(
            annotation_set_id=annotation_set.id,
            protein_accessions=protein_accessions,
            go_term_map=go_term_map,
            eco_map=eco_map,
        )
        totals = self._stream_and_store(session, p, store_ctx, effective_gp_ids, emit)

        result = {
            "annotation_set_id": str(annotation_set.id),
            "pages": totals.pages,
            "total_lines_read": totals.lines,
            "annotations_inserted": totals.inserted,
            "annotations_skipped": totals.skipped,
            "elapsed_seconds": time.perf_counter() - t0,
        }
        emit("load_quickgo_annotations.done", None, result, "info")
        return OperationResult(result=result)

    def _create_annotation_set(
        self,
        session: Session,
        p: LoadQuickGOAnnotationsPayload,
        snapshot_id: uuid.UUID,
        emit: EmitFn,
    ) -> AnnotationSet:
        annotation_set = AnnotationSet(
            source="quickgo",
            source_version=p.source_version,
            ontology_snapshot_id=snapshot_id,
            meta={"quickgo_base_url": p.quickgo_base_url},
        )
        session.add(annotation_set)
        session.flush()
        emit(
            "load_quickgo_annotations.annotation_set_created",
            None,
            {"annotation_set_id": str(annotation_set.id)},
            "info",
        )
        return annotation_set

    def _stream_and_store(
        self,
        session: Session,
        p: LoadQuickGOAnnotationsPayload,
        store_ctx: _QuickGoStoreCtx,
        effective_gp_ids: list[str] | None,
        emit: EmitFn,
    ) -> _QuickGoPageTotals:
        """Stream QuickGO records, page-flush via ``_flush_page``, return totals.

        Honours ``p.total_limit`` (early break) and ``p.commit_every_page``.
        """
        totals = _QuickGoPageTotals()
        buffer: list[QuickGoAnnotationRecord] = []
        for record in self._stream_quickgo(p, emit, gene_product_ids=effective_gp_ids):
            totals.lines += 1
            if p.total_limit is not None and totals.inserted >= p.total_limit:
                emit(
                    "load_quickgo_annotations.limit_reached",
                    None,
                    {"total_limit": p.total_limit},
                    "warning",
                )
                break
            buffer.append(record)
            if len(buffer) >= p.page_size:
                self._flush_page(session, buffer, store_ctx, totals, emit)
                if p.commit_every_page:
                    session.commit()
        if buffer:
            self._flush_page(session, buffer, store_ctx, totals, emit=None)
        return totals

    def _flush_page(
        self,
        session: Session,
        buffer: list[QuickGoAnnotationRecord],
        store_ctx: _QuickGoStoreCtx,
        totals: _QuickGoPageTotals,
        emit: EmitFn | None,
    ) -> None:
        """Flush the current buffer into the DB and bump ``totals``.

        Final flush after the loop passes ``emit=None`` to skip the per-page
        progress event; in-loop flushes pass the real emit.
        """
        inserted, skipped = self._store_buffer(
            session,
            buffer,
            store_ctx.annotation_set_id,
            store_ctx.protein_accessions,
            store_ctx.go_term_map,
            store_ctx.eco_map,
        )
        totals.pages += 1
        totals.inserted += inserted
        totals.skipped += skipped
        buffer.clear()
        if emit is not None:
            emit(
                "load_quickgo_annotations.page_done",
                None,
                {
                    "page": totals.pages,
                    "total_lines": totals.lines,
                    "total_inserted": totals.inserted,
                    "total_skipped": totals.skipped,
                },
                "info",
            )

    # ── helpers ──────────────────────────────────────────────────────────────

    def _load_accessions(self, session: Session, emit: EmitFn) -> tuple[set[str], set[str]]:
        """Returns (canonical_accessions, protein_accessions).

        canonical_accessions — used to build the QuickGO geneProductId filter.
        protein_accessions   — actual protein.accession values; used for FK-safe
                               filtering before insertion.
        """
        emit("load_quickgo_annotations.load_accessions_start", None, {}, "info")
        canonical_accessions = set(session.scalars(select(distinct(Protein.canonical_accession))))
        protein_accessions = set(session.scalars(select(distinct(Protein.accession))))
        emit(
            "load_quickgo_annotations.load_accessions_done",
            None,
            {
                "canonical_accessions": len(canonical_accessions),
                "protein_accessions": len(protein_accessions),
            },
            "info",
        )
        return canonical_accessions, protein_accessions

    def _load_go_term_map(
        self, session: Session, snapshot_id: uuid.UUID, emit: EmitFn
    ) -> dict[str, int]:
        emit("load_quickgo_annotations.load_go_terms_start", None, {}, "info")
        rows = (
            session.query(GOTerm.go_id, GOTerm.id)
            .filter(GOTerm.ontology_snapshot_id == snapshot_id)
            .all()
        )
        mapping = {go_id: term_id for go_id, term_id in rows}
        emit(
            "load_quickgo_annotations.load_go_terms_done", None, {"go_terms": len(mapping)}, "info"
        )
        return mapping

    def _load_eco_mapping(self, p: LoadQuickGOAnnotationsPayload, emit: EmitFn) -> dict[str, str]:
        """Delegate to the protea-sources QuickGoSource auxiliary fetch.

        D-MIGR-05 of F2A.6-real: ECO mapping is a separate small-file
        fetch (single shot, in-memory dict). The operation calls it
        once before iterating ``_stream_quickgo`` and caches the
        result for the duration of the load.
        """
        if not p.eco_mapping_url:
            return {}
        from protea_sources.quickgo import plugin as quickgo_plugin

        return quickgo_plugin.fetch_eco_mapping(
            EcoMappingPayload(url=p.eco_mapping_url),
            emit=emit,
        )

    def _stream_quickgo(
        self,
        p: LoadQuickGOAnnotationsPayload,
        emit: EmitFn,
        gene_product_ids: list[str] | None = None,
    ) -> Iterator[QuickGoAnnotationRecord]:
        """Delegate to the protea-sources QuickGoSource plugin.

        The plugin owns HTTP, TSV header detection, multi-batch URL
        construction (to dodge QuickGO's 400-on-long-URL response), and
        record construction. The operation owns DB filtering, GO term
        resolution, ECO map application, and bulk insert. See
        ``f2a6_real_migration_design.md`` (D-MIGR-01, D-MIGR-02,
        D-MIGR-05).
        """
        from protea_sources.quickgo import plugin as quickgo_plugin

        effective_ids = gene_product_ids or p.gene_product_ids
        yield from quickgo_plugin.stream(
            QuickGoStreamPayload(
                quickgo_base_url=p.quickgo_base_url,
                gene_product_ids=effective_ids,
                gene_product_batch_size=p.gene_product_batch_size,
                timeout_seconds=p.timeout_seconds,
            ),
            emit=emit,
        )

    def _store_buffer(
        self,
        session: Session,
        records: list[QuickGoAnnotationRecord],
        annotation_set_id: uuid.UUID,
        valid_accessions: set[str],
        go_term_map: dict[str, int],
        eco_map: dict[str, str],
    ) -> tuple[int, int]:
        to_add: list[dict] = []
        skipped = 0

        for rec in records:
            if rec.accession not in valid_accessions:
                skipped += 1
                continue

            go_term_id = go_term_map.get(rec.go_id)
            if go_term_id is None:
                skipped += 1
                continue

            evidence_code = (
                eco_map.get(rec.eco_id, rec.eco_id) if rec.eco_id else None
            )

            to_add.append(
                {
                    "annotation_set_id": annotation_set_id,
                    "protein_accession": rec.accession,
                    "go_term_id": go_term_id,
                    "qualifier": rec.qualifier,
                    "evidence_code": evidence_code,
                    "assigned_by": rec.assigned_by,
                    "db_reference": rec.db_reference,
                    "with_from": rec.with_from,
                    "annotation_date": rec.annotation_date,
                }
            )

        if to_add:
            from sqlalchemy.dialects.postgresql import insert as pg_insert

            chunk_size = 5000
            for i in range(0, len(to_add), chunk_size):
                chunk = to_add[i : i + chunk_size]
                stmt = pg_insert(ProteinGOAnnotation.__table__).values(chunk)
                stmt = stmt.on_conflict_do_nothing(constraint="uq_pga_set_protein_term_evidence")
                session.execute(stmt)

        return len(to_add), skipped
