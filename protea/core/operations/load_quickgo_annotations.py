from __future__ import annotations

import io
import time
import uuid
from collections.abc import Iterator
from typing import Annotated, Any

import requests
from pydantic import Field, field_validator
from sqlalchemy import select, distinct
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.annotation.protein_go_annotation import ProteinGOAnnotation
from protea.infrastructure.orm.models.protein.protein import Protein

PositiveInt = Annotated[int, Field(gt=0)]


class LoadQuickGOAnnotationsPayload(ProteaPayload, frozen=True):
    """Payload for loading GO annotations from the QuickGO bulk download endpoint.

    QuickGO returns a single streamed TSV for a given set of filters (reviewed,
    taxon, aspect…). The download is filtered in-stream against the canonical
    accessions already present in the DB — no external accession list is needed.

    ``eco_mapping_url`` (optional) points to a GAF-ECO mapping file
    (space-separated: ``ECO:XXXXXXX  CODE``). When provided, ECO IDs are
    resolved to GO evidence codes (IDA, IEA…) before insertion. If omitted,
    the raw ECO ID is stored as-is in ``evidence_code``.
    """

    ontology_snapshot_id: str
    source_version: str
    quickgo_base_url: str = (
        "https://www.ebi.ac.uk/QuickGO/services/annotation/downloadSearch"
    )
    reviewed: bool = True
    taxon_ids: list[int] | None = None
    aspects: list[str] | None = None
    eco_mapping_url: str | None = None
    page_size: PositiveInt = 10000
    timeout_seconds: PositiveInt = 300
    commit_every_page: bool = True
    total_limit: PositiveInt | None = None

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

    def execute(self, session: Session, payload: dict[str, Any], *, emit: EmitFn) -> OperationResult:
        p = LoadQuickGOAnnotationsPayload.model_validate(payload)

        snapshot_id = uuid.UUID(p.ontology_snapshot_id)
        if session.get(OntologySnapshot, snapshot_id) is None:
            raise ValueError(f"OntologySnapshot {p.ontology_snapshot_id} not found")

        t0 = time.perf_counter()
        emit("load_quickgo_annotations.start", None, {
            "ontology_snapshot_id": p.ontology_snapshot_id,
            "source_version": p.source_version,
            "reviewed": p.reviewed,
            "taxon_ids": p.taxon_ids,
        }, "info")

        canonical_accessions = self._load_accessions(session, emit)
        if not canonical_accessions:
            emit("load_quickgo_annotations.no_proteins", None, {}, "warning")
            return OperationResult(result={"annotations_inserted": 0})

        go_term_map = self._load_go_term_map(session, snapshot_id, emit)
        eco_map = self._load_eco_mapping(p, emit)

        annotation_set = AnnotationSet(
            source="quickgo",
            source_version=p.source_version,
            ontology_snapshot_id=snapshot_id,
            meta={
                "quickgo_base_url": p.quickgo_base_url,
                "reviewed": p.reviewed,
                "taxon_ids": p.taxon_ids,
                "aspects": p.aspects,
            },
        )
        session.add(annotation_set)
        session.flush()

        emit("load_quickgo_annotations.annotation_set_created", None,
             {"annotation_set_id": str(annotation_set.id)}, "info")

        total_lines = 0
        total_inserted = 0
        total_skipped = 0
        pages = 0
        buffer: list[dict[str, str]] = []

        for record in self._stream_quickgo(p, emit):
            total_lines += 1

            if p.total_limit is not None and total_inserted >= p.total_limit:
                emit("load_quickgo_annotations.limit_reached", None,
                     {"total_limit": p.total_limit}, "warning")
                break

            buffer.append(record)

            if len(buffer) >= p.page_size:
                pages += 1
                inserted, skipped = self._store_buffer(
                    session, buffer, annotation_set.id,
                    canonical_accessions, go_term_map, eco_map,
                )
                total_inserted += inserted
                total_skipped += skipped
                buffer.clear()

                emit("load_quickgo_annotations.page_done", None, {
                    "page": pages,
                    "total_lines": total_lines,
                    "total_inserted": total_inserted,
                    "total_skipped": total_skipped,
                }, "info")

                if p.commit_every_page:
                    session.commit()

        if buffer:
            pages += 1
            inserted, skipped = self._store_buffer(
                session, buffer, annotation_set.id,
                canonical_accessions, go_term_map, eco_map,
            )
            total_inserted += inserted
            total_skipped += skipped

        elapsed = time.perf_counter() - t0
        result = {
            "annotation_set_id": str(annotation_set.id),
            "pages": pages,
            "total_lines_read": total_lines,
            "annotations_inserted": total_inserted,
            "annotations_skipped": total_skipped,
            "elapsed_seconds": elapsed,
        }
        emit("load_quickgo_annotations.done", None, result, "info")
        return OperationResult(result=result)

    # ── helpers ──────────────────────────────────────────────────────────────

    def _load_accessions(self, session: Session, emit: EmitFn) -> set[str]:
        emit("load_quickgo_annotations.load_accessions_start", None, {}, "info")
        accessions = set(session.scalars(select(distinct(Protein.canonical_accession))))
        emit("load_quickgo_annotations.load_accessions_done", None,
             {"canonical_accessions": len(accessions)}, "info")
        return accessions

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
        emit("load_quickgo_annotations.load_go_terms_done", None,
             {"go_terms": len(mapping)}, "info")
        return mapping

    def _load_eco_mapping(
        self, p: LoadQuickGOAnnotationsPayload, emit: EmitFn
    ) -> dict[str, str]:
        """Download and parse gaf-eco-mapping-derived.txt → {ECO:XXXXXXX: CODE}."""
        if not p.eco_mapping_url:
            return {}
        emit("load_quickgo_annotations.eco_mapping_start", None,
             {"url": p.eco_mapping_url}, "info")
        resp = requests.get(p.eco_mapping_url, timeout=60)
        resp.raise_for_status()
        mapping: dict[str, str] = {}
        for line in resp.text.splitlines():
            parts = line.strip().split()
            if len(parts) >= 2 and parts[0].startswith("ECO:"):
                mapping[parts[0]] = parts[1]
        emit("load_quickgo_annotations.eco_mapping_done", None,
             {"entries": len(mapping)}, "info")
        return mapping

    def _stream_quickgo(
        self, p: LoadQuickGOAnnotationsPayload, emit: EmitFn
    ) -> Iterator[dict[str, str]]:
        params: dict[str, Any] = {"geneProductType": "protein"}
        if p.reviewed:
            params["reviewed"] = "true"
        if p.taxon_ids:
            params["taxonId"] = ",".join(str(t) for t in p.taxon_ids)
        if p.aspects:
            params["aspect"] = ",".join(p.aspects)

        headers = {
            "Accept": "text/tsv",
            "User-Agent": "PROTEA/load_quickgo_annotations",
        }
        emit("load_quickgo_annotations.download_start", None, {"params": params}, "info")

        resp = requests.get(
            p.quickgo_base_url,
            params=params,
            headers=headers,
            stream=True,
            timeout=p.timeout_seconds,
        )
        resp.raise_for_status()

        resp.raw.decode_content = True
        stream = io.TextIOWrapper(resp.raw, encoding="utf-8", errors="replace")

        header: list[str] | None = None
        with stream:
            for raw in stream:
                line = raw.rstrip("\n")
                if not line:
                    continue
                parts = line.split("\t")
                if header is None:
                    header = parts
                    continue
                if len(parts) < len(header):
                    continue
                yield dict(zip(header, parts))

    def _store_buffer(
        self,
        session: Session,
        records: list[dict[str, str]],
        annotation_set_id: uuid.UUID,
        valid_accessions: set[str],
        go_term_map: dict[str, int],
        eco_map: dict[str, str],
    ) -> tuple[int, int]:
        to_add: list[ProteinGOAnnotation] = []
        skipped = 0

        for row in records:
            accession = row.get("GENE PRODUCT ID", "").strip()
            if not accession or accession not in valid_accessions:
                skipped += 1
                continue

            go_id = row.get("GO TERM", "").strip()
            go_term_id = go_term_map.get(go_id)
            if go_term_id is None:
                skipped += 1
                continue

            eco_id = row.get("ECO ID", "").strip() or None
            evidence_code = (eco_map.get(eco_id, eco_id) if eco_id else None)

            to_add.append(ProteinGOAnnotation(
                annotation_set_id=annotation_set_id,
                protein_accession=accession,
                go_term_id=go_term_id,
                qualifier=row.get("QUALIFIER", "").strip() or None,
                evidence_code=evidence_code,
                assigned_by=row.get("ASSIGNED BY", "").strip() or None,
                db_reference=row.get("REFERENCE", "").strip() or None,
                with_from=row.get("WITH/FROM", "").strip() or None,
                annotation_date=row.get("DATE", "").strip() or None,
            ))

        if to_add:
            session.add_all(to_add)
            session.flush()

        return len(to_add), skipped
