from __future__ import annotations

import gzip
import io
import time
import uuid
from collections.abc import Iterator
from typing import Annotated, Any

import requests
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


class LoadGOAAnnotationsPayload(ProteaPayload, frozen=True):
    ontology_snapshot_id: str
    gaf_url: str
    source_version: str
    page_size: PositiveInt = 10000
    timeout_seconds: PositiveInt = 300
    commit_every_page: bool = True
    total_limit: PositiveInt | None = None

    @field_validator("ontology_snapshot_id", "gaf_url", "source_version", mode="before")
    @classmethod
    def must_be_non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()


class LoadGOAAnnotationsOperation:
    """Streams a GOA GAF file (gzip or plain) and upserts ProteinGOAnnotation rows.

    The GAF file is streamed line by line from ``gaf_url`` — it is never fully
    loaded into memory, making it suitable for the full UniProt GAF (hundreds
    of millions of lines).

    Only accessions present in the ``protein`` table are stored; all others are
    silently skipped. The canonical accession set is loaded once from the DB at
    the start of the operation.

    GAF 2.2 columns used (1-indexed, tab-separated):
      2  → DB_Object_ID (accession)
      5  → GO ID
      4  → Qualifier
      7  → Evidence Code
      6  → DB:Reference
      8  → With/From
      15 → Assigned By
      14 → Date (YYYYMMDD)
    """

    name = "load_goa_annotations"

    # GAF 2.x column indices (0-based after splitting on tab)
    _IDX_ACCESSION = 1
    _IDX_QUALIFIER = 3
    _IDX_GO_ID = 4
    _IDX_DB_REFERENCE = 5
    _IDX_EVIDENCE = 6
    _IDX_WITH_FROM = 7
    _IDX_ASSIGNED_BY = 14
    _IDX_DATE = 13

    def execute(self, session: Session, payload: dict[str, Any], *, emit: EmitFn) -> OperationResult:
        p = LoadGOAAnnotationsPayload.model_validate(payload)

        snapshot_id = uuid.UUID(p.ontology_snapshot_id)
        snapshot = session.get(OntologySnapshot, snapshot_id)
        if snapshot is None:
            raise ValueError(f"OntologySnapshot {p.ontology_snapshot_id} not found")

        t0 = time.perf_counter()
        emit("load_goa_annotations.start", None, {
            "gaf_url": p.gaf_url,
            "ontology_snapshot_id": p.ontology_snapshot_id,
        }, "info")

        canonical_accessions = self._load_accessions(session, emit)
        if not canonical_accessions:
            emit("load_goa_annotations.no_proteins", None, {}, "warning")
            return OperationResult(result={"annotations_inserted": 0})

        go_term_map = self._load_go_term_map(session, snapshot_id, emit)

        annotation_set = AnnotationSet(
            source="goa",
            source_version=p.source_version,
            ontology_snapshot_id=snapshot_id,
            meta={"gaf_url": p.gaf_url},
        )
        session.add(annotation_set)
        session.flush()

        emit("load_goa_annotations.annotation_set_created", None,
             {"annotation_set_id": str(annotation_set.id)}, "info")

        total_lines = 0
        total_inserted = 0
        total_skipped = 0
        pages = 0
        buffer: list[dict[str, str]] = []

        for record in self._stream_gaf(p, emit):
            total_lines += 1

            if p.total_limit is not None and total_inserted >= p.total_limit:
                emit("load_goa_annotations.limit_reached", None, {"total_limit": p.total_limit}, "warning")
                break

            buffer.append(record)

            if len(buffer) >= p.page_size:
                pages += 1
                inserted, skipped = self._store_buffer(
                    session, buffer, annotation_set.id, canonical_accessions, go_term_map
                )
                total_inserted += inserted
                total_skipped += skipped
                buffer.clear()

                emit("load_goa_annotations.page_done", None, {
                    "page": pages,
                    "total_lines": total_lines,
                    "total_inserted": total_inserted,
                    "total_skipped": total_skipped,
                }, "info")

                if p.commit_every_page:
                    session.commit()

        # flush remaining
        if buffer:
            pages += 1
            inserted, skipped = self._store_buffer(
                session, buffer, annotation_set.id, canonical_accessions, go_term_map
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
        emit("load_goa_annotations.done", None, result, "info")
        return OperationResult(result=result)

    def _load_accessions(self, session: Session, emit: EmitFn) -> set[str]:
        emit("load_goa_annotations.load_accessions_start", None, {}, "info")
        accessions = set(session.scalars(select(distinct(Protein.canonical_accession))))
        emit("load_goa_annotations.load_accessions_done", None,
             {"canonical_accessions": len(accessions)}, "info")
        return accessions

    def _load_go_term_map(
        self, session: Session, snapshot_id: uuid.UUID, emit: EmitFn
    ) -> dict[str, int]:
        emit("load_goa_annotations.load_go_terms_start", None, {}, "info")
        rows = (
            session.query(GOTerm.go_id, GOTerm.id)
            .filter(GOTerm.ontology_snapshot_id == snapshot_id)
            .all()
        )
        mapping = {go_id: term_id for go_id, term_id in rows}
        emit("load_goa_annotations.load_go_terms_done", None, {"go_terms": len(mapping)}, "info")
        return mapping

    def _stream_gaf(
        self, p: LoadGOAAnnotationsPayload, emit: EmitFn
    ) -> Iterator[dict[str, str]]:
        emit("load_goa_annotations.download_start", None, {"gaf_url": p.gaf_url}, "info")
        resp = requests.get(p.gaf_url, stream=True, timeout=p.timeout_seconds)
        resp.raise_for_status()

        compressed = p.gaf_url.endswith(".gz")
        raw_stream = resp.raw
        raw_stream.decode_content = True

        stream: io.TextIOWrapper
        if compressed:
            gz = gzip.GzipFile(fileobj=raw_stream)
            stream = io.TextIOWrapper(gz, encoding="utf-8", errors="replace")
        else:
            stream = io.TextIOWrapper(raw_stream, encoding="utf-8", errors="replace")

        with stream:
            for raw in stream:
                line = raw.rstrip("\n")
                if not line or line.startswith("!"):
                    continue
                parts = line.split("\t")
                if len(parts) < 15:
                    continue
                yield {
                    "accession": parts[self._IDX_ACCESSION],
                    "go_id": parts[self._IDX_GO_ID],
                    "qualifier": parts[self._IDX_QUALIFIER],
                    "evidence_code": parts[self._IDX_EVIDENCE],
                    "db_reference": parts[self._IDX_DB_REFERENCE],
                    "with_from": parts[self._IDX_WITH_FROM],
                    "assigned_by": parts[self._IDX_ASSIGNED_BY],
                    "annotation_date": parts[self._IDX_DATE],
                }

    def _store_buffer(
        self,
        session: Session,
        records: list[dict[str, str]],
        annotation_set_id: uuid.UUID,
        valid_accessions: set[str],
        go_term_map: dict[str, int],
    ) -> tuple[int, int]:
        to_add: list[dict] = []
        skipped = 0
        seen: set[tuple] = set()

        for rec in records:
            accession = rec["accession"].strip()
            if not accession or accession not in valid_accessions:
                skipped += 1
                continue

            go_id = rec["go_id"].strip()
            go_term_id = go_term_map.get(go_id)
            if go_term_id is None:
                skipped += 1
                continue

            evidence_code = rec["evidence_code"] or None
            dedup_key = (annotation_set_id, accession, go_term_id, evidence_code)
            if dedup_key in seen:
                skipped += 1
                continue
            seen.add(dedup_key)

            to_add.append({
                "annotation_set_id": annotation_set_id,
                "protein_accession": accession,
                "go_term_id": go_term_id,
                "qualifier": rec["qualifier"] or None,
                "evidence_code": evidence_code,
                "assigned_by": rec["assigned_by"] or None,
                "db_reference": rec["db_reference"] or None,
                "with_from": rec["with_from"] or None,
                "annotation_date": rec["annotation_date"] or None,
            })

        if to_add:
            from sqlalchemy.dialects.postgresql import insert as pg_insert
            chunk_size = 5000
            for i in range(0, len(to_add), chunk_size):
                chunk = to_add[i: i + chunk_size]
                stmt = pg_insert(ProteinGOAnnotation.__table__).values(chunk)
                stmt = stmt.on_conflict_do_nothing(
                    constraint="uq_pga_set_protein_term_evidence"
                )
                session.execute(stmt)

        return len(to_add), skipped
