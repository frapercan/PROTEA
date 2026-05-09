from __future__ import annotations

import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Annotated, Any, NamedTuple

from protea_contracts import GoaAnnotationRecord, GoaStreamPayload
from pydantic import Field, field_validator
from sqlalchemy import distinct, select
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.annotation.protein_go_annotation import ProteinGOAnnotation
from protea.infrastructure.orm.models.job import Job, JobEvent
from protea.infrastructure.orm.models.protein.protein import Protein

_AUTO_EVAL_QUEUE = "protea.jobs"

PositiveInt = Annotated[int, Field(gt=0)]


class _GoaStoreCtx(NamedTuple):
    """Immutable per-stream context handed to ``_store_buffer`` / ``_flush_page``."""

    annotation_set_id: uuid.UUID
    canonical_accessions: set[str]
    go_term_map: dict[str, int]


@dataclass
class _GoaPageTotals:
    """Mutable accumulator for the GAF page loop."""

    pages: int = 0
    lines: int = 0
    inserted: int = 0
    skipped: int = 0


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

    The GAF file is streamed line by line from ``gaf_url``; it is never fully
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
    description = (
        "Stream a UniProt GOA GAF release line by line and bulk-insert "
        "ProteinGOAnnotation rows for accessions already present in the DB."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        p = payload or {}
        version = p.get("source_version")
        url = p.get("gaf_url", "")
        # Strip everything before the filename to keep it short
        filename = url.rsplit("/", 1)[-1] if url else ""
        bits = []
        if version:
            bits.append(f"release={version}")
        if filename:
            bits.append(filename)
        if p.get("total_limit"):
            bits.append(f"limit={p['total_limit']}")
        return " · ".join(bits)

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = LoadGOAAnnotationsPayload.model_validate(payload)

        snapshot_id = uuid.UUID(p.ontology_snapshot_id)
        snapshot = session.get(OntologySnapshot, snapshot_id)
        if snapshot is None:
            raise ValueError(f"OntologySnapshot {p.ontology_snapshot_id} not found")

        t0 = time.perf_counter()
        emit(
            "load_goa_annotations.start",
            None,
            {
                "gaf_url": p.gaf_url,
                "ontology_snapshot_id": p.ontology_snapshot_id,
            },
            "info",
        )

        canonical_accessions = self._load_accessions(session, emit)
        if not canonical_accessions:
            emit("load_goa_annotations.no_proteins", None, {}, "warning")
            return OperationResult(result={"annotations_inserted": 0})

        go_term_map = self._load_go_term_map(session, snapshot_id, emit)
        annotation_set = self._create_annotation_set(session, p, snapshot_id, emit)
        store_ctx = _GoaStoreCtx(
            annotation_set_id=annotation_set.id,
            canonical_accessions=canonical_accessions,
            go_term_map=go_term_map,
        )
        totals = self._stream_and_store(session, p, store_ctx, emit)

        result: dict[str, Any] = {
            "annotation_set_id": str(annotation_set.id),
            "pages": totals.pages,
            "total_lines_read": totals.lines,
            "annotations_inserted": totals.inserted,
            "annotations_skipped": totals.skipped,
            "elapsed_seconds": time.perf_counter() - t0,
        }

        # Auto-trigger an atomic generate_evaluation_set against the latest
        # prior goa AnnotationSet (numeric source_version sort).  Cascade
        # cumulative deltas (V_K → V_target) remain manual via
        # scripts/materialize_lab_intervals.py --mode cascade.
        publish_after_commit: list[tuple[str, uuid.UUID]] = []
        child_job_id = self._maybe_enqueue_atomic_eval(session, annotation_set, emit)
        if child_job_id is not None:
            publish_after_commit.append((_AUTO_EVAL_QUEUE, child_job_id))
            result["auto_eval_job_id"] = str(child_job_id)

        emit("load_goa_annotations.done", None, result, "info")
        return OperationResult(result=result, publish_after_commit=publish_after_commit)

    def _create_annotation_set(
        self,
        session: Session,
        p: LoadGOAAnnotationsPayload,
        snapshot_id: uuid.UUID,
        emit: EmitFn,
    ) -> AnnotationSet:
        annotation_set = AnnotationSet(
            source="goa",
            source_version=p.source_version,
            ontology_snapshot_id=snapshot_id,
            meta={"gaf_url": p.gaf_url},
        )
        session.add(annotation_set)
        session.flush()
        emit(
            "load_goa_annotations.annotation_set_created",
            None,
            {"annotation_set_id": str(annotation_set.id)},
            "info",
        )
        return annotation_set

    def _stream_and_store(
        self,
        session: Session,
        p: LoadGOAAnnotationsPayload,
        store_ctx: _GoaStoreCtx,
        emit: EmitFn,
    ) -> _GoaPageTotals:
        """Stream GAF records, page-flush via ``_flush_page``, return totals.

        Honours ``p.total_limit`` (early break) and ``p.commit_every_page``.
        """
        totals = _GoaPageTotals()
        buffer: list[GoaAnnotationRecord] = []
        for record in self._stream_gaf(p, emit):
            totals.lines += 1
            if p.total_limit is not None and totals.inserted >= p.total_limit:
                emit(
                    "load_goa_annotations.limit_reached",
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
        buffer: list[GoaAnnotationRecord],
        store_ctx: _GoaStoreCtx,
        totals: _GoaPageTotals,
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
            store_ctx.canonical_accessions,
            store_ctx.go_term_map,
        )
        totals.pages += 1
        totals.inserted += inserted
        totals.skipped += skipped
        buffer.clear()
        if emit is not None:
            emit(
                "load_goa_annotations.page_done",
                None,
                {
                    "page": totals.pages,
                    "total_lines": totals.lines,
                    "total_inserted": totals.inserted,
                    "total_skipped": totals.skipped,
                },
                "info",
            )

    @staticmethod
    def _numeric_version_key(v: str | None) -> tuple[int, str]:
        """Sort key that orders ``"160" < "211" < "215"`` numerically and
        falls back to lexicographic for non-numeric values (which sort last)."""
        if v is None:
            return (10**9, "")
        try:
            return (int(v), "")
        except (TypeError, ValueError):
            return (10**9, str(v))

    def _maybe_enqueue_atomic_eval(
        self,
        session: Session,
        new_set: AnnotationSet,
        emit: EmitFn,
    ) -> uuid.UUID | None:
        prior = self._select_prior_annotation_set(session, new_set)
        if prior is None:
            emit(
                "load_goa_annotations.auto_eval_skipped",
                None,
                {"reason": "no_prior_goa_annotation_set"},
                "info",
            )
            return None
        existing_id = self._existing_evaluation_set_id(session, prior, new_set)
        if existing_id is not None:
            emit(
                "load_goa_annotations.auto_eval_skipped",
                None,
                {
                    "reason": "evaluation_set_exists",
                    "existing_evaluation_set_id": str(existing_id),
                    "old_annotation_set_id": str(prior.id),
                    "new_annotation_set_id": str(new_set.id),
                },
                "info",
            )
            return None
        return self._enqueue_auto_eval_job(session, prior, new_set, emit)

    def _select_prior_annotation_set(
        self,
        session: Session,
        new_set: AnnotationSet,
    ) -> AnnotationSet | None:
        """Return the most recent ``goa`` set strictly older than ``new_set``."""
        candidates = (
            session.query(AnnotationSet)
            .filter(AnnotationSet.source == "goa", AnnotationSet.id != new_set.id)
            .all()
        )
        new_key = self._numeric_version_key(new_set.source_version)
        prior_candidates = [
            s for s in candidates if self._numeric_version_key(s.source_version) < new_key
        ]
        if not prior_candidates:
            return None
        return max(prior_candidates, key=lambda s: self._numeric_version_key(s.source_version))

    def _existing_evaluation_set_id(
        self,
        session: Session,
        prior: AnnotationSet,
        new_set: AnnotationSet,
    ) -> uuid.UUID | None:
        """Look up an EvaluationSet already covering this (old, new) pair."""
        row = (
            session.query(EvaluationSet.id)
            .filter(
                EvaluationSet.old_annotation_set_id == prior.id,
                EvaluationSet.new_annotation_set_id == new_set.id,
            )
            .first()
        )
        return row[0] if row is not None else None

    def _enqueue_auto_eval_job(
        self,
        session: Session,
        prior: AnnotationSet,
        new_set: AnnotationSet,
        emit: EmitFn,
    ) -> uuid.UUID:
        """Create a ``generate_evaluation_set`` child job + audit trail."""
        payload = {
            "old_annotation_set_id": str(prior.id),
            "new_annotation_set_id": str(new_set.id),
        }
        child = Job(
            operation="generate_evaluation_set",
            queue_name=_AUTO_EVAL_QUEUE,
            payload=payload,
        )
        session.add(child)
        session.flush()
        session.add(
            JobEvent(
                job_id=child.id,
                event="job.created",
                fields={
                    "operation": "generate_evaluation_set",
                    "queue": _AUTO_EVAL_QUEUE,
                    "trigger": "load_goa_annotations.auto",
                    "old_annotation_set_id": str(prior.id),
                    "new_annotation_set_id": str(new_set.id),
                },
            )
        )
        emit(
            "load_goa_annotations.auto_eval_enqueued",
            None,
            {
                "child_job_id": str(child.id),
                "old_annotation_set_id": str(prior.id),
                "new_annotation_set_id": str(new_set.id),
                "old_source_version": prior.source_version,
                "new_source_version": new_set.source_version,
            },
            "info",
        )
        return child.id

    def _load_accessions(self, session: Session, emit: EmitFn) -> set[str]:
        emit("load_goa_annotations.load_accessions_start", None, {}, "info")
        accessions = set(session.scalars(select(distinct(Protein.canonical_accession))))
        emit(
            "load_goa_annotations.load_accessions_done",
            None,
            {"canonical_accessions": len(accessions)},
            "info",
        )
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
    ) -> Iterator[GoaAnnotationRecord]:
        """Delegate to the protea-sources GoaSource plugin.

        The plugin owns HTTP, gzip decoding, and GAF line parsing; the
        operation owns DB filtering, GO term resolution, dedup, and
        bulk insert. See ``f2a6_real_migration_design.md`` (D-MIGR-01,
        D-MIGR-02, D-MIGR-06).
        """
        from protea_sources.goa import plugin as goa_plugin

        yield from goa_plugin.stream(
            GoaStreamPayload(gaf_url=p.gaf_url, timeout_seconds=p.timeout_seconds),
            emit=emit,
        )

    def _store_buffer(
        self,
        session: Session,
        records: list[GoaAnnotationRecord],
        annotation_set_id: uuid.UUID,
        valid_accessions: set[str],
        go_term_map: dict[str, int],
    ) -> tuple[int, int]:
        to_add: list[dict] = []
        skipped = 0
        seen: set[tuple] = set()

        for rec in records:
            accession = rec.accession.strip()
            if not accession or accession not in valid_accessions:
                skipped += 1
                continue

            go_id = rec.go_id.strip()
            go_term_id = go_term_map.get(go_id)
            if go_term_id is None:
                skipped += 1
                continue

            evidence_code = rec.evidence_code
            dedup_key = (annotation_set_id, accession, go_term_id, evidence_code)
            if dedup_key in seen:
                skipped += 1
                continue
            seen.add(dedup_key)

            to_add.append(
                {
                    "annotation_set_id": annotation_set_id,
                    "protein_accession": accession,
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
