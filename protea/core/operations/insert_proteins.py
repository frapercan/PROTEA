from __future__ import annotations

import time
from collections.abc import Iterator
from collections.abc import Sequence as Seq
from dataclasses import dataclass
from typing import Annotated, Any

from protea_contracts import UniProtFastaStreamPayload, UniProtProteinRecord
from pydantic import Field, field_validator
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, Operation, OperationResult, ProteaPayload
from protea.core.utils import chunks
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.sequence.sequence import Sequence as SequenceModel

PositiveInt = Annotated[int, Field(gt=0)]
NonNegativeFloat = Annotated[float, Field(ge=0.0)]


@dataclass
class _InsertTotals:
    """Mutable counters threaded through the per-page flush loop."""

    pages: int = 0
    retrieved: int = 0
    isoforms: int = 0
    proteins_inserted: int = 0
    proteins_updated: int = 0
    sequences_inserted: int = 0
    sequences_reused: int = 0


class InsertProteinsPayload(ProteaPayload, frozen=True):
    search_criteria: str
    page_size: PositiveInt = 500
    total_limit: PositiveInt | None = None
    timeout_seconds: PositiveInt = 60
    include_isoforms: bool = True
    compressed: bool = False
    max_retries: PositiveInt = 6
    backoff_base_seconds: NonNegativeFloat = 0.8
    backoff_max_seconds: NonNegativeFloat = 20.0
    jitter_seconds: NonNegativeFloat = 0.4
    user_agent: str = "PROTEA/insert_proteins (contact: you@example.org)"

    @field_validator("search_criteria", "user_agent", mode="before")
    @classmethod
    def must_be_non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()


class InsertProteinsOperation(Operation):
    """Fetches protein sequences from UniProt (FASTA) and upserts them into the DB.

    Uses cursor-based pagination, exponential backoff with jitter, and MD5-based
    sequence deduplication. Many proteins can share one Sequence row.
    Isoforms (``<canonical>-<n>``) are stored as separate Protein rows grouped
    by ``canonical_accession``.
    """

    name = "insert_proteins"
    description = (
        "Fetch protein sequences from UniProt (FASTA, cursor-paginated) and upsert "
        "Protein + Sequence rows; isoforms are stored grouped by canonical accession."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        criteria = (payload or {}).get("search_criteria")
        limit = (payload or {}).get("total_limit")
        bits = []
        if criteria:
            short = str(criteria)
            if len(short) > 60:
                short = short[:57] + "..."
            bits.append(f"query={short}")
        if limit:
            bits.append(f"limit={limit}")
        return " · ".join(bits)

    def __init__(self) -> None:
        # Plugin instance is reused across executions for connection
        # pooling; counters are reset by the plugin at the start of
        # each ``stream_fasta`` call.
        from protea_sources.uniprot import plugin as _uniprot_plugin

        self._uniprot_plugin = _uniprot_plugin

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = InsertProteinsPayload.model_validate(payload)
        t0 = time.perf_counter()
        emit(
            "insert_proteins.start",
            None,
            {"search_criteria": p.search_criteria, "page_size": p.page_size},
            "info",
        )

        totals = _InsertTotals()
        buffer: list[UniProtProteinRecord] = []
        for record in self._stream_fasta(p, emit):
            if p.total_limit is not None and totals.retrieved >= p.total_limit:
                emit(
                    "insert_proteins.limit_reached",
                    None,
                    {"total_limit": p.total_limit},
                    "warning",
                )
                break
            buffer.append(record)
            totals.retrieved += 1
            if record.isoform_index is not None:
                totals.isoforms += 1
            if len(buffer) >= p.page_size:
                self._flush_page(session, buffer, p, emit, totals)
                buffer.clear()

        if buffer:
            totals.pages += 1
            ins_p, upd_p, ins_s, re_s = self._store_records(session, buffer, emit)
            totals.proteins_inserted += ins_p
            totals.proteins_updated += upd_p
            totals.sequences_inserted += ins_s
            totals.sequences_reused += re_s

        http_req, http_ret = self._uniprot_plugin.http_counters
        result_dict = {
            "pages": totals.pages,
            "retrieved_records": totals.retrieved,
            "isoform_records": totals.isoforms,
            "proteins_inserted": totals.proteins_inserted,
            "proteins_updated": totals.proteins_updated,
            "sequences_inserted": totals.sequences_inserted,
            "sequences_reused": totals.sequences_reused,
            "http_requests": http_req,
            "http_retries": http_ret,
            "elapsed_seconds": time.perf_counter() - t0,
        }
        emit("insert_proteins.done", None, result_dict, "info")
        return OperationResult(result=result_dict)

    def _flush_page(
        self,
        session: Session,
        buffer: list[UniProtProteinRecord],
        p: InsertProteinsPayload,
        emit: EmitFn,
        totals: _InsertTotals,
    ) -> None:
        """Persist one page worth of buffered records + emit progress."""
        totals.pages += 1
        ins_p, upd_p, ins_s, re_s = self._store_records(session, buffer, emit)
        totals.proteins_inserted += ins_p
        totals.proteins_updated += upd_p
        totals.sequences_inserted += ins_s
        totals.sequences_reused += re_s
        http_req, http_ret = self._uniprot_plugin.http_counters
        fields: dict[str, Any] = {
            "page": totals.pages,
            "retrieved_total": totals.retrieved,
            "proteins_inserted_total": totals.proteins_inserted,
            "proteins_updated_total": totals.proteins_updated,
            "sequences_inserted_total": totals.sequences_inserted,
            "sequences_reused_total": totals.sequences_reused,
            "http_requests": http_req,
            "http_retries": http_ret,
            "_progress_current": totals.retrieved,
        }
        if p.total_limit:
            fields["_progress_total"] = p.total_limit
        emit("insert_proteins.page_done", None, fields, "info")

    def _stream_fasta(
        self, p: InsertProteinsPayload, emit: EmitFn
    ) -> Iterator[UniProtProteinRecord]:
        """Delegate to the protea-sources UniProtSource plugin.

        Plugin owns HTTP retries, cursor pagination, gzip decoding,
        and FASTA parsing. The operation owns batching, dedup, and
        bulk insert. See ``f2a6_real_migration_design.md``.
        """
        yield from self._uniprot_plugin.stream_fasta(
            UniProtFastaStreamPayload(
                search_criteria=p.search_criteria,
                page_size=p.page_size,
                timeout_seconds=p.timeout_seconds,
                include_isoforms=p.include_isoforms,
                compressed=p.compressed,
                max_retries=p.max_retries,
                backoff_base_seconds=p.backoff_base_seconds,
                backoff_max_seconds=p.backoff_max_seconds,
                jitter_seconds=p.jitter_seconds,
                user_agent=p.user_agent,
            ),
            emit=emit,
        )

    # ---- DB storage ----
    def _store_records(
        self, session: Session, records: list[UniProtProteinRecord], emit: EmitFn
    ) -> tuple[int, int, int, int]:
        if not records:
            return 0, 0, 0, 0

        # 1) Deduplicate sequences
        hash_to_seq: dict[str, str] = {}
        for r in records:
            if r.sequence_hash not in hash_to_seq:
                hash_to_seq[r.sequence_hash] = r.sequence

        unique_hashes = list(hash_to_seq.keys())
        emit("db.lookup_sequences_start", None, {"count": len(unique_hashes)}, "info")

        existing_seq_ids = self._load_existing_sequences(session, unique_hashes)
        sequences_reused = len(existing_seq_ids)

        emit("db.lookup_sequences_done", None, {"existing": sequences_reused}, "info")

        # Insert missing sequences
        missing_hashes = [h for h in unique_hashes if h not in existing_seq_ids]
        sequences_inserted = 0

        if missing_hashes:
            emit("db.insert_sequences_start", None, {"rows": len(missing_hashes)}, "info")

            new_sequences = [
                SequenceModel(sequence=hash_to_seq[h], sequence_hash=h) for h in missing_hashes
            ]
            session.add_all(new_sequences)
            session.flush()

            for s in new_sequences:
                existing_seq_ids[s.sequence_hash] = s.id

            sequences_inserted = len(new_sequences)
            emit("db.insert_sequences_done", None, {"rows": sequences_inserted}, "info")

        # 2) Load existing proteins
        accessions = [r.accession for r in records]
        existing_prot = self._load_existing_proteins(session, accessions)

        # 3) Upsert proteins (insert new, conservative update existing)
        proteins_inserted = 0
        proteins_updated = 0
        to_add: list[Protein] = []

        for r in records:
            seq_id = existing_seq_ids[r.sequence_hash]

            if r.accession in existing_prot:
                p = existing_prot[r.accession]
                changed = False

                if getattr(p, "sequence_id", None) is None and seq_id is not None:
                    p.sequence_id = seq_id
                    changed = True

                if getattr(p, "entry_name", None) in (None, "") and r.entry_name:
                    p.entry_name = r.entry_name
                    changed = True

                if getattr(p, "canonical_accession", None) != r.canonical_accession:
                    p.canonical_accession = r.canonical_accession
                    changed = True

                if getattr(p, "is_canonical", None) != r.is_canonical:
                    p.is_canonical = r.is_canonical
                    changed = True

                if getattr(p, "isoform_index", None) != r.isoform_index:
                    p.isoform_index = r.isoform_index
                    changed = True

                if getattr(p, "reviewed", None) is None:
                    p.reviewed = r.reviewed
                    changed = True

                if getattr(p, "taxonomy_id", None) in (None, "") and r.taxonomy_id:
                    p.taxonomy_id = r.taxonomy_id
                    changed = True

                if getattr(p, "organism", None) in (None, "") and r.organism:
                    p.organism = r.organism
                    changed = True

                if getattr(p, "gene_name", None) in (None, "") and r.gene_name:
                    p.gene_name = r.gene_name
                    changed = True

                if getattr(p, "length", None) is None:
                    p.length = r.length
                    changed = True

                if changed:
                    proteins_updated += 1

            else:
                to_add.append(
                    Protein(
                        accession=r.accession,
                        canonical_accession=r.canonical_accession,
                        is_canonical=r.is_canonical,
                        isoform_index=r.isoform_index,
                        reviewed=r.reviewed,
                        entry_name=r.entry_name,
                        organism=r.organism,
                        taxonomy_id=r.taxonomy_id,
                        gene_name=r.gene_name,
                        length=r.length,
                        sequence_id=seq_id,
                    )
                )
                proteins_inserted += 1

        if to_add:
            emit("db.insert_proteins_start", None, {"rows": len(to_add)}, "info")
            session.add_all(to_add)
            session.flush()
            emit("db.insert_proteins_done", None, {"rows": len(to_add)}, "info")

        return proteins_inserted, proteins_updated, sequences_inserted, sequences_reused

    def _load_existing_sequences(
        self, session: Session, hashes: Seq[str], chunk_size: int = 5000
    ) -> dict[str, int]:
        existing: dict[str, int] = {}
        for chunk in chunks(hashes, chunk_size):
            rows = (
                session.query(SequenceModel.sequence_hash, SequenceModel.id)
                .filter(SequenceModel.sequence_hash.in_(chunk))
                .all()
            )
            for h, sid in rows:
                existing[h] = sid
        return existing

    def _load_existing_proteins(
        self, session: Session, accessions: Seq[str], chunk_size: int = 5000
    ) -> dict[str, Protein]:
        existing: dict[str, Protein] = {}
        for chunk in chunks(accessions, chunk_size):
            rows = session.query(Protein).filter(Protein.accession.in_(chunk)).all()
            for p in rows:
                existing[p.accession] = p
        return existing
