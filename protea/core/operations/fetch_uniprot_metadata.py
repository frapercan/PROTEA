from __future__ import annotations

import time
from collections.abc import Iterator, Sequence
from typing import Annotated, Any

from protea_contracts import (
    UniProtMetadataRecord,
    UniProtMetadataStreamPayload,
    parse_isoform,
)
from pydantic import Field, field_validator
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.utils import chunks
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.protein.protein_metadata import ProteinUniProtMetadata

PositiveInt = Annotated[int, Field(gt=0)]
NonNegativeFloat = Annotated[float, Field(ge=0.0)]


class FetchUniProtMetadataPayload(ProteaPayload, frozen=True):
    search_criteria: str
    page_size: PositiveInt = 500
    total_limit: PositiveInt | None = None
    timeout_seconds: PositiveInt = 60
    compressed: bool = True
    max_retries: PositiveInt = 6
    backoff_base_seconds: NonNegativeFloat = 0.8
    backoff_max_seconds: NonNegativeFloat = 20.0
    jitter_seconds: NonNegativeFloat = 0.4
    user_agent: str = "PROTEA/fetch_uniprot_metadata (contact: you@example.org)"
    commit_every_page: bool = True
    update_protein_core: bool = True

    @field_validator("search_criteria", "user_agent", mode="before")
    @classmethod
    def must_be_non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()


class FetchUniProtMetadataOperation:
    """Fetches functional annotations from UniProt (TSV) and upserts ProteinUniProtMetadata rows.

    One metadata row is stored per canonical accession. Isoforms share the same
    metadata record. Optionally updates core Protein fields (reviewed, organism,
    gene_name, length) if they are missing. Uses the same cursor-based pagination
    and backoff strategy as InsertProteinsOperation.
    """

    name = "fetch_uniprot_metadata"
    description = (
        "Fetch functional annotations (TSV) from UniProt and upsert "
        "ProteinUniProtMetadata rows keyed by canonical accession."
    )

    # UniProt field identifiers requested in the TSV query, in display
    # order. Kept here (operation-side) because the field set is a
    # persistence concern: it determines which DB columns get populated.
    UNIPROT_FIELDS: list[str] = [
        "accession",
        "reviewed",
        "id",
        "protein_name",
        "gene_names",
        "organism_name",
        "length",
        "absorption",
        "ft_act_site",
        "ft_binding",
        "cc_catalytic_activity",
        "cc_cofactor",
        "ft_dna_bind",
        "ec",
        "cc_activity_regulation",
        "cc_function",
        "cc_pathway",
        "kinetics",
        "ph_dependence",
        "redox_potential",
        "rhea",
        "ft_site",
        "temp_dependence",
        "keyword",
        "feature_count",
    ]

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

    # DB column -> TSV header
    FIELD_MAP: dict[str, str] = {
        "absorption": "Absorption",
        "active_site": "Active site",
        "binding_site": "Binding site",
        "catalytic_activity": "Catalytic activity",
        "cofactor": "Cofactor",
        "dna_binding": "DNA binding",
        "ec_number": "EC number",
        "activity_regulation": "Activity regulation",
        "function_cc": "Function [CC]",
        "pathway": "Pathway",
        "kinetics": "Kinetics",
        "ph_dependence": "pH dependence",
        "redox_potential": "Redox potential",
        "rhea_id": "Rhea ID",
        "site": "Site",
        "temperature_dependence": "Temperature dependence",
        "keywords": "Keywords",
        "features": "Features",
    }

    def __init__(self) -> None:
        # Plugin instance is reused across executions for connection
        # pooling; counters are reset by the plugin at the start of
        # each ``stream_metadata`` call.
        from protea_sources.uniprot import plugin as _uniprot_plugin

        self._uniprot_plugin = _uniprot_plugin

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = FetchUniProtMetadataPayload.model_validate(payload)

        t0 = time.perf_counter()
        emit(
            "fetch_uniprot_metadata.start",
            None,
            {"search_criteria": p.search_criteria, "page_size": p.page_size},
            "info",
        )

        pages = 0
        total_rows = 0
        proteins_touched = 0
        metadata_upserted = 0

        # Buffer per-record into operation-controlled pages of size
        # ``p.page_size``. Plugin yields one record at a time; operation
        # owns batching policy + commits.
        buffer: list[UniProtMetadataRecord] = []
        for record in self._stream_metadata(p, emit):
            if p.total_limit is not None and total_rows >= p.total_limit:
                emit(
                    "fetch_uniprot_metadata.limit_reached",
                    None,
                    {"total_limit": p.total_limit},
                    "warning",
                )
                break

            buffer.append(record)
            total_rows += 1

            if len(buffer) >= p.page_size:
                pages += 1
                touched, upserted = self._store_rows(session, buffer, p, emit)
                proteins_touched += touched
                metadata_upserted += upserted
                buffer.clear()

                http_req, http_ret = self._uniprot_plugin.http_counters
                emit(
                    "fetch_uniprot_metadata.page_done",
                    None,
                    {
                        "page": pages,
                        "rows_total": total_rows,
                        "proteins_touched_total": proteins_touched,
                        "metadata_upserted_total": metadata_upserted,
                        "http_requests": http_req,
                        "http_retries": http_ret,
                        "_progress_current": total_rows,
                        **(
                            {"_progress_total": p.total_limit}
                            if p.total_limit
                            else {}
                        ),
                    },
                    "info",
                )

                if p.commit_every_page:
                    session.commit()

        # Flush remaining buffer.
        if buffer:
            pages += 1
            touched, upserted = self._store_rows(session, buffer, p, emit)
            proteins_touched += touched
            metadata_upserted += upserted

        elapsed = time.perf_counter() - t0
        http_req, http_ret = self._uniprot_plugin.http_counters
        result = {
            "pages": pages,
            "rows": total_rows,
            "proteins_touched": proteins_touched,
            "metadata_upserted": metadata_upserted,
            "http_requests": http_req,
            "http_retries": http_ret,
            "elapsed_seconds": elapsed,
        }
        emit("fetch_uniprot_metadata.done", None, result, "info")
        return OperationResult(result=result)

    def _stream_metadata(
        self, p: FetchUniProtMetadataPayload, emit: EmitFn
    ) -> Iterator[UniProtMetadataRecord]:
        """Delegate to the protea-sources UniProtSource plugin.

        Plugin owns HTTP retries, cursor pagination, gzip decoding,
        and TSV parsing. The operation owns the field list (persistence
        concern) plus DB upsert. See ``f2a6_real_migration_design.md``.
        """
        yield from self._uniprot_plugin.stream_metadata(
            UniProtMetadataStreamPayload(
                search_criteria=p.search_criteria,
                fields=self.UNIPROT_FIELDS,
                page_size=p.page_size,
                timeout_seconds=p.timeout_seconds,
                compressed=p.compressed,
                max_retries=p.max_retries,
                backoff_base_seconds=p.backoff_base_seconds,
                backoff_max_seconds=p.backoff_max_seconds,
                jitter_seconds=p.jitter_seconds,
                user_agent=p.user_agent,
            ),
            emit=emit,
        )

    # ---------------- DB ----------------

    def _store_rows(
        self,
        session: Session,
        records: list[UniProtMetadataRecord],
        p: FetchUniProtMetadataPayload,
        emit: EmitFn,
    ) -> tuple[int, int]:
        accessions = [r.accession for r in records]
        canonicals = [parse_isoform(a)[0] for a in accessions]
        canonical_unique = list(dict.fromkeys([c for c in canonicals if c]))

        existing = self._load_existing_metadata(session, canonical_unique)

        protein_map: dict[str, Protein] = {}
        if p.update_protein_core and accessions:
            prot_rows = session.query(Protein).filter(Protein.accession.in_(accessions)).all()
            protein_map = {pr.accession: pr for pr in prot_rows}

        touched = 0
        upserted = 0

        for record in records:
            canonical, _, _ = parse_isoform(record.accession)
            row = record.raw_fields

            m = existing.get(canonical)
            if m is None:
                m = ProteinUniProtMetadata(canonical_accession=canonical)
                session.add(m)
                existing[canonical] = m

            changed = False
            for db_col, header in self.FIELD_MAP.items():
                val = row.get(header, "").strip()
                if getattr(m, db_col) != val:
                    setattr(m, db_col, val)
                    changed = True
            if changed:
                upserted += 1

            if p.update_protein_core:
                pr = protein_map.get(record.accession)
                if pr is not None and _backfill_protein_core(pr, row):
                    touched += 1

        return touched, upserted

    def _load_existing_metadata(
        self,
        session: Session,
        canonicals: Sequence[str],
        chunk_size: int = 5000,
    ) -> dict[str, ProteinUniProtMetadata]:
        existing: dict[str, ProteinUniProtMetadata] = {}
        for chunk in chunks(canonicals, chunk_size):
            rows = (
                session.query(ProteinUniProtMetadata)
                .filter(ProteinUniProtMetadata.canonical_accession.in_(chunk))
                .all()
            )
            for m in rows:
                existing[m.canonical_accession] = m
        return existing


def _backfill_protein_core(pr: Protein, row: dict[str, str]) -> bool:
    """Fill missing ``Protein`` core columns from a UniProt TSV row.

    Only NULL columns are touched; the goal is to backfill rows that
    came in via the streaming embeddings pipeline (which only knows
    accession + sequence) with the human-readable fields UniProt
    publishes alongside. Returns ``True`` if any column was changed.
    """
    changed = False

    reviewed = row.get("Reviewed", "").strip().lower()
    if pr.reviewed is None and reviewed:
        if reviewed == "reviewed":
            pr.reviewed = True
            changed = True
        elif reviewed == "unreviewed":
            pr.reviewed = False
            changed = True

    entry_name = row.get("Entry Name", "").strip()
    if pr.entry_name is None and entry_name:
        pr.entry_name = entry_name
        changed = True

    organism = row.get("Organism", "").strip()
    if pr.organism is None and organism:
        pr.organism = organism
        changed = True

    gene_names = row.get("Gene Names", "").strip()
    if pr.gene_name is None and gene_names:
        pr.gene_name = gene_names.split()[0]
        changed = True

    length = row.get("Length", "").strip()
    if pr.length is None and length.isdigit():
        pr.length = int(length)
        changed = True

    return changed
