from __future__ import annotations

import gzip
import re
import time
from collections.abc import Iterable
from collections.abc import Sequence as Seq
from io import BytesIO
from typing import Annotated, Any
from urllib.parse import quote

import requests
from pydantic import Field, field_validator
from requests import Response
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, Operation, OperationResult, ProteaPayload
from protea.core.utils import UniProtHttpMixin, chunks
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.sequence.sequence import Sequence as SequenceModel

PositiveInt = Annotated[int, Field(gt=0)]
NonNegativeFloat = Annotated[float, Field(ge=0.0)]


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


class InsertProteinsOperation(UniProtHttpMixin, Operation):
    """Fetches protein sequences from UniProt (FASTA) and upserts them into the DB.

    Uses cursor-based pagination, exponential backoff with jitter, and MD5-based
    sequence deduplication. Many proteins can share one Sequence row.
    Isoforms (``<canonical>-<n>``) are stored as separate Protein rows grouped
    by ``canonical_accession``.
    """

    name = "insert_proteins"
    UNIPROT_SEARCH_URL = "https://rest.uniprot.org/uniprotkb/search"

    _re_os = re.compile(r"\bOS=([^=]+?)\sOX=")
    _re_ox = re.compile(r"\bOX=(\d+)")
    _re_gn = re.compile(r"\bGN=([^\s]+)")

    def __init__(self) -> None:
        self._http_requests = 0
        self._http_retries = 0
        self._total_results: int | None = None
        self._http = requests.Session()

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        self._http_requests = 0
        self._http_retries = 0
        self._total_results = None

        p = InsertProteinsPayload.model_validate(payload)

        t0 = time.perf_counter()
        emit(
            "insert_proteins.start",
            None,
            {"search_criteria": p.search_criteria, "page_size": p.page_size},
            "info",
        )

        pages = 0
        retrieved = 0
        isoforms = 0
        proteins_inserted = 0
        proteins_updated = 0
        sequences_inserted = 0
        sequences_reused = 0

        for page_idx, records in enumerate(self._fetch_fasta_pages(p, emit), start=1):
            pages = page_idx
            if not records:
                continue

            if p.total_limit is not None and (retrieved + len(records)) > p.total_limit:
                records = records[: max(0, p.total_limit - retrieved)]
            if not records:
                break

            retrieved += len(records)
            isoforms += sum(1 for r in records if r["isoform_index"] is not None)

            ins_p, upd_p, ins_s, re_s = self._store_records(session, records, emit)
            proteins_inserted += ins_p
            proteins_updated += upd_p
            sequences_inserted += ins_s
            sequences_reused += re_s

            emit(
                "insert_proteins.page_done",
                None,
                {
                    "page": page_idx,
                    "retrieved_total": retrieved,
                    "proteins_inserted_total": proteins_inserted,
                    "proteins_updated_total": proteins_updated,
                    "sequences_inserted_total": sequences_inserted,
                    "sequences_reused_total": sequences_reused,
                    "http_requests": self._http_requests,
                    "http_retries": self._http_retries,
                    "_progress_current": retrieved,
                    **(
                        {"_progress_total": p.total_limit or self._total_results}
                        if (p.total_limit or self._total_results)
                        else {}
                    ),
                },
                "info",
            )

            if p.total_limit is not None and retrieved >= p.total_limit:
                emit(
                    "insert_proteins.limit_reached", None, {"total_limit": p.total_limit}, "warning"
                )
                break

        elapsed = time.perf_counter() - t0
        emit(
            "insert_proteins.done",
            None,
            {
                "pages": pages,
                "retrieved_records": retrieved,
                "isoform_records": isoforms,
                "proteins_inserted": proteins_inserted,
                "proteins_updated": proteins_updated,
                "sequences_inserted": sequences_inserted,
                "sequences_reused": sequences_reused,
                "http_requests": self._http_requests,
                "http_retries": self._http_retries,
                "elapsed_seconds": elapsed,
            },
            "info",
        )

        return OperationResult(
            result={
                "pages": pages,
                "retrieved_records": retrieved,
                "isoform_records": isoforms,
                "proteins_inserted": proteins_inserted,
                "proteins_updated": proteins_updated,
                "sequences_inserted": sequences_inserted,
                "sequences_reused": sequences_reused,
                "http_requests": self._http_requests,
                "http_retries": self._http_retries,
                "elapsed_seconds": elapsed,
            }
        )

    # ---- HTTP paging ----
    def _fetch_fasta_pages(
        self, p: InsertProteinsPayload, emit: EmitFn
    ) -> Iterable[list[dict[str, Any]]]:
        encoded_query = quote(p.search_criteria)
        params = ["format=fasta", f"query={encoded_query}", f"size={p.page_size}"]
        if p.include_isoforms:
            params.append("includeIsoform=true")
        if p.compressed:
            params.append("compressed=true")

        base_url = f"{self.UNIPROT_SEARCH_URL}?{'&'.join(params)}"
        next_cursor: str | None = None
        page = 0

        while True:
            page += 1
            url = base_url if not next_cursor else f"{base_url}&cursor={next_cursor}"
            emit(
                "uniprot.fetch_page_start",
                None,
                {"page": page, "has_cursor": bool(next_cursor)},
                "info",
            )

            resp = self._get_with_retries(url, p, emit)
            if self._total_results is None:
                try:
                    self._total_results = int(resp.headers.get("X-Total-Results", 0)) or None
                except (ValueError, TypeError):
                    pass
            text = self._decode_response(resp, p.compressed)
            records = self._parse_fasta(text)

            emit("uniprot.fetch_page_done", None, {"page": page, "records": len(records)}, "info")
            yield records

            next_cursor = self._extract_next_cursor(resp.headers.get("link", ""))
            if not next_cursor:
                break

    def _decode_response(self, resp: Response, compressed: bool) -> str:
        content = resp.content
        if compressed:
            with gzip.GzipFile(fileobj=BytesIO(content)) as f:
                return f.read().decode("utf-8", errors="replace")
        return content.decode("utf-8", errors="replace")

    # ---- FASTA parsing ----
    def _parse_fasta(self, fasta_text: str) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        header: str | None = None
        seq_lines: list[str] = []

        def flush() -> None:
            nonlocal header, seq_lines
            if not header:
                return
            seq = "".join(seq_lines).replace(" ", "").strip()
            if not seq:
                header = None
                seq_lines = []
                return

            parsed = self._parse_header(header)
            parsed["sequence"] = seq
            parsed["length"] = len(seq)
            parsed["sequence_hash"] = SequenceModel.compute_hash(seq)
            records.append(parsed)

            header = None
            seq_lines = []

        for line in fasta_text.splitlines():
            line = line.strip()
            if not line:
                continue
            if line.startswith(">"):
                flush()
                header = line[1:]
            else:
                seq_lines.append(line)
        flush()
        return records

    def _parse_header(self, header: str) -> dict[str, Any]:
        parts = header.split("|")
        reviewed = header.startswith("sp|")

        if len(parts) >= 3:
            accession = parts[1].strip()
            entry_name = parts[2].split(" ", 1)[0].strip()
        else:
            accession = header.split(" ", 1)[0].strip()
            entry_name = None

        canonical, is_canonical, iso_idx = Protein.parse_isoform(accession)

        organism = None
        taxonomy_id = None
        gene_name = None

        m = self._re_os.search(header)
        if m:
            organism = m.group(1).strip()
        m = self._re_ox.search(header)
        if m:
            taxonomy_id = m.group(1).strip()
        m = self._re_gn.search(header)
        if m:
            gene_name = m.group(1).strip()

        return {
            "accession": accession,
            "entry_name": entry_name,
            "canonical_accession": canonical,
            "is_canonical": is_canonical,
            "isoform_index": iso_idx,
            "organism": organism,
            "taxonomy_id": taxonomy_id,
            "gene_name": gene_name,
            "reviewed": reviewed,
        }

    # ---- DB storage ----
    def _store_records(
        self, session: Session, records: list[dict[str, Any]], emit: EmitFn
    ) -> tuple[int, int, int, int]:
        if not records:
            return 0, 0, 0, 0

        # 1) Deduplicate sequences
        hash_to_seq: dict[str, str] = {}
        for r in records:
            h = r["sequence_hash"]
            if h not in hash_to_seq:
                hash_to_seq[h] = r["sequence"]

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
        accessions = [r["accession"] for r in records]
        existing_prot = self._load_existing_proteins(session, accessions)

        # 3) Upsert proteins (insert new, conservative update existing)
        proteins_inserted = 0
        proteins_updated = 0
        to_add: list[Protein] = []

        for r in records:
            acc = r["accession"]
            seq_id = existing_seq_ids[r["sequence_hash"]]

            if acc in existing_prot:
                p = existing_prot[acc]
                changed = False

                if getattr(p, "sequence_id", None) is None and seq_id is not None:
                    p.sequence_id = seq_id
                    changed = True

                if getattr(p, "entry_name", None) in (None, "") and r.get("entry_name"):
                    p.entry_name = r["entry_name"]
                    changed = True

                if getattr(p, "canonical_accession", None) != r["canonical_accession"]:
                    p.canonical_accession = r["canonical_accession"]
                    changed = True

                if getattr(p, "is_canonical", None) != r["is_canonical"]:
                    p.is_canonical = r["is_canonical"]
                    changed = True

                if getattr(p, "isoform_index", None) != r["isoform_index"]:
                    p.isoform_index = r["isoform_index"]
                    changed = True

                if getattr(p, "reviewed", None) is None and r.get("reviewed") is not None:
                    p.reviewed = r["reviewed"]
                    changed = True

                if getattr(p, "taxonomy_id", None) in (None, "") and r.get("taxonomy_id"):
                    p.taxonomy_id = r["taxonomy_id"]
                    changed = True

                if getattr(p, "organism", None) in (None, "") and r.get("organism"):
                    p.organism = r["organism"]
                    changed = True

                if getattr(p, "gene_name", None) in (None, "") and r.get("gene_name"):
                    p.gene_name = r["gene_name"]
                    changed = True

                if getattr(p, "length", None) is None and r.get("length"):
                    p.length = int(r["length"])
                    changed = True

                if changed:
                    proteins_updated += 1

            else:
                to_add.append(
                    Protein(
                        accession=acc,
                        canonical_accession=r["canonical_accession"],
                        is_canonical=r["is_canonical"],
                        isoform_index=r["isoform_index"],
                        reviewed=r.get("reviewed"),
                        entry_name=r.get("entry_name"),
                        organism=r.get("organism"),
                        taxonomy_id=r.get("taxonomy_id"),
                        gene_name=r.get("gene_name"),
                        length=int(r["length"]) if r.get("length") else None,
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
