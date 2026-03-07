from __future__ import annotations

import gzip
import random
import re
import time
from collections.abc import Iterable
from collections.abc import Sequence as Seq
from dataclasses import dataclass
from io import BytesIO
from typing import Any
from urllib.parse import quote

import requests
from requests import Response
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, Operation, OperationResult
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.sequence.sequence import Sequence as SequenceModel


@dataclass(frozen=True)
class InsertProteinsPayload:
    search_criteria: str
    page_size: int = 500
    total_limit: int | None = None

    timeout_seconds: int = 60
    include_isoforms: bool = True
    compressed: bool = False

    max_retries: int = 6
    backoff_base_seconds: float = 0.8
    backoff_max_seconds: float = 20.0
    jitter_seconds: float = 0.4
    user_agent: str = "PROTEA/insert_proteins (contact: you@example.org)"

    @staticmethod
    def from_dict(d: dict[str, Any]) -> InsertProteinsPayload:
        sc = d.get("search_criteria")
        if not isinstance(sc, str) or not sc.strip():
            raise ValueError("payload.search_criteria must be a non-empty string")

        def _int(name: str, default: int) -> int:
            v = d.get(name, default)
            if not isinstance(v, int) or v <= 0:
                raise ValueError(f"payload.{name} must be a positive int")
            return v

        page_size = _int("page_size", 500)

        total_limit = d.get("total_limit", None)
        if total_limit is not None and (not isinstance(total_limit, int) or total_limit <= 0):
            raise ValueError("payload.total_limit must be a positive int or null")

        timeout_seconds = _int("timeout_seconds", 60)

        include_isoforms = d.get("include_isoforms", True)
        if not isinstance(include_isoforms, bool):
            raise ValueError("payload.include_isoforms must be bool")

        compressed = d.get("compressed", False)
        if not isinstance(compressed, bool):
            raise ValueError("payload.compressed must be bool")

        max_retries = _int("max_retries", 6)

        backoff_base_seconds = d.get("backoff_base_seconds", 0.8)
        backoff_max_seconds = d.get("backoff_max_seconds", 20.0)
        jitter_seconds = d.get("jitter_seconds", 0.4)
        for name, v in [
            ("backoff_base_seconds", backoff_base_seconds),
            ("backoff_max_seconds", backoff_max_seconds),
            ("jitter_seconds", jitter_seconds),
        ]:
            if not isinstance(v, (int, float)) or v < 0:
                raise ValueError(f"payload.{name} must be >= 0")

        user_agent = d.get("user_agent", "PROTEA/insert_proteins (contact: you@example.org)")
        if not isinstance(user_agent, str) or not user_agent.strip():
            raise ValueError("payload.user_agent must be a non-empty string")

        return InsertProteinsPayload(
            search_criteria=sc.strip(),
            page_size=page_size,
            total_limit=total_limit,
            timeout_seconds=timeout_seconds,
            include_isoforms=include_isoforms,
            compressed=compressed,
            max_retries=max_retries,
            backoff_base_seconds=float(backoff_base_seconds),
            backoff_max_seconds=float(backoff_max_seconds),
            jitter_seconds=float(jitter_seconds),
            user_agent=user_agent.strip(),
        )


class InsertProteinsOperation(Operation):
    name = "insert_proteins"
    UNIPROT_SEARCH_URL = "https://rest.uniprot.org/uniprotkb/search"

    _re_os = re.compile(r"\bOS=([^=]+?)\sOX=")
    _re_ox = re.compile(r"\bOX=(\d+)")
    _re_gn = re.compile(r"\bGN=([^\s]+)")

    def __init__(self) -> None:
        self._http_requests = 0
        self._http_retries = 0
        self._http = requests.Session()

    def execute(self, session: Session, payload: dict[str, Any], *, emit: EmitFn) -> OperationResult:
        p = InsertProteinsPayload.from_dict(payload)

        t0 = time.perf_counter()
        emit("insert_proteins.start", None, {"search_criteria": p.search_criteria, "page_size": p.page_size}, "info")

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
                },
                "info",
            )

            if p.total_limit is not None and retrieved >= p.total_limit:
                emit("insert_proteins.limit_reached", None, {"total_limit": p.total_limit}, "warning")
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
    def _fetch_fasta_pages(self, p: InsertProteinsPayload, emit: EmitFn) -> Iterable[list[dict[str, Any]]]:
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
            emit("uniprot.fetch_page_start", None, {"page": page, "has_cursor": bool(next_cursor)}, "info")

            resp = self._get_with_retries(url, p, emit)
            text = self._decode_response(resp, p.compressed)
            records = self._parse_fasta(text)

            emit("uniprot.fetch_page_done", None, {"page": page, "records": len(records)}, "info")
            yield records

            next_cursor = self._extract_next_cursor(resp.headers.get("link", ""))
            if not next_cursor:
                break

    def _get_with_retries(self, url: str, p: InsertProteinsPayload, emit: EmitFn) -> Response:
        headers = {"User-Agent": p.user_agent}
        attempt = 0

        while True:
            attempt += 1
            self._http_requests += 1

            try:
                resp = self._http.get(url, timeout=p.timeout_seconds, headers=headers)
            except requests.RequestException as e:
                if attempt > p.max_retries:
                    raise
                self._http_retries += 1
                self._sleep_backoff(p, attempt, emit, reason=f"request_exception:{e.__class__.__name__}")
                continue

            if 200 <= resp.status_code < 300:
                return resp

            if resp.status_code in (429, 500, 502, 503, 504):
                if attempt > p.max_retries:
                    resp.raise_for_status()
                self._http_retries += 1

                retry_after = resp.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_s = min(float(retry_after), p.backoff_max_seconds)
                    emit("http.retry", None,
                         {"attempt": attempt, "wait_seconds": wait_s, "reason": "retry_after"}, "warning")
                    time.sleep(wait_s)
                else:
                    self._sleep_backoff(p, attempt, emit, reason=f"status_{resp.status_code}")
                continue

            resp.raise_for_status()

    def _sleep_backoff(self, p: InsertProteinsPayload, attempt: int, emit: EmitFn, reason: str) -> None:
        base = p.backoff_base_seconds * (2 ** (attempt - 1))
        wait_s = min(base, p.backoff_max_seconds) + random.uniform(0.0, p.jitter_seconds)
        emit("http.retry", None, {"attempt": attempt, "wait_seconds": wait_s, "reason": reason}, "warning")
        time.sleep(wait_s)

    def _decode_response(self, resp: Response, compressed: bool) -> str:
        content = resp.content
        if compressed:
            with gzip.GzipFile(fileobj=BytesIO(content)) as f:
                return f.read().decode("utf-8", errors="replace")
        return content.decode("utf-8", errors="replace")

    def _extract_next_cursor(self, link_header: str) -> str | None:
        if not link_header or 'rel="next"' not in link_header or "cursor=" not in link_header:
            return None
        try:
            return link_header.split("cursor=")[-1].split(">")[0]
        except Exception:
            return None

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

            new_sequences = [SequenceModel(sequence=hash_to_seq[h], sequence_hash=h) for h in missing_hashes]
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

    def _load_existing_sequences(self, session: Session, hashes: Seq[str], chunk_size: int = 5000) -> dict[str, int]:
        existing: dict[str, int] = {}
        for chunk in _chunks(hashes, chunk_size):
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
        for chunk in _chunks(accessions, chunk_size):
            rows = session.query(Protein).filter(Protein.accession.in_(chunk)).all()
            for p in rows:
                existing[p.accession] = p
        return existing


def _chunks(seq: Seq[str], n: int) -> Iterable[Seq[str]]:
    for i in range(0, len(seq), n):
        yield seq[i : i + n]
