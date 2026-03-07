from __future__ import annotations

import csv
import gzip
import random
import time
from collections.abc import Iterable, Sequence
from io import BytesIO, StringIO
from typing import Annotated, Any
from urllib.parse import quote

import requests
from pydantic import Field, field_validator
from requests import Response
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
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
    UNIPROT_SEARCH_URL = "https://rest.uniprot.org/uniprotkb/search"

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
        self._http_requests = 0
        self._http_retries = 0
        self._http = requests.Session()

    def execute(self, session: Session, payload: dict[str, Any], *, emit: EmitFn) -> OperationResult:
        p = FetchUniProtMetadataPayload.model_validate(payload)

        t0 = time.perf_counter()
        emit("fetch_uniprot_metadata.start", None,
             {"search_criteria": p.search_criteria, "page_size": p.page_size}, "info")

        pages = 0
        total_rows = 0
        proteins_touched = 0
        metadata_upserted = 0

        for page_idx, rows in enumerate(self._fetch_tsv_pages(p, emit), start=1):
            pages = page_idx
            if not rows:
                continue

            if p.total_limit is not None and (total_rows + len(rows)) > p.total_limit:
                rows = rows[: max(0, p.total_limit - total_rows)]
            if not rows:
                break

            total_rows += len(rows)

            touched, upserted = self._store_rows(session, rows, p, emit)
            proteins_touched += touched
            metadata_upserted += upserted

            emit(
                "fetch_uniprot_metadata.page_done",
                None,
                {
                    "page": page_idx,
                    "rows_total": total_rows,
                    "proteins_touched_total": proteins_touched,
                    "metadata_upserted_total": metadata_upserted,
                    "http_requests": self._http_requests,
                    "http_retries": self._http_retries,
                },
                "info",
            )

            if p.commit_every_page:
                session.commit()

            if p.total_limit is not None and total_rows >= p.total_limit:
                emit("fetch_uniprot_metadata.limit_reached", None, {"total_limit": p.total_limit}, "warning")
                break

        elapsed = time.perf_counter() - t0
        result = {
            "pages": pages,
            "rows": total_rows,
            "proteins_touched": proteins_touched,
            "metadata_upserted": metadata_upserted,
            "http_requests": self._http_requests,
            "http_retries": self._http_retries,
            "elapsed_seconds": elapsed,
        }
        emit("fetch_uniprot_metadata.done", None, result, "info")
        return OperationResult(result=result)

    # ---------------- HTTP / paging ----------------

    def _fetch_tsv_pages(self, p: FetchUniProtMetadataPayload, emit: EmitFn) -> Iterable[list[dict[str, str]]]:
        encoded_query = quote(p.search_criteria)

        fields = [
            "accession", "reviewed", "id", "protein_name", "gene_names", "organism_name", "length",
            "absorption", "ft_act_site", "ft_binding", "cc_catalytic_activity", "cc_cofactor", "ft_dna_bind",
            "ec", "cc_activity_regulation", "cc_function", "cc_pathway", "kinetics", "ph_dependence",
            "redox_potential", "rhea", "ft_site", "temp_dependence", "keyword", "feature_count",
        ]

        params = [
            "format=tsv",
            f"query={encoded_query}",
            f"size={p.page_size}",
            "compressed=true" if p.compressed else "compressed=false",
            f"fields={quote(','.join(fields))}",
        ]
        base_url = f"{self.UNIPROT_SEARCH_URL}?{'&'.join(params)}"

        next_cursor: str | None = None
        page = 0

        while True:
            page += 1
            url = base_url if not next_cursor else f"{base_url}&cursor={next_cursor}"
            emit("uniprot.fetch_page_start", None, {"page": page, "has_cursor": bool(next_cursor)}, "info")

            resp = self._get_with_retries(url, p, emit)
            text = self._decode_response(resp, p.compressed)
            rows = self._parse_tsv(text)

            emit("uniprot.fetch_page_done", None, {"page": page, "rows": len(rows)}, "info")
            yield rows

            next_cursor = self._extract_next_cursor(resp.headers.get("link", ""))
            if not next_cursor:
                break

    def _get_with_retries(self, url: str, p: FetchUniProtMetadataPayload, emit: EmitFn) -> Response:
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

    def _sleep_backoff(self, p: FetchUniProtMetadataPayload, attempt: int, emit: EmitFn, reason: str) -> None:
        base = p.backoff_base_seconds * (2 ** (attempt - 1))
        wait_s = min(base, p.backoff_max_seconds) + random.uniform(0.0, p.jitter_seconds)
        emit("http.retry", None, {"attempt": attempt, "wait_seconds": wait_s, "reason": reason}, "warning")
        time.sleep(wait_s)

    def _decode_response(self, resp: Response, compressed: bool) -> str:
        if compressed:
            with gzip.GzipFile(fileobj=BytesIO(resp.content)) as f:
                return f.read().decode("utf-8", errors="replace")
        return resp.content.decode("utf-8", errors="replace")

    def _extract_next_cursor(self, link_header: str) -> str | None:
        if not link_header or 'rel="next"' not in link_header or "cursor=" not in link_header:
            return None
        try:
            return link_header.split("cursor=")[-1].split(">")[0]
        except Exception:
            return None

    # ---------------- TSV / DB ----------------

    def _parse_tsv(self, tsv_text: str) -> list[dict[str, str]]:
        reader = csv.DictReader(StringIO(tsv_text), delimiter="\t")
        return [{k: (v if v is not None else "") for k, v in row.items()} for row in reader]

    def _store_rows(
        self,
        session: Session,
        rows: list[dict[str, str]],
        p: FetchUniProtMetadataPayload,
        emit: EmitFn,
    ) -> tuple[int, int]:
        accessions = [r.get("Entry", "").strip() for r in rows if r.get("Entry")]
        canonicals = [Protein.parse_isoform(a)[0] for a in accessions]
        canonical_unique = list(dict.fromkeys([c for c in canonicals if c]))

        existing = self._load_existing_metadata(session, canonical_unique)

        protein_map: dict[str, Protein] = {}
        if p.update_protein_core and accessions:
            prot_rows = session.query(Protein).filter(Protein.accession.in_(accessions)).all()
            protein_map = {pr.accession: pr for pr in prot_rows}

        touched = 0
        upserted = 0

        for row in rows:
            acc = row.get("Entry", "").strip()
            if not acc:
                continue
            canonical, _, _ = Protein.parse_isoform(acc)

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
                pr = protein_map.get(acc)
                if pr is not None:
                    core_changed = False

                    reviewed = row.get("Reviewed", "").strip().lower()
                    if pr.reviewed is None and reviewed:
                        if reviewed == "reviewed":
                            pr.reviewed = True
                            core_changed = True
                        elif reviewed == "unreviewed":
                            pr.reviewed = False
                            core_changed = True

                    entry_name = row.get("Entry Name", "").strip()
                    if pr.entry_name is None and entry_name:
                        pr.entry_name = entry_name
                        core_changed = True

                    organism = row.get("Organism", "").strip()
                    if pr.organism is None and organism:
                        pr.organism = organism
                        core_changed = True

                    gene_names = row.get("Gene Names", "").strip()
                    if pr.gene_name is None and gene_names:
                        pr.gene_name = gene_names.split()[0]
                        core_changed = True

                    length = row.get("Length", "").strip()
                    if pr.length is None and length.isdigit():
                        pr.length = int(length)
                        core_changed = True

                    if core_changed:
                        touched += 1

        return touched, upserted

    def _load_existing_metadata(
        self,
        session: Session,
        canonicals: Sequence[str],
        chunk_size: int = 5000,
    ) -> dict[str, ProteinUniProtMetadata]:
        existing: dict[str, ProteinUniProtMetadata] = {}
        for chunk in _chunks(canonicals, chunk_size):
            rows = (
                session.query(ProteinUniProtMetadata)
                .filter(ProteinUniProtMetadata.canonical_accession.in_(chunk))
                .all()
            )
            for m in rows:
                existing[m.canonical_accession] = m
        return existing


def _chunks(seq: Sequence[str], n: int) -> Iterable[Sequence[str]]:
    for i in range(0, len(seq), n):
        yield seq[i: i + n]
