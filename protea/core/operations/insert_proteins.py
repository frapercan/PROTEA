from __future__ import annotations

import time
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Annotated, Any

from protea_contracts import UniProtFastaStreamPayload, UniProtProteinRecord
from pydantic import Field, field_validator
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, Operation, OperationResult, ProteaPayload
from protea.core.operations._protein_store import StoreCounts, store_records
from protea.core.utils import contract_payload

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

    def absorb(self, c: StoreCounts) -> None:
        """Fold one :func:`store_records` call into the run totals."""
        self.proteins_inserted += c.proteins_inserted
        self.proteins_updated += c.proteins_updated
        self.sequences_inserted += c.sequences_inserted
        self.sequences_reused += c.sequences_reused


class InsertProteinsPayload(ProteaPayload, frozen=True):
    #: The UniProt query to fetch. When ``release_fasta_urls`` is set it
    #: is not used to fetch anything, but it is still required and still
    #: recorded: it states which query the named files are being
    #: asserted to be equivalent to, so the declaration is readable
    #: without opening the URLs.
    search_criteria: str
    #: Gzipped FASTA files from a UniProt release directory to read
    #: instead of walking the search endpoint by cursor. Empty means
    #: paginate. See :meth:`InsertProteinsOperation._stream_fasta` for
    #: when each path is the right one.
    release_fasta_urls: list[str] = []
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

    @field_validator("release_fasta_urls", mode="after")
    @classmethod
    def must_be_http_urls(cls, v: list[str]) -> list[str]:
        for url in v:
            if not url.startswith(("http://", "https://")):
                raise ValueError(f"not an http(s) URL: {url!r}")
        return v


class InsertProteinsOperation(Operation):
    """Fetches protein sequences from UniProt (FASTA) and upserts them into the DB.

    Reads either a release directory's gzipped flat files or the search
    endpoint by cursor, depending on the payload; :meth:`_stream_fasta`
    documents which to use. Both paths share exponential backoff with
    jitter and MD5-based sequence deduplication. Many proteins can share
    one Sequence row. Isoforms (``<canonical>-<n>``) are stored as
    separate Protein rows grouped by ``canonical_accession``.

    The whole run is one transaction: nothing is visible until it
    commits, and a failure leaves no partial universe.
    """

    name = "insert_proteins"
    description = (
        "Fetch protein sequences from UniProt (FASTA, from release files or "
        "cursor-paginated) and upsert Protein + Sequence rows; isoforms are "
        "stored grouped by canonical accession."
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
        # Without this the summary reads as a live query on a run that
        # never issued one.
        urls = (payload or {}).get("release_fasta_urls") or ()
        if urls:
            bits.append(f"release files={len(urls)}")
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
        p = InsertProteinsPayload.model_validate(contract_payload(payload))
        t0 = time.perf_counter()
        source = "release_files" if p.release_fasta_urls else "cursor_pagination"
        self._emit_start(p, source, emit)

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
            totals.absorb(store_records(session, buffer, emit))

        http_req, http_ret = self._uniprot_plugin.http_counters
        result_dict = {
            "source": source,
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

    @staticmethod
    def _emit_start(p: InsertProteinsPayload, source: str, emit: EmitFn) -> None:
        """Record which source path the run took, and over which files."""
        emit(
            "insert_proteins.start",
            None,
            {
                "search_criteria": p.search_criteria,
                "page_size": p.page_size,
                "source": source,
                "release_fasta_urls": list(p.release_fasta_urls),
            },
            "info",
        )

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
        totals.absorb(store_records(session, buffer, emit))
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

        Plugin owns HTTP retries, pagination or release-file reading,
        gzip decoding, and FASTA parsing. The operation owns batching,
        dedup, and bulk insert. See ``f2a6_real_migration_design.md``.

        Two source paths, chosen by whether the payload names release
        files:

        * **Release files** (``release_fasta_urls`` set). Reads the
          gzipped flat files of a UniProt release directory. For a
          whole-database criterion such as ``reviewed:true`` this is the
          only practical path: UniProt rate-limits cursor pagination
          progressively, so throughput decays over a long walk and the
          wall-clock time of a full fetch is not bounded by the result
          size. Those files are the same query materialised, they are
          served at full bandwidth, and because a release directory is
          immutable they also pin the bytes. Pass the canonical file and
          its ``_varsplic`` companion, in that order, so isoforms arrive
          after the canonical entries they belong to. ``include_isoforms``
          has no effect here -- which files are read decides that.
        * **Cursor pagination** (the default). Right for a narrow
          criterion, where the result set is small enough that the walk
          finishes before throttling matters, and where no published
          file corresponds to the query.

        Prefer a versioned release path over ``current_release``, which
        moves: the job's event log records each file's md5, but only a
        pinned URL makes the fetch repeatable.
        """
        if p.release_fasta_urls:
            yield from self._stream_release_fasta(p, emit)
            return
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

    def _stream_release_fasta(
        self, p: InsertProteinsPayload, emit: EmitFn
    ) -> Iterator[UniProtProteinRecord]:
        """Read the release files named by the payload.

        The transport payload carries only the retry/timeout knobs; the
        plugin ignores its query fields on this path.
        """
        yield from self._uniprot_plugin.stream_release_fasta(
            p.release_fasta_urls,
            payload=UniProtFastaStreamPayload(
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
