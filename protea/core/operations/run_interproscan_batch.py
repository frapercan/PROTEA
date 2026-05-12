"""``run_interproscan_batch`` operation.

Runs InterProScan against a batch of proteins in fixed-size chunks,
parses the resulting TSV through the :class:`InterProSource` plugin
(IP.1b), and bulk-inserts :class:`InterProAnnotation` rows (IP.2).

Inputs are either a ``query_set_id`` (resolved via
:class:`QuerySetEntry` joins) or an explicit ``accessions`` list. An
optional ``ipr_release_floor`` lets re-runs at a newer release skip
proteins whose latest persisted annotation already meets the floor.

Per-chunk commits + ``emit`` progress events make the run resumable:
re-dispatching the same payload picks up where the prior run left off
because already-annotated proteins are filtered out via the same
``ipr_release_floor`` check on every invocation.

Out of scope for IP.3 (see slice spec):

* The payload class lives in PROTEA for now; promotion to
  ``protea-contracts`` is a follow-up bookkeeping commit.
* InterProScan installation runbook (post-defensa).
"""

from __future__ import annotations

import tempfile
import time
import uuid
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Any, NamedTuple

from pydantic import Field, field_validator, model_validator
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.infrastructure.orm.models.annotation.interpro_annotation import (
    InterProAnnotation,
)
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.query.query_set import QuerySet, QuerySetEntry
from protea.infrastructure.orm.models.sequence.sequence import Sequence as SequenceModel

PositiveInt = Annotated[int, Field(gt=0)]

# Default chunk size matches the IP.3 slice spec: 50 proteins per
# ``interproscan.sh`` invocation. Small enough to bound a single
# subprocess wall clock, large enough to amortise JVM startup.
_DEFAULT_CHUNK_SIZE = 50

# Default subprocess timeout per chunk. ``InterProRunPayload`` enforces
# the lower bound (must be > 0); 1h is generous for a 50-protein batch
# on a typical install with the default analysis set.
_DEFAULT_RUN_TIMEOUT_SECONDS = 3600


class _ChunkCtx(NamedTuple):
    """Per-chunk inputs handed to :meth:`_process_chunk`."""

    chunk_index: int
    total_chunks: int
    accessions: tuple[str, ...]
    sequences: dict[str, str]


@dataclass
class _BatchTotals:
    """Mutable accumulator for the chunk loop."""

    chunks_done: int = 0
    proteins_processed: int = 0
    proteins_skipped: int = 0
    annotations_inserted: int = 0
    annotations_skipped: int = 0
    ipr_releases: set[str] = field(default_factory=set)


class RunInterProScanBatchPayload(ProteaPayload, frozen=True):
    """Inputs for one ``run_interproscan_batch`` invocation.

    Exactly one of ``query_set_id`` / ``accessions`` must be provided.
    ``ipr_release_floor`` is optional: when set, proteins whose latest
    persisted ``InterProAnnotation.ipr_release`` is lexicographically
    greater than or equal to the floor are skipped, which gives the
    re-dispatch flow its resume semantics.
    """

    query_set_id: str | None = None
    accessions: list[str] | None = None
    ipr_release_floor: str | None = None
    chunk_size: PositiveInt = _DEFAULT_CHUNK_SIZE
    timeout_seconds: PositiveInt = _DEFAULT_RUN_TIMEOUT_SECONDS
    extra_args: list[str] = Field(default_factory=list)
    binary_path: str | None = None
    commit_every_chunk: bool = True

    @field_validator("query_set_id", "ipr_release_floor", "binary_path", mode="before")
    @classmethod
    def must_be_non_empty(cls, v: str | None) -> str | None:
        if v is None:
            return v
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string when provided")
        return v.strip()

    @field_validator("accessions", mode="before")
    @classmethod
    def must_be_non_empty_list(cls, v: list[str] | None) -> list[str] | None:
        if v is None:
            return v
        cleaned = [s.strip() for s in v if isinstance(s, str) and s.strip()]
        if not cleaned:
            raise ValueError("must be a non-empty list of accessions when provided")
        return cleaned

    @model_validator(mode="after")
    def exactly_one_input(self) -> RunInterProScanBatchPayload:
        if self.query_set_id is None and self.accessions is None:
            raise ValueError("must provide exactly one of query_set_id or accessions")
        if self.query_set_id is not None and self.accessions is not None:
            raise ValueError("must provide exactly one of query_set_id or accessions, not both")
        return self


def _accessions_from_query_set(session: Session, query_set_id: uuid.UUID) -> list[str]:
    """Resolve a ``QuerySet`` into the list of UniProt accessions it
    references that are also present in the ``protein`` table.

    The IP.3 batch only annotates proteins PROTEA has seen, so we
    intersect ``QuerySetEntry.accession`` with the ``protein.accession``
    PK. Unknown accessions are silently dropped — they are surfaced in
    the operation totals as ``proteins_skipped``.
    """
    qs = session.get(QuerySet, query_set_id)
    if qs is None:
        raise ValueError(f"QuerySet {query_set_id} not found")
    stmt = (
        select(QuerySetEntry.accession)
        .join(Protein, Protein.accession == QuerySetEntry.accession)
        .where(QuerySetEntry.query_set_id == query_set_id)
    )
    return list(session.scalars(stmt).unique())


def _resolve_targets(
    session: Session,
    payload: RunInterProScanBatchPayload,
) -> list[str]:
    """Resolve the target accession list per the payload's source."""
    if payload.query_set_id is not None:
        return _accessions_from_query_set(session, uuid.UUID(payload.query_set_id))
    assert payload.accessions is not None  # exactly_one_input validator
    return list(payload.accessions)


def _filter_by_ipr_floor(
    session: Session,
    accessions: Sequence[str],
    ipr_release_floor: str | None,
) -> tuple[list[str], int]:
    """Drop accessions whose latest persisted ``ipr_release`` >= floor.

    Returns ``(remaining, skipped_count)``. When no floor is supplied,
    every input accession is forwarded untouched.

    The "latest release" is the lexicographic max of the
    ``ipr_release`` strings for a given protein. InterProScan release
    tags sort lexicographically in the right order for the common
    ``InterProScan-<major>-<minor>.<patch>`` format used in the wild,
    and the floor semantics ("re-annotate if my latest is older than
    X") only depend on a stable ordering, not a semver-aware one.
    """
    if not ipr_release_floor:
        return list(accessions), 0
    if not accessions:
        return [], 0
    stmt = (
        select(
            InterProAnnotation.protein_accession,
            func.max(InterProAnnotation.ipr_release).label("latest"),
        )
        .where(InterProAnnotation.protein_accession.in_(list(accessions)))
        .group_by(InterProAnnotation.protein_accession)
    )
    latest_by_accession: dict[str, str] = dict(session.execute(stmt).all())
    remaining: list[str] = []
    skipped = 0
    for acc in accessions:
        latest = latest_by_accession.get(acc)
        if latest is not None and latest >= ipr_release_floor:
            skipped += 1
            continue
        remaining.append(acc)
    return remaining, skipped


def _load_sequences(session: Session, accessions: Sequence[str]) -> dict[str, str]:
    """Fetch the raw amino-acid sequence for each accession.

    Returns a mapping ``{accession: sequence_str}`` with one entry per
    accession that resolves through ``Protein → Sequence``. Missing
    rows are simply absent from the result; callers treat that as a
    skip.
    """
    if not accessions:
        return {}
    stmt = (
        select(Protein.accession, SequenceModel.sequence)
        .join(SequenceModel, SequenceModel.id == Protein.sequence_id)
        .where(Protein.accession.in_(list(accessions)))
    )
    return {acc: seq for acc, seq in session.execute(stmt).all()}


def _chunked(items: Sequence[str], n: int) -> Iterable[tuple[str, ...]]:
    """Yield successive ``n``-sized tuples from ``items``.

    Tuples (not lists) so the chunk is hashable + immutable on the
    way into :class:`_ChunkCtx`.
    """
    for i in range(0, len(items), n):
        yield tuple(items[i : i + n])


def _write_fasta(
    chunk_accessions: Sequence[str],
    sequences: dict[str, str],
    target: Path,
) -> int:
    """Write a FASTA file for the chunk, returning how many records
    were emitted.

    Missing sequences (``accession not in sequences``) are silently
    omitted; the caller's totals bookkeep them as skips.
    """
    written = 0
    with target.open("w", encoding="utf-8") as handle:
        for acc in chunk_accessions:
            seq = sequences.get(acc)
            if not seq:
                continue
            handle.write(f">{acc}\n{seq}\n")
            written += 1
    return written


class RunInterProScanBatchOperation:
    """Annotate a batch of proteins with InterProScan and persist hits.

    Workflow:

    1. Resolve target accessions (query set or explicit list).
    2. Drop already-annotated ones if a ``ipr_release_floor`` is set.
    3. Fetch sequences in one DB roundtrip.
    4. For each ``chunk_size`` chunk: write a temp FASTA, invoke
       ``InterProSource.run``, bulk-insert the parsed records, commit
       (optionally), and emit a progress event.
    """

    name = "run_interproscan_batch"
    description = (
        "Run InterProScan on a batch of proteins in fixed-size chunks "
        "and persist parsed domain hits into the interpro_annotation "
        "table."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        p = payload or {}
        bits: list[str] = []
        if p.get("query_set_id"):
            bits.append(f"query_set={p['query_set_id']}")
        elif p.get("accessions"):
            bits.append(f"accessions={len(p['accessions'])}")
        if p.get("ipr_release_floor"):
            bits.append(f"floor={p['ipr_release_floor']}")
        if p.get("chunk_size"):
            bits.append(f"chunk={p['chunk_size']}")
        return " · ".join(bits)

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = RunInterProScanBatchPayload.model_validate(payload)
        t0 = time.perf_counter()

        emit(
            "run_interproscan_batch.start",
            None,
            {
                "query_set_id": p.query_set_id,
                "accessions_count": len(p.accessions) if p.accessions else None,
                "ipr_release_floor": p.ipr_release_floor,
                "chunk_size": p.chunk_size,
            },
            "info",
        )

        all_targets = _resolve_targets(session, p)
        resolved_count = len(all_targets)
        targets, already_done = _filter_by_ipr_floor(session, all_targets, p.ipr_release_floor)
        emit(
            "run_interproscan_batch.resolved",
            None,
            {
                "resolved": resolved_count,
                "already_annotated": already_done,
                "to_process": len(targets),
            },
            "info",
        )

        totals = _BatchTotals(proteins_skipped=already_done)

        if not targets:
            result = self._build_result(p, resolved_count, totals, t0)
            emit("run_interproscan_batch.done", None, result, "info")
            return OperationResult(result=result)

        sequences = _load_sequences(session, targets)
        chunk_tuples = list(_chunked(targets, p.chunk_size))
        total_chunks = len(chunk_tuples)

        for idx, chunk in enumerate(chunk_tuples, start=1):
            ctx = _ChunkCtx(
                chunk_index=idx,
                total_chunks=total_chunks,
                accessions=chunk,
                sequences=sequences,
            )
            self._process_chunk(session, p, ctx, totals, emit)
            if p.commit_every_chunk:
                session.commit()

        result = self._build_result(p, resolved_count, totals, t0)
        emit("run_interproscan_batch.done", None, result, "info")
        return OperationResult(result=result)

    def _process_chunk(
        self,
        session: Session,
        payload: RunInterProScanBatchPayload,
        ctx: _ChunkCtx,
        totals: _BatchTotals,
        emit: EmitFn,
    ) -> None:
        """Run InterProScan on one chunk and bulk-insert the rows."""
        with tempfile.TemporaryDirectory(prefix="ipr-batch-") as tmpdir:
            fasta_path = Path(tmpdir) / f"chunk_{ctx.chunk_index}.fasta"
            written = _write_fasta(ctx.accessions, ctx.sequences, fasta_path)
            missing = len(ctx.accessions) - written
            totals.proteins_processed += written
            totals.proteins_skipped += missing
            if written == 0:
                emit(
                    "run_interproscan_batch.chunk_skipped",
                    None,
                    {
                        "chunk": ctx.chunk_index,
                        "total_chunks": ctx.total_chunks,
                        "reason": "no_sequences",
                    },
                    "warning",
                )
                return
            inserted, skipped, releases = self._run_and_store(session, payload, fasta_path)
        totals.chunks_done += 1
        totals.annotations_inserted += inserted
        totals.annotations_skipped += skipped
        totals.ipr_releases.update(releases)
        emit(
            "run_interproscan_batch.chunk_done",
            None,
            {
                "chunk": ctx.chunk_index,
                "total_chunks": ctx.total_chunks,
                "proteins_in_chunk": written,
                "annotations_inserted": inserted,
                "annotations_skipped": skipped,
                "running_total_inserted": totals.annotations_inserted,
            },
            "info",
        )

    def _run_and_store(
        self,
        session: Session,
        payload: RunInterProScanBatchPayload,
        fasta_path: Path,
    ) -> tuple[int, int, set[str]]:
        """Invoke ``InterProSource.run`` and persist the parsed records.

        Returns ``(inserted, skipped, releases_seen)``. Skipped covers
        records whose ``ipr_release_version`` is missing (the plugin
        always backfills it from ``--version`` so this should not
        happen in production; the guard is here so a mocked plugin
        cannot smuggle ``None`` past the NOT NULL column).
        """
        from protea_sources.interpro.payload import InterProRunPayload
        from protea_sources.interpro.source import InterProSource

        source = InterProSource()
        run_payload = InterProRunPayload(
            fasta_path=str(fasta_path),
            extra_args=list(payload.extra_args),
            timeout_seconds=payload.timeout_seconds,
            binary_path=payload.binary_path,
        )
        rows: list[dict[str, Any]] = []
        releases: set[str] = set()
        skipped = 0

        def _noop_emit(
            event: str,
            message: str | None,
            fields: dict[str, Any],
            level: str,
        ) -> None:
            """Source-level lifecycle events are not surfaced through
            the operation's emit; the operation already publishes
            chunk-level progress and the plugin's own events would just
            duplicate the audit trail."""
            return None

        for record in source.run(run_payload, emit=_noop_emit):
            release = record.ipr_release_version
            if not release:
                skipped += 1
                continue
            releases.add(release)
            rows.append(
                {
                    "protein_accession": record.accession,
                    "source_db": record.source_db,
                    "accession": record.ipr_version or "",
                    "start": record.start,
                    "end": record.end,
                    "evidence": record.evidence,
                    "ipr_release": release,
                }
            )
        if rows:
            session.execute(InterProAnnotation.__table__.insert(), rows)
        return len(rows), skipped, releases

    def _build_result(
        self,
        payload: RunInterProScanBatchPayload,
        resolved: int,
        totals: _BatchTotals,
        t0: float,
    ) -> dict[str, Any]:
        return {
            "query_set_id": payload.query_set_id,
            "ipr_release_floor": payload.ipr_release_floor,
            "chunk_size": payload.chunk_size,
            "resolved": resolved,
            "chunks_done": totals.chunks_done,
            "proteins_processed": totals.proteins_processed,
            "proteins_skipped": totals.proteins_skipped,
            "annotations_inserted": totals.annotations_inserted,
            "annotations_skipped": totals.annotations_skipped,
            "ipr_releases_seen": sorted(totals.ipr_releases),
            "elapsed_seconds": time.perf_counter() - t0,
        }
