"""Give every protein its current UniProt entry, stamped with the release that served it.

WHAT IT IS FOR: showing a protein as UniProt describes it today (names, organism,
function, catalytic activity, sites, keywords) and annotating it today.

WHAT IT IS NOT FOR: any experiment. The knowledge in an entry changes from one
release to the next, so what an entry says today was not known at an earlier cut.
Nothing that predicts or evaluates on a cut reads ``protein_uniprot_metadata``
(``tests/test_current_metadata_stays_out_of_experiments.py``). For the same reason
this operation never writes the ``protein`` row: feature code reads columns there
(organism, taxonomy), and filling them from today's entry would hand that code
knowledge from after its cut without anybody deciding it. A protein's organism at
a cut is in that cut's photo (``cut_photo_member.taxon_id``).

THE POPULATION is a query, as in ``resolve_protein_sequences``: every canonical
accession with no metadata row or with a row that names no release, in order and
through the accession grammar (one malformed member answers 400 for its whole
batch). A second run asks only about what the first could not resolve.

THE RELEASE. Each batch's answer names the UniProt release that served it, and
every row is stamped with it. A batch whose answer names no release is refused
rather than stamped with a guess: an unplaceable row is the hazard this guards.

THE BATCHES are cut by the plugin's ``accession_batches`` with the same fields the
request asks for, because the endpoint refuses on two limits: 1,000 accessions, and
a request target of 8,191 characters that 1,000 ten-character TrEMBL accessions with
these 26 fields exceed (414, measured 2026-11-22).

ABSENT accessions, those UniProt no longer serves, are missing from their batch's
answer; the result counts them and they get no row.

Runs on ``protea.jobs``.
"""

from __future__ import annotations

import time
from typing import Annotated, Any

from protea_contracts import UniProtMetadataRecord, parse_isoform
from pydantic import Field
from sqlalchemy import exists, select
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, Operation, OperationResult, ProteaPayload
from protea.core.operations._universe_sources import ACCESSION_GRAMMAR
from protea.core.operations.fetch_uniprot_metadata import FetchUniProtMetadataOperation
from protea.core.utils import contract_payload
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.protein.protein_metadata import ProteinUniProtMetadata

#: The functional fields of ``fetch_uniprot_metadata``, so both operations store an
#: entry alike, plus the organism's identifier for display.
FIELDS: tuple[str, ...] = (*FetchUniProtMetadataOperation.UNIPROT_FIELDS, "organism_id")
#: TSV header -> column, for the descriptive fields this operation adds.
_TEXT: dict[str, str] = {
    "entry_name": "Entry Name",
    "protein_names": "Protein names",
    "gene_names": "Gene Names",
    "organism_name": "Organism",
}
_INTEGER: dict[str, str] = {"organism_id": "Organism (ID)", "length": "Length"}


class ResolveProteinMetadataPayload(ProteaPayload, frozen=True):
    timeout_seconds: Annotated[int, Field(gt=0)] = 120
    dry_run: bool = False
    #: Stop after this many candidates, for a smoke run before the real one. Reported
    #: in the result as ``limit``, so a capped run is never read as a complete one.
    max_accessions: Annotated[int, Field(gt=0)] | None = None


class ResolveProteinMetadataOperation(Operation):
    """Fetch each protein's current UniProt entry, to show it; never for experiments.

    Runs on ``protea.jobs``.
    """

    name = "resolve_protein_metadata"
    payload_model = ResolveProteinMetadataPayload
    description = (
        "Fetch the current UniProt entry of every canonical protein that has none, "
        "stamped with the UniProt release that served it, for display and today's "
        "annotation. Writes protein_uniprot_metadata only, never the protein row, and "
        "nothing that predicts or evaluates on a cut reads it."
    )

    def __init__(self) -> None:
        from protea_sources.uniprot import plugin as _uniprot_plugin

        self._uniprot = _uniprot_plugin

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        bits = []
        if payload.get("max_accessions"):
            bits.append(f"limit={payload['max_accessions']}")
        if payload.get("dry_run"):
            bits.append("dry-run")
        return " · ".join(bits) or "every protein without a current entry"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        from protea_sources.uniprot import RetryKnobs, accession_batches, parse_metadata_tsv

        t0 = time.perf_counter()
        p = ResolveProteinMetadataPayload.model_validate(contract_payload(payload))
        candidates = self._population(session, p.max_accessions)
        emit(
            "resolve_protein_metadata.start",
            None,
            {"candidates": len(candidates), "limit": p.max_accessions},
            "info",
        )
        written = 0
        releases: set[str] = set()
        if not p.dry_run:
            knobs = RetryKnobs(timeout_seconds=p.timeout_seconds)
            fields = ",".join(FIELDS)
            for batch in accession_batches(candidates, fields=fields):
                answer = self._uniprot.fetch_accessions(
                    batch, fields=fields, emit=emit, knobs=knobs
                )
                if not answer.release:
                    raise RuntimeError(
                        "UniProt's answer named no release (no X-UniProt-Release header); "
                        "a row that cannot say which release it came from is not written"
                    )
                records = list(parse_metadata_tsv(answer.tsv))
                written += store_entries(
                    session, records, release=answer.release, release_date=answer.release_date
                )
                releases.add(answer.release)
                session.commit()
                emit(
                    "resolve_protein_metadata.batch_done",
                    None,
                    {"asked": len(batch), "answered": len(records), "release": answer.release},
                    "info",
                )
        result = {
            "candidates": len(candidates),
            "written": written,
            "absent": len(candidates) - written if not p.dry_run else None,
            "releases": sorted(releases),
            "limit": p.max_accessions,
            "dry_run": p.dry_run,
            "elapsed_seconds": round(time.perf_counter() - t0, 1),
        }
        emit("resolve_protein_metadata.done", None, result, "info")
        return OperationResult(result=result)

    @staticmethod
    def _population(session: Session, limit: int | None) -> list[str]:
        """Canonical accessions with no metadata row, or a row that names no release."""
        stamped = exists().where(
            ProteinUniProtMetadata.canonical_accession == Protein.accession,
            ProteinUniProtMetadata.uniprot_release.is_not(None),
        )
        stmt = (
            select(Protein.accession)
            .where(Protein.is_canonical.is_(True), ~stamped)
            .order_by(Protein.accession)
        )
        if limit is not None:
            stmt = stmt.limit(limit)
        return [a for a in session.scalars(stmt).all() if ACCESSION_GRAMMAR.match(a)]


def store_entries(
    session: Session,
    records: list[UniProtMetadataRecord],
    *,
    release: str,
    release_date: str | None,
) -> int:
    """Upsert one metadata row per canonical accession answered; returns how many.

    Writes ``protein_uniprot_metadata`` only. The ``protein`` row is never touched.
    """
    canonicals = list(dict.fromkeys(parse_isoform(r.accession)[0] for r in records))
    existing = {
        m.canonical_accession: m
        for m in session.scalars(
            select(ProteinUniProtMetadata).where(
                ProteinUniProtMetadata.canonical_accession.in_(canonicals)
            )
        )
    }
    for record in records:
        canonical = parse_isoform(record.accession)[0]
        row = record.raw_fields
        entry = existing.get(canonical)
        if entry is None:
            entry = ProteinUniProtMetadata(canonical_accession=canonical)
            session.add(entry)
            existing[canonical] = entry
        for column, header in FetchUniProtMetadataOperation.FIELD_MAP.items():
            setattr(entry, column, row.get(header, "").strip())
        for column, header in _TEXT.items():
            setattr(entry, column, row.get(header, "").strip() or None)
        for column, header in _INTEGER.items():
            value = row.get(header, "").strip()
            setattr(entry, column, int(value) if value.isdigit() else None)
        reviewed = row.get("Reviewed", "").strip().lower()
        entry.reviewed = {"reviewed": True, "unreviewed": False}.get(reviewed)
        entry.uniprot_release = release
        entry.uniprot_release_date = release_date
    session.flush()
    return len({parse_isoform(r.accession)[0] for r in records})
