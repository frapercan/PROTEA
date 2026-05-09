from __future__ import annotations

import re
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, Form, HTTPException, UploadFile
from sqlalchemy import func
from sqlalchemy.orm import Session, sessionmaker

from protea.api.deps import get_session_factory
from protea.infrastructure.orm.models.query.query_set import QuerySet, QuerySetEntry
from protea.infrastructure.orm.models.sequence.sequence import Sequence
from protea.infrastructure.session import session_scope

router = APIRouter(prefix="/query-sets", tags=["query-sets"])


_TAX_RE = re.compile(r"OX=(\d+)")
_SPECIES_RE = re.compile(r"OS=(.+?)\s+(?:OX|GN|PE|SV)=")


def extract_uniprot_header_metadata(description: str) -> dict[str, Any]:
    """Parse UniProt-style FASTA headers and extract taxonomy fields.

    Matches the SwissProt/TrEMBL convention ``sp|ACC|NAME OS=<species> OX=<taxid>
    GN=<gene> PE=<level> SV=<version>``. Returns ``{'taxonomy_id': int | None,
    'species': str | None}``. Silent no-op for headers that don't follow the
    convention; fields simply come back as ``None``.
    """
    tax_match = _TAX_RE.search(description)
    species_match = _SPECIES_RE.search(description)
    return {
        "taxonomy_id": int(tax_match.group(1)) if tax_match else None,
        "species": species_match.group(1).strip() if species_match else None,
    }


def _parse_fasta(content: str) -> list[tuple[str, str, str]]:
    """Return list of (accession, sequence, description) from FASTA text.

    ``accession`` is extracted from the first whitespace-delimited token of
    the header, unwrapping UniProt-style prefixes: ``sp|P04637|P53_HUMAN`` →
    ``P04637`` and ``tr|A0A...|...`` → ``A0A...``. For non-UniProt headers
    the first token is used verbatim.
    ``description`` is the full header (minus the leading ``>``) so downstream
    callers can extract UniProt metadata such as ``OX=`` / ``OS=``.
    Sequences with no residues are silently skipped.
    """
    records: list[tuple[str, str, str]] = []
    accession: str | None = None
    header: str = ""
    seq_parts: list[str] = []

    def _extract_accession(token: str) -> str:
        parts = token.split("|")
        if len(parts) >= 2 and parts[0] in ("sp", "tr") and parts[1]:
            return parts[1]
        return token

    def _flush() -> None:
        if accession is not None:
            seq = "".join(seq_parts).replace(" ", "").strip().upper()
            if seq:
                records.append((accession, seq, header))

    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith(">"):
            _flush()
            header_body = line[1:]
            first_token = header_body.split()[0] if header_body.strip() else None
            accession = _extract_accession(first_token) if first_token else None
            header = header_body
            seq_parts = []
        else:
            seq_parts.append(line)

    _flush()
    return records


def _query_set_to_dict(qs: QuerySet, entry_count: int) -> dict[str, Any]:
    return {
        "id": str(qs.id),
        "name": qs.name,
        "description": qs.description,
        "entry_count": entry_count,
        "created_at": qs.created_at.isoformat(),
    }


# ── Endpoints ─────────────────────────────────────────────────────────────────


def _parse_and_validate_fasta(raw: bytes, max_bytes: int) -> list[tuple[str, str, str]]:
    """Decode + parse a FASTA upload into ``(accession, sequence, header)`` rows.

    Raises ``HTTPException`` for size overflow, encoding errors, empty
    parses, and duplicate accessions within the same upload.
    """
    if len(raw) > max_bytes:
        raise HTTPException(
            status_code=413,
            detail=f"FASTA file exceeds {max_bytes // (1024 * 1024)} MB limit",
        )
    try:
        content = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise HTTPException(status_code=422, detail="FASTA file must be UTF-8 encoded") from None
    records = _parse_fasta(content)
    if not records:
        raise HTTPException(status_code=422, detail="No valid sequences found in the FASTA file")
    seen_accs: set[str] = set()
    for acc, _, _ in records:
        if acc in seen_accs:
            raise HTTPException(
                status_code=422,
                detail=f"Duplicate accession in FASTA: '{acc}'",
            )
        seen_accs.add(acc)
    return records


def _upsert_sequences(
    session: Session,
    records: list[tuple[str, str, str]],
    hashes: list[str],
) -> dict[str, int]:
    """Resolve sequence IDs for ``records``; insert new rows for unseen hashes.

    Returns ``hash -> sequence_id``, one entry per distinct hash.
    """
    hash_to_seq_id: dict[str, int] = {}
    existing = (
        session.query(Sequence.sequence_hash, Sequence.id)
        .filter(Sequence.sequence_hash.in_(hashes))
        .all()
    )
    for h, sid in existing:
        hash_to_seq_id[h] = sid
    for (_, seq, _), h in zip(records, hashes, strict=False):
        if h not in hash_to_seq_id:
            new_seq = Sequence(sequence=seq, sequence_hash=h)
            session.add(new_seq)
            session.flush()
            hash_to_seq_id[h] = new_seq.id
    return hash_to_seq_id


@router.post("", status_code=201)
async def create_query_set(
    file: UploadFile,
    name: str = Form(...),
    description: str | None = Form(None),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Upload a FASTA file and create a QuerySet.

    Each sequence in the FASTA is stored (or reused if already present) in the
    ``sequence`` table. A ``query_set_entry`` row is created per sequence,
    preserving the original FASTA accession. Duplicate accessions within the
    same upload are rejected with 422.
    """
    from protea.config.tuning import get_tuning

    raw = await file.read()
    records = _parse_and_validate_fasta(raw, get_tuning().api.max_fasta_bytes)

    with session_scope(factory) as session:
        hashes = [Sequence.compute_hash(seq) for _, seq, _ in records]
        hash_to_seq_id = _upsert_sequences(session, records, hashes)

        qs = QuerySet(name=name, description=description)
        session.add(qs)
        session.flush()

        entries = []
        for (acc, _, header), h in zip(records, hashes, strict=False):
            meta = extract_uniprot_header_metadata(header)
            entries.append(
                QuerySetEntry(
                    query_set_id=qs.id,
                    sequence_id=hash_to_seq_id[h],
                    accession=acc,
                    taxonomy_id=meta["taxonomy_id"],
                    species=meta["species"],
                )
            )
        session.add_all(entries)
        session.flush()

        result = _query_set_to_dict(qs, len(entries))

    return result


@router.get("", summary="List query sets")
def list_query_sets(
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    """List all uploaded FASTA query sets with their entry counts, newest first."""
    with session_scope(factory) as session:
        rows = session.query(QuerySet).order_by(QuerySet.created_at.desc()).all()
        counts = {
            qs_id: cnt
            for qs_id, cnt in session.query(
                QuerySetEntry.query_set_id,
                func.count(QuerySetEntry.id),
            )
            .group_by(QuerySetEntry.query_set_id)
            .all()
        }
        return [_query_set_to_dict(qs, counts.get(qs.id, 0)) for qs in rows]


@router.get("/{query_set_id}", summary="Get query set details")
def get_query_set(
    query_set_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Retrieve a query set with its full entry list (accessions and sequence IDs)."""
    with session_scope(factory) as session:
        qs = session.get(QuerySet, query_set_id)
        if qs is None:
            raise HTTPException(status_code=404, detail="QuerySet not found")

        entry_count = (
            session.query(func.count(QuerySetEntry.id))
            .filter(QuerySetEntry.query_set_id == query_set_id)
            .scalar()
        )
        entries = (
            session.query(
                QuerySetEntry.accession,
                QuerySetEntry.sequence_id,
                QuerySetEntry.taxonomy_id,
                QuerySetEntry.species,
            )
            .filter(QuerySetEntry.query_set_id == query_set_id)
            .order_by(QuerySetEntry.id)
            .all()
        )

        result = _query_set_to_dict(qs, entry_count)
        result["entries"] = [
            {
                "accession": acc,
                "sequence_id": seq_id,
                "taxonomy_id": tax_id,
                "species": species,
            }
            for acc, seq_id, tax_id, species in entries
        ]
        return result


@router.delete("/{query_set_id}", summary="Delete a query set")
def delete_query_set(
    query_set_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Delete a query set and all its entries. Sequences are not deleted (they may be shared)."""
    with session_scope(factory) as session:
        qs = session.get(QuerySet, query_set_id)
        if qs is None:
            raise HTTPException(status_code=404, detail="QuerySet not found")
        session.delete(qs)
    return {"deleted": str(query_set_id)}
