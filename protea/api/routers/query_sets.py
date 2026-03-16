from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, Form, HTTPException, UploadFile
from sqlalchemy import func
from sqlalchemy.orm import Session, sessionmaker
from starlette.requests import Request

from protea.infrastructure.orm.models.query.query_set import QuerySet, QuerySetEntry
from protea.infrastructure.orm.models.sequence.sequence import Sequence
from protea.infrastructure.session import session_scope

router = APIRouter(prefix="/query-sets", tags=["query-sets"])


def get_session_factory(request: Request) -> sessionmaker[Session]:
    factory = getattr(request.app.state, "session_factory", None)
    if factory is None:
        raise RuntimeError("app.state.session_factory is not set")
    return factory  # type: ignore[no-any-return]


def _parse_fasta(content: str) -> list[tuple[str, str]]:
    """Return list of (accession, sequence) from FASTA text.

    The accession is the first whitespace-delimited token of each header line.
    Sequences with no residues are silently skipped.
    """
    records: list[tuple[str, str]] = []
    accession: str | None = None
    seq_parts: list[str] = []

    def _flush() -> None:
        if accession is not None:
            seq = "".join(seq_parts).replace(" ", "").strip().upper()
            if seq:
                records.append((accession, seq))

    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith(">"):
            _flush()
            accession = line[1:].split()[0] if line[1:].strip() else None
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
    raw = await file.read()
    try:
        content = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise HTTPException(status_code=422, detail="FASTA file must be UTF-8 encoded") from None

    records = _parse_fasta(content)
    if not records:
        raise HTTPException(status_code=422, detail="No valid sequences found in the FASTA file")

    # Reject duplicate accessions within the upload
    seen_accs: set[str] = set()
    for acc, _ in records:
        if acc in seen_accs:
            raise HTTPException(
                status_code=422,
                detail=f"Duplicate accession in FASTA: '{acc}'",
            )
        seen_accs.add(acc)

    with session_scope(factory) as session:
        # 1) Upsert sequences (deduplicated by MD5 hash)
        hash_to_seq_id: dict[str, int] = {}
        hashes = [Sequence.compute_hash(seq) for _, seq in records]

        existing = (
            session.query(Sequence.sequence_hash, Sequence.id)
            .filter(Sequence.sequence_hash.in_(hashes))
            .all()
        )
        for h, sid in existing:
            hash_to_seq_id[h] = sid

        for (_, seq), h in zip(records, hashes, strict=False):
            if h not in hash_to_seq_id:
                new_seq = Sequence(sequence=seq, sequence_hash=h)
                session.add(new_seq)
                session.flush()
                hash_to_seq_id[h] = new_seq.id

        # 2) Create QuerySet
        qs = QuerySet(name=name, description=description)
        session.add(qs)
        session.flush()

        # 3) Create entries
        entries = [
            QuerySetEntry(
                query_set_id=qs.id,
                sequence_id=hash_to_seq_id[h],
                accession=acc,
            )
            for (acc, _), h in zip(records, hashes, strict=False)
        ]
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
            session.query(QuerySetEntry.accession, QuerySetEntry.sequence_id)
            .filter(QuerySetEntry.query_set_id == query_set_id)
            .order_by(QuerySetEntry.id)
            .all()
        )

        result = _query_set_to_dict(qs, entry_count)
        result["entries"] = [{"accession": acc, "sequence_id": seq_id} for acc, seq_id in entries]
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
