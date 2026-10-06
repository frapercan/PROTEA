"""The one place that turns UniProt records into ``protein`` and ``sequence`` rows.

WHY THIS MODULE EXISTS. Two operations need exactly this: ``insert_proteins``,
which walks a release flat file, and ``resolve_protein_sequences``, which asks
UniProt for the sequences the GAF-derived universe is missing. Until now the
second reached into the first's private ``_store_records``, with a comment
calling it "the lesser of two evils" and a test pinning the signature so the
coupling would break loudly. The evil was avoidable: the logic is a pure
function of ``(session, records)``, it names nothing about pagination or about a
GAF, and a module-level function is the shape the platform already uses for
shared domain logic (``_load_ontology_helpers``, ``_compute_embeddings_helpers``).

WHAT IT GUARANTEES, and why both callers need the same guarantee.

*Sequences are deduplicated by MD5.* Several accessions can carry one sequence
-- isoforms, merged entries, identical proteins across strains -- and
``sequence_id`` is deliberately non-unique on ``protein``. Embeddings are keyed
on ``Sequence``, not on ``Protein``, so two accessions over one sequence cost ONE
embedding and contribute ONE neighbour to the KNN bank. A second copy of this
dedup would have been a second chance to get that wrong.

*Existing rows are patched, never clobbered.* :func:`apply_record_updates` fills
nulls, flips canonicality and refreshes the isoform index, and leaves every
non-empty value alone. This is what makes the two callers composable in either
order: ``extract_goa_universe`` inserts an accession with no sequence,
``resolve_protein_sequences`` later fills the sequence and the audit dates on the
same row, and ``insert_proteins`` running over the same accession afterwards
neither duplicates it nor undoes either.
"""

from __future__ import annotations

from collections.abc import Sequence as Seq
from dataclasses import dataclass

from protea_contracts import UniProtProteinRecord
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn
from protea.core.utils import chunks
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.sequence.sequence import Sequence as SequenceModel

#: Chunk size for the ``IN`` lookups below. Well under the 65535 bind-parameter
#: ceiling of the Postgres wire protocol, which a batch of accessions can reach.
_LOOKUP_CHUNK = 5000


@dataclass
class StoreCounts:
    """What one :func:`store_records` call did, by kind.

    A named object rather than the 4-tuple the private method returned. The
    tuple was the reason the old coupling could not fail loudly: a reordering of
    its members would have been silently wrong for hours, and only a test
    asserting "four integers" stood between that and a mislabelled run.
    """

    proteins_inserted: int = 0
    proteins_updated: int = 0
    sequences_inserted: int = 0
    sequences_reused: int = 0

    def add(self, other: StoreCounts) -> None:
        self.proteins_inserted += other.proteins_inserted
        self.proteins_updated += other.proteins_updated
        self.sequences_inserted += other.sequences_inserted
        self.sequences_reused += other.sequences_reused


def store_records(
    session: Session, records: list[UniProtProteinRecord], emit: EmitFn
) -> StoreCounts:
    """Upsert ``records`` as ``sequence`` + ``protein`` rows.

    Does NOT commit: the caller owns the transaction boundary, because the two
    callers want different ones -- ``insert_proteins`` commits per page so an
    interrupted ingest leaves a partial-but-consistent dataset, and
    ``resolve_protein_sequences`` commits per UniProt batch for the same reason.
    """
    if not records:
        return StoreCounts()
    seq_ids, inserted, reused = _dedup_and_persist_sequences(session, records, emit)
    existing = load_existing_proteins(session, [r.accession for r in records])
    ins_p, upd_p = _upsert_proteins(session, records, existing, seq_ids, emit)
    return StoreCounts(ins_p, upd_p, inserted, reused)


def _dedup_and_persist_sequences(
    session: Session, records: list[UniProtProteinRecord], emit: EmitFn
) -> tuple[dict[str, int], int, int]:
    """Deduplicate by ``sequence_hash``; return ``(seq_id_by_hash, inserted, reused)``.

    The returned mapping covers every hash in ``records``, newly inserted ones
    included, so the caller resolves each record's ``sequence_id`` with one dict
    lookup.
    """
    hash_to_seq: dict[str, str] = {}
    for r in records:
        if r.sequence_hash not in hash_to_seq:
            hash_to_seq[r.sequence_hash] = r.sequence

    unique_hashes = list(hash_to_seq.keys())
    emit("db.lookup_sequences_start", None, {"count": len(unique_hashes)}, "info")
    existing_seq_ids = _load_existing_sequences(session, unique_hashes)
    sequences_reused = len(existing_seq_ids)
    emit("db.lookup_sequences_done", None, {"existing": sequences_reused}, "info")

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

    return existing_seq_ids, sequences_inserted, sequences_reused


def _upsert_proteins(
    session: Session,
    records: list[UniProtProteinRecord],
    existing_prot: dict[str, Protein],
    existing_seq_ids: dict[str, int],
    emit: EmitFn,
) -> tuple[int, int]:
    """Insert new ``Protein`` rows and conservatively patch the existing ones."""
    proteins_inserted = 0
    proteins_updated = 0
    to_add: list[Protein] = []
    for r in records:
        seq_id = existing_seq_ids[r.sequence_hash]
        if r.accession in existing_prot:
            if apply_record_updates(existing_prot[r.accession], r, seq_id):
                proteins_updated += 1
        else:
            to_add.append(build_protein(r, seq_id))
            proteins_inserted += 1
    if to_add:
        emit("db.insert_proteins_start", None, {"rows": len(to_add)}, "info")
        session.add_all(to_add)
        session.flush()
        emit("db.insert_proteins_done", None, {"rows": len(to_add)}, "info")
    return proteins_inserted, proteins_updated


def apply_record_updates(p: Protein, r: UniProtProteinRecord, seq_id: int | None) -> bool:
    """Patch ``p`` from ``r`` without clobbering; return ``True`` if anything changed.

    The null-filling is what lets the universe be built in two steps:
    ``extract_goa_universe`` writes a row whose ``sequence_id``, ``entry_name``,
    ``organism``, ``taxonomy_id``, ``gene_name`` and ``length`` are all NULL, and
    this fills every one of them when UniProt finally answers for it.
    """
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
    return changed


def build_protein(r: UniProtProteinRecord, seq_id: int | None) -> Protein:
    """A new ``Protein`` row from a UniProt record."""
    return Protein(
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


def _load_existing_sequences(
    session: Session, hashes: Seq[str], chunk_size: int = _LOOKUP_CHUNK
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


def load_existing_proteins(
    session: Session, accessions: Seq[str], chunk_size: int = _LOOKUP_CHUNK
) -> dict[str, Protein]:
    """The ``Protein`` rows that already exist, keyed by accession."""
    existing: dict[str, Protein] = {}
    for chunk in chunks(accessions, chunk_size):
        rows = session.query(Protein).filter(Protein.accession.in_(chunk)).all()
        for p in rows:
            existing[p.accession] = p
    return existing
