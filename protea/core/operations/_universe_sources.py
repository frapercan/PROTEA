"""Wire formats and pure decisions behind the GOA universe pass.

Split out of :mod:`protea.core.operations.ensure_goa_universe` when that module
crossed the 800 LOC smell budget. The split is by responsibility, not size: the
operation *orchestrates* -- it queries the database, dispatches HTTP requests and
emits job events -- while this module knows only about *formats*: how UniProt's
TSV response is laid out, where the audit dates live in its JSON, which evidence
codes each scope admits, and how a returned accession is classified as a merge or
a demerge.

Everything here is pure except :func:`_store_dates`, which needs a session
because the three date columns do not fit in ``UniProtProteinRecord``: that model
lives in ``protea-contracts``, so extending it would mean a contract change and
two lock bumps for three dates.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, NamedTuple

from protea_contracts import UniProtProteinRecord
from sqlalchemy import bindparam, update
from sqlalchemy.orm import Session

from protea.core.evidence_codes import EXPERIMENTAL
from protea.infrastructure.orm.models.protein.protein import Protein

#: The only automatic evidence code in GO. Every other code was assigned by a
#: person, though not with equal directness: ``IBA`` comes from the PAINT
#: phylogenetic pipeline, where a curator annotates an ancestral node and the
#: annotation propagates down the tree mechanically, and ``ND`` records that a
#: curator looked and found nothing. Both are curated information and neither is a
#: measurement on the protein in question, so they qualify a protein for the
#: *retrieval bank* but never as evaluation *truth*. The distinction is not baked
#: in here: ``evidence_code`` is persisted per annotation row, so the tier is a
#: choice made at analysis time.
_AUTOMATIC_CODES = frozenset({"IEA"})

#: Fields requested from ``/uniprotkb/accessions``, which returns sequence *and*
#: audit dates in a single call. The previous implementation asked for
#: ``format=fasta``, which carries no dates and would have required a second
#: request per batch. TSV also reports ``reviewed`` as a word rather than leaving
#: it to be inferred from the ``sp|``/``tr|`` prefix of a FASTA header.
#:
#: Column order is fixed here and verified against the response header in
#: :func:`_parse_tsv`.
_TSV_FIELDS = (
    "accession,id,reviewed,organism_name,organism_id,gene_names,"
    "length,sequence,sequence_version,date_created,date_sequence_modified"
)


#: Dates only, for the backfill pass over universe members that already have a
#: sequence. Same three values as :data:`_TSV_FIELDS` carries, without the payload.
_DATE_FIELDS = "accession,sequence_version,date_created,date_sequence_modified"


class _AuditDates(NamedTuple):
    """The three temporal fields that ``UniProtProteinRecord`` cannot carry.

    ``date_created`` separates *knowledge gain* from *entry creation*: a protein
    first published in 2022 sits in the universe of a 2016 release with no
    annotations, and a release-to-release delta would read that as
    ``NK -> gained``. It is not -- the entry did not exist.

    ``date_sequence_modified`` turns the sequence leak into a named subset. The
    stored sequence comes from today's UniProt; where the last sequence update is
    at or before a window's ``t0``, today's sequence *is* the sequence of then.
    Measured against Swiss-Prot release 2024_02: 350 of 73,863 proteins differ,
    0.47%.

    ``sequence_version`` of 1 means the sequence never changed over the entry's
    lifetime, so no window can leak through it. Measured over 109,320 canonical
    accessions: 71.5% are at version 1.
    """

    date_created: str | None
    date_sequence_modified: str | None
    sequence_version: int | None


def _parse_tsv(text: str) -> list[tuple[UniProtProteinRecord, _AuditDates]]:
    """Turn a UniProt TSV response into records paired with their audit dates.

    The record is built field by field rather than delegating to
    ``protea_sources.uniprot.parse_fasta_text`` because the TSV carries three
    columns a FASTA header does not.

    :raises RuntimeError: if the response header does not have the column count
        requested in :data:`_TSV_FIELDS`. Failing here is deliberate: a silently
        reordered response would write dates into the wrong column, which no
        downstream check would catch.
    """
    from protea_contracts import compute_sequence_hash

    lines = [ln for ln in text.split("\n") if ln.strip()]
    if not lines:
        return []
    expected = len(_TSV_FIELDS.split(","))
    header = lines[0].split("\t")
    if len(header) != expected:
        raise RuntimeError(
            f"UniProt returned {len(header)} columns for {expected} requested: {header}"
        )
    out: list[tuple[UniProtProteinRecord, _AuditDates]] = []
    for line in lines[1:]:
        col = line.split("\t")
        if len(col) != expected:
            continue
        acc, entry, reviewed, organism, taxon, genes, _len, seq, version, created, seqmod = col
        acc, seq = acc.strip(), seq.strip()
        if not acc or not seq:
            continue
        out.append(
            (
                UniProtProteinRecord(
                    accession=acc,
                    entry_name=entry.strip() or None,
                    canonical_accession=acc,
                    is_canonical=True,
                    isoform_index=None,
                    organism=organism.strip() or None,
                    taxonomy_id=taxon.strip() or None,
                    gene_name=(genes.split()[0] if genes.strip() else None),
                    reviewed=reviewed.strip().lower() == "reviewed",
                    sequence=seq,
                    length=len(seq),
                    sequence_hash=compute_sequence_hash(seq),
                ),
                _AuditDates(
                    date_created=created.strip() or None,
                    date_sequence_modified=seqmod.strip() or None,
                    sequence_version=int(version) if version.strip().isdigit() else None,
                ),
            )
        )
    return out


def _parse_dates_tsv(text: str) -> list[tuple[str, _AuditDates]]:
    """Parse a dates-only TSV response into ``(accession, dates)`` pairs.

    Separate from :func:`_parse_tsv` because this response has no sequence column,
    so there is no ``UniProtProteinRecord`` to build -- these proteins are already
    in the table. The header is checked the same way and for the same reason: a
    reordered response would write a creation date into the sequence-version
    column, and both are plausible-looking integers and dates.

    :raises RuntimeError: on a column count that does not match
        :data:`_DATE_FIELDS`.
    """
    lines = [ln for ln in text.split("\n") if ln.strip()]
    if not lines:
        return []
    expected = len(_DATE_FIELDS.split(","))
    header = lines[0].split("\t")
    if len(header) != expected:
        raise RuntimeError(
            f"UniProt returned {len(header)} date columns for {expected} requested: {header}"
        )
    out: list[tuple[str, _AuditDates]] = []
    for line in lines[1:]:
        col = line.split("\t")
        if len(col) != expected:
            continue
        acc, version, created, seqmod = col
        if not acc.strip():
            continue
        out.append(
            (
                acc.strip(),
                _AuditDates(
                    date_created=created.strip() or None,
                    date_sequence_modified=seqmod.strip() or None,
                    sequence_version=int(version) if version.strip().isdigit() else None,
                ),
            )
        )
    return out


def _audit_dates_of(entry: dict[str, Any]) -> _AuditDates:
    """Read the same three dates from a UniProt JSON entry.

    The secondary-accession path queries ``/uniprotkb/search`` and receives whole
    entries, so the dates arrive under ``entryAudit`` rather than as TSV columns.
    Both paths must populate them: if only one did, roughly half the corpus would
    carry NULL temporal columns and any date filter would exclude those proteins
    without saying so.
    """
    audit = entry.get("entryAudit") or {}
    version = audit.get("sequenceVersion")
    return _AuditDates(
        date_created=audit.get("firstPublicDate") or None,
        date_sequence_modified=audit.get("lastSequenceUpdateDate") or None,
        sequence_version=int(version) if isinstance(version, int) else None,
    )


def _store_dates(session: Session, rows: list[tuple[str, _AuditDates]]) -> None:
    """Write the three columns ``UniProtProteinRecord`` cannot carry.

    One executemany per batch, issued after the upsert so the rows exist. Kept out
    of the contract model on purpose: adding fields there would couple a schema
    detail of this database to a shared package and require two dependency bumps.
    """
    if not rows:
        return
    # Core UPDATE against ``Protein.__table__``, NOT the ORM entity. An ORM-enabled
    # update with extra WHERE criteria, executed with a list of parameter dicts,
    # raises ``InvalidRequestError: bulk synchronize of persistent objects not
    # supported when using bulk update with additional WHERE criteria`` -- verified
    # against this Postgres on 2026-10-05, where the first batch would have failed.
    # There is nothing to synchronize here: no Protein instances are loaded in this
    # session, the rows were just upserted by Core statements.
    session.execute(
        update(Protein.__table__)
        .where(Protein.__table__.c.accession == bindparam("_acc"))
        .values(
            date_created=bindparam("_created"),
            date_sequence_modified=bindparam("_seqmod"),
            sequence_version=bindparam("_version"),
        ),
        [
            {
                "_acc": acc,
                "_created": dates.date_created,
                "_seqmod": dates.date_sequence_modified,
                "_version": dates.sequence_version,
            }
            for acc, dates in rows
        ],
    )


def codes_for(scope: str) -> Callable[[str], bool]:
    """Resolve a declared evidence scope to a predicate over the GAF code column.

    ``curated``
        Any code other than ``IEA``. Since ``IEA`` is GO's only automatic
        category, this reads as "a person assigned it". Measured on GOA release
        156: 554,328 distinct proteins.
    ``reliable``
        The thirteen codes LAFA's ground truth filters on -- the eleven GO
        experimental codes plus ``IC`` and ``TAS``. Measured on GOA release 156:
        117,136 distinct proteins.

    The universe pass uses ``curated`` because breadth serves the retrieval bank,
    and because the per-row ``evidence_code`` keeps the narrower tier recoverable.
    Evaluation truth remains the thirteen, and that is decided by the evaluation,
    not here.

    :raises ValueError: on an unknown scope, rather than silently falling back to
        a default. A scope that quietly widened the corpus is how the original
        ``reviewed:true`` defect went unnoticed for a whole campaign.
    """
    if scope == "reliable":
        allowed = set(EXPERIMENTAL) | {"IC", "TAS"}
        return lambda ev: ev in allowed
    if scope == "curated":
        return lambda ev: bool(ev) and ev not in _AUTOMATIC_CODES
    raise ValueError(f"unknown evidence_scope: {scope!r}")


def _organism_of(entry: dict[str, Any]) -> str | None:
    return ((entry.get("organism") or {}).get("scientificName")) or None


def _taxon_of(entry: dict[str, Any]) -> str | None:
    taxon = (entry.get("organism") or {}).get("taxonId")
    return str(taxon) if taxon is not None else None


def _gene_of(entry: dict[str, Any]) -> str | None:
    genes = entry.get("genes") or []
    if not genes:
        return None
    return ((genes[0].get("geneName") or {}).get("value")) or None


def records_for_merge(
    entry: dict[str, Any], requested: set[str]
) -> tuple[str, list[UniProtProteinRecord], list[str]] | None:
    """The rows one UniProt entry contributes: its primary plus requested aliases.

    Aliases are created only for secondary accessions somebody actually asked
    for. An entry can list dozens -- ``P04439`` carries 135 -- and materialising
    the rest would invent proteins no GAF ever annotated.

    :returns: ``(primary, rows, matched_secondaries)``, or ``None`` when the entry
        has no sequence or none of its secondaries were requested.
    """
    from protea_contracts import compute_sequence_hash

    primary = entry.get("primaryAccession")
    seq = ((entry.get("sequence") or {}).get("value") or "").strip()
    if not primary or not seq:
        return None
    matched = [sec for sec in (entry.get("secondaryAccessions") or []) if sec in requested]
    if not matched:
        return None
    shared: dict[str, Any] = {
        "organism": _organism_of(entry),
        "taxonomy_id": _taxon_of(entry),
        "gene_name": _gene_of(entry),
        "reviewed": "reviewed" in (entry.get("entryType") or "").lower(),
        "sequence": seq,
        "length": len(seq),
        "sequence_hash": compute_sequence_hash(seq),
    }
    rows = [
        UniProtProteinRecord(
            accession=primary,
            canonical_accession=primary,
            is_canonical=True,
            isoform_index=None,
            **shared,
        )
    ]
    rows += [
        UniProtProteinRecord(
            accession=sec,
            canonical_accession=primary,
            is_canonical=False,
            isoform_index=None,
            **shared,
        )
        for sec in matched
    ]
    return primary, rows, matched


def classify(
    candidates: dict[str, list[tuple[str, list[UniProtProteinRecord]]]],
    alias: dict[str, str],
    demerges: dict[str, list[str]],
) -> list[UniProtProteinRecord]:
    """Separate merges from demerges and return the rows to persist.

    **A demerge has no single identity.** An accession returned against more than
    one primary was *split* across entries, so there is no one protein it points
    at: aliasing it to either picks arbitrarily, and the sequence it would inherit
    belongs to one of two distinct proteins. Such accessions are recorded with
    their destinations and left unresolved, because choosing is a curatorial
    decision this operation cannot make.

    **Rows are deduplicated by accession.** One primary can appear under several
    requested secondaries in the same batch. ``InsertProteinsOperation._store_records``
    splits inserts from updates by querying existing rows and does not deduplicate
    its own input -- its usual caller receives each accession once from a FASTA
    stream -- so a repeated accession reaches it as two INSERTs and raises
    ``duplicate key``. Observed with ``C8VQ65`` on 2026-10-05, after 317 correct
    merges.

    ``alias`` and ``demerges`` are mutated in place; the return value is the row
    list for the caller to persist.
    """
    rows: list[UniProtProteinRecord] = []
    seen: set[str] = set()
    for sec, options in candidates.items():
        destinations = {primary for primary, _ in options}
        if len(destinations) > 1:
            demerges[sec] = sorted(destinations)
            continue
        for row in options[0][1]:
            if row.accession in seen:
                continue
            seen.add(row.accession)
            rows.append(row)
        alias[sec] = options[0][0]
    return rows


def candidates_from(
    payload: dict[str, Any], requested: set[str]
) -> tuple[dict[str, list[tuple[str, list[UniProtProteinRecord]]]], dict[str, dict[str, Any]]]:
    """Group a ``sec_acc:`` search response by the accession that was requested.

    Everything is collected before anything is decided, because an accession may
    appear under several entries and is then a demerge rather than a merge.
    Deciding inside the loop would alias it to whichever entry came first, which
    is the order of UniProt's response and not a decision.

    :returns: ``(candidates, entry_by_accession)``. The second mapping is how
        :func:`_audit_dates_of` reaches ``entryAudit`` for rows produced on this
        path; without it those proteins would carry NULL temporal columns.
    """
    candidates: dict[str, list[tuple[str, list[UniProtProteinRecord]]]] = {}
    entry_by_accession: dict[str, dict[str, Any]] = {}
    for entry in payload.get("results") or []:
        resolved = records_for_merge(entry, requested)
        if resolved is None:
            continue
        primary, rows, matched = resolved
        for row in rows:
            entry_by_accession[row.accession] = entry
        for sec in matched:
            candidates.setdefault(sec, []).append((primary, rows))
    return candidates, entry_by_accession
