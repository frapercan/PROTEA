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

import re
from collections import Counter
from collections.abc import Sequence as Seq
from dataclasses import dataclass, field
from typing import Any, NamedTuple

from protea_contracts import UniProtProteinRecord
from sqlalchemy import bindparam, update
from sqlalchemy.orm import Session

from protea.core.evidence_codes import EXPERIMENTAL
from protea.infrastructure.orm.models.protein.protein import Protein

#: THE FOUR TIERS, and the partition is exact: 13 + 9 + 4 = 26, which is every
#: code the ECO mapping knows. Nothing unclassified, nothing invented. An
#: unknown code is therefore a new GO code, and it is REJECTED and counted
#: rather than admitted, because the previous criterion was a complement --
#: "anything that is not IEA" -- and a complement admits whatever GO invents
#: next without anybody deciding.
#:
#: The principle that orders them: a protein is admitted on evidence that is a
#: MEASUREMENT ON THAT PROTEIN, or on a curated database saying it reviewed the
#: entry. Everything excluded fails both.
#:
#: T1, the truth. The thirteen LAFA scores on: the eleven GO experimental codes
#: plus ``IC`` and ``TAS``. These are the only codes that make a protein an
#: evaluation TARGET, and the set is identical to
#: :data:`protea.core.ia_regimes.LAFA_EVIDENCE` -- admission criterion and truth
#: criterion are the same set, deliberately.
TRUTH_CODES = frozenset(EXPERIMENTAL) | {"IC", "TAS"}

#: T3, curated inference. A curator judged THIS protein's function, from
#: sequence similarity, orthology, a sequence model, genomic context or a
#: reviewed computational analysis. Never truth -- none of them is a measurement
#: -- but a real judgement about a specific protein, unlike T1's exclusions.
#:
#: They are in because they are the direct, independent probe of the question
#: the campaign asks: ``ISS`` and friends are curator-reviewed ALIGNMENT
#: inferences, so comparing a nearest-neighbour prediction against them tests
#: whether an embedding neighbourhood captures what curated alignment captures,
#: without touching the scored truth. Measured on GOA 156: 62,363 proteins enter
#: by these and by nothing else.
CURATED_INFERENCE_CODES = frozenset(
    {"ISS", "ISO", "ISA", "ISM", "IGC", "RCA", "NAS", "IKR", "IRD"}
)

#: Excluded, each for its own reason, and the reasons are not interchangeable.
#:
#: ``IEA`` is GO's only automatic code: no person involved.
#:
#: ``IBA`` and ``IBD`` come from the PAINT phylogenetic pipeline, where a curator
#: annotates an ancestral (or descendant) node and the annotation propagates
#: mechanically. There IS a person, but not one looking at this protein, and the
#: label is by construction its family's consensus. Measured: 58% of the corpus
#: entered by ``IBA`` alone when the criterion was "not IEA", and its share of
#: curated annotation rose from 52% on GOA 156 to 83% on GOA 231.
#:
#: ``ND`` records that a curator looked and found NOTHING. Its rows sit on the
#: three ontology ROOT terms, whose Information Accretion is zero by
#: construction -- IA(v) = -log2 P(v | parents(v)) and P = 1 for a root. So an
#: ND-only donor contributes nothing to an IA-weighted metric AND occupies a
#: slot among the k neighbours: not inert, harmful. Measured on GOA 156: 83,950
#: accessions carry only ``ND``, and 83,949 of them have every non-IEA row on a
#: root term.
AUTOMATIC_CODES = frozenset({"IEA"})
PROPAGATED_CODES = frozenset({"IBA", "IBD"})
ABSENCE_CODES = frozenset({"ND"})

#: Kept for the one thing it is still good for: asserting the partition.
ALL_KNOWN_CODES = (
    TRUTH_CODES | CURATED_INFERENCE_CODES | AUTOMATIC_CODES | PROPAGATED_CODES | ABSENCE_CODES
)

#: UniProtKB accession grammar, from UniProt's own documentation.
#:
#: Two operations need it, for two reasons. ``extract_goa_universe`` uses it as an
#: admission gate, because GOA's object column is not guaranteed to hold only
#: UniProtKB accessions and an identifier that is not one is not a protein of this
#: universe. ``resolve_protein_sequences`` uses it as a barrier before a batch:
#: ``/uniprotkb/accessions`` answers 400 for the WHOLE request when one member is
#: malformed, so one stray identifier would cost a thousand proteins.
ACCESSION_GRAMMAR = re.compile(
    r"^(?:[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9](?:[A-Z][A-Z0-9]{2}[0-9]){1,2})$"
)

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


@dataclass
class _Salida:
    """Lo que ``resolve_protein_sequences`` produce: todo sale de la red.

    Fue la mitad de ``ensure_goa_universe`` que hablaba con UniProt, y es hoy la
    operacion entera. El objeto se quedo con la misma forma porque su utilidad
    era exactamente esa: separar en el informe lo que genera un fichero de lo
    que genera un servicio remoto, para poder separar despues las operaciones.
    """

    candidatos: int = 0
    fetched: int = 0
    updated: int = 0
    inserted: int = 0
    sequences: int = 0
    fechas_pendientes: int = 0
    fechas_escritas: int = 0
    alias: dict[str, str] = field(default_factory=dict)
    sin_resolver: list[str] = field(default_factory=list)
    demerges: dict[str, list[str]] = field(default_factory=dict)
    artefactos: dict[str, Any] = field(default_factory=dict)


@dataclass
class _Cuentas:
    """Los contadores que el predicado llena y el resultado reporta.

    Agrupados en un objeto en vez de cinco ``nonlocal``, para que el predicado
    quepa en su propio metodo y se pueda construir --y probar-- aparte.
    """

    rows: int = 0
    #: Filas admitidas por evidencia o por nivel cuyo DB Object Type NO es una
    #: proteina. Contadas aparte de las accesiones malformadas porque son dos
    #: rechazos distintos y antes caian en el mismo numero: ``malformed_skipped``
    #: mezclaba "esto no es una accesion de UniProtKB" con "esto es un complejo o
    #: un RNA", y la segunda cifra es la que dice si GOA empezo a publicar un
    #: tipo nuevo. Se midio el dia que ``malformed_skipped`` bajo de 27.300 a
    #: 13.500 entre las releases 227 y 226 sin que nadie pudiera decir por que.
    no_proteina: int = 0
    por_tipo: Counter[str] = field(default_factory=Counter)
    desconocidos: Counter[str] = field(default_factory=Counter)
    por_nivel: Counter[str] = field(default_factory=Counter)


@dataclass
class _Escaneo:
    """Lo que ``extract_goa_universe`` produce: todo sale del fichero.

    Simetrico a :class:`_Salida`, y por la misma razon: una pasada del GAF no
    abre un socket contra UniProt, asi que todo lo que hay aqui se puede volver a
    calcular con el fichero en cache y nada de aqui depende de que un servicio
    remoto conteste.
    """

    cuentas: _Cuentas
    malformed: int = 0
    admisibles: int = 0
    missing: int = 0
    insertadas: int = 0
    primera_release_escrita: int = 0


def informe_de_extraccion(
    *,
    release: int,
    admit: list[str],
    dry_run: bool,
    escaneo: _Escaneo,
    elapsed: float,
) -> dict[str, Any]:
    """El resultado de una pasada del GAF, que es su unico registro.

    Aqui y no en la operacion porque es una funcion pura de sus entradas, que es
    lo que este modulo contiene.

    ``malformed_accessions`` y ``rows_not_a_protein`` son dos cifras y antes eran
    una: ver :class:`_Cuentas`.
    """
    c = escaneo.cuentas
    return {
        "release": release,
        "admit": admit,
        "rows_scanned": c.rows,
        "admissible_accessions": escaneo.admisibles,
        "malformed_accessions": escaneo.malformed,
        "rows_not_a_protein": c.no_proteina,
        "already_present": escaneo.admisibles - escaneo.missing,
        "missing": escaneo.missing,
        "proteins_inserted": escaneo.insertadas,
        "first_release_written": escaneo.primera_release_escrita,
        "tipos_fiables": dict(c.por_tipo.most_common()),
        "filas_por_nivel": dict(c.por_nivel),
        "codigos_desconocidos": dict(c.desconocidos.most_common()),
        "dry_run": dry_run,
        "elapsed_seconds": elapsed,
    }


def informe_de_resolucion(
    *,
    dry_run: bool,
    salida: _Salida,
    elapsed: float,
) -> dict[str, Any]:
    """El resultado de la pasada contra UniProt.

    ``None`` en un dry run en vez de ``0``, porque "cero recuperables" y "no se
    intento" no son lo mismo: un informe anterior leyo 0% de fusiones
    recuperables porque diez lotes habian dado 400 y los fallos se tallaron como
    ceros.
    """
    return {
        "candidates": salida.candidatos,
        "fetched": salida.fetched,
        "proteins_updated": salida.updated,
        "proteins_inserted": salida.inserted,
        "sequences_inserted": salida.sequences,
        "resolved_as_merge": None if dry_run else len(salida.alias),
        "not_retrievable": None if dry_run else len(salida.sin_resolver),
        "demerged": None if dry_run else len(salida.demerges),
        "dates_pending": salida.fechas_pendientes,
        "dates_backfilled": None if dry_run else salida.fechas_escritas,
        "artefactos": salida.artefactos,
        "dry_run": dry_run,
        "elapsed_seconds": elapsed,
    }


#: Los niveles que un payload puede pedir, por nombre.
TIER_TRUTH = "truth"
TIER_CURATED_INFERENCE = "curated_inference"
TIER_SWISSPROT = "swissprot_of_release"
TIERS = (TIER_TRUTH, TIER_CURATED_INFERENCE, TIER_SWISSPROT)


def codes_for_tiers(tiers: Seq[str]) -> frozenset[str]:
    """Which evidence codes the requested tiers admit.

    :raises ValueError: on an unknown tier name, rather than silently dropping
        it. A scope that quietly widened the corpus is how the original
        ``reviewed:true`` defect went unnoticed for a whole campaign, and a tier
        that quietly NARROWED it would be the same mistake mirrored.
    """
    desconocidos = [t for t in tiers if t not in TIERS]
    if desconocidos:
        raise ValueError(f"unknown admission tier(s): {desconocidos}; known: {list(TIERS)}")
    out: frozenset[str] = frozenset()
    if TIER_TRUTH in tiers:
        out |= TRUTH_CODES
    if TIER_CURATED_INFERENCE in tiers:
        out |= CURATED_INFERENCE_CODES
    return out


def is_swissprot_entry(accession: str, synonym_field: str) -> bool:
    """Was this entry reviewed AT THE RELEASE this GAF row comes from?

    UniProt names its entries ``<mnemonic>_<ORGANISM>`` in Swiss-Prot and
    ``<accession>_<ORGANISM>`` in TrEMBL. The GAF carries that name in the
    DB Object Synonym column, as the first ``|``-separated element -- and it
    carries the name OF ITS OWN RELEASE. So the contemporaneous reviewed status
    is already in the file being read: no historical download, no pairing of a
    GOA release to a UniProt release, and the date is the GAF's own rather than
    the nearest archived release's.

    That matters because the alternative is expensive and the shortcut does not
    work. UniProt's archive publishes no standalone Swiss-Prot FASTA: only
    ``uniprot_sprot-only<release>.tar.gz`` at ~1.5 GB, of which 541 MB must be
    streamed to reach the FASTA member, so ~40 GB for the series. And
    ``date_created`` cannot substitute: it is the UniProtKB integration date,
    not the Swiss-Prot promotion date, so an entry that sat in TrEMBL from 2014
    and was reviewed in 2024 carries 2014.

    MEASURED on GOA 156 against the archived Swiss-Prot of 2016_07:

    * 527,149 accessions classified Swiss-Prot, and the intersection with the
      archived set is 527,149 -- **zero false positives**, precision 100.000%.
    * The 24,556 archived entries this does not see are absent from the GAF
      altogether: 0 of a 20-accession sample appear anywhere in the file. They
      carry no GO annotation in that release, so they have nothing to donate and
      cannot be a target. Not seeing them is correct, not a gap.
    * Every data row carried an entry name: 0 of 280,916,291 were empty.

    The empty case still returns ``False`` rather than guessing, which leaves
    such a row to be decided by its evidence code alone.
    """
    nombre = synonym_field.split("|", 1)[0] if synonym_field else ""
    if not nombre:
        return False
    # TrEMBL iff the name is the accession followed by '_'. Anything else is a
    # mnemonic, which only Swiss-Prot entries have.
    return not (
        nombre.startswith(accession)
        and len(nombre) > len(accession)
        and nombre[len(accession)] == "_"
    )


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
