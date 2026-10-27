"""Grow the protein universe from one GAF, without asking anybody anything.

THE DEFECT THIS CLOSES. ``load_goa_annotations`` stores an annotation only when
its accession is already in ``protein``, and silently skips the rest -- it has to,
because ``protein_go_annotation.protein_accession`` is a FOREIGN KEY. For the
whole clean campaign that filter meant "reviewed only", a scope nobody had
written down: it was a ``search_criteria`` in one ``insert_proteins`` payload from
2026-09-15. Measured 2026-10-05, UniProtKB with the thirteen lafa evidence codes:
**93.526 reviewed against 149.774 in total**, so roughly 56.000 proteins carrying
curated experimental labels never reached the corpus, and the count of how many
were dropped lived only in a job event.

WHY THE UNIVERSE COMES FROM THE GAF AND NOT FROM A UNIPROT QUERY. A query
describes today. The campaign spans 2016 to 2026, and a protein that held
experimental evidence in 2018 and lost it, or left UniProt entirely, does not
appear in today's answer -- yet it took part in the deltas the model learns from.
Deriving the universe from each GAF as it loads is the only definition that moves
with the series. Measured over three releases (160, 194, 235): the union is
**196.164** accessions, **46.390 more** than today's query returns, and the curve
was still climbing, so even that is a floor.

WHY THIS OPERATION TOUCHES NO NETWORK BUT THE FILE. It used to. ``ensure_goa_universe``
scanned the GAF and then, in the same job, fetched the missing sequences from
UniProt, backfilled audit dates and resolved merged accessions. That put a remote
service in the middle of a pass that has to run 75 times, and it repeated the same
work every time: an accession UniProt no longer serves was asked for again at every
release that annotates it, measured at 34% of the requests in the ten passes that
ran that way.

Splitting it is not a tidier arrangement of the same work, it is less work. The
sequences are needed ONCE, at the end, over the union of all 75 releases, and
``protein.sequence_id`` is nullable precisely so a row can exist before its
sequence does. So this operation inserts accessions and nothing else, and
:mod:`protea.core.operations.resolve_protein_sequences` runs once when the series
is finished. A release pass is then a pure function of a cached file, which is
also what makes it safe to re-run: no state anywhere else can have moved.

What phase 2 needs is exactly what this writes. ``load_goa_annotations`` gates on
``select(Protein.accession)``, so an accession-only row admits every annotation
the GAF carries for it. Annotations for a protein whose sequence never arrives are
kept, not dropped: a protein deleted from UniProt still took part in the deltas,
and the queries that need a sequence filter on ``sequence_id IS NOT NULL``.

NOT-QUALIFIED ROWS COUNT. A NOT annotation is curated knowledge and a scarce kind
-- negative results are hard to publish -- and the evaluation already uses it:
``_reconcile_not_side`` propagates NOT to descendants and subtracts them. So a
protein whose only reliable annotation is a NOT belongs in the universe. The
donor policy still excludes NOT rows when picking neighbours, which is a
different question and stays as it is.

WHICH ACCESSIONS, EXACTLY. The criterion is four tiers and lives in the payload;
:mod:`protea.core.operations._universe_sources` holds the evidence sets, the
measurements behind each inclusion and exclusion, and the Swiss-Prot-from-GAF
rule. ADR-D49 is the decision record.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from typing import Annotated, Any

from protea_contracts import GoaStreamPayload
from pydantic import Field, field_validator
from sqlalchemy import insert, or_, update
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, Operation, OperationResult, ProteaPayload
from protea.core.operations._universe_sources import (
    ACCESSION_GRAMMAR,
    ALL_KNOWN_CODES,
    TIER_CURATED_INFERENCE,
    TIER_SWISSPROT,
    TIER_TRUTH,
    _RowCounters,
    _ScanOutcome,
    assert_the_tier_is_derivable,
    codes_for_tiers,
    entry_name_is_readable,
    extraction_report,
    is_swissprot_entry,
)
from protea.core.utils import chunks, contract_payload
from protea.infrastructure.orm.models.protein.protein import Protein

#: Rows per statement when inserting or updating ``protein``. Well under the 65535
#: bind-parameter ceiling of the Postgres wire protocol, which one release's
#: admissible set reaches several times over: 167.573 accessions on GOA 235.
_DB_CHUNK = 20000

#: 0-indexed GAF column holding the DB Object Type: ``protein``, ``complex``,
#: ``rna``. This is how the file itself states what each row is, and it replaces
#: the accession regex as the type filter -- the regex agreed with it on GOA 156
#: but by coincidence, not by construction.
_GAF_TYPE = 11

#: Object types that are NOT a protein with a chain to embed. They are REJECTED
#: by name rather than admitting ``protein`` alone, and the difference matters: a
#: type GOA starts publishing enters the corpus and shows up in the result's
#: ``tipos_fiables`` histogram instead of disappearing silently. Dropping a real
#: protein is worse than admitting a complex, because the accession regex stops
#: complexes anyway -- measured on GOA 156, 0 of 1,032 IntAct and RNAcentral
#: identifiers match it.
#:
#: The vocabulary is not stable across the series: GOA 158 renames ``rna`` to
#: ``ncrna`` and ``complex`` to ``protein_complex``. Both spellings are listed for
#: that reason.
_NOT_A_PROTEIN = frozenset({"complex", "protein_complex", "rna", "ncrna", "mrna",
                            "trna", "rrna", "snrna", "snorna", "lncrna",
                            "transcript", "gene", "small molecule"})

#: 0-indexed GAF column holding the evidence code. We read the raw column
#: instead of the parsed record so the evidence test can run before the plugin
#: builds anything -- see ``_admissible_accessions``. The plugin keeps the same
#: index privately; ``test_the_evidence_column_is_where_we_think`` pins ours
#: against the plugin's own parse rather than against its private name.
_GAF_EVIDENCE = 6

#: 0-indexed GAF column holding the accession. The predicate needs it to decide
#: Swiss-Prot membership, which is a comparison between the accession and the
#: entry name.
_GAF_ID = 1

#: 0-indexed GAF column holding DB Object Synonym, whose first ``|``-separated
#: element is the UniProtKB entry name. That name is what distinguishes
#: Swiss-Prot from TrEMBL, and the GAF carries the name of its OWN release --
#: which is why the reviewed status needs no historical download. Verified
#: against the plugin's own parse by ``test_the_synonym_column_is_where_we_think``.
_GAF_SYNONYM = 10


class ExtractGoaUniversePayload(ProteaPayload, frozen=True):
    gaf_url: str
    #: WHICH RELEASE THIS IS, stored on every row the pass inserts as
    #: ``protein.first_admitted_release``. Required rather than parsed out of
    #: ``gaf_url``, because a number read from a URL is a guess about a filename
    #: convention, and this one is written into the corpus.
    release: Annotated[int, Field(gt=0)]
    timeout_seconds: Annotated[int, Field(gt=0)] = 120
    dry_run: bool = False
    #: WHICH TIERS ADMIT A PROTEIN. This lives in the PAYLOAD deliberately: it
    #: is the decision that defines the corpus, and keeping it in a module
    #: constant is precisely how a ``reviewed:true`` search criterion fixed the
    #: scope of a whole campaign on 2026-09-15 without anybody declaring it.
    #: Here it is on the job row, queryable after the fact.
    #:
    #: ``truth``
    #:     The thirteen LAFA codes. The only tier that makes a protein an
    #:     evaluation TARGET. Measured on GOA 156: 117,136 accessions.
    #: ``curated_inference``
    #:     ``ISS ISO ISA ISM IGC RCA NAS IKR IRD`` -- a curator's judgement about
    #:     THIS protein, never truth. Measured on GOA 156: 62,363 proteins enter
    #:     by these and nothing else.
    #: ``swissprot_of_release``
    #:     The entry was reviewed AT THIS RELEASE, read from the entry name the
    #:     GAF itself carries. See
    #:     :func:`protea.core.operations._universe_sources.is_swissprot_entry`
    #:     for the measurement: 527,149 on GOA 156, zero false positives.
    #:
    #: WHAT IS NOT HERE, and why the list is an enumeration rather than a
    #: complement: the previous version admitted "anything that is not IEA",
    #: which let in ``IBA`` (58% of the corpus, a mechanically propagated family
    #: consensus) and ``ND`` (a curator recording that they found NOTHING, on
    #: root terms whose Information Accretion is zero). A complement admits
    #: whatever GO invents next without anybody deciding; an enumeration does
    #: not, and an unknown code is counted and reported instead.
    admit: list[str] = [TIER_TRUTH, TIER_CURATED_INFERENCE, TIER_SWISSPROT]

    @field_validator("gaf_url", mode="before")
    @classmethod
    def must_be_non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("gaf_url must be a non-empty string")
        return v.strip()

    @field_validator("admit", mode="after")
    @classmethod
    def tiers_must_be_known_and_nonempty(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("admit must name at least one tier; an empty corpus is not a scope")
        codes_for_tiers(v)  # raises ValueError naming the unknown tier
        return v


class ExtractGoaUniverseOperation(Operation):
    """Make every accession a GAF admits exist in ``protein``, accession only.

    Runs BEFORE ``load_goa_annotations`` for the same release, on the same cached
    file. One pass over one GAF costs roughly 4,4 minutes of CPU on top of the
    thirty-five a release already takes, and nothing in it waits on a remote
    service, so a release pass is as fast as the disk.

    Runs on ``protea.jobs``.
    """

    name = "extract_goa_universe"
    payload_model = ExtractGoaUniversePayload
    description = (
        "Scan one GOA release and make every accession it admits under the "
        "declared tiers exist in the protein table, as an accession-only row. "
        "Runs before load_goa_annotations for the same release, because "
        "protein_go_annotation.protein_accession is a foreign key and the loader "
        "can only skip what is not there. Touches no network but the GAF: "
        "sequences, audit dates and merged accessions are "
        "resolve_protein_sequences' job, once, at the end of the series. "
        "NOT-qualified rows count: a NOT is curated knowledge and the evaluation "
        "propagates it."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        bits = [f"release={payload.get('release')}"]
        if payload.get("dry_run"):
            bits.append("dry-run")
        return " · ".join(bits)

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        t0 = time.perf_counter()
        p = ExtractGoaUniversePayload.model_validate(contract_payload(payload))
        emit(
            "extract_goa_universe.start",
            None,
            {"gaf_url": p.gaf_url, "release": p.release, "admit": list(p.admit)},
            "info",
        )

        wanted, malformed, counters = self._admissible_accessions(p, emit)
        # ANTES DE TOCAR LA BASE: si el nivel pedido no es derivable de esta
        # release, esto levanta. Un aviso no basto -- la release 179 emitio
        # `swissprot_tier_unavailable` y siguio adelante e inserto 1.601.408 filas.
        assert_the_tier_is_derivable(counters, admit=p.admit)
        emit(
            "extract_goa_universe.scanned",
            None,
            {
                "rows": counters.rows,
                "admissible_accessions": len(wanted),
                "malformed_accessions": malformed,
                "rows_not_a_protein": counters.not_a_protein,
            },
            "info",
        )

        missing = self._missing(session, wanted)
        scan = _ScanOutcome(counters, malformed, len(wanted), len(missing))
        emit(
            "extract_goa_universe.missing",
            None,
            {"already_present": len(wanted) - len(missing), "missing": len(missing)},
            "info",
        )

        if not p.dry_run:
            scan.inserted_rows = self._insert_accessions(session, missing, p.release, emit)
            scan.first_release_written = self._write_first_release(
                session, wanted, p.release, emit
            )

        result = extraction_report(
            release=p.release,
            admit=list(p.admit),
            dry_run=p.dry_run,
            scan=scan,
            elapsed=round(time.perf_counter() - t0, 1),
        )
        emit("extract_goa_universe.done", None, result, "info")
        return OperationResult(result=result)

    def _admissible_accessions(
        self, p: ExtractGoaUniversePayload, emit: EmitFn
    ) -> tuple[set[str], int, _RowCounters]:
        """Every accession the GAF admits under the requested tiers, NOT included.

        A ``NOT`` row admits: the qualifier is never read. A ``NOT`` with
        experimental evidence IS a measurement on that protein, and scarce
        curated knowledge, so the protein belongs in the universe even though it
        has no positive truth.
        """
        counters = _RowCounters()
        accept = self._build_accept(p, counters)
        wanted: set[str] = set()
        malformed = 0
        for rec in self._stream_gaf(p, emit, accept):
            accession = rec.accession.strip()
            if ACCESSION_GRAMMAR.match(accession):
                wanted.add(accession)
            else:
                malformed += 1
        # UNA RELEASE SIN NIVEL swissprot_of_release TIENE QUE DECIRLO. GOA dejo
        # de publicar el nombre de entrada en la 179, y el criterio pierde ahi un
        # nivel entero: 52 de las 75 releases de esta serie. Sin este aviso, el
        # unico sintoma seria un recuento de admisibles mas bajo de lo esperado,
        # que es precisamente lo que nadie mira.
        if counters.entry_name_unreadable and counters.rows:
            parte = counters.entry_name_unreadable / counters.rows
            if parte > 0.5:
                emit(
                    "extract_goa_universe.swissprot_tier_unavailable",
                    "this release does not publish the UniProtKB entry name, so the "
                    "swissprot_of_release tier admits nobody here",
                    {
                        "rows_unreadable": counters.entry_name_unreadable,
                        "rows": counters.rows,
                        "fraction": round(parte, 4),
                        "since": "GOA dropped it at release 179",
                    },
                    "warning",
                )
        if counters.unknown_codes:
            emit(
                "extract_goa_universe.unknown_evidence_codes",
                None,
                {"codes": dict(counters.unknown_codes.most_common())},
                "warning",
            )
        return wanted, malformed, counters

    def _build_accept(
        self, p: ExtractGoaUniversePayload, counters: _RowCounters
    ) -> Callable[[list[str]], bool]:
        """The row predicate, deciding on the RAW columns before a record exists.

        Of 280.922.738 lines in GOA 156, 671.138 carry one of the thirteen --
        0,24%. Testing ``rec.evidence_code`` instead would have the plugin
        validate a record for each of the other 99,76% and then drop it, which
        measured 11,5 minutes a release against 4,4.

        ``counters.rows`` keeps meaning exactly what it meant before the predicate
        existed: the plugin calls this for every line that is neither a comment
        nor short, which is precisely the set of lines that used to yield a
        record, and it is the denominator the run reports.

        An unknown evidence code is counted and REJECTED. The tiers enumerate the
        26 codes the ECO mapping knows, partitioned exactly, so an unknown one is
        a code GO added after this was written: it needs a decision, not a
        default. The previous criterion was the complement of ``IEA`` and would
        have admitted it in silence.
        """
        codes = codes_for_tiers(p.admit)
        wants_swissprot = TIER_SWISSPROT in p.admit

        def accept(cols: list[str]) -> bool:
            counters.rows += 1
            ev = cols[_GAF_EVIDENCE].strip()
            if ev in codes:
                counters.by_tier["por_codigo"] += 1
            elif ev and ev not in ALL_KNOWN_CODES:
                counters.unknown_codes[ev] += 1
                return False
            elif wants_swissprot and is_swissprot_entry(cols[_GAF_ID].strip(), cols[_GAF_SYNONYM]):
                counters.by_tier["swissprot_of_release"] += 1
            else:
                if wants_swissprot and not entry_name_is_readable(cols[_GAF_SYNONYM]):
                    # CANNOT TELL, which is not the same as NO. GOA dropped the
                    # entry name at release 179, so from there on every row lands
                    # here, and the count is what makes a release with no tier
                    # distinguishable from a release where the tier admitted nobody.
                    counters.entry_name_unreadable += 1
                return False
            obj_type = cols[_GAF_TYPE].strip().lower()
            counters.by_type[obj_type or "(vacio)"] += 1
            if obj_type in _NOT_A_PROTEIN:
                counters.not_a_protein += 1
                return False
            return True

        return accept

    def _stream_gaf(
        self,
        p: ExtractGoaUniversePayload,
        emit: EmitFn,
        accept: Callable[[list[str]], bool],
    ) -> Iterator[Any]:
        from protea_sources.goa import plugin as goa_plugin

        yield from goa_plugin.stream(
            GoaStreamPayload(gaf_url=p.gaf_url, timeout_seconds=p.timeout_seconds),
            emit=emit,
            accept=accept,
        )

    def _missing(self, session: Session, wanted: set[str]) -> list[str]:
        """Which of ``wanted`` are absent from ``protein``.

        Chunked because the Postgres wire protocol caps a statement at 65535 bind
        parameters, and a release brings more admissible accessions than that:
        167573 in GOA 235. An unchunked ``IN`` would be accepted by the payload
        validator and die here, hours into the run.
        """
        present: set[str] = set()
        for chunk in chunks(sorted(wanted), _DB_CHUNK):
            rows = session.query(Protein.accession).filter(Protein.accession.in_(chunk)).all()
            present.update(r[0] for r in rows)
        return sorted(wanted - present)

    def _insert_accessions(
        self, session: Session, missing: list[str], release: int, emit: EmitFn
    ) -> int:
        """Insert the absent accessions with no sequence and no metadata.

        A Core ``INSERT`` of plain dicts, not ORM objects: a first release brings
        some 600.000 rows and the ORM unit of work is the wrong tool for that
        shape. Deliberately WITHOUT ``ON CONFLICT DO NOTHING`` -- ``missing`` was
        computed from this same table moments earlier, so a conflict means that
        query lied and that is worth an exception rather than a silent skip.

        Commits per chunk, like the other bulk loads: an interrupted pass leaves a
        partial-but-consistent universe, and the next run recomputes ``missing``
        from the table rather than from where it thinks it got to.
        """
        if not missing:
            return 0
        written = 0
        for chunk in chunks(missing, _DB_CHUNK):
            session.execute(
                insert(Protein.__table__),
                [self._accession_row(acc, release) for acc in chunk],
            )
            session.commit()
            written += len(chunk)
            emit(
                "extract_goa_universe.inserted",
                None,
                {"rows": len(chunk), "inserted_total": written, "of": len(missing)},
                "info",
            )
        return written

    @staticmethod
    def _accession_row(accession: str, release: int) -> dict[str, Any]:
        """One accession-only ``protein`` row.

        ``canonical_accession`` is NOT NULL and the isoform split is the only
        thing an accession tells us by itself, so it is parsed here with the same
        function the FASTA path uses. Everything else UniProt owns stays NULL for
        ``resolve_protein_sequences`` to fill.
        """
        canonical, is_canonical, isoform_index = Protein.parse_isoform(accession)
        return {
            "accession": accession,
            "canonical_accession": canonical,
            "is_canonical": is_canonical,
            "isoform_index": isoform_index,
            "first_admitted_release": release,
        }

    def _write_first_release(
        self, session: Session, wanted: set[str], release: int, emit: EmitFn
    ) -> int:
        """Lower ``first_admitted_release`` on rows this release admits.

        A MINIMUM, not a first writer: ``IS NULL OR > release``. The series is
        walked ascending, so in a clean run this only ever fires for rows another
        source put there -- everything ``insert_proteins`` loaded carries NULL --
        but the minimum is what keeps the column true if the releases are ever
        processed out of order, and it makes a repeated pass a no-op instead of a
        corruption.

        Rows this pass just inserted already carry the value, so they do not match
        and the returned count is the backfill alone.
        """
        col = Protein.__table__.c
        written = 0
        for chunk in chunks(sorted(wanted), _DB_CHUNK):
            res = session.execute(
                update(Protein.__table__)
                .where(col.accession.in_(chunk))
                .where(
                    or_(
                        col.first_admitted_release.is_(None),
                        col.first_admitted_release > release,
                    )
                )
                .values(first_admitted_release=release)
            )
            written += res.rowcount or 0
            session.commit()
        if written:
            emit(
                "extract_goa_universe.first_release_written",
                None,
                {"rows": written, "release": release},
                "info",
            )
        return written
