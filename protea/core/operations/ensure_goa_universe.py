"""Grow the protein universe from a GAF, so the loader stops dropping in silence.

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

NOT-QUALIFIED ROWS COUNT. A NOT annotation is curated knowledge and a scarce kind
-- negative results are hard to publish -- and the evaluation already uses it:
``_reconcile_not_side`` propagates NOT to descendants and subtracts them. So a
protein whose only reliable annotation is a NOT belongs in the universe. The
donor policy still excludes NOT rows when picking neighbours, which is a
different question and stays as it is.

ALL 75 UNIVERSE PASSES BEFORE THE FIRST ANNOTATION LOAD. Interleaving them per
release -- universe 156, load 156, universe 157, load 157 -- would truncate the
history of every protein admitted late: a protein that first appears in release
200 would exist only from 200 onwards, and its annotations in 156 to 199 would be
dropped by the same foreign key this operation exists to satisfy. Running phase 1
over the whole series first makes the universe the union over all releases before
any annotation is stored, so each release loads against the final universe. It
also restores the parallelism: embeddings can start once phase 1 closes, instead
of waiting behind the loads.

SECONDARY ACCESSIONS, AND WHY ASKING FOR THEM IS NOT ENOUGH. ``GET
/uniprotkb/accessions`` matches PRIMARY accessions only. An accession merged into
another entry -- a *secondary* -- is not returned, and UniProt counts it in
``X-Total-Results`` regardless, so the response reports "7 results" with an empty
body and a 200. Measured 2026-10-05 over seven accessions, in FASTA (0 bytes) and
in JSON (``{"results":[]}``). The single-entry route DOES follow the merge and
redirects, so the same accession looks alive one way and deleted the other:

    P30456  ->  secondary of P04439 (HLA-A, which carries 135 secondaries)
    Q9NPA5  ->  secondary of Q9NTW7
    E1BZ05  ->  secondary of P02542

HOW MUCH. On GOA 156, of 10,791 reliable accessions the batch endpoint did not
return, a systematic sample of 600 resolved **18.2%** as merges; the rest are
DELETED with no successor. Of the recoverable ones, 72% already had their primary
in ``protein`` and 28% did not. Extrapolated: ~1,960 recoverable, ~558 proteins
genuinely absent and ~1,403 annotations the foreign key would have dropped even
though the protein was present under its primary accession.

WHAT THIS FIXES BEYOND COVERAGE. If GOA used P30456 in 2016 and P04439 today, a
series over accessions sees one disappearance and one appearance where there is a
single protein. That contaminates the very delta the campaign measures, and no
amount of counting fixes it -- only recording the link does.

HOW IT IS STORED, AND WHY IT DOES NOT DUPLICATE EMBEDDINGS. Two rows are
inserted: the primary as usual, and the secondary with ``canonical_accession``
pointing at it, ``is_canonical=False`` and ``isoform_index=None``. Both share a
``sequence_id``, because ``_store_records`` deduplicates sequences by hash -- and
embeddings are keyed on ``Sequence`` rather than ``Protein``
(``compute_embeddings.py``), so two accessions over one sequence yield ONE
embedding and ONE neighbour in the KNN bank.

``is_canonical`` is the population filter in every code path that counts proteins
(``proteins_stats``, ``proteins``, ``showcase``), and a merged secondary is not a
distinct protein to count, so the value is the correct one. What distinguishes it
from an isoform is ``isoform_index``: an integer for an isoform, ``None`` for a
merge alias.

WHAT CANNOT BE RESCUED, AND WHY IT IS NOW A NUMBER. An accession deleted from
UniProt has no sequence to fetch, so it can never be embedded. Those are reported
as ``not_retrievable`` instead of vanishing: the quantity the old filter threw
away becomes a measurement the run carries.
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable, Iterator
from typing import Annotated, Any

from protea_contracts import GoaStreamPayload, UniProtProteinRecord
from pydantic import Field, field_validator
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, Operation, OperationResult, ProteaPayload
from protea.core.operations import _universe_http as _uhttp
from protea.core.operations._universe_sources import (
    _DATE_FIELDS,
    _TSV_FIELDS,
    ALL_KNOWN_CODES,
    TIER_CURATED_INFERENCE,
    TIER_SWISSPROT,
    TIER_TRUTH,
    _audit_dates_of,
    _Cuentas,
    _Escaneo,
    _parse_dates_tsv,
    _parse_tsv,
    _Salida,
    _store_dates,
    candidates_from,
    classify,
    codes_for_tiers,
    informe_de_pasada,
    is_swissprot_entry,
)
from protea.core.utils import chunks, contract_payload
from protea.infrastructure.orm.models.protein.protein import Protein

#: UniProtKB accession grammar, from UniProt's own documentation. Checked before
#: batching because the accessions endpoint answers 400 for the WHOLE request
#: when one member is malformed -- one stray identifier would cost a thousand
#: proteins, and GOA's object column is not guaranteed to hold only accessions.
_ACCESSION = re.compile(r"^(?:[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9](?:[A-Z][A-Z0-9]{2}[0-9]){1,2})$")

#: Hard limit of ``/uniprotkb/accessions``: 1001 answers
#: "Only '1000' accessions are allowed in each request". Measured, not assumed.
_BATCH = 1000

#: Search endpoint, used to resolve secondary accessions. See
#: :meth:`EnsureGoaUniverseOperation._resolve_secondary`: the batch endpoint
#: matches primary accessions ONLY.
_SEARCH_URL = "https://rest.uniprot.org/uniprotkb/search"

#: Cap on OR conditions per query, stated by UniProt in the body of its 400:
#: "Too many OR conditions in query. Maximum allowed is 100." Measured, not
#: assumed.
_SEC_BATCH = 100

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

_ACCESSIONS_URL = "https://rest.uniprot.org/uniprotkb/accessions"


def universe_key_for(job_id: Any, nombre: str) -> str:
    """Clave de almacenamiento de un artefacto de esta operacion."""
    return f"goa_universe/{job_id}/{nombre}"


class EnsureGoaUniversePayload(ProteaPayload, frozen=True):
    gaf_url: str
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
        codes_for_tiers(v)  # levanta ValueError nombrando el nivel desconocido
        return v


class EnsureGoaUniverseOperation(Operation):
    """Make every accession a GAF annotates reliably exist in ``protein``.

    Runs BEFORE ``load_goa_annotations`` for the same release, on the same cached
    file. Two passes over one GAF cost roughly fourteen minutes on top of the
    thirty-five a release already takes; the alternative -- asking UniProt from
    inside the annotation load -- puts network latency in the middle of a long
    paged transaction, which is the shape that OOM-killed the worker in
    September.

    Runs on ``protea.jobs``.
    """

    name = "ensure_goa_universe"
    payload_model = EnsureGoaUniversePayload
    description = (
        "Scan one GOA release and make every accession it annotates with a "
        "reliable evidence code exist in the protein table, fetching the absent "
        "ones from UniProt. Runs before load_goa_annotations for the same "
        "release, because protein_go_annotation.protein_accession is a foreign "
        "key and the loader can only skip what is not there. NOT-qualified rows "
        "count: a NOT is curated knowledge and the evaluation propagates it. "
        "Accessions UniProt no longer serves are reported as not_retrievable "
        "instead of vanishing."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        bits = [f"gaf={str(payload.get('gaf_url', ''))[-28:]}"]
        if payload.get("dry_run"):
            bits.append("dry-run")
        return " · ".join(bits)

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        t0 = time.perf_counter()
        p = EnsureGoaUniversePayload.model_validate(contract_payload(payload))
        emit("ensure_goa_universe.start", None, {"gaf_url": p.gaf_url}, "info")

        wanted, malformed, cuentas = self._admissible_accessions(p, emit)
        emit(
            "ensure_goa_universe.scanned",
            None,
            {"rows": cuentas.rows, "admissible_accessions": len(wanted), "malformed": malformed},
            "info",
        )

        missing = self._missing(session, wanted)
        emit(
            "ensure_goa_universe.missing",
            None,
            {"already_present": len(wanted) - len(missing), "missing": len(missing)},
            "info",
        )

        salida = _Salida()
        if missing and not p.dry_run:
            salida.fetched, salida.inserted, salida.sequences = self._fetch_and_store(
                session, missing, p, emit
            )
            salida.sin_fechas = self._fill_dates(session, wanted, p, emit)
            salida.alias, salida.sin_resolver, mas_p, mas_s = self._segunda_pasada(
                session, missing, p, emit
            )
            salida.inserted += mas_p
            salida.sequences += mas_s
            salida.artefactos = self._guardar_artefactos(
                payload.get("_job_id"), salida.alias, salida.sin_resolver,
                getattr(self, "_demerges", {}),
            )

        result = informe_de_pasada(
            admit=list(p.admit),
            dry_run=p.dry_run,
            escaneo=_Escaneo(cuentas, malformed, len(wanted), len(missing)),
            salida=salida,
            demerges=len(getattr(self, "_demerges", {})),
            elapsed=round(time.perf_counter() - t0, 1),
        )
        emit("ensure_goa_universe.done", None, result, "info")
        return OperationResult(result=result)

    def _segunda_pasada(
        self,
        session: Session,
        missing: list[str],
        p: EnsureGoaUniversePayload,
        emit: EmitFn,
    ) -> tuple[dict[str, str], list[str], int, int]:
        """Lo que el endpoint de lote no devolvio: fusion o baja.

        La pregunta se hace a la base y no a la aritmetica. ``missing`` menos
        ``fetched`` cuenta bien, pero no dice QUIEN falta, y quien falta es lo que
        hay que resolver y lo que hay que registrar si no se resuelve.
        """
        pendientes = self._missing(session, set(missing))
        if not pendientes:
            return {}, [], 0, 0
        alias, mas_p, mas_s = self._resolve_secondary(session, pendientes, p, emit)
        sin_resolver = sorted(set(pendientes) - set(alias))
        emit(
            "ensure_goa_universe.secondary",
            None,
            {
                "pending": len(pendientes),
                "resolved_as_merge": len(alias),
                "unresolved": len(sin_resolver),
            },
            "info",
        )
        return alias, sin_resolver, mas_p, mas_s

    def _guardar_artefactos(
        self,
        job_id: Any,
        alias: dict[str, str],
        sin_resolver: list[str],
        demerges: dict[str, list[str]] | None = None,
    ) -> dict[str, Any]:
        """Persiste el mapa de fusiones y la lista de las que no se resolvieron.

        Hasta ahora ``not_retrievable`` era un numero sin nombres: proteinas con
        evidencia experimental curada que no entran al corpus y que no se podian
        citar. Un numero no se puede auditar; una lista si.
        """
        if job_id is None:
            return {}
        import csv
        import tempfile
        from pathlib import Path

        from protea.infrastructure.settings import load_settings
        from protea.infrastructure.storage import get_artifact_store

        store = get_artifact_store(load_settings(Path(__file__).resolve().parents[3]))
        out: dict[str, Any] = {}
        with tempfile.TemporaryDirectory() as tmp:
            if alias:
                ruta = Path(tmp) / "fusiones.tsv"
                with ruta.open("w", encoding="utf-8", newline="") as fh:
                    w = csv.writer(fh, delimiter="\t")
                    w.writerow(["accesion_gaf", "accesion_primaria"])
                    w.writerows(sorted(alias.items()))
                out["fusiones"] = {
                    "uri": store.put(universe_key_for(job_id, "fusiones.tsv"), str(ruta)),
                    "filas": len(alias),
                }
            if sin_resolver:
                ruta = Path(tmp) / "sin_resolver.txt"
                ruta.write_text("\n".join(sin_resolver) + "\n", encoding="utf-8")
                out["sin_resolver"] = {
                    "uri": store.put(universe_key_for(job_id, "sin_resolver.txt"), str(ruta)),
                    "filas": len(sin_resolver),
                }
            if demerges:
                # Aparte de las borradas, y con sus destinos: una borrada no tiene
                # a donde ir, un demerge tiene varios y elegir es una decision
                # curatorial que esta operacion no puede tomar.
                ruta = Path(tmp) / "demerges.tsv"
                with ruta.open("w", encoding="utf-8", newline="") as fh:
                    w = csv.writer(fh, delimiter="\t")
                    w.writerow(["accesion_gaf", "destinos"])
                    w.writerows((k, ",".join(v)) for k, v in sorted(demerges.items()))
                out["demerges"] = {
                    "uri": store.put(universe_key_for(job_id, "demerges.tsv"), str(ruta)),
                    "filas": len(demerges),
                }
        return out

    def _admissible_accessions(
        self, p: EnsureGoaUniversePayload, emit: EmitFn
    ) -> tuple[set[str], int, _Cuentas]:
        """Every accession the GAF admits under the requested tiers, NOT included.

        A ``NOT`` row admits: the qualifier is never read. A ``NOT`` with
        experimental evidence IS a measurement on that protein, and scarce
        curated knowledge, so the protein belongs in the universe even though it
        has no positive truth.
        """
        cuentas = _Cuentas()
        accept = self._build_accept(p, cuentas)
        wanted: set[str] = set()
        malformed = 0
        for rec in self._stream_gaf(p, emit, accept):
            accession = rec.accession.strip()
            if _ACCESSION.match(accession):
                wanted.add(accession)
            else:
                malformed += 1
        if cuentas.desconocidos:
            emit(
                "ensure_goa_universe.unknown_evidence_codes",
                None,
                {"codes": dict(cuentas.desconocidos.most_common())},
                "warning",
            )
        return wanted, malformed, cuentas

    def _build_accept(
        self, p: EnsureGoaUniversePayload, cuentas: _Cuentas
    ) -> Callable[[list[str]], bool]:
        """The row predicate, deciding on the RAW columns before a record exists.

        Of 280.922.738 lines in GOA 156, 671.138 carry one of the thirteen --
        0,24%. Testing ``rec.evidence_code`` instead would have the plugin
        validate a record for each of the other 99,76% and then drop it, which
        measured 11,5 minutes a release against 4,4.

        ``cuentas.rows`` keeps meaning exactly what it meant before the predicate
        existed: the plugin calls this for every line that is neither a comment
        nor short, which is precisely the set of lines that used to yield a
        record, and it is the denominator the run reports.

        An unknown evidence code is counted and REJECTED. The tiers enumerate the
        26 codes the ECO mapping knows, partitioned exactly, so an unknown one is
        a code GO added after this was written: it needs a decision, not a
        default. The previous criterion was the complement of ``IEA`` and would
        have admitted it in silence.
        """
        codigos = codes_for_tiers(p.admit)
        quiere_sp = TIER_SWISSPROT in p.admit

        def accept(cols: list[str]) -> bool:
            cuentas.rows += 1
            ev = cols[_GAF_EVIDENCE].strip()
            if ev in codigos:
                cuentas.por_nivel["por_codigo"] += 1
            elif ev and ev not in ALL_KNOWN_CODES:
                cuentas.desconocidos[ev] += 1
                return False
            elif quiere_sp and is_swissprot_entry(cols[_GAF_ID].strip(), cols[_GAF_SYNONYM]):
                cuentas.por_nivel["swissprot_of_release"] += 1
            else:
                return False
            tipo = cols[_GAF_TYPE].strip().lower()
            cuentas.por_tipo[tipo or "(vacio)"] += 1
            return tipo not in _NOT_A_PROTEIN

        return accept

    def _stream_gaf(
        self,
        p: EnsureGoaUniversePayload,
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
        parameters, and a release brings more reliable accessions than that: 167573
        in GOA 235. An unchunked ``IN`` would be accepted by the payload validator
        and die here, hours into the run.
        """
        present: set[str] = set()
        for chunk in chunks(sorted(wanted), 20000):
            rows = session.query(Protein.accession).filter(Protein.accession.in_(chunk)).all()
            present.update(r[0] for r in rows)
        return sorted(wanted - present)

    def _fill_dates(
        self,
        session: Session,
        wanted: set[str],
        p: EnsureGoaUniversePayload,
        emit: EmitFn,
    ) -> int:
        """Fill the audit dates for universe members that still lack them.

        WHY THIS IS A SEPARATE PASS. ``_fetch_and_store`` only ever sees
        ``missing`` -- the accessions absent from ``protein`` -- so only those
        would carry dates. Every protein already in the table keeps NULL in all
        three columns, and that includes everything a prior release's pass
        admitted and everything ``insert_proteins`` loaded. With the reviewed set
        in place that is roughly 575,000 of some 680,000 rows, so a date filter
        would silently exclude 85% of the corpus while looking like it worked.
        Four independent reviewers flagged this on 2026-10-05 before it shipped.

        Only ``date_created IS NULL`` rows are queried, so the pass is cheap after
        the first release: steady state is zero requests. ``fields``-only TSV, no
        sequence, so the responses are small.
        """
        # Chunked for the 65535 bind-parameter ceiling, same reason as ``_missing``.
        pendientes: list[str] = []
        for chunk in chunks(sorted(wanted), 20000):
            pendientes.extend(
                session.scalars(
                    select(Protein.accession).where(
                        Protein.accession.in_(chunk),
                        Protein.date_created.is_(None),
                    )
                ).all()
            )
        if not pendientes:
            return 0
        escritas = 0
        for batch in chunks(pendientes, _BATCH):
            filas = _parse_dates_tsv(self._get_dates_tsv(batch, p.timeout_seconds, emit))
            if filas:
                _store_dates(session, filas)
                session.commit()
                escritas += len(filas)
        emit(
            "ensure_goa_universe.dates_backfilled",
            None,
            {"pending": len(pendientes), "written": escritas},
            "info",
        )
        return escritas

    def _get_dates_tsv(self, accessions: list[str], timeout: int, emit: EmitFn) -> str:
        """Dates only, no sequence: these proteins already have theirs."""
        url = (
            f"{_ACCESSIONS_URL}?accessions={','.join(accessions)}"
            f"&fields={_DATE_FIELDS}&format=tsv"
        )
        return _uhttp.get(url, label="dates", timeout=timeout, emit=emit)

    def _fetch_and_store(
        self,
        session: Session,
        missing: list[str],
        p: EnsureGoaUniversePayload,
        emit: EmitFn,
    ) -> tuple[int, int, int]:
        """Fetch the absent accessions from UniProt and upsert them.

        An accession UniProt no longer serves is simply absent from the answer --
        measured: a well-formed but unknown identifier gives 200 and is omitted,
        while a MALFORMED one gives 400 for the whole batch, which is why the
        regex gate runs first. So ``fetched`` below is smaller than ``missing``
        by exactly the number of proteins that existed when the GAF was published
        and do not exist now. That difference is the quantity the old silent
        filter threw away.
        """
        from protea.core.operations.insert_proteins import InsertProteinsOperation

        # DELIBERATE COUPLING, PINNED BY A TEST. ``_store_records`` is private to
        # insert_proteins, and reaching into it is the lesser of two evils: the
        # alternative is a second copy of the MD5 dedup and the protein upsert,
        # and two copies of that logic would drift without anybody noticing,
        # which is worse than one import that can break loudly. It cannot break
        # loudly on its own -- the return is a plain 4-tuple, so a changed
        # signature would fail at runtime hours into a load -- so
        # ``tests/test_ensure_goa_universe.py`` asserts the method exists and
        # still returns four integers. Same device as the EXECUTED_SUBMODULE map
        # in count_backend_parameters, for the same reason.
        inserter = InsertProteinsOperation()
        fetched = proteins = sequences = 0
        for batch in chunks(missing, _BATCH):
            parsed = _parse_tsv(self._get_tsv(batch, p.timeout_seconds, emit))
            records: list[UniProtProteinRecord] = [r for r, _d in parsed]
            fetched += len(records)
            if records:
                ins_p, _upd, ins_s, _re = inserter._store_records(session, records, emit)
                _store_dates(session, [(r.accession, d) for r, d in parsed])
                proteins += ins_p
                sequences += ins_s
                session.commit()
            emit(
                "ensure_goa_universe.batch",
                None,
                {"requested": len(batch), "returned": len(records), "fetched_total": fetched},
                "info",
            )
        return fetched, proteins, sequences

    def _resolve_secondary(
        self,
        session: Session,
        pending: list[str],
        p: EnsureGoaUniversePayload,
        emit: EmitFn,
    ) -> tuple[dict[str, str], int, int]:
        """Rescata las accesiones FUSIONADAS que el endpoint de lote no devuelve.

        The why, the how much and the storage layout are in the module
        docstring, under SECONDARY ACCESSIONS.
        """
        from protea.core.operations.insert_proteins import InsertProteinsOperation

        inserter = InsertProteinsOperation()
        alias: dict[str, str] = {}
        demerges: dict[str, list[str]] = {}
        proteins = sequences = 0

        for batch in chunks(pending, _SEC_BATCH):
            payload = self._search_secondary(batch, p.timeout_seconds, emit)
            pedidas = set(batch)
            # UNA ACCESION PUEDE SALIR EN VARIAS ENTRADAS, y entonces no es una
            # fusion. Se recoge todo primero y se decide despues, por accesion.
            candidates, entry_by_acc = candidates_from(payload, pedidas)
            records = classify(candidates, alias, demerges)
            dates = [
                (r.accession, _audit_dates_of(entry_by_acc[r.accession]))
                for r in records
                if r.accession in entry_by_acc
            ]
            if records:
                ins_p, _upd, ins_s, _re = inserter._store_records(session, records, emit)
                _store_dates(session, dates)
                proteins += ins_p
                sequences += ins_s
                session.commit()
            emit(
                "ensure_goa_universe.secondary_batch",
                None,
                {
                    "requested": len(batch),
                    "resolved": len(alias),
                    "demerged": len(demerges),
                    "rows": len(records),
                },
                "info",
            )
        self._demerges = demerges
        return alias, proteins, sequences

    def _search_secondary(
        self, accessions: list[str], timeout: int, emit: EmitFn
    ) -> dict[str, Any]:
        """One ``sec_acc:`` query per batch. Without ``fields``, deliberately.

        ``fields=accession,sec_acc`` returns 400 ``Invalid fields parameter value
        'sec_acc'``: it is a valid QUERY field but not a return field. And
        ``secondaryAccessions`` is needed in the response to know which of the
        requested accessions each entry resolved, so the whole entry is requested.

        :raises RuntimeError: on any HTTP error, carrying UniProt's own message.
            A batch that failed must not be counted as a batch that found nothing
            -- an earlier measurement reported 0% recoverable merges because ten
            batches had 400'd and the failures were tallied as zeroes.
        """
        import json
        from urllib import parse

        q = " OR ".join(f"sec_acc:{a}" for a in accessions)
        url = f"{_SEARCH_URL}?query={parse.quote(q)}&format=json&size=500"
        cuerpo = _uhttp.get(
            url,
            label="sec_acc",
            timeout=timeout,
            emit=emit,
            accept="application/json",
        )
        return json.loads(cuerpo)  # type: ignore[no-any-return]

    def _get_tsv(self, accessions: list[str], timeout: int, emit: EmitFn) -> str:
        url = (
            f"{_ACCESSIONS_URL}?accessions={','.join(accessions)}"
            f"&fields={_TSV_FIELDS}&format=tsv"
        )
        return _uhttp.get(
            url,
            label=f"batch of {len(accessions)} accessions",
            timeout=timeout,
            emit=emit,
        )
