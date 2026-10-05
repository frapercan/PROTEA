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
from collections import Counter
from collections.abc import Callable, Iterator
from typing import Annotated, Any, Literal

from protea_contracts import GoaStreamPayload, UniProtProteinRecord
from pydantic import Field, field_validator
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, Operation, OperationResult, ProteaPayload
from protea.core.operations import _universe_http as _uhttp
from protea.core.operations._universe_sources import (
    _DATE_FIELDS,
    _TSV_FIELDS,
    _audit_dates_of,
    _parse_dates_tsv,
    _parse_tsv,
    _store_dates,
    candidates_from,
    classify,
    codes_for,
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
#: builds anything -- see ``_reliable_accessions``. The plugin keeps the same
#: index privately; ``test_the_evidence_column_is_where_we_think`` pins ours
#: against the plugin's own parse rather than against its private name.
_GAF_EVIDENCE = 6

_ACCESSIONS_URL = "https://rest.uniprot.org/uniprotkb/accessions"


def universe_key_for(job_id: Any, nombre: str) -> str:
    """Clave de almacenamiento de un artefacto de esta operacion."""
    return f"goa_universe/{job_id}/{nombre}"


class EnsureGoaUniversePayload(ProteaPayload, frozen=True):
    gaf_url: str
    timeout_seconds: Annotated[int, Field(gt=0)] = 120
    dry_run: bool = False
    #: What counts as an annotation for admission to the universe. This lives in
    #: the PAYLOAD deliberately: it is the decision that defines the corpus scope,
    #: and the previous version kept it hidden in a module constant -- which is
    #: precisely how a ``reviewed:true`` search criterion fixed the scope of a
    #: whole campaign on 2026-09-15 without anybody declaring it. Here it is on
    #: the job row, queryable after the fact.
    #:
    #: ``curated``
    #:     Any code other than ``IEA``, GO's only automatic category, so it reads
    #:     as "a person assigned it". Measured on GOA 156: 554,328 proteins.
    #: ``reliable``
    #:     The thirteen LAFA codes: eleven experimental plus ``IC`` and ``TAS``.
    #:     Measured on GOA 156: 117,136 proteins.
    #:
    #: The universe uses ``curated`` because breadth serves the retrieval bank and
    #: because ``evidence_code`` is persisted per row, so the narrower tier stays
    #: recoverable at analysis time. Evaluation truth remains the thirteen, and
    #: that is the evaluation's decision, not this operation's.
    evidence_scope: Literal["curated", "reliable"] = "curated"

    @field_validator("gaf_url", mode="before")
    @classmethod
    def must_be_non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("gaf_url must be a non-empty string")
        return v.strip()


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

        wanted, malformed, rows = self._reliable_accessions(p, emit)
        emit(
            "ensure_goa_universe.scanned",
            None,
            {"rows": rows, "reliable_accessions": len(wanted), "malformed": malformed},
            "info",
        )

        missing = self._missing(session, wanted)
        emit(
            "ensure_goa_universe.missing",
            None,
            {"already_present": len(wanted) - len(missing), "missing": len(missing)},
            "info",
        )

        fetched = inserted = sequences = sin_fechas = 0
        alias: dict[str, str] = {}
        sin_resolver: list[str] = []
        artefactos: dict[str, Any] = {}
        if missing and not p.dry_run:
            fetched, inserted, sequences = self._fetch_and_store(session, missing, p, emit)
            sin_fechas = self._fill_dates(session, wanted, p, emit)
            alias, sin_resolver, mas_p, mas_s = self._segunda_pasada(session, missing, p, emit)
            inserted += mas_p
            sequences += mas_s
            artefactos = self._guardar_artefactos(
                payload.get("_job_id"), alias, sin_resolver, getattr(self, "_demerges", {})
            )

        result = {
            "rows_scanned": rows,
            "reliable_accessions": len(wanted),
            "malformed_skipped": malformed,
            "already_present": len(wanted) - len(missing),
            "missing": len(missing),
            "fetched": fetched,
            "resolved_as_merge": len(alias) if not p.dry_run else None,
            "not_retrievable": len(sin_resolver) if not p.dry_run else None,
            "demerged": len(getattr(self, "_demerges", {})) if not p.dry_run else None,
            "dates_backfilled": sin_fechas if not p.dry_run else None,
            "tipos_fiables": getattr(self, "_tipos_fiables", {}),
            "artefactos": artefactos,
            "proteins_inserted": inserted,
            "sequences_inserted": sequences,
            "dry_run": p.dry_run,
            "elapsed_seconds": round(time.perf_counter() - t0, 1),
        }
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

    def _reliable_accessions(
        self, p: EnsureGoaUniversePayload, emit: EmitFn
    ) -> tuple[set[str], int, int]:
        """Every accession the GAF annotates with a reliable code, NOT included.

        Uses ``EXPERIMENTAL`` -- the eleven GO experimental codes -- plus ``IC``
        and ``TAS``, which is what LAFA's own ground truth filters on
        (``democafa/groundtruth/process_ground_truth.py``: ``selected=
        'Experimental,IC,TAS'``). Not the eight of classic CAFA, which omit the
        five high-throughput codes, and not the six a stats router still uses.
        """
        accepts = codes_for(p.evidence_scope)
        wanted: set[str] = set()
        malformed = 0
        rows = 0
        por_tipo: Counter[str] = Counter()

        def accept(cols: list[str]) -> bool:
            """Reject on the raw column, before a record exists.

            Of 280.922.738 lines in GOA 156, 671.138 carry a reliable code --
            0,24%. Testing ``rec.evidence_code`` instead would have the plugin
            validate a record for each of the other 99,76% and then drop it,
            which measured 11,5 minutes a release against 4,4.

            Counting here rather than in the loop keeps ``rows`` meaning exactly
            what it meant before the predicate existed: the plugin calls this for
            every line that is neither a comment nor short, which is precisely
            the set of lines that used to yield a record.
            """
            nonlocal rows
            rows += 1
            if not accepts(cols[_GAF_EVIDENCE].strip()):
                return False
            tipo = cols[_GAF_TYPE].strip().lower()
            por_tipo[tipo or "(vacio)"] += 1
            return tipo not in _NOT_A_PROTEIN

        for rec in self._stream_gaf(p, emit, accept):
            accession = rec.accession.strip()
            if _ACCESSION.match(accession):
                wanted.add(accession)
            else:
                malformed += 1
        self._tipos_fiables = dict(por_tipo.most_common())
        return wanted, malformed, rows

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
